//! EWMA volatility estimation from Coinbase microprice returns

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Snapshot of volatility estimator state for logging
#[derive(Debug, Clone, Copy)]
pub struct VolatilitySnapshot {
    /// Floored annualized volatility (for pricing, 12s window)
    pub sigma: f64,
    /// Pre-floor annualized volatility (12s window)
    pub sigma_dyn: f64,
    /// Raw per-second EWMA variance (12s window)
    pub ewma_variance: f64,
    /// Last alpha value used in EWMA update
    pub alpha: f64,
    /// Annualized volatility from 1-minute window
    pub sigma_1m: f64,
    /// Annualized volatility from 5-minute window
    pub sigma_5m: f64,
}

/// EWMA volatility estimator
///
/// Computes real-time volatility from Coinbase microprice log returns.
/// Updated on each orderbook update (~50ms), provides annualized sigma.
/// Tracks three windows: 12s (pricing), 1m and 5m (regime detection).
pub struct VolatilityEstimator {
    /// EWMA variance estimate for 12s window (per-second units)
    ewma_variance_12s: Option<f64>,
    /// EWMA variance estimate for 1-minute window (per-second units)
    ewma_variance_1m: Option<f64>,
    /// EWMA variance estimate for 5-minute window (per-second units)
    ewma_variance_5m: Option<f64>,
    /// Last microprice for log return computation
    last_microprice: Option<f64>,
    /// Last timestamp (ms) for dt computation
    last_timestamp_ms: Option<i64>,
    /// Last alpha value used for 12s window (for logging)
    last_alpha: Option<f64>,
}

impl VolatilityEstimator {
    /// EWMA half-life for 12s window (pricing)
    const HALF_LIFE_12S: f64 = 12.0;
    /// EWMA half-life for 1-minute window (regime detection)
    const HALF_LIFE_1M: f64 = 60.0;
    /// EWMA half-life for 5-minute window (regime detection)
    const HALF_LIFE_5M: f64 = 300.0;

    /// Minimum annualized volatility floor (20%)
    const SIGMA_FLOOR: f64 = 0.20;

    /// Volatility scaling factor applied before floor
    const SIGMA_SCALE: f64 = 1.2;

    /// Seconds per year for annualization
    const SECONDS_PER_YEAR: f64 = 31_557_600.0;

    /// Coinbase candles API endpoint
    const CANDLES_URL: &'static str =
        "https://api.exchange.coinbase.com/products/BTC-USD/candles";

    /// Create a new volatility estimator (cold start, no warmup)
    pub fn new() -> Self {
        Self {
            ewma_variance_12s: None,
            ewma_variance_1m: None,
            ewma_variance_5m: None,
            last_microprice: None,
            last_timestamp_ms: None,
            last_alpha: None,
        }
    }

    /// Create a new volatility estimator with warmup from historical candles
    pub async fn new_with_warmup() -> Result<Self, reqwest::Error> {
        // Fetch 1-minute candles (last 60 candles = 1 hour)
        let client = reqwest::Client::new();
        let resp = client
            .get(Self::CANDLES_URL)
            .query(&[("granularity", "60")])
            .send()
            .await?
            .json::<Vec<Vec<serde_json::Value>>>()
            .await?;

        // Candles are [[timestamp, low, high, open, close, volume], ...]
        // Extract close prices (index 4)
        let closes: Vec<f64> = resp
            .iter()
            .filter_map(|c| c.get(4).and_then(|v| v.as_f64()))
            .collect();

        if closes.len() < 2 {
            info!("Not enough candles for warmup, starting cold");
            return Ok(Self::new());
        }

        // Compute log returns and realized variance (per-second)
        let mut sum_sq_returns = 0.0;
        let mut count = 0;
        for i in 1..closes.len() {
            let log_ret = (closes[i - 1] / closes[i]).ln(); // older/newer since candles are reverse chronological
            sum_sq_returns += log_ret * log_ret;
            count += 1;
        }

        // Variance per 60-second interval, convert to per-second
        let var_per_interval = sum_sq_returns / count as f64;
        let var_per_second = var_per_interval / 60.0;

        let sigma_annual = var_per_second.sqrt() * Self::SECONDS_PER_YEAR.sqrt();
        info!(
            candles = closes.len(),
            var_per_second = var_per_second,
            sigma_annual = format!("{:.1}%", sigma_annual * 100.0),
            "Volatility estimator warmed up from candles"
        );

        Ok(Self {
            ewma_variance_12s: Some(var_per_second),
            ewma_variance_1m: Some(var_per_second),
            ewma_variance_5m: Some(var_per_second),
            last_microprice: None,
            last_timestamp_ms: None,
            last_alpha: None,
        })
    }

    /// Compute EWMA update for a given half-life
    fn ewma_update(prev: Option<f64>, inst_variance: f64, dt_secs: f64, half_life: f64) -> f64 {
        let alpha = 1.0 - (-dt_secs * 2.0_f64.ln() / half_life).exp();
        match prev {
            Some(prev_var) => alpha * inst_variance + (1.0 - alpha) * prev_var,
            None => inst_variance,
        }
    }

    /// Convert per-second variance to annualized sigma
    fn annualize(var_per_sec: f64) -> f64 {
        var_per_sec.sqrt() * Self::SECONDS_PER_YEAR.sqrt()
    }

    /// Update EWMA variance with new microprice observation
    ///
    /// Called on each Coinbase orderbook update. Updates all three windows.
    pub fn update(&mut self, microprice: f64, timestamp_ms: i64) {
        // Guard against invalid microprice
        if microprice <= 0.0 {
            return;
        }

        // Get previous values
        let prev_price = self.last_microprice;
        let prev_ms = self.last_timestamp_ms;

        // Update stored values for next iteration
        self.last_microprice = Some(microprice);
        self.last_timestamp_ms = Some(timestamp_ms);

        // Need previous data to compute return
        let (prev_p, prev_t) = match (prev_price, prev_ms) {
            (Some(p), Some(t)) if p > 0.0 => (p, t),
            _ => return, // No history yet, skip
        };

        // Compute time delta in seconds
        let dt_ms = timestamp_ms - prev_t;
        if dt_ms <= 0 {
            // Same timestamp or time went backwards, skip update
            return;
        }
        let dt_secs = dt_ms as f64 / 1000.0;

        // Compute log return: r_t = ln(S_t / S_{t-1})
        let log_return = (microprice / prev_p).ln();

        // Instantaneous variance: v_t = r_t^2 / dt (per-second units)
        let inst_variance = log_return * log_return / dt_secs;

        // Update all three EWMA windows
        self.ewma_variance_12s = Some(Self::ewma_update(
            self.ewma_variance_12s,
            inst_variance,
            dt_secs,
            Self::HALF_LIFE_12S,
        ));
        self.ewma_variance_1m = Some(Self::ewma_update(
            self.ewma_variance_1m,
            inst_variance,
            dt_secs,
            Self::HALF_LIFE_1M,
        ));
        self.ewma_variance_5m = Some(Self::ewma_update(
            self.ewma_variance_5m,
            inst_variance,
            dt_secs,
            Self::HALF_LIFE_5M,
        ));

        // Store alpha for 12s window (for logging)
        let alpha_12s = 1.0 - (-dt_secs * 2.0_f64.ln() / Self::HALF_LIFE_12S).exp();
        self.last_alpha = Some(alpha_12s);
    }

    /// Get current annualized volatility with scale and floor applied (12s window)
    pub fn sigma(&self) -> f64 {
        match self.ewma_variance_12s {
            Some(var) => {
                let sigma_annual = Self::annualize(var);
                (Self::SIGMA_SCALE * sigma_annual).max(Self::SIGMA_FLOOR)
            }
            None => Self::SIGMA_FLOOR,
        }
    }

    /// Get annualized volatility from 1-minute window
    pub fn sigma_1m(&self) -> f64 {
        match self.ewma_variance_1m {
            Some(var) => Self::annualize(var),
            None => Self::SIGMA_FLOOR,
        }
    }

    /// Get annualized volatility from 5-minute window
    pub fn sigma_5m(&self) -> f64 {
        match self.ewma_variance_5m {
            Some(var) => Self::annualize(var),
            None => Self::SIGMA_FLOOR,
        }
    }

    /// Get full snapshot of volatility state for logging
    pub fn snapshot(&self) -> VolatilitySnapshot {
        let ewma_variance = self.ewma_variance_12s.unwrap_or(0.0);
        let sigma_dyn = Self::annualize(ewma_variance);
        let sigma = (Self::SIGMA_SCALE * sigma_dyn).max(Self::SIGMA_FLOOR);
        let alpha = self.last_alpha.unwrap_or(0.0);

        VolatilitySnapshot {
            sigma,
            sigma_dyn,
            ewma_variance,
            alpha,
            sigma_1m: self.sigma_1m(),
            sigma_5m: self.sigma_5m(),
        }
    }
}

impl Default for VolatilityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared volatility estimator for cross-task access
pub type SharedVolatility = Arc<RwLock<VolatilityEstimator>>;
