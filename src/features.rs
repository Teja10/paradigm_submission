//! Feature computation for Coinbase orderbook and volatility
//!
//! Computes features used for predicting oracle direction and Polymarket price movements.

use crate::volatility::VolatilitySnapshot;
use polyfill_rs::types::OrderBook;
use rust_decimal::prelude::ToPrimitive;

/// Volatility features from the EWMA estimator
#[derive(Debug, Clone, Copy, Default)]
pub struct VolatilityFeatures {
    /// Floored annualized volatility (12s window, for pricing)
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

impl From<VolatilitySnapshot> for VolatilityFeatures {
    fn from(snap: VolatilitySnapshot) -> Self {
        Self {
            sigma: snap.sigma,
            sigma_dyn: snap.sigma_dyn,
            ewma_variance: snap.ewma_variance,
            alpha: snap.alpha,
            sigma_1m: snap.sigma_1m,
            sigma_5m: snap.sigma_5m,
        }
    }
}

/// Computed features from a Coinbase orderbook snapshot
#[derive(Debug, Clone, Default)]
pub struct CoinbaseFeatures {
    pub mid: f64,
    pub microprice: f64,
    pub spread: f64,
    pub imbalance_1: f64,
    pub imbalance_10: f64,
    pub imbalance_20: f64,
    pub imbalance_50: f64,
    pub imbalance_100: f64,
    pub liquidity_1bp: f64,
    pub liquidity_2bp: f64,
}

impl CoinbaseFeatures {
    /// Compute features from an OrderBook snapshot
    pub fn from_book(book: &OrderBook) -> Self {
        // Best bid/ask prices and sizes
        let (bid_price, bid_size) = book
            .bids
            .first()
            .map(|l| {
                (
                    l.price.to_f64().unwrap_or(0.0),
                    l.size.to_f64().unwrap_or(0.0),
                )
            })
            .unwrap_or((0.0, 0.0));

        let (ask_price, ask_size) = book
            .asks
            .first()
            .map(|l| {
                (
                    l.price.to_f64().unwrap_or(0.0),
                    l.size.to_f64().unwrap_or(0.0),
                )
            })
            .unwrap_or((0.0, 0.0));

        // Mid price
        let mid = if bid_price > 0.0 && ask_price > 0.0 {
            (bid_price + ask_price) / 2.0
        } else {
            0.0
        };

        // Spread
        let spread = ask_price - bid_price;

        // Microprice: weighted by opposite side liquidity
        // p_μ = (p_ask * B_1 + p_bid * A_1) / (A_1 + B_1)
        let microprice = if bid_size + ask_size > 0.0 {
            (ask_price * bid_size + bid_price * ask_size) / (bid_size + ask_size)
        } else {
            mid
        };

        // Depth imbalances at various levels
        let imbalance_1 = compute_imbalance(&book.bids, &book.asks, 1);
        let imbalance_10 = compute_imbalance(&book.bids, &book.asks, 10);
        let imbalance_20 = compute_imbalance(&book.bids, &book.asks, 20);
        let imbalance_50 = compute_imbalance(&book.bids, &book.asks, 50);
        let imbalance_100 = compute_imbalance(&book.bids, &book.asks, 100);

        // Liquidity within X bps of mid
        let liquidity_1bp = compute_liquidity_near_mid(&book.bids, &book.asks, mid, 0.0001);
        let liquidity_2bp = compute_liquidity_near_mid(&book.bids, &book.asks, mid, 0.0002);

        Self {
            mid,
            microprice,
            spread,
            imbalance_1,
            imbalance_10,
            imbalance_20,
            imbalance_50,
            imbalance_100,
            liquidity_1bp,
            liquidity_2bp,
        }
    }
}

/// Compute imbalance I_N = (sum(B_i) - sum(A_i)) / (sum(B_i) + sum(A_i)) for top N levels
fn compute_imbalance(
    bids: &[polyfill_rs::types::BookLevel],
    asks: &[polyfill_rs::types::BookLevel],
    depth: usize,
) -> f64 {
    let bid_sum: f64 = bids
        .iter()
        .take(depth)
        .filter_map(|l| l.size.to_f64())
        .sum();

    let ask_sum: f64 = asks
        .iter()
        .take(depth)
        .filter_map(|l| l.size.to_f64())
        .sum();

    if bid_sum + ask_sum > 0.0 {
        (bid_sum - ask_sum) / (bid_sum + ask_sum)
    } else {
        0.0
    }
}

/// Compute total liquidity within +/- bps of mid price
fn compute_liquidity_near_mid(
    bids: &[polyfill_rs::types::BookLevel],
    asks: &[polyfill_rs::types::BookLevel],
    mid: f64,
    bps: f64,
) -> f64 {
    if mid <= 0.0 {
        return 0.0;
    }

    let threshold = mid * bps;
    let lower = mid - threshold;
    let upper = mid + threshold;

    let bid_liq: f64 = bids
        .iter()
        .filter_map(|l| {
            let p = l.price.to_f64()?;
            if p >= lower {
                l.size.to_f64()
            } else {
                None
            }
        })
        .sum();

    let ask_liq: f64 = asks
        .iter()
        .filter_map(|l| {
            let p = l.price.to_f64()?;
            if p <= upper {
                l.size.to_f64()
            } else {
                None
            }
        })
        .sum();

    bid_liq + ask_liq
}
