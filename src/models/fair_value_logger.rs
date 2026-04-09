//! Fair value logger model - writes fair value features to Parquet files
//!
//! This model consumes FairValueUpdated events and logs them to Parquet files.

use anyhow::Result;
use async_trait::async_trait;
use polars::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Mutex;

use crate::engine::{CollectorMessageType, Event, Model};

/// Row accumulator for building DataFrame
#[derive(Default)]
struct RowBuffer {
    timestamp_ms: Vec<i64>,
    /// Timestamp when the Polymarket book snapshot was captured
    book_timestamp_ms: Vec<i64>,
    tau_secs: Vec<f64>,
    oracle_price: Vec<f64>,
    reference_price: Vec<f64>,
    fair_up: Vec<f64>,
    sigma: Vec<f64>,
    coinbase_mid: Vec<f64>,
    coinbase_microprice: Vec<f64>,
    coinbase_spread: Vec<f64>,
    coinbase_imb_1: Vec<f64>,
    coinbase_imb_10: Vec<f64>,
    coinbase_imb_20: Vec<f64>,
    coinbase_imb_50: Vec<f64>,
    coinbase_imb_100: Vec<f64>,
    coinbase_liq_1bp: Vec<f64>,
    coinbase_liq_2bp: Vec<f64>,
    delta_microprice_1s: Vec<Option<f64>>,
    delta_microprice_2s: Vec<Option<f64>>,
    delta_microprice_5s: Vec<Option<f64>>,
    delta_imb_1_1s: Vec<Option<f64>>,
    delta_imb_1_2s: Vec<Option<f64>>,
    delta_imb_1_5s: Vec<Option<f64>>,
    bid_up: Vec<f64>,
    ask_up: Vec<f64>,
    bid_down: Vec<f64>,
    ask_down: Vec<f64>,
    up_mid: Vec<f64>,
    // Price blending fields
    blended_price: Vec<f64>,
    basis: Vec<f64>,
    blend_weight: Vec<f64>,
    // Volatility internals
    sigma_dyn: Vec<f64>,
    ewma_variance: Vec<f64>,
    alpha: Vec<f64>,
    // Regime detection windows
    sigma_1m: Vec<f64>,
    sigma_5m: Vec<f64>,
}

impl RowBuffer {
    fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        df! {
            "timestamp_ms" => &self.timestamp_ms,
            "book_timestamp_ms" => &self.book_timestamp_ms,
            "tau_secs" => &self.tau_secs,
            "oracle_price" => &self.oracle_price,
            "reference_price" => &self.reference_price,
            "fair_up" => &self.fair_up,
            "sigma" => &self.sigma,
            "coinbase_mid" => &self.coinbase_mid,
            "coinbase_microprice" => &self.coinbase_microprice,
            "coinbase_spread" => &self.coinbase_spread,
            "coinbase_imb_1" => &self.coinbase_imb_1,
            "coinbase_imb_10" => &self.coinbase_imb_10,
            "coinbase_imb_20" => &self.coinbase_imb_20,
            "coinbase_imb_50" => &self.coinbase_imb_50,
            "coinbase_imb_100" => &self.coinbase_imb_100,
            "coinbase_liq_1bp" => &self.coinbase_liq_1bp,
            "coinbase_liq_2bp" => &self.coinbase_liq_2bp,
            "delta_microprice_1s" => &self.delta_microprice_1s,
            "delta_microprice_2s" => &self.delta_microprice_2s,
            "delta_microprice_5s" => &self.delta_microprice_5s,
            "delta_imb_1_1s" => &self.delta_imb_1_1s,
            "delta_imb_1_2s" => &self.delta_imb_1_2s,
            "delta_imb_1_5s" => &self.delta_imb_1_5s,
            "bid_up" => &self.bid_up,
            "ask_up" => &self.ask_up,
            "bid_down" => &self.bid_down,
            "ask_down" => &self.ask_down,
            "up_mid" => &self.up_mid,
            // Price blending fields
            "blended_price" => &self.blended_price,
            "basis" => &self.basis,
            "blend_weight" => &self.blend_weight,
            // Volatility internals
            "sigma_dyn" => &self.sigma_dyn,
            "ewma_variance" => &self.ewma_variance,
            "alpha" => &self.alpha,
            // Regime detection windows
            "sigma_1m" => &self.sigma_1m,
            "sigma_5m" => &self.sigma_5m
        }
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn is_empty(&self) -> bool {
        self.timestamp_ms.is_empty()
    }
}

/// Fair value logger model
pub struct FairValueLoggerModel {
    buffer: Mutex<RowBuffer>,
    current_file: Mutex<Option<String>>,
    /// Current window end time (unix timestamp in seconds) - used to detect missed MarketStart
    current_window_end: Mutex<Option<i64>>,
}

/// Duration of each market window in seconds (15 minutes)
const WINDOW_DURATION_SECS: i64 = 900;

impl FairValueLoggerModel {
    pub fn new() -> Self {
        Self {
            buffer: Mutex::new(RowBuffer::default()),
            current_file: Mutex::new(None),
            current_window_end: Mutex::new(None),
        }
    }

    fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.lock().unwrap();
        let file = self.current_file.lock().unwrap();

        if let Some(ref path) = *file {
            if !buffer.is_empty() {
                let mut df = buffer.to_dataframe()?;
                let file = std::fs::File::create(path)?;
                ParquetWriter::new(file).finish(&mut df)?;
                tracing::info!("Wrote {} rows to {}", buffer.timestamp_ms.len(), path);
                buffer.clear();
            }
        }
        Ok(())
    }

    /// Start a new file for the given window start timestamp
    fn start_new_file(&self, window_start_secs: i64) -> Result<()> {
        // Flush previous buffer first
        self.flush()?;

        // Calculate the date string from the window start
        let dt = chrono::DateTime::from_timestamp(window_start_secs, 0)
            .unwrap_or_else(|| chrono::Utc::now());
        let date_str = dt.format("%Y-%m-%d").to_string();

        // Create directory
        let dir = format!("parquet_files/{}/features", date_str);
        std::fs::create_dir_all(&dir).ok();

        // Create new file path
        let path = format!("{}/data_{}.parquet", dir, window_start_secs);
        tracing::info!("Creating Parquet file: {} (auto-detected new window)", path);

        *self.current_file.lock().unwrap() = Some(path);
        *self.current_window_end.lock().unwrap() = Some(window_start_secs + WINDOW_DURATION_SECS);

        Ok(())
    }

    /// Check if timestamp is in a new window and handle accordingly
    fn check_and_rotate_file(&self, timestamp_secs: i64) -> Result<()> {
        let window_end = *self.current_window_end.lock().unwrap();

        match window_end {
            Some(end) if timestamp_secs >= end => {
                // We're past the current window - a new market must have started
                // Align to 15-minute window boundary
                let new_window_start = (timestamp_secs / WINDOW_DURATION_SECS) * WINDOW_DURATION_SECS;
                tracing::warn!(
                    "Detected data for new window (ts={}, window_end={}). MarketStart may have been missed. Auto-creating new file.",
                    timestamp_secs, end
                );
                self.start_new_file(new_window_start)?;
            }
            None => {
                // No window set yet - probably first data point, infer the window
                let new_window_start = (timestamp_secs / WINDOW_DURATION_SECS) * WINDOW_DURATION_SECS;
                tracing::info!("No current window set, inferring window start: {}", new_window_start);
                self.start_new_file(new_window_start)?;
            }
            _ => {
                // Still within current window, nothing to do
            }
        }
        Ok(())
    }
}

impl Default for FairValueLoggerModel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Model for FairValueLoggerModel {
    fn name(&self) -> &str {
        "fair-value-logger"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[
            CollectorMessageType::MarketStart,
            CollectorMessageType::MarketEnd,
            CollectorMessageType::FairValueUpdated,
        ]
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Event> {
        if !self.should_handle(&event) {
            return vec![];
        }

        match event {
            Event::MarketStart { market } => {
                // Flush previous file
                if let Err(e) = self.flush() {
                    tracing::error!("Failed to flush parquet buffer: {}", e);
                }

                // Create directory structure: parquet_files/{date}/features/
                let date_str = market.window_start.format("%Y-%m-%d").to_string();
                let dir = format!("parquet_files/{}/features", date_str);
                std::fs::create_dir_all(&dir).ok();

                // Create new file path
                let window_start_secs = market.window_start.timestamp();
                let path = format!("{}/data_{}.parquet", dir, window_start_secs);
                tracing::info!("Creating Parquet file: {}", path);
                *self.current_file.lock().unwrap() = Some(path);
                *self.current_window_end.lock().unwrap() = Some(window_start_secs + WINDOW_DURATION_SECS);
            }

            Event::MarketEnd => {
                // Flush current buffer to parquet file
                if let Err(e) = self.flush() {
                    tracing::error!("Failed to flush parquet buffer on market end: {}", e);
                }
            }

            Event::FairValueUpdated {
                timestamp,
                book_timestamp,
                tau_secs,
                oracle_price,
                reference_price,
                coinbase_features,
                delta_microprice_1s,
                delta_microprice_2s,
                delta_microprice_5s,
                delta_imb_1_1s,
                delta_imb_1_2s,
                delta_imb_1_5s,
                volatility_features,
                blended_price,
                basis,
                blend_weight,
                fair_up,
                up_book,
                down_book,
                ..
            } => {
                // Check if we've moved to a new window (handles missed MarketStart events)
                if let Err(e) = self.check_and_rotate_file(timestamp.timestamp()) {
                    tracing::error!("Failed to rotate parquet file: {}", e);
                }

                // Extract best bid/ask from polyfill OrderBook snapshots
                let bid_up = up_book.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
                let ask_up = up_book.asks.first().map(|l| l.price.to_f64().unwrap_or(1.0)).unwrap_or(1.0);
                let bid_down = down_book.bids.first().map(|l| l.price.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
                let ask_down = down_book.asks.first().map(|l| l.price.to_f64().unwrap_or(1.0)).unwrap_or(1.0);
                let mid_up = (bid_up + ask_up) / 2.0;

                // Console output (includes blended price)
                let status = if oracle_price > reference_price { "ITM" } else { "OTM" };
                let diff = oracle_price - reference_price;
                println!(
                    "t-{:>4}s | S=${:.2} B=${:.2} K=${:.2} {} ({:+.2}) | P={:.3} sig={:.1}% | CB=${:.2} imb={:+.3} | UP {:.2}/{:.2}",
                    tau_secs as i64,
                    oracle_price,
                    blended_price,
                    reference_price,
                    status,
                    diff,
                    fair_up,
                    volatility_features.sigma * 100.0,
                    coinbase_features.mid,
                    coinbase_features.imbalance_1,
                    bid_up,
                    ask_up,
                );

                // Buffer for parquet
                let mut buffer = self.buffer.lock().unwrap();

                buffer.timestamp_ms.push(timestamp.timestamp_millis());
                buffer.book_timestamp_ms.push(book_timestamp.timestamp_millis());
                buffer.tau_secs.push(tau_secs);
                buffer.oracle_price.push(oracle_price);
                buffer.reference_price.push(reference_price);
                buffer.fair_up.push(fair_up);
                buffer.sigma.push(volatility_features.sigma);
                buffer.coinbase_mid.push(coinbase_features.mid);
                buffer.coinbase_microprice.push(coinbase_features.microprice);
                buffer.coinbase_spread.push(coinbase_features.spread);
                buffer.coinbase_imb_1.push(coinbase_features.imbalance_1);
                buffer.coinbase_imb_10.push(coinbase_features.imbalance_10);
                buffer.coinbase_imb_20.push(coinbase_features.imbalance_20);
                buffer.coinbase_imb_50.push(coinbase_features.imbalance_50);
                buffer.coinbase_imb_100.push(coinbase_features.imbalance_100);
                buffer.coinbase_liq_1bp.push(coinbase_features.liquidity_1bp);
                buffer.coinbase_liq_2bp.push(coinbase_features.liquidity_2bp);
                buffer.delta_microprice_1s.push(delta_microprice_1s);
                buffer.delta_microprice_2s.push(delta_microprice_2s);
                buffer.delta_microprice_5s.push(delta_microprice_5s);
                buffer.delta_imb_1_1s.push(delta_imb_1_1s);
                buffer.delta_imb_1_2s.push(delta_imb_1_2s);
                buffer.delta_imb_1_5s.push(delta_imb_1_5s);
                buffer.bid_up.push(bid_up);
                buffer.ask_up.push(ask_up);
                buffer.bid_down.push(bid_down);
                buffer.ask_down.push(ask_down);
                buffer.up_mid.push(mid_up);
                // Price blending fields
                buffer.blended_price.push(blended_price);
                buffer.basis.push(basis);
                buffer.blend_weight.push(blend_weight);
                // Volatility features
                buffer.sigma_dyn.push(volatility_features.sigma_dyn);
                buffer.ewma_variance.push(volatility_features.ewma_variance);
                buffer.alpha.push(volatility_features.alpha);
                buffer.sigma_1m.push(volatility_features.sigma_1m);
                buffer.sigma_5m.push(volatility_features.sigma_5m);
            }

            // Other events are not handled by this model
            _ => {}
        }

        // This model doesn't emit derived events
        vec![]
    }
}

impl Drop for FairValueLoggerModel {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
