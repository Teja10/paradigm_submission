//! Event loader for reading parquet files and merging into sorted event stream

use anyhow::{Context, Result};
use polars::prelude::*;
use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use super::config::BacktestConfig;
use super::types::{BacktestEvent, EventPayload, EventPriority, FeatureRow};

/// Loaded events from a single parquet file
struct FileEvents {
    events: Vec<BacktestEvent>,
    index: usize,
}

impl FileEvents {
    fn current(&self) -> Option<&BacktestEvent> {
        self.events.get(self.index)
    }

    fn advance(&mut self) {
        self.index += 1;
    }
}

/// Event loader that merges multiple parquet files into a sorted stream
pub struct EventLoader {
    config: BacktestConfig,
}

impl EventLoader {
    pub fn new(config: BacktestConfig) -> Self {
        Self { config }
    }

    /// Discover all parquet files for the configured window
    fn discover_files(&self) -> Result<Vec<(PathBuf, EventPriority)>> {
        let mut files = Vec::new();
        let ts = self.config.window_start_ts();
        let date = self.config.window_date();

        tracing::info!("Loading window {} (date: {})", ts, date);

        let date_dir = Path::new(&self.config.data_dir).join(&date);
        if !date_dir.exists() {
            tracing::warn!("Date directory does not exist: {:?}", date_dir);
            return Ok(files);
        }

        // Snapshots and Deltas
        let orderbooks_dir = date_dir.join("orderbooks");
        if orderbooks_dir.exists() {
            let snap = orderbooks_dir.join(format!("snapshots_{}.parquet", ts));
            if snap.exists() {
                files.push((snap, EventPriority::Snapshot));
            }
            let deltas = orderbooks_dir.join(format!("deltas_{}.parquet", ts));
            if deltas.exists() {
                files.push((deltas, EventPriority::Delta));
            }
        }

        // Trades
        let trades_dir = date_dir.join("trades");
        if trades_dir.exists() {
            let trades = trades_dir.join(format!("trades_{}.parquet", ts));
            if trades.exists() {
                files.push((trades, EventPriority::Trade));
            }
        }

        // Features
        let features_dir = date_dir.join("features");
        if features_dir.exists() {
            let features = features_dir.join(format!("data_{}.parquet", ts));
            if features.exists() {
                files.push((features, EventPriority::Feature));
            }
        }

        Ok(files)
    }

    /// Helper to get string value from DataFrame row
    fn get_str(df: &DataFrame, col: &str, row: usize) -> Option<String> {
        df.column(col)
            .ok()?
            .get(row)
            .ok()
            .and_then(|v| v.get_str().map(|s| s.to_string()))
    }

    /// Helper to get i64 value from DataFrame row
    fn get_i64(df: &DataFrame, col: &str, row: usize) -> Option<i64> {
        df.column(col)
            .ok()?
            .get(row)
            .ok()
            .and_then(|v| v.try_extract::<i64>().ok())
    }

    /// Helper to get f64 value from DataFrame row
    fn get_f64(df: &DataFrame, col: &str, row: usize) -> Option<f64> {
        df.column(col)
            .ok()?
            .get(row)
            .ok()
            .and_then(|v| v.try_extract::<f64>().ok())
    }

    /// Load events from a snapshot parquet file
    fn load_snapshots(&self, path: &Path, base_sequence: u64) -> Result<Vec<BacktestEvent>> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()
            .context("Failed to read snapshot parquet")?;

        let mut events = Vec::new();
        let n_rows = df.height();

        for row_idx in 0..n_rows {
            let timestamp_ms = Self::get_i64(&df, "timestamp_ms", row_idx).unwrap_or(0);
            let token_id = Self::get_str(&df, "token_id", row_idx).unwrap_or_default();

            if !self.config.should_include_token(&token_id) {
                continue;
            }

            // Parse bid levels
            let mut bids = Vec::new();
            for i in 0..20 {
                let price_col = format!("bid_{}_price", i);
                let size_col = format!("bid_{}_size", i);

                if let (Some(price_str), Some(size_str)) = (
                    Self::get_str(&df, &price_col, row_idx),
                    Self::get_str(&df, &size_col, row_idx),
                ) {
                    if let (Ok(price), Ok(size)) =
                        (Decimal::from_str(&price_str), Decimal::from_str(&size_str))
                    {
                        if size > Decimal::ZERO {
                            bids.push((price, size));
                        }
                    }
                }
            }

            // Parse ask levels
            let mut asks = Vec::new();
            for i in 0..20 {
                let price_col = format!("ask_{}_price", i);
                let size_col = format!("ask_{}_size", i);

                if let (Some(price_str), Some(size_str)) = (
                    Self::get_str(&df, &price_col, row_idx),
                    Self::get_str(&df, &size_col, row_idx),
                ) {
                    if let (Ok(price), Ok(size)) =
                        (Decimal::from_str(&price_str), Decimal::from_str(&size_str))
                    {
                        if size > Decimal::ZERO {
                            asks.push((price, size));
                        }
                    }
                }
            }

            events.push(BacktestEvent {
                timestamp_ms,
                priority: EventPriority::Snapshot,
                sequence: base_sequence + row_idx as u64,
                payload: EventPayload::Snapshot {
                    token_id,
                    bids,
                    asks,
                },
            });
        }

        Ok(events)
    }

    /// Load events from a delta parquet file
    fn load_deltas(&self, path: &Path, base_sequence: u64) -> Result<Vec<BacktestEvent>> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()
            .context("Failed to read delta parquet")?;

        let mut events = Vec::new();
        let n_rows = df.height();

        for row_idx in 0..n_rows {
            let timestamp_ms = Self::get_i64(&df, "timestamp_ms", row_idx).unwrap_or(0);
            let token_id = Self::get_str(&df, "token_id", row_idx).unwrap_or_default();

            if !self.config.should_include_token(&token_id) {
                continue;
            }

            let side_str = Self::get_str(&df, "side", row_idx).unwrap_or_default();
            let side = match side_str.to_lowercase().as_str() {
                "buy" => Side::BUY,
                "sell" => Side::SELL,
                _ => continue,
            };

            let price_str = Self::get_str(&df, "price", row_idx).unwrap_or_default();
            let size_str = Self::get_str(&df, "size", row_idx).unwrap_or_default();

            let price = Decimal::from_str(&price_str).unwrap_or(Decimal::ZERO);
            let size = Decimal::from_str(&size_str).unwrap_or(Decimal::ZERO);

            events.push(BacktestEvent {
                timestamp_ms,
                priority: EventPriority::Delta,
                sequence: base_sequence + row_idx as u64,
                payload: EventPayload::Delta {
                    token_id,
                    side,
                    price,
                    size,
                },
            });
        }

        Ok(events)
    }

    /// Load events from a trades parquet file
    fn load_trades(&self, path: &Path, base_sequence: u64) -> Result<Vec<BacktestEvent>> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()
            .context("Failed to read trades parquet")?;

        let mut events = Vec::new();
        let n_rows = df.height();

        for row_idx in 0..n_rows {
            let timestamp_ms = Self::get_i64(&df, "timestamp_ms", row_idx).unwrap_or(0);
            let token_id = Self::get_str(&df, "token_id", row_idx).unwrap_or_default();

            if !self.config.should_include_token(&token_id) {
                continue;
            }

            let side_str = Self::get_str(&df, "side", row_idx).unwrap_or_default();
            let side = match side_str.to_lowercase().as_str() {
                "buy" => Side::BUY,
                "sell" => Side::SELL,
                _ => continue,
            };

            let price_str = Self::get_str(&df, "price", row_idx).unwrap_or_default();
            let size_str = Self::get_str(&df, "size", row_idx).unwrap_or_default();

            let price = Decimal::from_str(&price_str).unwrap_or(Decimal::ZERO);
            let size = Decimal::from_str(&size_str).unwrap_or(Decimal::ZERO);

            let trade_id = Self::get_str(&df, "trade_id", row_idx);

            events.push(BacktestEvent {
                timestamp_ms,
                priority: EventPriority::Trade,
                sequence: base_sequence + row_idx as u64,
                payload: EventPayload::Trade {
                    token_id,
                    side,
                    price,
                    size,
                    trade_id,
                },
            });
        }

        Ok(events)
    }

    /// Load events from a features parquet file
    fn load_features(&self, path: &Path, base_sequence: u64) -> Result<Vec<BacktestEvent>> {
        let df = LazyFrame::scan_parquet(path, Default::default())?
            .collect()
            .context("Failed to read features parquet")?;

        let mut events = Vec::new();
        let n_rows = df.height();

        for row_idx in 0..n_rows {
            let timestamp_ms = Self::get_i64(&df, "timestamp_ms", row_idx).unwrap_or(0);

            let feature = FeatureRow {
                timestamp_ms,
                // Optional: falls back to None for old parquet files without this field
                book_timestamp_ms: Self::get_i64(&df, "book_timestamp_ms", row_idx),
                tau_secs: Self::get_f64(&df, "tau_secs", row_idx).unwrap_or(0.0),
                oracle_price: Self::get_f64(&df, "oracle_price", row_idx).unwrap_or(0.0),
                reference_price: Self::get_f64(&df, "reference_price", row_idx).unwrap_or(0.0),
                fair_up: Self::get_f64(&df, "fair_up", row_idx).unwrap_or(0.5),
                sigma: Self::get_f64(&df, "sigma", row_idx).unwrap_or(0.0),
                coinbase_mid: Self::get_f64(&df, "coinbase_mid", row_idx).unwrap_or(0.0),
                coinbase_microprice: Self::get_f64(&df, "coinbase_microprice", row_idx)
                    .unwrap_or(0.0),
                coinbase_spread: Self::get_f64(&df, "coinbase_spread", row_idx).unwrap_or(0.0),
                coinbase_imb_1: Self::get_f64(&df, "coinbase_imb_1", row_idx).unwrap_or(0.0),
                coinbase_imb_10: Self::get_f64(&df, "coinbase_imb_10", row_idx).unwrap_or(0.0),
                coinbase_imb_20: Self::get_f64(&df, "coinbase_imb_20", row_idx).unwrap_or(0.0),
                coinbase_imb_50: Self::get_f64(&df, "coinbase_imb_50", row_idx).unwrap_or(0.0),
                coinbase_imb_100: Self::get_f64(&df, "coinbase_imb_100", row_idx).unwrap_or(0.0),
                coinbase_liq_1bp: Self::get_f64(&df, "coinbase_liq_1bp", row_idx).unwrap_or(0.0),
                coinbase_liq_2bp: Self::get_f64(&df, "coinbase_liq_2bp", row_idx).unwrap_or(0.0),
                delta_microprice_1s: Self::get_f64(&df, "delta_microprice_1s", row_idx),
                delta_microprice_2s: Self::get_f64(&df, "delta_microprice_2s", row_idx),
                delta_microprice_5s: Self::get_f64(&df, "delta_microprice_5s", row_idx),
                delta_imb_1_1s: Self::get_f64(&df, "delta_imb_1_1s", row_idx),
                delta_imb_1_2s: Self::get_f64(&df, "delta_imb_1_2s", row_idx),
                delta_imb_1_5s: Self::get_f64(&df, "delta_imb_1_5s", row_idx),
                bid_up: Self::get_f64(&df, "bid_up", row_idx).unwrap_or(0.0),
                ask_up: Self::get_f64(&df, "ask_up", row_idx).unwrap_or(1.0),
                bid_down: Self::get_f64(&df, "bid_down", row_idx).unwrap_or(0.0),
                ask_down: Self::get_f64(&df, "ask_down", row_idx).unwrap_or(1.0),
                up_mid: Self::get_f64(&df, "up_mid", row_idx).unwrap_or(0.5),
                blended_price: Self::get_f64(&df, "blended_price", row_idx).unwrap_or(0.0),
                basis: Self::get_f64(&df, "basis", row_idx).unwrap_or(0.0),
                blend_weight: Self::get_f64(&df, "blend_weight", row_idx).unwrap_or(0.0),
                sigma_dyn: Self::get_f64(&df, "sigma_dyn", row_idx).unwrap_or(0.0),
                ewma_variance: Self::get_f64(&df, "ewma_variance", row_idx).unwrap_or(0.0),
                alpha: Self::get_f64(&df, "alpha", row_idx).unwrap_or(0.0),
                // Optional: falls back to None for old parquet files without these fields
                sigma_1m: Self::get_f64(&df, "sigma_1m", row_idx),
                sigma_5m: Self::get_f64(&df, "sigma_5m", row_idx),
            };

            events.push(BacktestEvent {
                timestamp_ms,
                priority: EventPriority::Feature,
                sequence: base_sequence + row_idx as u64,
                payload: EventPayload::Feature(feature),
            });
        }

        Ok(events)
    }

    /// Load all events and return a sorted iterator
    pub fn load_events(&self) -> Result<impl Iterator<Item = BacktestEvent>> {
        let files = self.discover_files()?;
        tracing::info!("Discovered {} parquet files", files.len());

        let mut all_file_events: Vec<FileEvents> = Vec::new();
        let mut sequence_offset = 0u64;

        for (path, priority) in files {
            let events = match priority {
                EventPriority::Snapshot => self.load_snapshots(&path, sequence_offset)?,
                EventPriority::Delta => self.load_deltas(&path, sequence_offset)?,
                EventPriority::Trade => self.load_trades(&path, sequence_offset)?,
                EventPriority::Feature => self.load_features(&path, sequence_offset)?,
            };

            tracing::debug!(
                "Loaded {} events from {:?}",
                events.len(),
                path.file_name().unwrap_or_default()
            );

            sequence_offset += events.len() as u64;

            if !events.is_empty() {
                all_file_events.push(FileEvents { events, index: 0 });
            }
        }

        // Build min-heap for k-way merge
        let mut heap: BinaryHeap<Reverse<(BacktestEvent, usize)>> = BinaryHeap::new();

        for (file_idx, file_events) in all_file_events.iter().enumerate() {
            if let Some(event) = file_events.current() {
                heap.push(Reverse((event.clone(), file_idx)));
            }
        }

        // Return iterator that performs k-way merge
        Ok(std::iter::from_fn(move || {
            let Reverse((event, file_idx)) = heap.pop()?;

            // Advance the file and push next event
            all_file_events[file_idx].advance();
            if let Some(next_event) = all_file_events[file_idx].current() {
                heap.push(Reverse((next_event.clone(), file_idx)));
            }

            Some(event)
        }))
    }
}
