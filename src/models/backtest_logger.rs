//! Backtest data logger model - writes raw orderbook and trade data for backtesting
//!
//! This model consumes raw orderbook events and logs them to Parquet files.

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use polars::prelude::*;
use polyfill_rs::OrderBook;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Mutex;

use crate::engine::{CollectorMessageType, Event, Model};

/// Number of price levels to store per side
const BOOK_DEPTH: usize = 20;

/// Row accumulator for orderbook snapshots (wide format: one row per snapshot)
/// Columns: timestamp_ms, token_id, bid_0_price, bid_0_size, ..., ask_19_price, ask_19_size
#[derive(Default)]
struct SnapshotBuffer {
    timestamp_ms: Vec<i64>,
    token_id: Vec<String>,
    // Bid levels (price, size) for levels 0-19
    bid_prices: [Vec<Option<String>>; BOOK_DEPTH],
    bid_sizes: [Vec<Option<String>>; BOOK_DEPTH],
    // Ask levels (price, size) for levels 0-19
    ask_prices: [Vec<Option<String>>; BOOK_DEPTH],
    ask_sizes: [Vec<Option<String>>; BOOK_DEPTH],
}

impl SnapshotBuffer {
    fn new() -> Self {
        Self {
            timestamp_ms: Vec::new(),
            token_id: Vec::new(),
            bid_prices: std::array::from_fn(|_| Vec::new()),
            bid_sizes: std::array::from_fn(|_| Vec::new()),
            ask_prices: std::array::from_fn(|_| Vec::new()),
            ask_sizes: std::array::from_fn(|_| Vec::new()),
        }
    }

    fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        // Start with base DataFrame
        let mut df = df! {
            "timestamp_ms" => &self.timestamp_ms,
            "token_id" => &self.token_id,
        }?;

        // Add bid columns
        for i in 0..BOOK_DEPTH {
            let price_name = format!("bid_{}_price", i);
            let size_name = format!("bid_{}_size", i);
            let price_col = Series::new((&*price_name).into(), &self.bid_prices[i]);
            let size_col = Series::new((&*size_name).into(), &self.bid_sizes[i]);
            df = df.with_column(price_col)?.clone();
            df = df.with_column(size_col)?.clone();
        }

        // Add ask columns
        for i in 0..BOOK_DEPTH {
            let price_name = format!("ask_{}_price", i);
            let size_name = format!("ask_{}_size", i);
            let price_col = Series::new((&*price_name).into(), &self.ask_prices[i]);
            let size_col = Series::new((&*size_name).into(), &self.ask_sizes[i]);
            df = df.with_column(price_col)?.clone();
            df = df.with_column(size_col)?.clone();
        }

        Ok(df)
    }

    fn clear(&mut self) {
        self.timestamp_ms.clear();
        self.token_id.clear();
        for i in 0..BOOK_DEPTH {
            self.bid_prices[i].clear();
            self.bid_sizes[i].clear();
            self.ask_prices[i].clear();
            self.ask_sizes[i].clear();
        }
    }

    fn is_empty(&self) -> bool {
        self.timestamp_ms.is_empty()
    }

    fn len(&self) -> usize {
        self.timestamp_ms.len()
    }

    /// Add a polyfill OrderBook snapshot (unified source of truth)
    fn add_polyfill_snapshot(
        &mut self,
        timestamp: DateTime<Utc>,
        token_id: &str,
        book: &OrderBook,
    ) {
        self.timestamp_ms.push(timestamp.timestamp_millis());
        self.token_id.push(token_id.to_string());

        // Polyfill OrderBook.bids and .asks are already sorted correctly:
        // - bids: highest price first (best bid first)
        // - asks: lowest price first (best ask first)

        // Add bid levels (None if not enough levels)
        for i in 0..BOOK_DEPTH {
            if i < book.bids.len() {
                let level = &book.bids[i];
                self.bid_prices[i].push(Some(level.price.to_string()));
                self.bid_sizes[i].push(Some(level.size.to_string()));
            } else {
                self.bid_prices[i].push(None);
                self.bid_sizes[i].push(None);
            }
        }

        // Add ask levels (None if not enough levels)
        for i in 0..BOOK_DEPTH {
            if i < book.asks.len() {
                let level = &book.asks[i];
                self.ask_prices[i].push(Some(level.price.to_string()));
                self.ask_sizes[i].push(Some(level.size.to_string()));
            } else {
                self.ask_prices[i].push(None);
                self.ask_sizes[i].push(None);
            }
        }
    }
}

/// Row accumulator for orderbook deltas
#[derive(Default)]
struct DeltaBuffer {
    timestamp_ms: Vec<i64>,
    token_id: Vec<String>,
    side: Vec<String>,
    price: Vec<String>,
    size: Vec<String>,
}

impl DeltaBuffer {
    fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        df! {
            "timestamp_ms" => &self.timestamp_ms,
            "token_id" => &self.token_id,
            "side" => &self.side,
            "price" => &self.price,
            "size" => &self.size,
        }
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn is_empty(&self) -> bool {
        self.timestamp_ms.is_empty()
    }

    /// Add polyfill deltas (unified source of truth)
    fn add_polyfill_delta(
        &mut self,
        timestamp: DateTime<Utc>,
        token_id: &str,
        changes: &[(String, Decimal, Decimal)],  // (side, price, size) as Decimals
    ) {
        let ts = timestamp.timestamp_millis();

        for (side, price, size) in changes {
            self.timestamp_ms.push(ts);
            self.token_id.push(token_id.to_string());
            self.side.push(side.clone());
            self.price.push(price.to_string());
            self.size.push(size.to_string());
        }
    }
}

/// Row accumulator for public trades
#[derive(Default)]
struct TradeBuffer {
    timestamp_ms: Vec<i64>,
    token_id: Vec<String>,
    side: Vec<String>,
    price: Vec<String>,
    size: Vec<String>,
    trade_id: Vec<Option<String>>,
}

impl TradeBuffer {
    fn to_dataframe(&self) -> Result<DataFrame, PolarsError> {
        df! {
            "timestamp_ms" => &self.timestamp_ms,
            "token_id" => &self.token_id,
            "side" => &self.side,
            "price" => &self.price,
            "size" => &self.size,
            "trade_id" => &self.trade_id,
        }
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    fn is_empty(&self) -> bool {
        self.timestamp_ms.is_empty()
    }

    fn add_trade(
        &mut self,
        timestamp: DateTime<Utc>,
        token_id: &str,
        side: &str,
        price: &str,
        size: &str,
        trade_id: Option<String>,
    ) {
        self.timestamp_ms.push(timestamp.timestamp_millis());
        self.token_id.push(token_id.to_string());
        self.side.push(side.to_string());
        self.price.push(price.to_string());
        self.size.push(size.to_string());
        self.trade_id.push(trade_id);
    }
}

/// Per-market buffers
struct MarketBuffers {
    snapshots: SnapshotBuffer,
    deltas: DeltaBuffer,
    trades: TradeBuffer,
}

impl Default for MarketBuffers {
    fn default() -> Self {
        Self {
            snapshots: SnapshotBuffer::new(),
            deltas: DeltaBuffer::default(),
            trades: TradeBuffer::default(),
        }
    }
}

/// Market metadata (date and window start)
struct MarketInfo {
    date_str: String,
    window_start: i64,
}

/// Backtest data logger model - uses per-market buffers to avoid mixing data
pub struct BacktestLoggerModel {
    /// Buffers per condition_id
    buffers: Mutex<HashMap<String, MarketBuffers>>,
    /// Market info per condition_id (date, window_start)
    market_info: Mutex<HashMap<String, MarketInfo>>,
}

impl BacktestLoggerModel {
    pub fn new() -> Self {
        Self {
            buffers: Mutex::new(HashMap::new()),
            market_info: Mutex::new(HashMap::new()),
        }
    }

    fn flush_market(&self, condition_id: &str) -> Result<()> {
        let info = self.market_info.lock().unwrap();
        let mut buffers = self.buffers.lock().unwrap();

        if let (Some(market_info), Some(market_buffers)) = (info.get(condition_id), buffers.get_mut(condition_id)) {
            let date_str = &market_info.date_str;
            let ts = market_info.window_start;

            // Flush snapshots
            if !market_buffers.snapshots.is_empty() {
                let dir = format!("parquet_files/{}/orderbooks", date_str);
                std::fs::create_dir_all(&dir).ok();
                let path = format!("{}/snapshots_{}.parquet", dir, ts);
                let count = market_buffers.snapshots.len();
                let mut df = market_buffers.snapshots.to_dataframe()?;
                let file = std::fs::File::create(&path)?;
                ParquetWriter::new(file).finish(&mut df)?;
                tracing::info!("Wrote {} snapshot rows to {}", count, path);
                market_buffers.snapshots.clear();
            }

            // Flush deltas
            if !market_buffers.deltas.is_empty() {
                let dir = format!("parquet_files/{}/orderbooks", date_str);
                std::fs::create_dir_all(&dir).ok();
                let path = format!("{}/deltas_{}.parquet", dir, ts);
                let mut df = market_buffers.deltas.to_dataframe()?;
                let file = std::fs::File::create(&path)?;
                ParquetWriter::new(file).finish(&mut df)?;
                tracing::info!("Wrote {} delta rows to {}", market_buffers.deltas.timestamp_ms.len(), path);
                market_buffers.deltas.clear();
            }

            // Flush trades
            if !market_buffers.trades.is_empty() {
                let dir = format!("parquet_files/{}/trades", date_str);
                std::fs::create_dir_all(&dir).ok();
                let path = format!("{}/trades_{}.parquet", dir, ts);
                let mut df = market_buffers.trades.to_dataframe()?;
                let file = std::fs::File::create(&path)?;
                ParquetWriter::new(file).finish(&mut df)?;
                tracing::info!("Wrote {} trade rows to {}", market_buffers.trades.timestamp_ms.len(), path);
                market_buffers.trades.clear();
            }
        }

        Ok(())
    }

    fn flush_all(&self) -> Result<()> {
        let condition_ids: Vec<String> = self.market_info.lock().unwrap().keys().cloned().collect();
        for cid in condition_ids {
            self.flush_market(&cid)?;
        }
        Ok(())
    }
}

impl Default for BacktestLoggerModel {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Model for BacktestLoggerModel {
    fn name(&self) -> &str {
        "backtest-logger"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[
            CollectorMessageType::MarketStart,
            CollectorMessageType::MarketEnd,
            CollectorMessageType::PolyfillSnapshot,
            CollectorMessageType::PolyfillDelta,
            CollectorMessageType::PublicTrade,
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
                // Register this market
                let date_str = market.window_start.format("%Y-%m-%d").to_string();
                self.market_info.lock().unwrap().insert(
                    market.condition_id.clone(),
                    MarketInfo {
                        date_str,
                        window_start: market.window_start.timestamp(),
                    },
                );
                // Create buffer for this market
                self.buffers.lock().unwrap().entry(market.condition_id).or_default();
            }

            Event::MarketEnd => {
                // Flush all markets' buffers (MarketEnd doesn't have condition_id)
                if let Err(e) = self.flush_all() {
                    tracing::error!("Failed to flush backtest buffers: {}", e);
                }
                // Note: We don't clean up here since we might have multiple markets
                // and MarketEnd doesn't tell us which one ended
            }

            Event::PolyfillSnapshot { condition_id, token_id, timestamp, book } => {
                let mut buffers = self.buffers.lock().unwrap();
                if let Some(market_buffers) = buffers.get_mut(&condition_id) {
                    market_buffers.snapshots.add_polyfill_snapshot(timestamp, &token_id, &book);
                }
            }

            Event::PolyfillDelta { condition_id, token_id, timestamp, changes } => {
                let mut buffers = self.buffers.lock().unwrap();
                if let Some(market_buffers) = buffers.get_mut(&condition_id) {
                    market_buffers.deltas.add_polyfill_delta(timestamp, &token_id, &changes);
                }
            }

            Event::PublicTrade { condition_id, token_id, side, price, size, timestamp, trade_id } => {
                let mut buffers = self.buffers.lock().unwrap();
                if let Some(market_buffers) = buffers.get_mut(&condition_id) {
                    market_buffers.trades.add_trade(timestamp, &token_id, &side, &price, &size, trade_id);
                }
            }

            // Other events are not handled by this model
            _ => {}
        }

        // This model doesn't emit derived events
        vec![]
    }
}

impl Drop for BacktestLoggerModel {
    fn drop(&mut self) {
        let _ = self.flush_all();
    }
}
