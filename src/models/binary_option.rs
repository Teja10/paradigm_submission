//! Binary option pricing model for BTC 15-minute markets
//!
//! This model computes fair values for binary options and emits FairValueUpdated events.
//! It handles the pricing logic that was previously in BinaryOptionStrategy.

use std::collections::VecDeque;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use polyfill_rs::OrderBook;
use tracing::{debug, info};

use crate::engine::{CollectorMessageType, Event, Model};
use crate::features::{CoinbaseFeatures, VolatilityFeatures};
use crate::pricing::{blend_price, price_up_share};
use crate::types::{FairValueSnapshot, MarketState, SharedFairValues, SharedOrderBook};
use crate::volatility::SharedVolatility;

/// Historical snapshot for computing delta features
#[derive(Clone)]
struct HistoricalSnapshot {
    timestamp_ms: i64,
    microprice: f64,
    imbalance_1: f64,
}

/// Binary option pricing model
///
/// This model:
/// - Tracks the current market's window times
/// - Captures the reference price K from the first oracle price after window_start
/// - Computes theoretical fair values using the binary option pricing model
/// - Emits FairValueUpdated events for logging and trading strategies
pub struct BinaryOptionModel {
    /// Current market state
    current_market: Option<MarketState>,
    /// Reference price K (captured at window start)
    reference_price: Option<f64>,
    /// Last oracle price
    last_oracle_price: f64,
    /// Current UP token orderbook snapshot
    up_book: OrderBook,
    /// Current DOWN token orderbook snapshot
    down_book: OrderBook,
    /// Timestamp when the Polymarket book was last updated
    book_timestamp: DateTime<Utc>,
    /// Shared Coinbase orderbook
    coinbase_book: SharedOrderBook,
    /// Shared volatility estimator
    volatility: SharedVolatility,
    /// Shared fair values (updated by this model)
    shared_fair_values: SharedFairValues,
    /// Historical snapshots for delta computation (ring buffer)
    history: VecDeque<HistoricalSnapshot>,
}

impl BinaryOptionModel {
    /// Capacity for history buffer (~6 seconds at ~100Hz, plenty for 5s lookback)
    const HISTORY_CAPACITY: usize = 600;

    /// Create a new binary option model
    pub fn new(
        coinbase_book: SharedOrderBook,
        volatility: SharedVolatility,
        shared_fair_values: SharedFairValues,
    ) -> Self {
        let empty_book = || OrderBook {
            token_id: String::new(),
            timestamp: Utc::now(),
            bids: vec![],
            asks: vec![],
            sequence: 0,
        };
        Self {
            current_market: None,
            reference_price: None,
            last_oracle_price: 0.0,
            up_book: empty_book(),
            down_book: empty_book(),
            book_timestamp: Utc::now(),
            coinbase_book,
            volatility,
            shared_fair_values,
            history: VecDeque::with_capacity(Self::HISTORY_CAPACITY),
        }
    }

    /// Look up historical snapshot at approximately t - delta_ms
    fn lookup_historical(&self, current_ms: i64, delta_ms: i64) -> Option<&HistoricalSnapshot> {
        let target = current_ms - delta_ms;
        // Linear scan for closest timestamp <= target
        self.history.iter().rev().find(|s| s.timestamp_ms <= target)
    }

    /// Push new snapshot, trim old entries beyond 6 seconds
    fn record_snapshot(&mut self, snap: HistoricalSnapshot) {
        let cutoff = snap.timestamp_ms - 6000;
        self.history.push_back(snap);
        while self
            .history
            .front()
            .map(|s| s.timestamp_ms < cutoff)
            .unwrap_or(false)
        {
            self.history.pop_front();
        }
    }

    /// Compute fair values and return an event if we have all required data
    fn compute_fair_value(
        &self,
        timestamp: DateTime<Utc>,
        coinbase_features: CoinbaseFeatures,
        volatility_features: VolatilityFeatures,
        delta_microprice_1s: Option<f64>,
        delta_microprice_2s: Option<f64>,
        delta_microprice_5s: Option<f64>,
        delta_imb_1_1s: Option<f64>,
        delta_imb_1_2s: Option<f64>,
        delta_imb_1_5s: Option<f64>,
    ) -> Option<Event> {
        let market = self.current_market.as_ref()?;
        let reference_price = self.reference_price?;

        if self.last_oracle_price <= 0.0 {
            return None;
        }

        let tau_secs = (market.end_time - timestamp).num_seconds() as f64;
        if tau_secs <= 0.0 {
            return None;
        }

        // Compute blended price from oracle and Coinbase microprice
        let blend = blend_price(self.last_oracle_price, coinbase_features.microprice);

        // Price using blended price (use floored sigma from volatility features)
        let fair_up = price_up_share(blend.blended_price, reference_price, tau_secs, volatility_features.sigma);
        let fair_down = 1.0 - fair_up;

        Some(Event::FairValueUpdated {
            condition_id: market.condition_id.clone(),
            timestamp,
            book_timestamp: self.book_timestamp,
            window_start: market.window_start,
            end_time: market.end_time,
            tau_secs,
            oracle_price: self.last_oracle_price,
            reference_price,
            coinbase_features,
            delta_microprice_1s,
            delta_microprice_2s,
            delta_microprice_5s,
            delta_imb_1_1s,
            delta_imb_1_2s,
            delta_imb_1_5s,
            volatility_features,
            blended_price: blend.blended_price,
            basis: blend.basis,
            blend_weight: blend.blend_weight,
            fair_up,
            fair_down,
            up_book: self.up_book.clone(),
            down_book: self.down_book.clone(),
        })
    }

    /// Compute delta features and fair value, returning the event if successful
    async fn compute_and_emit(&mut self, timestamp: DateTime<Utc>) -> Vec<Event> {
        // Read Coinbase book and compute features
        let coinbase_features = {
            let book = self.coinbase_book.read().await;
            CoinbaseFeatures::from_book(&book)
        };

        let current_ms = timestamp.timestamp_millis();

        // Compute delta features from historical snapshots
        let delta_1s = self.lookup_historical(current_ms, 1000);
        let delta_2s = self.lookup_historical(current_ms, 2000);
        let delta_5s = self.lookup_historical(current_ms, 5000);

        let delta_microprice_1s = delta_1s.map(|h| coinbase_features.microprice - h.microprice);
        let delta_microprice_2s = delta_2s.map(|h| coinbase_features.microprice - h.microprice);
        let delta_microprice_5s = delta_5s.map(|h| coinbase_features.microprice - h.microprice);
        let delta_imb_1_1s = delta_1s.map(|h| coinbase_features.imbalance_1 - h.imbalance_1);
        let delta_imb_1_2s = delta_2s.map(|h| coinbase_features.imbalance_1 - h.imbalance_1);
        let delta_imb_1_5s = delta_5s.map(|h| coinbase_features.imbalance_1 - h.imbalance_1);

        // Record current snapshot for future delta computations
        self.record_snapshot(HistoricalSnapshot {
            timestamp_ms: current_ms,
            microprice: coinbase_features.microprice,
            imbalance_1: coinbase_features.imbalance_1,
        });

        // Read volatility snapshot and convert to features
        let vol_snap = self.volatility.read().await.snapshot();
        let volatility_features = VolatilityFeatures::from(vol_snap);

        // Compute fair value
        if let Some(event) = self.compute_fair_value(
            timestamp,
            coinbase_features,
            volatility_features,
            delta_microprice_1s,
            delta_microprice_2s,
            delta_microprice_5s,
            delta_imb_1_1s,
            delta_imb_1_2s,
            delta_imb_1_5s,
        ) {
            // Update shared fair values
            if let Event::FairValueUpdated { fair_up, fair_down, timestamp, .. } = &event {
                if let Some(ref market) = self.current_market {
                    self.shared_fair_values.write().await.insert(
                        market.condition_id.clone(),
                        FairValueSnapshot {
                            fair_up: *fair_up,
                            fair_down: *fair_down,
                            timestamp: *timestamp,
                        },
                    );
                }
            }
            vec![event]
        } else {
            debug!(
                "Waiting for data: market={}, K={:?}",
                self.current_market.is_some(),
                self.reference_price
            );
            vec![]
        }
    }
}

#[async_trait]
impl Model for BinaryOptionModel {
    fn name(&self) -> &str {
        "binary-option-model"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[
            CollectorMessageType::OraclePrice,
            CollectorMessageType::BookUpdate,
            CollectorMessageType::MarketStart,
            CollectorMessageType::MarketEnd,
            CollectorMessageType::CoinbaseUpdate,
            // Note: Does NOT handle FairValueUpdated to prevent feedback loops
        ]
    }

    async fn sync_state(&mut self) -> Result<()> {
        // No initial state to sync
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Event> {
        if !self.should_handle(&event) {
            return vec![];
        }

        match event {
            Event::MarketStart { market } => {
                info!("Model: New market started: {}", market.question);
                info!("  Window: {} to {}", market.window_start, market.end_time);

                self.current_market = Some(market);
                self.reference_price = None; // Reset K for new market
                self.history.clear(); // Clear history for new market
                vec![]
            }

            Event::MarketEnd => {
                info!("Model: Market ended");

                // Remove from shared fair values
                if let Some(ref market) = self.current_market {
                    self.shared_fair_values
                        .write()
                        .await
                        .remove(&market.condition_id);
                }

                self.current_market = None;
                self.reference_price = None;
                vec![]
            }

            Event::OraclePrice { price, timestamp } => {
                self.last_oracle_price = price;

                // Check if we need to capture K
                if let Some(ref market) = self.current_market {
                    if self.reference_price.is_none() && timestamp >= market.window_start {
                        self.reference_price = Some(price);
                        info!("Model: Captured K=${:.2} from oracle at window start", price);
                    }
                }

                self.compute_and_emit(timestamp).await
            }

            Event::BookUpdate { up_book, down_book } => {
                // Use the UP book's timestamp as the book_timestamp
                // This comes from Polymarket's API now (not local time)
                self.book_timestamp = up_book.timestamp;
                self.up_book = up_book;
                self.down_book = down_book;
                // Don't emit on book updates alone, wait for oracle/coinbase price
                vec![]
            }

            Event::CoinbaseUpdate { timestamp } => {
                self.compute_and_emit(timestamp).await
            }

            // These events are not handled by this model
            _ => vec![],
        }
    }
}
