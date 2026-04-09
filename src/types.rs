//! Core types for the BTC 15-minute market maker

use chrono::{DateTime, Utc};
use polyfill_rs::OrderBook;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared Coinbase orderbook snapshot that can be written by collector and read by strategy
pub type SharedOrderBook = Arc<RwLock<OrderBook>>;

/// Snapshot of computed fair values for a market
#[derive(Debug, Clone)]
pub struct FairValueSnapshot {
    pub fair_up: f64,
    pub fair_down: f64,
    pub timestamp: DateTime<Utc>,
}

/// Shared fair values map (condition_id -> snapshot) for access by strategies
pub type SharedFairValues = Arc<RwLock<HashMap<String, FairValueSnapshot>>>;

/// State of a 15-minute BTC up/down market
#[derive(Debug, Clone)]
pub struct MarketState {
    /// Market condition ID
    pub condition_id: String,
    /// Token ID for UP shares
    pub up_token_id: String,
    /// Token ID for DOWN shares
    pub down_token_id: String,
    /// Reference price K (strike), captured from oracle at window start
    pub reference_price: f64,
    /// When the 15-minute window starts (K is first oracle price after this)
    pub window_start: DateTime<Utc>,
    /// When the market resolves (window_start + 15 minutes)
    pub end_time: DateTime<Utc>,
    /// Market question text
    pub question: String,
}

/// Inputs for the pricing model
#[derive(Debug, Clone)]
pub struct PricingInputs {
    /// Current oracle price S
    pub oracle_price: f64,
    /// Time remaining in seconds
    pub time_remaining_secs: f64,
    /// Annualized volatility sigma
    pub sigma: f64,
}
