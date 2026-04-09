//! Backtest-specific types for simulation

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::cmp::Ordering;

/// Event priority for tie-breaking (lower = earlier)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EventPriority {
    Snapshot = 0,
    Delta = 1,
    Trade = 2,
    Feature = 3,
}

/// Unified backtest event loaded from parquet
#[derive(Debug, Clone)]
pub struct BacktestEvent {
    pub timestamp_ms: i64,
    pub priority: EventPriority,
    pub sequence: u64, // For stable sorting within same ts+priority
    pub payload: EventPayload,
}

impl PartialEq for BacktestEvent {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ms == other.timestamp_ms
            && self.priority == other.priority
            && self.sequence == other.sequence
    }
}

impl Eq for BacktestEvent {}

impl PartialOrd for BacktestEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BacktestEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.timestamp_ms, self.priority, self.sequence)
            .cmp(&(other.timestamp_ms, other.priority, other.sequence))
    }
}

/// Event payload variants
#[derive(Debug, Clone)]
pub enum EventPayload {
    Snapshot {
        token_id: String,
        bids: Vec<(Decimal, Decimal)>, // (price, size)
        asks: Vec<(Decimal, Decimal)>,
    },
    Delta {
        token_id: String,
        side: Side,
        price: Decimal,
        size: Decimal, // 0 = remove level
    },
    Trade {
        token_id: String,
        side: Side,      // Taker side (BUY = taker bought, hit asks)
        price: Decimal,
        size: Decimal,
        trade_id: Option<String>,
    },
    Feature(FeatureRow),
}

/// Feature row from parquet (matching FairValueLoggerModel schema)
#[derive(Debug, Clone)]
pub struct FeatureRow {
    pub timestamp_ms: i64,
    /// Timestamp when the Polymarket book snapshot was captured (optional for backwards compatibility)
    pub book_timestamp_ms: Option<i64>,
    pub tau_secs: f64,
    pub oracle_price: f64,
    pub reference_price: f64,
    pub fair_up: f64,
    pub sigma: f64,
    pub coinbase_mid: f64,
    pub coinbase_microprice: f64,
    pub coinbase_spread: f64,
    pub coinbase_imb_1: f64,
    pub coinbase_imb_10: f64,
    pub coinbase_imb_20: f64,
    pub coinbase_imb_50: f64,
    pub coinbase_imb_100: f64,
    pub coinbase_liq_1bp: f64,
    pub coinbase_liq_2bp: f64,
    pub delta_microprice_1s: Option<f64>,
    pub delta_microprice_2s: Option<f64>,
    pub delta_microprice_5s: Option<f64>,
    pub delta_imb_1_1s: Option<f64>,
    pub delta_imb_1_2s: Option<f64>,
    pub delta_imb_1_5s: Option<f64>,
    pub bid_up: f64,
    pub ask_up: f64,
    pub bid_down: f64,
    pub ask_down: f64,
    pub up_mid: f64,
    pub blended_price: f64,
    pub basis: f64,
    pub blend_weight: f64,
    pub sigma_dyn: f64,
    pub ewma_variance: f64,
    pub alpha: f64,
    /// 1-minute window volatility (optional for backwards compatibility)
    pub sigma_1m: Option<f64>,
    /// 5-minute window volatility (optional for backwards compatibility)
    pub sigma_5m: Option<f64>,
}

/// Order status in simulation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimOrderStatus {
    PendingNew,    // Submitted, awaiting ack
    PendingCancel, // Cancel requested, awaiting ack
    Live,          // Resting on book
    PartialFilled, // Has fills, still resting
    Filled,        // Fully filled
    Canceled,      // Canceled (may have partial fills)
    Rejected,      // Rejected (e.g., post-only crossed)
}

/// A simulated order
#[derive(Debug, Clone)]
pub struct SimOrder {
    pub order_id: u64,
    pub client_id: Option<String>,
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub original_size: Decimal,
    pub filled_size: Decimal,
    pub status: SimOrderStatus,
    pub submit_ts: i64,
    pub ack_ts: i64,
    pub cancel_req_ts: Option<i64>,
    pub cancel_ack_ts: Option<i64>,
    pub post_only: bool,
    /// Whether this was a taker order (FOK/FAK)
    pub is_taker: bool,
}

impl SimOrder {
    pub fn remaining(&self) -> Decimal {
        self.original_size - self.filled_size
    }

    pub fn is_open(&self) -> bool {
        matches!(
            self.status,
            SimOrderStatus::Live | SimOrderStatus::PartialFilled
        )
    }

    pub fn is_fillable(&self) -> bool {
        // Can be filled if live, partial, or pending cancel (until cancel acks)
        matches!(
            self.status,
            SimOrderStatus::Live | SimOrderStatus::PartialFilled | SimOrderStatus::PendingCancel
        )
    }
}

/// A fill event
#[derive(Debug, Clone)]
pub struct Fill {
    pub fill_id: u64,
    pub order_id: u64,
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub fill_ts: i64,
    pub trade_id: Option<String>,
}

/// Queue entry for tracking our order in the queue
#[derive(Debug, Clone)]
pub struct QueueEntry {
    pub order_id: u64,
    pub size: Decimal,
}

/// Pending action type for ack scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingActionType {
    OrderAck,
    CancelAck,
}

/// Scheduled pending action
#[derive(Debug, Clone)]
pub struct PendingAction {
    pub ack_ts: i64,
    pub order_id: u64,
    pub action_type: PendingActionType,
}

impl PartialEq for PendingAction {
    fn eq(&self, other: &Self) -> bool {
        self.ack_ts == other.ack_ts && self.order_id == other.order_id
    }
}

impl Eq for PendingAction {}

impl PartialOrd for PendingAction {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingAction {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order for min-heap behavior
        other
            .ack_ts
            .cmp(&self.ack_ts)
            .then_with(|| other.order_id.cmp(&self.order_id))
    }
}

/// Order event for recording
#[derive(Debug, Clone)]
pub struct OrderEvent {
    pub timestamp_ms: i64,
    pub order_id: u64,
    pub event_type: String,
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub status: SimOrderStatus,
}

