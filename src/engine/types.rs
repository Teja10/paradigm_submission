//! Event and Action types for the Artemis-style engine

use chrono::{DateTime, Utc};
use polyfill_rs::{OrderBook, OrderType, Side};
use rust_decimal::Decimal;

use crate::features::{CoinbaseFeatures, VolatilityFeatures};
use crate::types::MarketState;

/// Message types emitted by collectors and models (Events)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CollectorMessageType {
    OraclePrice,
    BookUpdate,
    MarketStart,
    MarketEnd,
    CoinbaseUpdate,
    // Backtest data events (polyfill-based for unified source of truth)
    PolyfillSnapshot,
    PolyfillDelta,
    PublicTrade,
    // Derived events from models
    FairValueUpdated,
}

/// Message types emitted by strategies (Actions)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecutorMessageType {
    PlaceOrder,
    PlaceOrders,
    CancelOrder,
}

/// Cancellation modes for orders
#[derive(Debug, Clone)]
pub enum CancelType {
    /// Cancel a single order by ID
    Single { order_id: String },
    /// Cancel multiple orders by IDs
    Multiple { order_ids: Vec<String> },
    /// Cancel all open orders
    All,
}

/// Events emitted by Collectors and processed by Strategies
#[derive(Debug, Clone)]
pub enum Event {
    /// New oracle price from Chainlink
    OraclePrice {
        price: f64,
        timestamp: DateTime<Utc>,
    },
    /// Orderbook update for UP/DOWN tokens
    BookUpdate {
        up_book: OrderBook,
        down_book: OrderBook,
    },
    /// New market discovered and started
    MarketStart { market: MarketState },
    /// Current market has ended
    MarketEnd,
    /// Coinbase orderbook update (for high-frequency fair value updates)
    CoinbaseUpdate { timestamp: DateTime<Utc> },
    // Backtest data events (polyfill-based for unified source of truth)
    /// Periodic polyfill orderbook snapshot (checkpoint for backtest reconstruction)
    PolyfillSnapshot {
        condition_id: String,
        token_id: String,
        timestamp: DateTime<Utc>,
        /// Full polyfill OrderBook state (top 20 levels)
        book: OrderBook,
    },
    /// Delta applied to polyfill orderbook
    PolyfillDelta {
        condition_id: String,
        token_id: String,
        timestamp: DateTime<Utc>,
        /// Changes applied: (side, price, size) as Decimals
        changes: Vec<(String, Decimal, Decimal)>,
    },
    /// Public trade from WebSocket (for backtest logging)
    PublicTrade {
        condition_id: String,
        token_id: String,
        side: String,
        price: String,
        size: String,
        timestamp: DateTime<Utc>,
        trade_id: Option<String>,
    },
    /// Fair value computed by BinaryOptionModel
    FairValueUpdated {
        condition_id: String,
        timestamp: DateTime<Utc>,
        /// Timestamp when the Polymarket book snapshot was captured
        book_timestamp: DateTime<Utc>,
        // Market timing
        window_start: DateTime<Utc>,
        end_time: DateTime<Utc>,
        tau_secs: f64,
        // Inputs
        oracle_price: f64,
        reference_price: f64,
        // Coinbase features
        coinbase_features: CoinbaseFeatures,
        // Delta features
        delta_microprice_1s: Option<f64>,
        delta_microprice_2s: Option<f64>,
        delta_microprice_5s: Option<f64>,
        delta_imb_1_1s: Option<f64>,
        delta_imb_1_2s: Option<f64>,
        delta_imb_1_5s: Option<f64>,
        // Volatility features
        volatility_features: VolatilityFeatures,
        // Blending
        blended_price: f64,
        basis: f64,
        blend_weight: f64,
        // Outputs
        fair_up: f64,
        fair_down: f64,
        // Polymarket books (for logging)
        up_book: OrderBook,
        down_book: OrderBook,
    },
}

impl Event {
    /// Get the message type for this event
    pub fn message_type(&self) -> CollectorMessageType {
        match self {
            Event::OraclePrice { .. } => CollectorMessageType::OraclePrice,
            Event::BookUpdate { .. } => CollectorMessageType::BookUpdate,
            Event::MarketStart { .. } => CollectorMessageType::MarketStart,
            Event::MarketEnd => CollectorMessageType::MarketEnd,
            Event::CoinbaseUpdate { .. } => CollectorMessageType::CoinbaseUpdate,
            Event::PolyfillSnapshot { .. } => CollectorMessageType::PolyfillSnapshot,
            Event::PolyfillDelta { .. } => CollectorMessageType::PolyfillDelta,
            Event::PublicTrade { .. } => CollectorMessageType::PublicTrade,
            Event::FairValueUpdated { .. } => CollectorMessageType::FairValueUpdated,
        }
    }
}

/// Parameters for a single order (used in batch submissions)
#[derive(Debug, Clone)]
pub struct PlaceOrderParams {
    /// Token ID (UP or DOWN token)
    pub token_id: String,
    /// Market condition ID
    pub condition_id: String,
    /// Order side
    pub side: Side,
    /// Price (0.01 to 0.99)
    pub price: Decimal,
    /// Size in shares
    pub size: Decimal,
    /// Tick size for this market
    pub tick_size: Decimal,
    /// Whether this is a neg_risk market
    pub neg_risk: bool,
    /// Order type (GTC, GTD, or FOK)
    pub order_type: OrderType,
    /// Post-only flag (maker-only, rejected if would cross spread)
    pub post_only: bool,
}

/// Actions emitted by Strategies and executed by Executors
#[derive(Debug, Clone)]
pub enum Action {
    /// Place a new order on Polymarket
    PlaceOrder {
        /// Token ID (UP or DOWN token)
        token_id: String,
        /// Market condition ID
        condition_id: String,
        /// Order side
        side: Side,
        /// Price (0.01 to 0.99)
        price: Decimal,
        /// Size in shares
        size: Decimal,
        /// Tick size for this market
        tick_size: Decimal,
        /// Whether this is a neg_risk market
        neg_risk: bool,
        /// Order type (GTC, GTD, or FOK)
        order_type: OrderType,
        /// Post-only flag (maker-only, rejected if would cross spread)
        post_only: bool,
    },
    /// Place multiple orders in parallel on Polymarket
    PlaceOrders {
        /// Orders to place in parallel
        orders: Vec<PlaceOrderParams>,
    },
    /// Cancel order(s) on Polymarket
    CancelOrder {
        /// Cancellation mode
        cancel_type: CancelType,
    },
}

impl Action {
    /// Get the message type for this action
    pub fn message_type(&self) -> ExecutorMessageType {
        match self {
            Action::PlaceOrder { .. } => ExecutorMessageType::PlaceOrder,
            Action::PlaceOrders { .. } => ExecutorMessageType::PlaceOrders,
            Action::CancelOrder { .. } => ExecutorMessageType::CancelOrder,
        }
    }
}
