//! Test suite for backtest engine

mod event_ordering_tests;
pub mod fixture_generator;
mod integration_tests;
mod invariant_tests;
mod latency_tests;
mod post_only_tests;
mod queue_fill_tests;
mod snapshot_delta_tests;
mod taker_tests;

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::backtest::{
    BacktestConfig, BacktestEvent, EventPayload, EventPriority, FeatureRow, L2Book,
    SimOrder, SimOrderStatus,
};

// ============================================================================
// Decimal helpers
// ============================================================================

/// Parse a decimal from string (panics on invalid input - test only)
pub fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).expect("Invalid decimal string")
}

/// Assert two decimals are equal with a custom message
pub fn assert_decimal_eq(a: Decimal, b: Decimal, msg: &str) {
    assert_eq!(a, b, "{}: expected {}, got {}", msg, b, a);
}

/// Assert decimal approximately equals (within epsilon)
pub fn assert_decimal_approx(a: Decimal, b: Decimal, epsilon: Decimal, msg: &str) {
    let diff = (a - b).abs();
    assert!(
        diff <= epsilon,
        "{}: expected {} +/- {}, got {} (diff: {})",
        msg,
        b,
        epsilon,
        a,
        diff
    );
}

// ============================================================================
// Event builders
// ============================================================================

/// Create a snapshot event
pub fn snapshot_event(
    ts: i64,
    seq: u64,
    token: &str,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
) -> BacktestEvent {
    let bids: Vec<(Decimal, Decimal)> = bids
        .iter()
        .map(|(p, s)| {
            (
                Decimal::try_from(*p).unwrap(),
                Decimal::try_from(*s).unwrap(),
            )
        })
        .collect();
    let asks: Vec<(Decimal, Decimal)> = asks
        .iter()
        .map(|(p, s)| {
            (
                Decimal::try_from(*p).unwrap(),
                Decimal::try_from(*s).unwrap(),
            )
        })
        .collect();

    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Snapshot,
        sequence: seq,
        payload: EventPayload::Snapshot {
            token_id: token.to_string(),
            bids,
            asks,
        },
    }
}

/// Create a delta event
pub fn delta_event(
    ts: i64,
    seq: u64,
    token: &str,
    side: Side,
    price: f64,
    size: f64,
) -> BacktestEvent {
    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Delta,
        sequence: seq,
        payload: EventPayload::Delta {
            token_id: token.to_string(),
            side,
            price: Decimal::try_from(price).unwrap(),
            size: Decimal::try_from(size).unwrap(),
        },
    }
}

/// Create a trade event
pub fn trade_event(
    ts: i64,
    seq: u64,
    token: &str,
    side: Side,
    price: f64,
    size: f64,
) -> BacktestEvent {
    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Trade,
        sequence: seq,
        payload: EventPayload::Trade {
            token_id: token.to_string(),
            side,
            price: Decimal::try_from(price).unwrap(),
            size: Decimal::try_from(size).unwrap(),
            trade_id: None,
        },
    }
}

/// Create a trade event with trade_id
pub fn trade_event_with_id(
    ts: i64,
    seq: u64,
    token: &str,
    side: Side,
    price: f64,
    size: f64,
    trade_id: &str,
) -> BacktestEvent {
    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Trade,
        sequence: seq,
        payload: EventPayload::Trade {
            token_id: token.to_string(),
            side,
            price: Decimal::try_from(price).unwrap(),
            size: Decimal::try_from(size).unwrap(),
            trade_id: Some(trade_id.to_string()),
        },
    }
}

/// Create a feature event with default values
pub fn feature_event(ts: i64, seq: u64) -> BacktestEvent {
    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Feature,
        sequence: seq,
        payload: EventPayload::Feature(FeatureRow {
            timestamp_ms: ts,
            book_timestamp_ms: Some(ts),  // Use same timestamp for tests
            tau_secs: 900.0,
            oracle_price: 100000.0,
            reference_price: 100000.0,
            fair_up: 0.5,
            sigma: 0.01,
            coinbase_mid: 100000.0,
            coinbase_microprice: 100000.0,
            coinbase_spread: 1.0,
            coinbase_imb_1: 0.0,
            coinbase_imb_10: 0.0,
            coinbase_imb_20: 0.0,
            coinbase_imb_50: 0.0,
            coinbase_imb_100: 0.0,
            coinbase_liq_1bp: 100.0,
            coinbase_liq_2bp: 200.0,
            delta_microprice_1s: None,
            delta_microprice_2s: None,
            delta_microprice_5s: None,
            delta_imb_1_1s: None,
            delta_imb_1_2s: None,
            delta_imb_1_5s: None,
            bid_up: 0.49,
            ask_up: 0.51,
            bid_down: 0.49,
            ask_down: 0.51,
            up_mid: 0.50,
            blended_price: 100000.0,
            basis: 0.0,
            blend_weight: 0.5,
            sigma_dyn: 0.01,
            ewma_variance: 0.0001,
            alpha: 0.1,
            sigma_1m: Some(0.01),
            sigma_5m: Some(0.01),
        }),
    }
}

/// Create a feature event with custom bid/ask
pub fn feature_event_with_book(
    ts: i64,
    seq: u64,
    bid_up: f64,
    ask_up: f64,
) -> BacktestEvent {
    BacktestEvent {
        timestamp_ms: ts,
        priority: EventPriority::Feature,
        sequence: seq,
        payload: EventPayload::Feature(FeatureRow {
            timestamp_ms: ts,
            book_timestamp_ms: Some(ts),  // Use same timestamp for tests
            tau_secs: 900.0,
            oracle_price: 100000.0,
            reference_price: 100000.0,
            fair_up: 0.5,
            sigma: 0.01,
            coinbase_mid: 100000.0,
            coinbase_microprice: 100000.0,
            coinbase_spread: 1.0,
            coinbase_imb_1: 0.0,
            coinbase_imb_10: 0.0,
            coinbase_imb_20: 0.0,
            coinbase_imb_50: 0.0,
            coinbase_imb_100: 0.0,
            coinbase_liq_1bp: 100.0,
            coinbase_liq_2bp: 200.0,
            delta_microprice_1s: None,
            delta_microprice_2s: None,
            delta_microprice_5s: None,
            delta_imb_1_1s: None,
            delta_imb_1_2s: None,
            delta_imb_1_5s: None,
            bid_up,
            ask_up,
            bid_down: 0.49,
            ask_down: 0.51,
            up_mid: (bid_up + ask_up) / 2.0,
            blended_price: 100000.0,
            basis: 0.0,
            blend_weight: 0.5,
            sigma_dyn: 0.01,
            ewma_variance: 0.0001,
            alpha: 0.1,
            sigma_1m: Some(0.01),
            sigma_5m: Some(0.01),
        }),
    }
}

// ============================================================================
// Config builder
// ============================================================================

/// Create a test config with specified data and output directories
pub fn test_config(data_dir: &str, output_dir: &str) -> BacktestConfig {
    BacktestConfig {
        window_start: 1735689600, // 2025-01-01 00:00:00 UTC
        token_filter: None,
        delta_mode: crate::backtest::DeltaMode::Absolute,
        queue_added_ahead: false,
        latency_min_ms: 50,
        latency_max_ms: 50, // Fixed latency for deterministic tests
        seed: 42,
        data_dir: data_dir.to_string(),
        output_dir: output_dir.to_string(),
        condition_id: "test_condition".to_string(),
        up_token_id: "TEST_TOKEN".to_string(),
        down_token_id: "TEST_TOKEN_DOWN".to_string(),
        initial_complete_sets: 1000, // Seed initial inventory for SELL tests
        complete_set_price: 1.0,
        initial_cash: 0.0,
    }
}

/// Create a test config with custom latency range
pub fn test_config_with_latency(
    data_dir: &str,
    output_dir: &str,
    latency_min_ms: u64,
    latency_max_ms: u64,
    seed: u64,
) -> BacktestConfig {
    BacktestConfig {
        window_start: 1735689600, // 2025-01-01 00:00:00 UTC
        token_filter: None,
        delta_mode: crate::backtest::DeltaMode::Absolute,
        queue_added_ahead: false,
        latency_min_ms,
        latency_max_ms,
        seed,
        data_dir: data_dir.to_string(),
        output_dir: output_dir.to_string(),
        condition_id: "test_condition".to_string(),
        up_token_id: "TEST_TOKEN".to_string(),
        down_token_id: "TEST_TOKEN_DOWN".to_string(),
        initial_complete_sets: 1000, // Seed initial inventory for SELL tests
        complete_set_price: 1.0,
        initial_cash: 0.0,
    }
}

// ============================================================================
// Book assertion helpers
// ============================================================================

/// Assert a book level exists with expected size
pub fn assert_book_level(book: &L2Book, side: Side, price: Decimal, expected_size: Decimal) {
    let actual_size = book.size_at(side, price);
    assert_eq!(
        actual_size, expected_size,
        "Book {:?} @ {}: expected size {}, got {}",
        side, price, expected_size, actual_size
    );
}

/// Assert book has specific best bid
pub fn assert_best_bid(book: &L2Book, expected_price: Decimal, expected_size: Decimal) {
    let (price, size) = book.best_bid().expect("No bids in book");
    assert_eq!(price, expected_price, "Best bid price mismatch");
    assert_eq!(size, expected_size, "Best bid size mismatch");
}

/// Assert book has specific best ask
pub fn assert_best_ask(book: &L2Book, expected_price: Decimal, expected_size: Decimal) {
    let (price, size) = book.best_ask().expect("No asks in book");
    assert_eq!(price, expected_price, "Best ask price mismatch");
    assert_eq!(size, expected_size, "Best ask size mismatch");
}

/// Assert book has no bids
pub fn assert_no_bids(book: &L2Book) {
    assert!(book.best_bid().is_none(), "Expected no bids, but found some");
}

/// Assert book has no asks
pub fn assert_no_asks(book: &L2Book) {
    assert!(book.best_ask().is_none(), "Expected no asks, but found some");
}

// ============================================================================
// Order assertion helpers
// ============================================================================

/// Assert order has expected status
pub fn assert_order_status(order: &SimOrder, expected: SimOrderStatus) {
    assert_eq!(
        order.status, expected,
        "Order {} status: expected {:?}, got {:?}",
        order.order_id, expected, order.status
    );
}

/// Assert order fill amounts
pub fn assert_order_fill(order: &SimOrder, expected_filled: Decimal, expected_remaining: Decimal) {
    assert_eq!(
        order.filled_size, expected_filled,
        "Order {} filled: expected {}, got {}",
        order.order_id, expected_filled, order.filled_size
    );
    assert_eq!(
        order.remaining(),
        expected_remaining,
        "Order {} remaining: expected {}, got {}",
        order.order_id,
        expected_remaining,
        order.remaining()
    );
}

// ============================================================================
// Test strategy that places orders on specific timestamps
// ============================================================================

use anyhow::Result;
use async_trait::async_trait;
use crate::engine::{Action, CollectorMessageType, Event, Strategy};

/// A test strategy that places orders at specific timestamps
pub struct TestStrategy {
    /// Orders to place: (trigger_ts, token_id, side, price, size)
    pub orders_to_place: Vec<(i64, String, Side, Decimal, Decimal)>,
    /// Cancels to request: (trigger_ts, order_id_string)
    pub cancels_to_request: Vec<(i64, String)>,
    /// Track how many times process_event was called
    pub event_count: usize,
    /// Track timestamps of events received
    pub event_timestamps: Vec<i64>,
}

impl TestStrategy {
    pub fn new() -> Self {
        Self {
            orders_to_place: Vec::new(),
            cancels_to_request: Vec::new(),
            event_count: 0,
            event_timestamps: Vec::new(),
        }
    }

    pub fn with_orders(orders: Vec<(i64, String, Side, Decimal, Decimal)>) -> Self {
        Self {
            orders_to_place: orders,
            cancels_to_request: Vec::new(),
            event_count: 0,
            event_timestamps: Vec::new(),
        }
    }

    pub fn with_orders_and_cancels(
        orders: Vec<(i64, String, Side, Decimal, Decimal)>,
        cancels: Vec<(i64, String)>,
    ) -> Self {
        Self {
            orders_to_place: orders,
            cancels_to_request: cancels,
            event_count: 0,
            event_timestamps: Vec::new(),
        }
    }
}

#[async_trait]
impl Strategy for TestStrategy {
    fn name(&self) -> &str {
        "TestStrategy"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[CollectorMessageType::FairValueUpdated]
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Action> {
        self.event_count += 1;

        let timestamp_ms = match &event {
            Event::FairValueUpdated { timestamp, .. } => timestamp.timestamp_millis(),
            _ => return vec![],
        };

        self.event_timestamps.push(timestamp_ms);

        let mut actions = Vec::new();

        // Check for orders to place at this timestamp
        for (trigger_ts, token_id, side, price, size) in &self.orders_to_place {
            if *trigger_ts == timestamp_ms {
                actions.push(Action::PlaceOrder {
                    token_id: token_id.clone(),
                    condition_id: "test_condition".to_string(),
                    side: *side,
                    price: *price,
                    size: *size,
                    tick_size: dec("0.01"),
                    neg_risk: false,
                    order_type: polyfill_rs::OrderType::GTC,
                    post_only: true,
                });
            }
        }

        // Check for cancels to request at this timestamp
        for (trigger_ts, order_id) in &self.cancels_to_request {
            if *trigger_ts == timestamp_ms {
                actions.push(Action::CancelOrder {
                    cancel_type: crate::engine::types::CancelType::Single {
                        order_id: order_id.clone(),
                    },
                });
            }
        }

        actions
    }
}

/// A no-op strategy that does nothing (for baseline tests)
pub struct NoOpStrategy;

#[async_trait]
impl Strategy for NoOpStrategy {
    fn name(&self) -> &str {
        "NoOpStrategy"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[CollectorMessageType::FairValueUpdated]
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, _event: Event) -> Vec<Action> {
        vec![]
    }
}

/// A counting strategy that just counts events
pub struct CountingStrategy {
    pub event_count: usize,
    pub feature_count: usize,
}

impl CountingStrategy {
    pub fn new() -> Self {
        Self {
            event_count: 0,
            feature_count: 0,
        }
    }
}

#[async_trait]
impl Strategy for CountingStrategy {
    fn name(&self) -> &str {
        "CountingStrategy"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[CollectorMessageType::FairValueUpdated]
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Action> {
        self.event_count += 1;
        if matches!(event, Event::FairValueUpdated { .. }) {
            self.feature_count += 1;
        }
        vec![]
    }
}
