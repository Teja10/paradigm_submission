//! Tests for taker order behavior (FOK/FAK)

use anyhow::Result;
use async_trait::async_trait;
use polyfill_rs::{OrderType, Side};
use rust_decimal::Decimal;
use tempfile::TempDir;

use super::{dec, delta_event, feature_event, snapshot_event, test_config};
use crate::backtest::{BacktestEngine, ExchangeSimulator, SimOrder, SimOrderStatus};
use crate::engine::{Action, CollectorMessageType, Event, Strategy};
use crate::tracking::TrackedOrderStatus;

struct OneShotTakerStrategy {
    trigger_ts: i64,
    token_id: String,
    side: Side,
    price: Decimal,
    size: Decimal,
    order_type: OrderType,
    fired: bool,
}

impl OneShotTakerStrategy {
    fn new(
        trigger_ts: i64,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
    ) -> Self {
        Self {
            trigger_ts,
            token_id: token_id.to_string(),
            side,
            price,
            size,
            order_type,
            fired: false,
        }
    }
}

#[async_trait]
impl Strategy for OneShotTakerStrategy {
    fn name(&self) -> &str {
        "OneShotTakerStrategy"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        &[CollectorMessageType::FairValueUpdated]
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Action> {
        let timestamp_ms = match &event {
            Event::FairValueUpdated { timestamp, .. } => timestamp.timestamp_millis(),
            _ => return vec![],
        };

        if self.fired || timestamp_ms != self.trigger_ts {
            return vec![];
        }
        self.fired = true;

        vec![Action::PlaceOrder {
            token_id: self.token_id.clone(),
            condition_id: "test_condition".to_string(),
            side: self.side,
            price: self.price,
            size: self.size,
            tick_size: dec("0.01"),
            neg_risk: false,
            order_type: self.order_type,
            post_only: false,
        }]
    }
}

fn run_manual_events<S: Strategy>(
    events: Vec<crate::backtest::BacktestEvent>,
    strategy: &mut S,
) -> BacktestEngine {
    let data_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let config = test_config(
        data_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut engine = BacktestEngine::new(config);
    engine.run(strategy, events.into_iter()).unwrap();
    engine
}

#[test]
fn test_fak_partial_fill_records_once_in_order_tracker() {
    let events = vec![
        snapshot_event(1000, 0, "TEST_TOKEN", &[(0.50, 100.0)], &[(0.51, 5.0)]),
        feature_event(1010, 1),
    ];

    let mut strategy = OneShotTakerStrategy::new(
        1010,
        "TEST_TOKEN",
        Side::BUY,
        dec("0.52"),
        dec("10"),
        OrderType::FAK,
    );
    let engine = run_manual_events(events, &mut strategy);

    let order = engine.simulator().get_order(1).unwrap();
    assert_eq!(order.status, SimOrderStatus::Filled);
    assert_eq!(order.filled_size, dec("5"));
    assert!(order.is_taker);

    let tracker = engine.order_tracker().blocking_read();
    let tracked = tracker.get("1").unwrap();
    assert_eq!(tracked.size_matched, dec("5"));
    assert_eq!(tracked.status, TrackedOrderStatus::Filled);
    drop(tracker);

    assert_eq!(engine.all_fills().len(), 1);
    assert_eq!(engine.all_fills()[0].size, dec("5"));

    let book = engine.simulator().get_book("TEST_TOKEN").unwrap();
    assert_eq!(book.size_at(Side::SELL, dec("0.51")), Decimal::ZERO);
}

#[test]
fn test_fok_rejection_is_tracked() {
    let events = vec![
        snapshot_event(1000, 0, "TEST_TOKEN", &[(0.50, 100.0)], &[(0.51, 5.0)]),
        feature_event(1010, 1),
    ];

    let mut strategy = OneShotTakerStrategy::new(
        1010,
        "TEST_TOKEN",
        Side::BUY,
        dec("0.52"),
        dec("10"),
        OrderType::FOK,
    );
    let engine = run_manual_events(events, &mut strategy);

    let order = engine.simulator().get_order(1).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
    assert_eq!(order.filled_size, Decimal::ZERO);
    assert!(order.is_taker);

    let tracker = engine.order_tracker().blocking_read();
    let tracked = tracker.get("1").unwrap();
    assert_eq!(tracked.status, TrackedOrderStatus::Rejected);
    assert_eq!(tracked.size_matched, Decimal::ZERO);
    drop(tracker);

    assert!(engine.all_fills().is_empty());
}

#[test]
fn test_fok_uses_ack_time_book_after_latency() {
    let events = vec![
        snapshot_event(1000, 0, "TEST_TOKEN", &[(0.50, 100.0)], &[(0.51, 12.0)]),
        // Submit FOK at 1010 (ack at 1060 with test latency=50ms).
        feature_event(1010, 1),
        // Liquidity is removed before ack time.
        delta_event(1040, 2, "TEST_TOKEN", Side::SELL, 0.51, 3.0),
    ];

    let mut strategy = OneShotTakerStrategy::new(
        1010,
        "TEST_TOKEN",
        Side::BUY,
        dec("0.52"),
        dec("10"),
        OrderType::FOK,
    );
    let engine = run_manual_events(events, &mut strategy);

    let order = engine.simulator().get_order(1).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
    assert_eq!(order.filled_size, Decimal::ZERO);

    let tracker = engine.order_tracker().blocking_read();
    let tracked = tracker.get("1").unwrap();
    assert_eq!(tracked.status, TrackedOrderStatus::Rejected);
    assert_eq!(tracked.size_matched, Decimal::ZERO);
}

#[test]
fn test_taker_buy_reduces_ext_ahead_for_existing_sell_queue() {
    let mut sim = ExchangeSimulator::new(false);
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))],
        1000,
    );

    let maker_order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("5"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1005,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };
    let maker_id = sim.submit_order(maker_order);
    sim.process_order_ack(maker_id).unwrap();

    let taker_fills = sim.execute_taker_order("TOKEN", Side::BUY, dec("0.51"), dec("7"), 1010);
    assert_eq!(taker_fills.len(), 1);
    assert_eq!(taker_fills[0].size, dec("7"));

    // Remaining external liquidity at this level should now be 3.
    let fills1 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("3"), 1020, None);
    assert!(fills1.is_empty());

    // Next trade reaches our resting maker order.
    let fills2 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("5"), 1030, None);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].order_id, maker_id);
    assert_eq!(fills2[0].size, dec("5"));
}
