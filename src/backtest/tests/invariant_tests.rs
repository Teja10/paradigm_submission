//! Invariant and property tests for the backtest engine

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::collections::HashSet;

use super::dec;
use crate::backtest::{
    EventPriority, ExchangeSimulator, L2Book,
    QueueKey, SimOrder, SimOrderStatus,
};

/// Test that book never has negative sizes
#[test]
fn test_no_negative_sizes_in_book() {
    let mut book = L2Book::new();

    // Apply various operations
    book.apply_snapshot(
        &[
            (dec("0.50"), dec("100")),
            (dec("0.49"), dec("200")),
        ],
        &[
            (dec("0.51"), dec("100")),
            (dec("0.52"), dec("200")),
        ],
    );

    // Update levels
    book.apply_delta(Side::BUY, dec("0.50"), dec("50"));
    book.apply_delta(Side::SELL, dec("0.51"), dec("0")); // Remove level

    // Check all levels
    for (_, size) in book.bids.iter() {
        assert!(*size >= Decimal::ZERO, "Negative bid size: {}", size);
    }
    for (_, size) in book.asks.iter() {
        assert!(*size >= Decimal::ZERO, "Negative ask size: {}", size);
    }
}

/// Test that ext_ahead never becomes negative
#[test]
fn test_no_negative_ext_ahead() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("50"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Large trade that exceeds ext_ahead
    sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("100"), 1020, None);

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    if let Some(level) = sim.queues.get(&key) {
        assert!(
            level.ext_ahead >= Decimal::ZERO,
            "ext_ahead became negative: {}",
            level.ext_ahead
        );
    }
}

/// Test that ext_ahead never increases from a trade
#[test]
fn test_ext_ahead_never_increases_from_trade() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));

    // Multiple trades
    for i in 0..10 {
        let before = sim.queues.get(&key).map(|l| l.ext_ahead).unwrap_or(Decimal::ZERO);
        sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("5"), 1020 + i, None);
        let after = sim.queues.get(&key).map(|l| l.ext_ahead).unwrap_or(Decimal::ZERO);

        assert!(
            after <= before,
            "ext_ahead increased from {} to {} after trade",
            before,
            after
        );
    }
}

/// Test that fill size never exceeds trade size at that price
#[test]
fn test_fill_size_never_exceeds_trade_size() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("0"))], // No external liquidity
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("100"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Trade of 10
    let trade_size = dec("10");
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), trade_size, 1020, None);

    // Total fill size should not exceed trade size
    let total_fill: Decimal = fills.iter().map(|f| f.size).sum();
    assert!(
        total_fill <= trade_size,
        "Fill size {} exceeds trade size {}",
        total_fill,
        trade_size
    );
}

/// Test that filled + remaining equals original for all orders
#[test]
fn test_filled_plus_remaining_equals_original() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("50"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Partial fill
    sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("25"), 1020, None);

    let order = sim.get_order(order_id).unwrap();
    let invariant = order.filled_size + order.remaining();
    assert_eq!(
        invariant, order.original_size,
        "filled ({}) + remaining ({}) != original ({})",
        order.filled_size,
        order.remaining(),
        order.original_size
    );
}

/// Test no fill before submit ack
#[test]
fn test_no_fill_before_submit_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1050, // Acks later
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    // Don't ack yet

    // Trade before ack
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("100"), 1020, None);

    // Should have no fills (order not acked)
    for fill in &fills {
        assert_ne!(
            fill.order_id, order_id,
            "Got fill for unacked order"
        );
    }

    assert_eq!(
        sim.get_order(order_id).unwrap().filled_size,
        Decimal::ZERO
    );
}

/// Test no fill after cancel ack
#[test]
fn test_no_fill_after_cancel_ack_invariant() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();
    sim.request_cancel(order_id);
    sim.process_cancel_ack(order_id);

    let fill_before = sim.get_order(order_id).unwrap().filled_size;

    // Trade after cancel
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("100"), 1020, None);

    // Should have no fills for canceled order
    for fill in &fills {
        assert_ne!(
            fill.order_id, order_id,
            "Got fill for canceled order"
        );
    }

    let fill_after = sim.get_order(order_id).unwrap().filled_size;
    assert_eq!(fill_before, fill_after, "Canceled order got filled");
}

/// Test that event timestamps are non-decreasing when sorted
#[test]
fn test_timestamps_nondecreasing() {
    use super::{delta_event, feature_event, snapshot_event, trade_event};

    // Create events out of order
    let mut events = vec![
        feature_event(3000, 0),
        snapshot_event(1000, 0, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]),
        trade_event(2500, 0, "TOKEN", Side::BUY, 0.51, 10.0),
        delta_event(2000, 0, "TOKEN", Side::BUY, 0.50, 150.0),
    ];

    events.sort();

    // Check monotonicity
    let mut prev_ts = i64::MIN;
    for event in &events {
        assert!(
            event.timestamp_ms >= prev_ts,
            "Timestamp went backwards: {} < {}",
            event.timestamp_ms,
            prev_ts
        );
        prev_ts = event.timestamp_ms;
    }
}

/// Test that order IDs are unique
#[test]
fn test_order_id_unique() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    let mut order_ids = HashSet::new();

    // Submit multiple orders
    for _ in 0..10 {
        let order = SimOrder {
            order_id: 0, // Will be assigned by sim
            client_id: None,
            token_id: "TOKEN".to_string(),
            side: Side::BUY,
            price: dec("0.49"),
            original_size: dec("10"),
            filled_size: Decimal::ZERO,
            status: SimOrderStatus::PendingNew,
            submit_ts: 1000,
            ack_ts: 1010,
            cancel_req_ts: None,
            cancel_ack_ts: None,
            post_only: true,
            is_taker: false,
        };

        let order_id = sim.submit_order(order);
        assert!(
            order_ids.insert(order_id),
            "Duplicate order ID: {}",
            order_id
        );
    }
}

/// Test that fill IDs are unique
#[test]
fn test_fill_id_unique() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("0"))], // No external liquidity
        1000,
    );

    // Submit multiple orders
    for _ in 0..5 {
        let order = SimOrder {
            order_id: 0,
            client_id: None,
            token_id: "TOKEN".to_string(),
            side: Side::SELL,
            price: dec("0.51"),
            original_size: dec("10"),
            filled_size: Decimal::ZERO,
            status: SimOrderStatus::PendingNew,
            submit_ts: 1000,
            ack_ts: 1010,
            cancel_req_ts: None,
            cancel_ack_ts: None,
            post_only: true,
            is_taker: false,
        };

        let order_id = sim.submit_order(order);
        sim.process_order_ack(order_id).unwrap();
    }

    // Multiple trades generating fills
    let mut fill_ids = HashSet::new();
    for i in 0..5 {
        let fills = sim.process_trade(
            "TOKEN",
            Side::BUY,
            dec("0.51"),
            dec("10"),
            1020 + i as i64,
            None,
        );

        for fill in fills {
            assert!(
                fill_ids.insert(fill.fill_id),
                "Duplicate fill ID: {}",
                fill.fill_id
            );
        }
    }
}

/// Test position updates are consistent
#[test]
fn test_position_consistency() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("0"))],
        1000,
    );

    // Initial position is 1000 from seeding
    assert_eq!(sim.get_position("TOKEN"), dec("1000"));

    // Submit and ack SELL order
    let sell_order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("20"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let sell_id = sim.submit_order(sell_order);
    sim.process_order_ack(sell_id).unwrap();

    // Fill SELL order
    sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1020, None);

    // Position should be 1000 - 10 = 990 (we sold 10)
    assert_eq!(sim.get_position("TOKEN"), dec("990"));

    // Now submit and fill a BUY order
    sim.apply_delta("TOKEN", Side::BUY, dec("0.50"), dec("0"), 1030); // Clear bids
    let buy_order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.50"),
        original_size: dec("15"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1030,
        ack_ts: 1040,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let buy_id = sim.submit_order(buy_order);
    sim.process_order_ack(buy_id).unwrap();

    // Fill BUY order
    sim.process_trade("TOKEN", Side::SELL, dec("0.50"), dec("15"), 1050, None);

    // Position should be 990 + 15 = 1005
    assert_eq!(sim.get_position("TOKEN"), dec("1005"));
}

/// Test event priority ordering is correct
#[test]
fn test_event_priority_values() {
    assert!(EventPriority::Snapshot < EventPriority::Delta);
    assert!(EventPriority::Delta < EventPriority::Trade);
    assert!(EventPriority::Trade < EventPriority::Feature);

    // Verify numeric ordering
    assert_eq!(EventPriority::Snapshot as u8, 0);
    assert_eq!(EventPriority::Delta as u8, 1);
    assert_eq!(EventPriority::Trade as u8, 2);
    assert_eq!(EventPriority::Feature as u8, 3);
}

/// Test that all fill amounts are non-negative
#[test]
fn test_fill_amounts_non_negative() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("5"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("100"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Multiple trades
    for i in 0..10 {
        let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1020 + i, None);

        for fill in fills {
            assert!(fill.size >= Decimal::ZERO, "Negative fill size: {}", fill.size);
        }
    }
}

/// Test remaining size is never negative
#[test]
fn test_remaining_never_negative() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("0"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Fill exactly the order size
    sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1020, None);

    let order = sim.get_order(order_id).unwrap();
    assert!(
        order.remaining() >= Decimal::ZERO,
        "Remaining is negative: {}",
        order.remaining()
    );
    assert_eq!(order.remaining(), Decimal::ZERO);
}

/// Test order status transitions are valid
#[test]
fn test_order_status_transitions() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.49"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // PendingNew -> Live
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::PendingNew);
    sim.process_order_ack(order_id).unwrap();
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Live);

    // Live -> PendingCancel
    sim.request_cancel(order_id);
    assert_eq!(
        sim.get_order(order_id).unwrap().status,
        SimOrderStatus::PendingCancel
    );

    // PendingCancel -> Canceled
    sim.process_cancel_ack(order_id);
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Canceled);
}
