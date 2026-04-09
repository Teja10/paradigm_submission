//! Tests for latency model and pending ack timing

use polyfill_rs::Side;
use rust_decimal::Decimal;

use super::dec;
use crate::backtest::{
    ExchangeSimulator, LatencyModel, SimOrder, SimOrderStatus,
};

/// Test that order is not live before ack time
#[test]
fn test_order_not_live_before_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit order
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.50"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1050, // Acks at 1050
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // Order should be PendingNew before ack
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::PendingNew);
    assert!(!order.is_open());
}

/// Test that order becomes live at ack time
#[test]
fn test_order_becomes_live_at_ack_time() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit order
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.49"), // Below best bid - won't cross
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // Process ack
    let result = sim.process_order_ack(order_id);
    assert!(result.is_ok());

    // Order should now be Live
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Live);
    assert!(order.is_open());
}

/// Test that cancel is not effective before ack
#[test]
fn test_cancel_not_effective_before_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit and ack order
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Request cancel
    assert!(sim.request_cancel(order_id));

    // Order should be PendingCancel, not Canceled
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::PendingCancel);
}

/// Test that cancel is effective after ack
#[test]
fn test_cancel_effective_after_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit and ack order
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Request and process cancel
    sim.request_cancel(order_id);
    sim.process_cancel_ack(order_id);

    // Order should be Canceled
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Canceled);
    assert!(!order.is_open());
}

/// Test that fills can occur during pending cancel (before cancel ack)
#[test]
fn test_fill_can_occur_during_pending_cancel() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book with small external liquidity at the ask
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))], // Only 10 external liquidity
        1000,
    );

    // Submit and ack a SELL order at the ask
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // ext_ahead = 10 (book size at price)

    // Request cancel (but don't ack yet)
    sim.request_cancel(order_id);

    // Order is in PendingCancel state
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::PendingCancel);

    // Trade of 15 - consumes 10 ext_ahead + 5 of our order
    // BUY trade at 0.51 fills SELL orders (our order)
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("15"), 1060, None);

    // Should have gotten a fill of 5 (15 - 10 ext_ahead)
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].size, dec("5"));

    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.filled_size, dec("5"));
}

/// Test that no fills occur after cancel ack
#[test]
fn test_no_fill_after_cancel_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit and ack a SELL order at the ask
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Request and process cancel
    sim.request_cancel(order_id);
    sim.process_cancel_ack(order_id);

    // Order is canceled
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Canceled);

    // Trade occurs - should NOT fill our canceled order
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("5"), 1100, None);

    // Should have no fills (order was removed from queue when canceled)
    assert_eq!(fills.len(), 0);

    // Order fill size unchanged
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.filled_size, Decimal::ZERO);
}

/// Test latency model determinism with same seed
#[test]
fn test_latency_model_determinism() {
    let mut model1 = LatencyModel::new(42, 50, 100);
    let mut model2 = LatencyModel::new(42, 50, 100);

    // Same seed should produce identical sequences
    for _ in 0..100 {
        assert_eq!(model1.sample(), model2.sample());
    }
}

/// Test latency model produces values within range
#[test]
fn test_latency_model_range() {
    let mut model = LatencyModel::new(123, 50, 100);

    for _ in 0..1000 {
        let sample = model.sample();
        assert!(sample >= 50, "Sample {} below min 50", sample);
        assert!(sample <= 100, "Sample {} above max 100", sample);
    }
}

/// Test latency model with different seeds produces different sequences
#[test]
fn test_latency_model_different_seeds() {
    let mut model1 = LatencyModel::new(42, 50, 100);
    let mut model2 = LatencyModel::new(43, 50, 100);

    // Different seeds should (eventually) produce different values
    let samples1: Vec<u64> = (0..100).map(|_| model1.sample()).collect();
    let samples2: Vec<u64> = (0..100).map(|_| model2.sample()).collect();

    // They shouldn't be identical
    assert_ne!(samples1, samples2);
}

/// Test latency model with fixed latency (min == max)
#[test]
fn test_latency_model_fixed() {
    let mut model = LatencyModel::new(42, 50, 50);

    // All samples should be exactly 50
    for _ in 0..100 {
        assert_eq!(model.sample(), 50);
    }
}

/// Test that PendingNew order cannot be filled
#[test]
fn test_pending_new_order_not_fillable() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit order but don't ack
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    sim.submit_order(order);

    // Trade at our price - but order isn't acked yet
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("5"), 1020, None);

    // Should have no fills - order not yet acked
    assert_eq!(fills.len(), 0);
}

/// Test multiple orders with different ack times
#[test]
fn test_multiple_orders_different_ack_times() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book with small external liquidity
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("5"))], // Only 5 external liquidity
        1000,
    );

    // Submit first order - acks at 1050
    let order1 = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    // Submit second order - acks at 1080
    let order2 = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1010,
        ack_ts: 1080,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let id1 = sim.submit_order(order1);
    let id2 = sim.submit_order(order2);

    // Ack first order
    sim.process_order_ack(id1).unwrap();

    // First order is live, second still pending
    assert_eq!(sim.get_order(id1).unwrap().status, SimOrderStatus::Live);
    assert_eq!(sim.get_order(id2).unwrap().status, SimOrderStatus::PendingNew);

    // ext_ahead for first order = 5 (book size at price)
    // Trade of 10: 5 ext_ahead + 5 of our first order
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1060, None);

    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id, id1);
    assert_eq!(fills[0].size, dec("5"));

    // Now ack second order
    sim.process_order_ack(id2).unwrap();
    assert_eq!(sim.get_order(id2).unwrap().status, SimOrderStatus::Live);
}

/// Test cancel request on non-open order is ignored
#[test]
fn test_cancel_request_on_pending_ignored() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit order but don't ack
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // Try to cancel - should return false since not open
    let result = sim.request_cancel(order_id);
    assert!(!result);

    // Order still pending
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::PendingNew);
}

/// Test cancel request on already canceled order is ignored
#[test]
fn test_cancel_request_on_canceled_ignored() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit, ack, and cancel order
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();
    sim.request_cancel(order_id);
    sim.process_cancel_ack(order_id);

    // Order is canceled
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Canceled);

    // Try to cancel again - should return false
    let result = sim.request_cancel(order_id);
    assert!(!result);
}

/// Test order remains fillable during cancel request and ack
#[test]
fn test_order_fillable_status() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // PendingNew - not fillable
    assert!(!sim.get_order(order_id).unwrap().is_fillable());

    // Live - fillable
    sim.process_order_ack(order_id).unwrap();
    assert!(sim.get_order(order_id).unwrap().is_fillable());

    // PendingCancel - still fillable
    sim.request_cancel(order_id);
    assert!(sim.get_order(order_id).unwrap().is_fillable());

    // Canceled - not fillable
    sim.process_cancel_ack(order_id);
    assert!(!sim.get_order(order_id).unwrap().is_fillable());
}
