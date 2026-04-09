//! Tests for queue position tracking and fill logic

use polyfill_rs::Side;
use rust_decimal::Decimal;

use super::dec;
use crate::backtest::{
    ExchangeSimulator, QueueKey, QueueModel, SimOrder, SimOrderStatus,
};

// ============================================================================
// FIFO Tests
// ============================================================================

/// Test that multiple orders at same price fill in FIFO order
#[test]
fn test_multiple_orders_same_price_fill_fifo() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book with ask at 0.51
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // Submit first SELL order at 0.51
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
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    // Submit second SELL order at same price
    let order2 = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1005,
        ack_ts: 1015,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let id1 = sim.submit_order(order1);
    let id2 = sim.submit_order(order2);

    // Ack both orders
    sim.process_order_ack(id1).unwrap();
    sim.process_order_ack(id2).unwrap();

    // Trade that partially fills - should fill first order first
    // BUY trade at 0.51 fills SELL orders
    // ext_ahead = 100 (book size at ack), so first 100 consumes ext, then fills us
    // We need a trade larger than ext_ahead to reach our orders
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("105"), 1020, None);

    // Should only fill first order (5 units after 100 ext_ahead consumed)
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id, id1);
    assert_eq!(fills[0].size, dec("5"));

    // First order partially filled, second untouched
    assert_eq!(sim.get_order(id1).unwrap().filled_size, dec("5"));
    assert_eq!(sim.get_order(id2).unwrap().filled_size, Decimal::ZERO);
}

/// Test partial fill preserves queue position
#[test]
fn test_partial_fill_preserves_queue_position() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Book with small external liquidity
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))], // Only 10 external
        1000,
    );

    // Submit SELL at 0.51
    let order = SimOrder {
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

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // ext_ahead = 10 after ack

    // First trade: consumes 10 ext + 5 of our order
    let fills1 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("15"), 1020, None);
    assert_eq!(fills1.len(), 1);
    assert_eq!(fills1[0].size, dec("5"));

    // Order now has 5 filled, 15 remaining
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.filled_size, dec("5"));
    assert_eq!(order.remaining(), dec("15"));
    assert_eq!(order.status, SimOrderStatus::PartialFilled);

    // Second trade: fills more of our order
    let fills2 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1030, None);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].size, dec("10"));

    // Order now has 15 filled, 5 remaining
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.filled_size, dec("15"));
    assert_eq!(order.remaining(), dec("5"));
}

/// Test first order fully filled then second starts
#[test]
fn test_first_order_fully_filled_then_second() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Small external liquidity
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("5"))],
        1000,
    );

    // Two orders
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
        ack_ts: 1010,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order2 = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1001,
        ack_ts: 1011,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let id1 = sim.submit_order(order1);
    let id2 = sim.submit_order(order2);
    sim.process_order_ack(id1).unwrap();
    sim.process_order_ack(id2).unwrap();

    // Trade that fills all ext + all of first + part of second
    // ext_ahead = 5, order1 = 10, order2 = 10
    // Trade of 20: 5 ext + 10 order1 + 5 order2
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("20"), 1020, None);

    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].order_id, id1);
    assert_eq!(fills[0].size, dec("10"));
    assert_eq!(fills[1].order_id, id2);
    assert_eq!(fills[1].size, dec("5"));

    // First fully filled, second partially
    assert_eq!(sim.get_order(id1).unwrap().status, SimOrderStatus::Filled);
    assert_eq!(
        sim.get_order(id2).unwrap().status,
        SimOrderStatus::PartialFilled
    );
}

// ============================================================================
// Queue-ahead (ext_ahead) Tests
// ============================================================================

/// Test ext_ahead is set correctly on order ack
#[test]
fn test_ext_ahead_set_on_order_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Book with 100 at the ask
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

    // Check queue state
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();

    // ext_ahead should be book size at price
    assert_eq!(level.ext_ahead, dec("100"));
}

/// Test trade reduces ext_ahead before filling our orders
#[test]
fn test_trade_reduces_ext_ahead_before_filling_us() {
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

    // Trade that doesn't reach us (smaller than ext_ahead)
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("50"), 1020, None);

    // No fills for us
    assert_eq!(fills.len(), 0);

    // ext_ahead should be reduced
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, dec("50")); // 100 - 50

    // Our order untouched
    assert_eq!(sim.get_order(order_id).unwrap().filled_size, Decimal::ZERO);
}

/// Test no fill while ext_ahead is positive
#[test]
fn test_no_fill_while_ext_ahead_positive() {
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

    // Multiple small trades, none reaching us
    for _ in 0..5 {
        let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1020, None);
        assert_eq!(fills.len(), 0);
    }

    // ext_ahead now at 50, we still haven't been filled
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, dec("50"));
    assert_eq!(sim.get_order(order_id).unwrap().filled_size, Decimal::ZERO);
}

/// Test fill happens after ext_ahead is consumed
#[test]
fn test_fill_after_ext_ahead_consumed() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("50"))], // Small ext
        1000,
    );

    let order = SimOrder {
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

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Trade exactly consuming ext_ahead
    let fills1 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("50"), 1020, None);
    assert_eq!(fills1.len(), 0);

    // ext_ahead now 0
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, Decimal::ZERO);

    // Next trade fills us
    let fills2 = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("10"), 1030, None);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].size, dec("10"));
}

// ============================================================================
// Trade + Delta Double-counting Tests
// ============================================================================

/// Test trade and delta at same timestamp don't double-count
#[test]
fn test_trade_and_delta_same_ts_no_double_count() {
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

    // ext_ahead = 100

    // Trade of 30 at timestamp 1020
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("30"), 1020, None);
    assert_eq!(fills.len(), 0);

    // ext_ahead should be 70 now

    // Delta at same timestamp showing size decrease to 70 (matches trade)
    // This should NOT further reduce ext_ahead since we already counted the trade
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("70"), 1020);

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    // ext_ahead should be 70, not further reduced
    assert_eq!(level.ext_ahead, dec("70"));
}

/// Test cancel-like removal (size decrease without trade) reduces ext_ahead
#[test]
fn test_cancel_like_removal_reduces_ext_ahead() {
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

    // ext_ahead = 100

    // Delta showing size decrease to 80 (no trade, so it's a cancel)
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("80"), 1020);

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    // Cancel-like removal of 20 should reduce ext_ahead
    assert_eq!(level.ext_ahead, dec("80"));
}

/// Test trade volume tracking at timestamp
#[test]
fn test_trade_vol_at_ts_tracked_correctly() {
    let mut queues = QueueModel::new(false);
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));

    // Create a queue level
    queues.add_order(key.clone(), 1, dec("10"), dec("100"));

    // Record trade volume
    queues.record_trade_volume(&key, 1020, dec("30"));

    let level = queues.get(&key).unwrap();
    assert_eq!(level.get_trade_vol(1020), dec("30"));

    // Different timestamp should return 0
    assert_eq!(level.get_trade_vol(1021), Decimal::ZERO);

    // Record more at same timestamp
    queues.record_trade_volume(&key, 1020, dec("20"));
    let level = queues.get(&key).unwrap();
    assert_eq!(level.get_trade_vol(1020), dec("50"));
}

// ============================================================================
// Size Change Tests
// ============================================================================

/// Test size increase with queue_added_ahead=false doesn't change ext_ahead
#[test]
fn test_size_increase_ext_ahead_unchanged_default() {
    let mut sim = ExchangeSimulator::new(false); // queue_added_ahead = false

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
    let initial_ext_ahead = sim.queues.get(&key).unwrap().ext_ahead;

    // Size increase delta
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("150"), 1020);

    // ext_ahead should be unchanged
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, initial_ext_ahead);
}

/// Test size increase with queue_added_ahead=true increases ext_ahead
#[test]
fn test_size_increase_ext_ahead_increases_when_queue_added_ahead() {
    let mut sim = ExchangeSimulator::new(true); // queue_added_ahead = true

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
    assert_eq!(sim.queues.get(&key).unwrap().ext_ahead, dec("100"));

    // Size increase delta (+50)
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("150"), 1020);

    // ext_ahead should increase by 50 (pessimistic mode)
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, dec("150"));
}

/// Test size decrease reduces ext_ahead
#[test]
fn test_size_decrease_reduces_ext_ahead() {
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

    // Size decrease delta
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("60"), 1020);

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    // ext_ahead reduced by 40 (cancel-like, no trade at this ts)
    assert_eq!(level.ext_ahead, dec("60"));
}

// ============================================================================
// Cancel Timing with Fills
// ============================================================================

/// Test fill during pending cancel state (before ack)
#[test]
fn test_fill_during_pending_cancel_before_ack() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("10"))], // Small ext
        1000,
    );

    let order = SimOrder {
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

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Request cancel
    sim.request_cancel(order_id);
    assert_eq!(
        sim.get_order(order_id).unwrap().status,
        SimOrderStatus::PendingCancel
    );

    // Trade occurs while cancel pending - should still fill
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("15"), 1020, None);

    // 10 ext + 5 of our order
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].size, dec("5"));
}

/// Test no fill after cancel ack
#[test]
fn test_no_fill_after_cancel_ack_queue() {
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

    let order_id = sim.submit_order(order);
    sim.process_order_ack(order_id).unwrap();

    // Request and ack cancel
    sim.request_cancel(order_id);
    sim.process_cancel_ack(order_id);

    // Trade occurs after cancel - should NOT fill
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("50"), 1020, None);

    // No fills (order removed from queue)
    assert_eq!(fills.len(), 0);
    assert_eq!(sim.get_order(order_id).unwrap().filled_size, Decimal::ZERO);
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test queue level for different token is independent
#[test]
fn test_queue_isolation_between_tokens() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN_A", "TOKEN_A_DOWN", dec("1000"), dec("1.00"));
    sim.seed_complete_sets("TOKEN_B", "TOKEN_B_DOWN", dec("1000"), dec("1.00"));

    // Set up two tokens
    sim.apply_snapshot(
        "TOKEN_A",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );
    sim.apply_snapshot(
        "TOKEN_B",
        &[(dec("0.45"), dec("50"))],
        &[(dec("0.46"), dec("50"))],
        1000,
    );

    // Order on TOKEN_A
    let order_a = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN_A".to_string(),
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

    let id_a = sim.submit_order(order_a);
    sim.process_order_ack(id_a).unwrap();

    // Trade on TOKEN_B should not affect TOKEN_A queue
    let fills = sim.process_trade("TOKEN_B", Side::BUY, dec("0.46"), dec("100"), 1020, None);
    assert_eq!(fills.len(), 0);

    // TOKEN_A queue unchanged
    let key_a = QueueKey::new("TOKEN_A".to_string(), Side::SELL, dec("0.51"));
    let level_a = sim.queues.get(&key_a).unwrap();
    assert_eq!(level_a.ext_ahead, dec("100"));
}

/// Test queue level for different price is independent
#[test]
fn test_queue_isolation_between_prices() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100")), (dec("0.52"), dec("100"))],
        1000,
    );

    // Order at 0.51
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

    // Trade at 0.52 should not affect 0.51 queue
    let fills = sim.process_trade("TOKEN", Side::BUY, dec("0.52"), dec("100"), 1020, None);
    assert_eq!(fills.len(), 0);

    // 0.51 queue unchanged
    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    assert_eq!(level.ext_ahead, dec("100"));
}

/// Test ext_ahead doesn't go negative
#[test]
fn test_ext_ahead_floor_at_zero() {
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

    // Trade larger than ext_ahead
    sim.process_trade("TOKEN", Side::BUY, dec("0.51"), dec("150"), 1020, None);

    let key = QueueKey::new("TOKEN".to_string(), Side::SELL, dec("0.51"));
    let level = sim.queues.get(&key).unwrap();
    // ext_ahead should be 0, not negative
    assert!(level.ext_ahead >= Decimal::ZERO);
}
