//! Tests for post-only order rejection logic

use polyfill_rs::Side;
use rust_decimal::Decimal;

use super::dec;
use crate::backtest::{ExchangeSimulator, SimOrder, SimOrderStatus};

/// Test that BUY post-only is rejected when price >= best ask
#[test]
fn test_buy_post_only_rejected_when_crosses_ask() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book with ask at 0.51
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY at 0.51 would cross the ask
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.51"), // >= best ask
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

    // Process ack - should be rejected
    let result = sim.process_order_ack(order_id);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("cross"));

    // Order should be Rejected
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
}

/// Test that SELL post-only is rejected when price <= best bid
#[test]
fn test_sell_post_only_rejected_when_crosses_bid() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book with bid at 0.50
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // SELL at 0.50 would cross the bid
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.50"), // <= best bid
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

    // Process ack - should be rejected
    let result = sim.process_order_ack(order_id);
    assert!(result.is_err());

    // Order should be Rejected
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
}

/// Test that BUY post-only is accepted when price < best ask
#[test]
fn test_buy_post_only_accepted_below_ask() {
    let mut sim = ExchangeSimulator::new(false);

    // Set up book with ask at 0.51
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY at 0.505 is below the ask - should be accepted
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.505"), // < best ask
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

    // Process ack - should be accepted
    let result = sim.process_order_ack(order_id);
    assert!(result.is_ok());

    // Order should be Live
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Live);
}

/// Test that SELL post-only is accepted when price > best bid
#[test]
fn test_sell_post_only_accepted_above_bid() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Set up book with bid at 0.50
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // SELL at 0.505 is above the bid - should be accepted
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.505"), // > best bid
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

    // Process ack - should be accepted
    let result = sim.process_order_ack(order_id);
    assert!(result.is_ok());

    // Order should be Live
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Live);
}

/// Test that post-only check happens at ack time, not submit time
#[test]
fn test_post_only_check_at_ack_time_not_submit() {
    let mut sim = ExchangeSimulator::new(false);

    // Initial book where order would be accepted
    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.52"), dec("100"))], // Ask at 0.52
        1000,
    );

    // Submit BUY at 0.51 - would be accepted now
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
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

    // Book changes - ask moves down to 0.51
    sim.apply_delta("TOKEN", Side::SELL, dec("0.51"), dec("100"), 1020);
    sim.apply_delta("TOKEN", Side::SELL, dec("0.52"), dec("0"), 1020); // Remove old ask

    // Now at ack time, the order would cross
    let result = sim.process_order_ack(order_id);
    assert!(result.is_err());

    // Order should be Rejected (check at ack time, not submit time)
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
}

/// Test that post-only rejection is recorded correctly
#[test]
fn test_post_only_rejection_recorded() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY at ask price - will be rejected
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
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
    let result = sim.process_order_ack(order_id);

    // Check error message
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Post-only"));
    assert!(err.contains("cross"));

    // Order has correct state
    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Rejected);
    assert_eq!(order.filled_size, Decimal::ZERO);
}

/// Test non-post-only order would be accepted even if crossing (hypothetical)
/// Note: Our implementation only supports post_only=true, but test verifies the check
#[test]
fn test_non_post_only_order_accepted_when_crossing() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY at ask price but with post_only=false
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.51"),
        original_size: dec("10"),
        filled_size: Decimal::ZERO,
        status: SimOrderStatus::PendingNew,
        submit_ts: 1000,
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: false, // Not post-only
        is_taker: false,
    };

    let order_id = sim.submit_order(order);
    let result = sim.process_order_ack(order_id);

    // Should be accepted (not post-only, so no cross check)
    assert!(result.is_ok());

    let order = sim.get_order(order_id).unwrap();
    assert_eq!(order.status, SimOrderStatus::Live);
}

/// Test post-only on empty book (no ask/bid to cross)
#[test]
fn test_post_only_accepted_on_empty_book() {
    let mut sim = ExchangeSimulator::new(false);

    // Empty book
    sim.apply_snapshot("TOKEN", &[], &[], 1000);

    // BUY post-only with no asks - should be accepted
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
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
    let result = sim.process_order_ack(order_id);

    assert!(result.is_ok());
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Live);
}

/// Test post-only sell on book with no bids
#[test]
fn test_post_only_sell_accepted_no_bids() {
    let mut sim = ExchangeSimulator::new(false);

    // Seed inventory for SELL orders
    sim.seed_complete_sets("TOKEN", "TOKEN_DOWN", dec("1000"), dec("1.00"));

    // Book with only asks (no bids)
    sim.apply_snapshot("TOKEN", &[], &[(dec("0.51"), dec("100"))], 1000);

    // SELL post-only with no bids - should be accepted
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.50"),
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
    let result = sim.process_order_ack(order_id);

    assert!(result.is_ok());
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Live);
}

/// Test exact price equality triggers rejection
#[test]
fn test_post_only_exact_price_match_rejected() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY exactly at ask price
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.51"), // Exact match with ask
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
    let result = sim.process_order_ack(order_id);

    // BUY price >= best_ask should reject
    assert!(result.is_err());
}

/// Test BUY above ask is rejected
#[test]
fn test_post_only_buy_above_ask_rejected() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // BUY above ask price
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::BUY,
        price: dec("0.52"), // Above ask
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
    let result = sim.process_order_ack(order_id);

    assert!(result.is_err());
}

/// Test SELL below bid is rejected
#[test]
fn test_post_only_sell_below_bid_rejected() {
    let mut sim = ExchangeSimulator::new(false);

    sim.apply_snapshot(
        "TOKEN",
        &[(dec("0.50"), dec("100"))],
        &[(dec("0.51"), dec("100"))],
        1000,
    );

    // SELL below bid price
    let order = SimOrder {
        order_id: 0,
        client_id: None,
        token_id: "TOKEN".to_string(),
        side: Side::SELL,
        price: dec("0.49"), // Below bid
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
    let result = sim.process_order_ack(order_id);

    assert!(result.is_err());
}

/// Test processing already-acked order is idempotent
#[test]
fn test_double_ack_idempotent() {
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
        ack_ts: 1050,
        cancel_req_ts: None,
        cancel_ack_ts: None,
        post_only: true,
        is_taker: false,
    };

    let order_id = sim.submit_order(order);

    // First ack
    let result1 = sim.process_order_ack(order_id);
    assert!(result1.is_ok());
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Live);

    // Second ack - should be idempotent
    let result2 = sim.process_order_ack(order_id);
    assert!(result2.is_ok());
    assert_eq!(sim.get_order(order_id).unwrap().status, SimOrderStatus::Live);
}
