//! Tests for event ordering and sorting

use polyfill_rs::Side;

use super::{delta_event, feature_event, snapshot_event, trade_event};
use crate::backtest::EventPriority;

/// Test that at the same timestamp, Snapshot comes before Delta
#[test]
fn test_same_timestamp_sorts_snapshot_before_delta() {
    let snapshot = snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let delta = delta_event(1000, 2, "TOKEN", Side::BUY, 0.50, 150.0);

    // Snapshot should be "less than" delta (comes first)
    assert!(snapshot < delta);
    assert!(snapshot.priority < delta.priority);

    // Verify sorting
    let mut events = vec![delta.clone(), snapshot.clone()];
    events.sort();
    assert_eq!(events[0].priority, EventPriority::Snapshot);
    assert_eq!(events[1].priority, EventPriority::Delta);
}

/// Test that at the same timestamp, Delta comes before Trade
#[test]
fn test_same_timestamp_sorts_delta_before_trade() {
    let delta = delta_event(1000, 1, "TOKEN", Side::BUY, 0.50, 150.0);
    let trade = trade_event(1000, 2, "TOKEN", Side::BUY, 0.51, 10.0);

    assert!(delta < trade);
    assert!(delta.priority < trade.priority);

    let mut events = vec![trade.clone(), delta.clone()];
    events.sort();
    assert_eq!(events[0].priority, EventPriority::Delta);
    assert_eq!(events[1].priority, EventPriority::Trade);
}

/// Test that at the same timestamp, Trade comes before Feature
#[test]
fn test_same_timestamp_sorts_trade_before_feature() {
    let trade = trade_event(1000, 1, "TOKEN", Side::BUY, 0.51, 10.0);
    let feature = feature_event(1000, 2);

    assert!(trade < feature);
    assert!(trade.priority < feature.priority);

    let mut events = vec![feature.clone(), trade.clone()];
    events.sort();
    assert_eq!(events[0].priority, EventPriority::Trade);
    assert_eq!(events[1].priority, EventPriority::Feature);
}

/// Test full ordering: Snapshot < Delta < Trade < Feature at same timestamp
#[test]
fn test_full_priority_ordering_same_timestamp() {
    let snapshot = snapshot_event(1000, 0, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let delta = delta_event(1000, 1, "TOKEN", Side::BUY, 0.50, 150.0);
    let trade = trade_event(1000, 2, "TOKEN", Side::BUY, 0.51, 10.0);
    let feature = feature_event(1000, 3);

    // Create in reverse order
    let mut events = vec![feature, trade, delta, snapshot];
    events.sort();

    assert_eq!(events[0].priority, EventPriority::Snapshot);
    assert_eq!(events[1].priority, EventPriority::Delta);
    assert_eq!(events[2].priority, EventPriority::Trade);
    assert_eq!(events[3].priority, EventPriority::Feature);
}

/// Test that sequence number provides stable ordering within same timestamp and priority
#[test]
fn test_stable_order_within_same_type() {
    let delta1 = delta_event(1000, 1, "TOKEN", Side::BUY, 0.50, 100.0);
    let delta2 = delta_event(1000, 2, "TOKEN", Side::BUY, 0.49, 100.0);
    let delta3 = delta_event(1000, 3, "TOKEN", Side::SELL, 0.51, 100.0);

    // Lower sequence should come first
    assert!(delta1 < delta2);
    assert!(delta2 < delta3);

    let mut events = vec![delta3.clone(), delta1.clone(), delta2.clone()];
    events.sort();

    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(events[2].sequence, 3);
}

/// Test that different timestamps sort chronologically regardless of priority
#[test]
fn test_different_timestamps_sort_chronologically() {
    let feature_early = feature_event(1000, 0);
    let snapshot_late = snapshot_event(2000, 0, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);

    // Even though Feature has higher priority number, earlier timestamp wins
    assert!(feature_early < snapshot_late);

    let mut events = vec![snapshot_late.clone(), feature_early.clone()];
    events.sort();

    assert_eq!(events[0].timestamp_ms, 1000);
    assert_eq!(events[1].timestamp_ms, 2000);
}

/// Test complex scenario with multiple events at multiple timestamps
#[test]
fn test_complex_multi_timestamp_ordering() {
    let events_unsorted = vec![
        feature_event(2000, 10),
        trade_event(1000, 3, "TOKEN", Side::BUY, 0.51, 10.0),
        snapshot_event(2000, 5, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]),
        delta_event(1000, 2, "TOKEN", Side::BUY, 0.50, 150.0),
        snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]),
        trade_event(2000, 8, "TOKEN", Side::SELL, 0.50, 5.0),
        delta_event(2000, 6, "TOKEN", Side::SELL, 0.51, 80.0),
        feature_event(1000, 4),
    ];

    let mut events = events_unsorted;
    events.sort();

    // Verify timestamp ordering
    assert_eq!(events[0].timestamp_ms, 1000);
    assert_eq!(events[1].timestamp_ms, 1000);
    assert_eq!(events[2].timestamp_ms, 1000);
    assert_eq!(events[3].timestamp_ms, 1000);
    assert_eq!(events[4].timestamp_ms, 2000);
    assert_eq!(events[5].timestamp_ms, 2000);
    assert_eq!(events[6].timestamp_ms, 2000);
    assert_eq!(events[7].timestamp_ms, 2000);

    // Verify priority ordering within timestamp 1000
    assert_eq!(events[0].priority, EventPriority::Snapshot);
    assert_eq!(events[1].priority, EventPriority::Delta);
    assert_eq!(events[2].priority, EventPriority::Trade);
    assert_eq!(events[3].priority, EventPriority::Feature);

    // Verify priority ordering within timestamp 2000
    assert_eq!(events[4].priority, EventPriority::Snapshot);
    assert_eq!(events[5].priority, EventPriority::Delta);
    assert_eq!(events[6].priority, EventPriority::Trade);
    assert_eq!(events[7].priority, EventPriority::Feature);
}

/// Test that event equality is correctly defined
#[test]
fn test_event_equality() {
    let event1 = snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let event2 = snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);

    // Same timestamp, priority, and sequence should be equal
    assert_eq!(event1, event2);
}

/// Test that events with different sequences are not equal
#[test]
fn test_event_inequality_different_sequence() {
    let event1 = snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let event2 = snapshot_event(1000, 2, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);

    assert_ne!(event1, event2);
}

/// Test BacktestEvent Ord implementation is consistent
#[test]
fn test_ord_consistency() {
    let a = snapshot_event(1000, 1, "TOKEN", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let b = delta_event(1000, 2, "TOKEN", Side::BUY, 0.50, 150.0);
    let c = trade_event(1000, 3, "TOKEN", Side::BUY, 0.51, 10.0);

    // Transitivity: a < b and b < c implies a < c
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);

    // Antisymmetry: a < b implies !(b < a)
    assert!(!(b < a));

    // Reflexivity for Eq: a == a
    assert!(a == a);
}

/// Test priority enum ordering
#[test]
fn test_event_priority_ordering() {
    assert!(EventPriority::Snapshot < EventPriority::Delta);
    assert!(EventPriority::Delta < EventPriority::Trade);
    assert!(EventPriority::Trade < EventPriority::Feature);

    // Numeric values
    assert_eq!(EventPriority::Snapshot as u8, 0);
    assert_eq!(EventPriority::Delta as u8, 1);
    assert_eq!(EventPriority::Trade as u8, 2);
    assert_eq!(EventPriority::Feature as u8, 3);
}

/// Test that multiple snapshots at same timestamp sort by sequence
#[test]
fn test_multiple_snapshots_same_timestamp() {
    let snap1 = snapshot_event(1000, 0, "TOKEN_A", &[(0.50, 100.0)], &[(0.51, 100.0)]);
    let snap2 = snapshot_event(1000, 1, "TOKEN_B", &[(0.45, 50.0)], &[(0.46, 50.0)]);
    let snap3 = snapshot_event(1000, 2, "TOKEN_C", &[(0.40, 25.0)], &[(0.41, 25.0)]);

    let mut events = vec![snap3.clone(), snap1.clone(), snap2.clone()];
    events.sort();

    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[1].sequence, 1);
    assert_eq!(events[2].sequence, 2);
}

/// Test that multiple trades at same timestamp sort by sequence
#[test]
fn test_multiple_trades_same_timestamp() {
    let trade1 = trade_event(1000, 0, "TOKEN", Side::BUY, 0.51, 10.0);
    let trade2 = trade_event(1000, 1, "TOKEN", Side::BUY, 0.51, 20.0);
    let trade3 = trade_event(1000, 2, "TOKEN", Side::SELL, 0.50, 5.0);

    let mut events = vec![trade3.clone(), trade1.clone(), trade2.clone()];
    events.sort();

    assert_eq!(events[0].sequence, 0);
    assert_eq!(events[1].sequence, 1);
    assert_eq!(events[2].sequence, 2);
}
