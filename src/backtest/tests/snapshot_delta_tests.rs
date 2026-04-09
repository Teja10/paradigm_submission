//! Tests for snapshot and delta application to L2 book

use polyfill_rs::Side;
use rust_decimal::Decimal;

use super::{assert_best_ask, assert_best_bid, assert_book_level, assert_no_asks, assert_no_bids, dec};
use crate::backtest::L2Book;

/// Test that snapshot overwrites all existing levels
#[test]
fn test_snapshot_overwrites_all_levels() {
    let mut book = L2Book::new();

    // Set up initial book state
    book.bids.insert(dec("0.45"), dec("50"));
    book.bids.insert(dec("0.44"), dec("100"));
    book.asks.insert(dec("0.55"), dec("50"));
    book.asks.insert(dec("0.56"), dec("100"));

    // Apply snapshot with different levels
    let new_bids = vec![
        (dec("0.50"), dec("100")),
        (dec("0.49"), dec("200")),
    ];
    let new_asks = vec![
        (dec("0.51"), dec("100")),
        (dec("0.52"), dec("200")),
    ];

    book.apply_snapshot(&new_bids, &new_asks);

    // Old levels should be gone
    assert_eq!(book.size_at(Side::BUY, dec("0.45")), Decimal::ZERO);
    assert_eq!(book.size_at(Side::BUY, dec("0.44")), Decimal::ZERO);
    assert_eq!(book.size_at(Side::SELL, dec("0.55")), Decimal::ZERO);
    assert_eq!(book.size_at(Side::SELL, dec("0.56")), Decimal::ZERO);

    // New levels should exist
    assert_book_level(&book, Side::BUY, dec("0.50"), dec("100"));
    assert_book_level(&book, Side::BUY, dec("0.49"), dec("200"));
    assert_book_level(&book, Side::SELL, dec("0.51"), dec("100"));
    assert_book_level(&book, Side::SELL, dec("0.52"), dec("200"));
}

/// Test that snapshot removes levels not present in new snapshot
#[test]
fn test_snapshot_removes_levels_not_present() {
    let mut book = L2Book::new();

    // Start with 3 bid levels
    book.bids.insert(dec("0.50"), dec("100"));
    book.bids.insert(dec("0.49"), dec("200"));
    book.bids.insert(dec("0.48"), dec("300"));

    // Apply snapshot with only 1 bid level
    let new_bids = vec![(dec("0.50"), dec("150"))];
    let new_asks = vec![(dec("0.51"), dec("100"))];

    book.apply_snapshot(&new_bids, &new_asks);

    // Only new level should remain
    assert_eq!(book.bids.len(), 1);
    assert_book_level(&book, Side::BUY, dec("0.50"), dec("150"));
    assert_eq!(book.size_at(Side::BUY, dec("0.49")), Decimal::ZERO);
    assert_eq!(book.size_at(Side::BUY, dec("0.48")), Decimal::ZERO);
}

/// Test that delta adds a new level to the book
#[test]
fn test_delta_adds_new_level() {
    let mut book = L2Book::new();

    // Apply delta to add a bid level
    let (old_size, new_size) = book.apply_delta(Side::BUY, dec("0.50"), dec("100"));

    assert_eq!(old_size, Decimal::ZERO);
    assert_eq!(new_size, dec("100"));
    assert_book_level(&book, Side::BUY, dec("0.50"), dec("100"));
}

/// Test that delta updates an existing level
#[test]
fn test_delta_updates_existing_level() {
    let mut book = L2Book::new();

    // Initial level
    book.bids.insert(dec("0.50"), dec("100"));

    // Update via delta
    let (old_size, new_size) = book.apply_delta(Side::BUY, dec("0.50"), dec("150"));

    assert_eq!(old_size, dec("100"));
    assert_eq!(new_size, dec("150"));
    assert_book_level(&book, Side::BUY, dec("0.50"), dec("150"));
}

/// Test that delta with size 0 removes a level
#[test]
fn test_delta_zero_size_removes_level() {
    let mut book = L2Book::new();

    // Initial level
    book.bids.insert(dec("0.50"), dec("100"));
    assert_eq!(book.bids.len(), 1);

    // Remove via delta with size 0
    let (old_size, new_size) = book.apply_delta(Side::BUY, dec("0.50"), dec("0"));

    assert_eq!(old_size, dec("100"));
    assert_eq!(new_size, Decimal::ZERO);
    assert_eq!(book.bids.len(), 0);
    assert_eq!(book.size_at(Side::BUY, dec("0.50")), Decimal::ZERO);
}

/// Test that delta preserves other levels
#[test]
fn test_delta_preserves_other_levels() {
    let mut book = L2Book::new();

    // Initial book
    book.bids.insert(dec("0.50"), dec("100"));
    book.bids.insert(dec("0.49"), dec("200"));
    book.asks.insert(dec("0.51"), dec("100"));
    book.asks.insert(dec("0.52"), dec("200"));

    // Update only one level
    book.apply_delta(Side::BUY, dec("0.50"), dec("150"));

    // Other levels should be unchanged
    assert_book_level(&book, Side::BUY, dec("0.49"), dec("200"));
    assert_book_level(&book, Side::SELL, dec("0.51"), dec("100"));
    assert_book_level(&book, Side::SELL, dec("0.52"), dec("200"));
}

/// Test best bid/ask after snapshot
#[test]
fn test_best_bid_ask_after_snapshot() {
    let mut book = L2Book::new();

    let bids = vec![
        (dec("0.50"), dec("100")),
        (dec("0.49"), dec("200")),
        (dec("0.48"), dec("300")),
    ];
    let asks = vec![
        (dec("0.51"), dec("100")),
        (dec("0.52"), dec("200")),
        (dec("0.53"), dec("300")),
    ];

    book.apply_snapshot(&bids, &asks);

    // Best bid is highest price
    assert_best_bid(&book, dec("0.50"), dec("100"));

    // Best ask is lowest price
    assert_best_ask(&book, dec("0.51"), dec("100"));
}

/// Test mid calculation
#[test]
fn test_mid_calculation() {
    let mut book = L2Book::new();

    book.bids.insert(dec("0.50"), dec("100"));
    book.asks.insert(dec("0.52"), dec("100"));

    let mid = book.mid().expect("Should have mid");
    assert_eq!(mid, dec("0.51"));
}

/// Test mid with different spread
#[test]
fn test_mid_with_wide_spread() {
    let mut book = L2Book::new();

    book.bids.insert(dec("0.40"), dec("100"));
    book.asks.insert(dec("0.60"), dec("100"));

    let mid = book.mid().expect("Should have mid");
    assert_eq!(mid, dec("0.50"));
}

/// Test mid returns None when no bids
#[test]
fn test_mid_no_bids() {
    let mut book = L2Book::new();
    book.asks.insert(dec("0.51"), dec("100"));

    assert!(book.mid().is_none());
}

/// Test mid returns None when no asks
#[test]
fn test_mid_no_asks() {
    let mut book = L2Book::new();
    book.bids.insert(dec("0.50"), dec("100"));

    assert!(book.mid().is_none());
}

/// Test empty snapshot clears book
#[test]
fn test_empty_snapshot_clears_book() {
    let mut book = L2Book::new();

    book.bids.insert(dec("0.50"), dec("100"));
    book.asks.insert(dec("0.51"), dec("100"));

    // Apply empty snapshot
    book.apply_snapshot(&[], &[]);

    assert_no_bids(&book);
    assert_no_asks(&book);
}

/// Test snapshot returns correct changes
#[test]
fn test_snapshot_returns_changes() {
    let mut book = L2Book::new();

    // Initial book
    book.bids.insert(dec("0.50"), dec("100"));
    book.asks.insert(dec("0.51"), dec("100"));

    // Apply snapshot with changes
    let new_bids = vec![(dec("0.50"), dec("150"))]; // Update
    let new_asks = vec![(dec("0.52"), dec("200"))]; // New level, old removed

    let changes = book.apply_snapshot(&new_bids, &new_asks);

    // Should have changes for:
    // - bid 0.50: 100 -> 150
    // - ask 0.51: 100 -> 0 (removed)
    // - ask 0.52: 0 -> 200 (added)
    assert!(!changes.is_empty());

    // Verify bid change
    let bid_change = changes.iter().find(|(s, p, _, _)| *s == Side::BUY && *p == dec("0.50"));
    assert!(bid_change.is_some());
    let (_, _, old, new) = bid_change.unwrap();
    assert_eq!(*old, dec("100"));
    assert_eq!(*new, dec("150"));

    // Verify old ask removed
    let old_ask_change = changes.iter().find(|(s, p, _, _)| *s == Side::SELL && *p == dec("0.51"));
    assert!(old_ask_change.is_some());
    let (_, _, old, new) = old_ask_change.unwrap();
    assert_eq!(*old, dec("100"));
    assert_eq!(*new, Decimal::ZERO);

    // Verify new ask added
    let new_ask_change = changes.iter().find(|(s, p, _, _)| *s == Side::SELL && *p == dec("0.52"));
    assert!(new_ask_change.is_some());
    let (_, _, old, new) = new_ask_change.unwrap();
    assert_eq!(*old, Decimal::ZERO);
    assert_eq!(*new, dec("200"));
}

/// Test delta on ask side
#[test]
fn test_delta_ask_side() {
    let mut book = L2Book::new();

    // Add ask
    let (old, new) = book.apply_delta(Side::SELL, dec("0.51"), dec("100"));
    assert_eq!(old, Decimal::ZERO);
    assert_eq!(new, dec("100"));
    assert_book_level(&book, Side::SELL, dec("0.51"), dec("100"));

    // Update ask
    let (old, new) = book.apply_delta(Side::SELL, dec("0.51"), dec("150"));
    assert_eq!(old, dec("100"));
    assert_eq!(new, dec("150"));
    assert_book_level(&book, Side::SELL, dec("0.51"), dec("150"));

    // Remove ask
    let (old, new) = book.apply_delta(Side::SELL, dec("0.51"), dec("0"));
    assert_eq!(old, dec("150"));
    assert_eq!(new, Decimal::ZERO);
    assert_eq!(book.size_at(Side::SELL, dec("0.51")), Decimal::ZERO);
}

/// Test sequential deltas
#[test]
fn test_sequential_deltas() {
    let mut book = L2Book::new();

    // Series of deltas building up a book
    book.apply_delta(Side::BUY, dec("0.50"), dec("100"));
    book.apply_delta(Side::BUY, dec("0.49"), dec("200"));
    book.apply_delta(Side::SELL, dec("0.51"), dec("100"));
    book.apply_delta(Side::SELL, dec("0.52"), dec("200"));

    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 2);

    // Update one
    book.apply_delta(Side::BUY, dec("0.50"), dec("150"));

    // Remove one
    book.apply_delta(Side::SELL, dec("0.52"), dec("0"));

    assert_eq!(book.bids.len(), 2);
    assert_eq!(book.asks.len(), 1);
    assert_book_level(&book, Side::BUY, dec("0.50"), dec("150"));
    assert_eq!(book.size_at(Side::SELL, dec("0.52")), Decimal::ZERO);
}

/// Test snapshot followed by deltas (typical pattern)
#[test]
fn test_snapshot_then_deltas() {
    let mut book = L2Book::new();

    // Initial snapshot
    let bids = vec![(dec("0.50"), dec("100"))];
    let asks = vec![(dec("0.51"), dec("100"))];
    book.apply_snapshot(&bids, &asks);

    // Delta updates
    book.apply_delta(Side::BUY, dec("0.50"), dec("120")); // Update
    book.apply_delta(Side::BUY, dec("0.49"), dec("200")); // Add
    book.apply_delta(Side::SELL, dec("0.51"), dec("80")); // Update

    assert_book_level(&book, Side::BUY, dec("0.50"), dec("120"));
    assert_book_level(&book, Side::BUY, dec("0.49"), dec("200"));
    assert_book_level(&book, Side::SELL, dec("0.51"), dec("80"));
}

/// Test book with many levels maintains correct ordering
#[test]
fn test_many_levels_ordering() {
    let mut book = L2Book::new();

    // Add bids in random order
    book.bids.insert(dec("0.45"), dec("100"));
    book.bids.insert(dec("0.50"), dec("100"));
    book.bids.insert(dec("0.47"), dec("100"));
    book.bids.insert(dec("0.48"), dec("100"));
    book.bids.insert(dec("0.49"), dec("100"));

    // Best bid should be highest price
    let (best_price, _) = book.best_bid().unwrap();
    assert_eq!(best_price, dec("0.50"));

    // Add asks in random order
    book.asks.insert(dec("0.55"), dec("100"));
    book.asks.insert(dec("0.51"), dec("100"));
    book.asks.insert(dec("0.53"), dec("100"));
    book.asks.insert(dec("0.52"), dec("100"));
    book.asks.insert(dec("0.54"), dec("100"));

    // Best ask should be lowest price
    let (best_price, _) = book.best_ask().unwrap();
    assert_eq!(best_price, dec("0.51"));
}

/// Test precision is maintained in book levels
#[test]
fn test_precision_maintained() {
    let mut book = L2Book::new();

    // Use precise decimal values
    let precise_price = dec("0.123456789");
    let precise_size = dec("999.123456789");

    book.apply_delta(Side::BUY, precise_price, precise_size);

    let size = book.size_at(Side::BUY, precise_price);
    assert_eq!(size, precise_size);
}

/// Test zero-size levels are not stored after removal
#[test]
fn test_zero_size_not_stored() {
    let mut book = L2Book::new();

    book.bids.insert(dec("0.50"), dec("100"));
    assert_eq!(book.bids.len(), 1);

    // Remove via delta
    book.apply_delta(Side::BUY, dec("0.50"), Decimal::ZERO);

    // Should be completely removed, not stored as zero
    assert_eq!(book.bids.len(), 0);
    assert!(!book.bids.contains_key(&dec("0.50")));
}
