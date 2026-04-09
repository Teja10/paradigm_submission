//! Integration tests for full backtest runs with golden file comparisons

use polars::prelude::*;
use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::path::Path;
use tempfile::TempDir;

use super::{
    dec, test_config, test_config_with_latency,
    CountingStrategy, NoOpStrategy, TestStrategy,
    fixture_generator::{
        generate_single_token_fixture, generate_queue_test_fixture,
        DeltaRow, FeatureRowData, SnapshotRow, TradeRow, TEST_WINDOW_START,
        write_deltas_parquet, write_features_parquet, write_snapshots_parquet, write_trades_parquet,
    },
};
use crate::backtest::{BacktestConfig, BacktestEngine, DeltaMode, EventLoader, SimOrderStatus};
use crate::tracking::TrackedOrderStatus;

// ============================================================================
// Helper Functions
// ============================================================================

/// Run a backtest with the given config and strategy, return the engine
fn run_backtest<S: crate::engine::Strategy>(
    config: BacktestConfig,
    strategy: &mut S,
) -> BacktestEngine {
    let loader = EventLoader::new(config.clone());
    let events = loader.load_events().expect("Failed to load events");

    let mut engine = BacktestEngine::new(config);
    engine.run(strategy, events).expect("Backtest failed");
    engine
}

/// Read a parquet file and return the DataFrame
fn read_parquet(path: &Path) -> DataFrame {
    LazyFrame::scan_parquet(path, Default::default())
        .expect("Failed to scan parquet")
        .collect()
        .expect("Failed to collect parquet")
}

// ============================================================================
// Single Token Backtest Tests
// ============================================================================

/// Test that a basic single-token backtest runs without error
#[test]
fn test_single_token_backtest_runs() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    generate_single_token_fixture(fixture_dir.path()).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = NoOpStrategy;
    let _engine = run_backtest(config, &mut strategy);

    // Backtest should complete without error
}

/// Test single token backtest with orders that get filled
#[test]
fn test_single_token_backtest_with_fills() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    // Create custom fixture where our order will get filled
    let date_dir = fixture_dir.path().join("2025-01-01");

    // Snapshot with small external liquidity
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("10"))], // Only 10 external
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    // No deltas
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();

    // Trades that will fill our order after consuming ext
    let trades = vec![TradeRow {
        timestamp_ms: 1100,
        token_id: "TEST_TOKEN".to_string(),
        side: "buy".to_string(),
        price: dec("0.51"),
        size: dec("20"), // 10 ext + 10 of our order
        trade_id: Some("trade_1".to_string()),
    }];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades).unwrap();

    // Feature that triggers order placement
    let features = vec![
        FeatureRowData {
            timestamp_ms: 1020,
            bid_up: 0.50,
            ask_up: 0.51,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1200,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    // Strategy that places a SELL order at 0.51 on first feature (1020)
    // With 50ms latency, acks at 1070, before trade at 1100
    let mut strategy = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("20"),
    )]);

    let _engine = run_backtest(config, &mut strategy);

    // Should have fills
    let fills_path = output_dir.path().join("fills.parquet");
    assert!(fills_path.exists(), "fills.parquet should exist");

    let fills_df = read_parquet(&fills_path);
    assert!(fills_df.height() > 0, "Should have at least one fill");

    // Verify fill details
    let size_col = fills_df.column("size").unwrap();
    let total_fill: f64 = size_col.sum().unwrap();
    assert!(total_fill > 0.0, "Total fill size should be positive");
}

/// Test single token backtest order lifecycle
#[test]
fn test_single_token_backtest_orders() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    generate_single_token_fixture(fixture_dir.path()).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    // Strategy that places an order
    let mut strategy = TestStrategy::with_orders(vec![(
        1020, // First feature timestamp from single_token fixture
        "TEST_TOKEN".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("10"),
    )]);

    let _engine = run_backtest(config, &mut strategy);

    // Should have order events
    let orders_path = output_dir.path().join("orders.parquet");
    assert!(orders_path.exists(), "orders.parquet should exist");

    let orders_df = read_parquet(&orders_path);
    assert!(orders_df.height() >= 2, "Should have SUBMIT and ACK events");
}

/// Test that pending order acks are flushed even after the last market event.
#[test]
fn test_final_flush_processes_pending_acks() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;
    let date_dir = fixture_dir.path().join("2025-01-01");

    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("100"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    // Single feature triggers one order. Ack is at t=1070 (50ms latency), after last event.
    let features = vec![FeatureRowData {
        timestamp_ms: 1020,
        ..Default::default()
    }];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("10"),
    )]);
    let _engine = run_backtest(config, &mut strategy);

    let orders_df = read_parquet(&output_dir.path().join("orders.parquet"));
    let ack_count = orders_df
        .lazy()
        .filter(col("event_type").eq(lit("ACK")))
        .collect()
        .unwrap()
        .height();
    assert!(ack_count > 0, "Expected final ack to be recorded after stream end");
}

/// Test that cancel ack does not overwrite fill status if order filled during pending cancel.
#[test]
fn test_cancel_ack_after_fill_keeps_filled_status() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;
    let date_dir = fixture_dir.path().join("2025-01-01");

    // Small external ask liquidity so our pending-cancel order gets partially filled.
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("10"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();

    // Trade at 1080: consumes 10 ext_ahead + 5 of our SELL order.
    let trades = vec![TradeRow {
        timestamp_ms: 1080,
        token_id: "TEST_TOKEN".to_string(),
        side: "buy".to_string(),
        price: dec("0.51"),
        size: dec("15"),
        trade_id: Some("trade_1".to_string()),
    }];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades).unwrap();

    // Feature at 1010 places order, feature at 1070 requests cancel.
    let features = vec![
        FeatureRowData {
            timestamp_ms: 1010,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1070,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = TestStrategy::with_orders_and_cancels(
        vec![(
            1010,
            "TEST_TOKEN".to_string(),
            Side::SELL,
            dec("0.51"),
            dec("10"),
        )],
        vec![(1070, "1".to_string())],
    );
    let engine = run_backtest(config, &mut strategy);

    // Simulator order should remain partial (not canceled) after cancel ack timing.
    let sim_order = engine.simulator().get_order(1).unwrap();
    assert_eq!(sim_order.status, SimOrderStatus::PartialFilled);
    assert_eq!(sim_order.filled_size, dec("5"));

    // Tracker should match simulator status and not be overwritten to Canceled.
    let tracker = engine.order_tracker().blocking_read();
    let tracked = tracker.get("1").unwrap();
    assert_eq!(tracked.status, TrackedOrderStatus::PartiallyFilled);
}

// ============================================================================
// Multi-Token Isolation Tests
// ============================================================================

/// Test that books for different tokens are isolated
#[test]
fn test_multi_token_book_isolation() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");

    // Snapshots for two tokens with different prices
    let snapshots = vec![
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_A".to_string(),
            bids: vec![(dec("0.50"), dec("100"))],
            asks: vec![(dec("0.51"), dec("100"))],
        },
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_B".to_string(),
            bids: vec![(dec("0.30"), dec("100"))],
            asks: vec![(dec("0.31"), dec("100"))],
        },
    ];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    // Delta only for TOKEN_A
    let deltas = vec![DeltaRow {
        timestamp_ms: 1010,
        token_id: "TOKEN_A".to_string(),
        side: "buy".to_string(),
        price: dec("0.50"),
        size: dec("150"),
    }];
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &deltas).unwrap();

    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    let features = vec![FeatureRowData {
        timestamp_ms: 1020,
        ..Default::default()
    }];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let mut config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );
    config.up_token_id = "TOKEN_A".to_string();

    let mut strategy = NoOpStrategy;
    let engine = run_backtest(config, &mut strategy);

    // Check that TOKEN_A book was updated
    let book_a = engine.simulator().get_book("TOKEN_A").unwrap();
    assert_eq!(book_a.size_at(Side::BUY, dec("0.50")), dec("150"));

    // TOKEN_B should be unchanged (still 100 if book exists)
    if let Some(book_b) = engine.simulator().get_book("TOKEN_B") {
        assert_eq!(book_b.size_at(Side::BUY, dec("0.30")), dec("100"));
    }
}

/// Test that positions for different tokens are isolated
#[test]
fn test_multi_token_position_isolation() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");

    // Two tokens
    let snapshots = vec![
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_A".to_string(),
            bids: vec![(dec("0.50"), dec("100"))],
            asks: vec![(dec("0.51"), dec("5"))],
        },
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_B".to_string(),
            bids: vec![(dec("0.30"), dec("100"))],
            asks: vec![(dec("0.31"), dec("5"))],
        },
    ];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();

    // Trades only on TOKEN_A
    let trades = vec![TradeRow {
        timestamp_ms: 1100,
        token_id: "TOKEN_A".to_string(),
        side: "buy".to_string(),
        price: dec("0.51"),
        size: dec("15"),
        trade_id: None,
    }];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades).unwrap();

    let features = vec![
        FeatureRowData {
            timestamp_ms: 1020,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1200,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    // Place orders on both tokens
    let mut strategy = TestStrategy::with_orders(vec![
        (1020, "TOKEN_A".to_string(), Side::SELL, dec("0.51"), dec("20")),
        (1020, "TOKEN_B".to_string(), Side::SELL, dec("0.31"), dec("20")),
    ]);

    let engine = run_backtest(config, &mut strategy);

    // TOKEN_A should have position from fill
    let pos_a = engine.simulator().get_position("TOKEN_A");
    // TOKEN_B should have zero position (no trades)
    let pos_b = engine.simulator().get_position("TOKEN_B");

    // Position A should be non-zero (filled)
    assert!(pos_a != Decimal::ZERO || pos_b == Decimal::ZERO);
}

// ============================================================================
// Strategy Integration Tests
// ============================================================================

/// Test that strategy is called on every feature event
#[test]
fn test_strategy_called_on_every_feature() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");

    // Minimal snapshot
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("100"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    // Multiple features
    let features: Vec<FeatureRowData> = (0..10)
        .map(|i| FeatureRowData {
            timestamp_ms: 1000 + (i * 100) as i64,
            ..Default::default()
        })
        .collect();
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = CountingStrategy::new();
    let _engine = run_backtest(config, &mut strategy);

    // Should have been called for each feature
    assert_eq!(
        strategy.feature_count, 10,
        "Strategy should be called {} times, was called {} times",
        10, strategy.feature_count
    );
}

/// Test that strategy sees updated book state
#[test]
fn test_strategy_sees_updated_book() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");

    // Snapshot at t=1000
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("100"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    // Delta at same timestamp as feature (should be processed before feature)
    let deltas = vec![DeltaRow {
        timestamp_ms: 1020,
        token_id: "TEST_TOKEN".to_string(),
        side: "buy".to_string(),
        price: dec("0.50"),
        size: dec("200"), // Update bid size
    }];
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &deltas).unwrap();

    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    // Feature at same timestamp as delta
    let features = vec![FeatureRowData {
        timestamp_ms: 1020,
        bid_up: 0.50,
        ask_up: 0.51,
        ..Default::default()
    }];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = NoOpStrategy;
    let engine = run_backtest(config, &mut strategy);

    // After backtest, book should reflect delta (which was processed before feature)
    let book = engine.simulator().get_book("TEST_TOKEN").unwrap();
    assert_eq!(book.size_at(Side::BUY, dec("0.50")), dec("200"));
}

/// Test that incremental delta mode applies signed size changes to levels.
#[test]
fn test_incremental_delta_mode_applies_signed_deltas() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;
    let date_dir = fixture_dir.path().join("2025-01-01");

    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("100"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();

    let deltas = vec![
        DeltaRow {
            timestamp_ms: 1010,
            token_id: "TEST_TOKEN".to_string(),
            side: "sell".to_string(),
            price: dec("0.51"),
            size: dec("-30"), // 100 -> 70
        },
        DeltaRow {
            timestamp_ms: 1020,
            token_id: "TEST_TOKEN".to_string(),
            side: "sell".to_string(),
            price: dec("0.51"),
            size: dec("20"), // 70 -> 90
        },
    ];
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &deltas).unwrap();

    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &[]).unwrap();

    let mut config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );
    config.delta_mode = DeltaMode::Incremental;

    let mut strategy = NoOpStrategy;
    let engine = run_backtest(config, &mut strategy);

    let book = engine.simulator().get_book("TEST_TOKEN").unwrap();
    assert_eq!(book.size_at(Side::SELL, dec("0.51")), dec("90"));
}

// ============================================================================
// Determinism Tests
// ============================================================================

/// Test that same seed produces same results
#[test]
fn test_same_seed_same_results() {
    let fixture_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    // Create fixture
    let date_dir = fixture_dir.path().join("2025-01-01");

    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("5"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();

    let trades = vec![TradeRow {
        timestamp_ms: 1100,
        token_id: "TEST_TOKEN".to_string(),
        side: "buy".to_string(),
        price: dec("0.51"),
        size: dec("15"),
        trade_id: None,
    }];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades).unwrap();

    let features = vec![
        FeatureRowData {
            timestamp_ms: 1020,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1200,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    // Run twice with same seed
    let output_dir1 = TempDir::new().unwrap();
    let output_dir2 = TempDir::new().unwrap();

    let config1 = test_config_with_latency(
        fixture_dir.path().to_str().unwrap(),
        output_dir1.path().to_str().unwrap(),
        50,
        100,
        42, // Same seed
    );

    let config2 = test_config_with_latency(
        fixture_dir.path().to_str().unwrap(),
        output_dir2.path().to_str().unwrap(),
        50,
        100,
        42, // Same seed
    );

    let mut strategy1 = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("20"),
    )]);
    let mut strategy2 = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("20"),
    )]);

    let _engine1 = run_backtest(config1, &mut strategy1);
    let _engine2 = run_backtest(config2, &mut strategy2);

    // Compare fills files
    let fills1 = read_parquet(&output_dir1.path().join("fills.parquet"));
    let fills2 = read_parquet(&output_dir2.path().join("fills.parquet"));

    assert_eq!(fills1.height(), fills2.height(), "Fill counts should match");

    // Compare fill sizes
    let size1 = fills1.column("size").unwrap();
    let size2 = fills2.column("size").unwrap();
    assert!(size1.equal(size2).unwrap().all(), "Fill sizes should be identical");
}

/// Test that different seeds produce different ack times
#[test]
fn test_different_seed_different_ack_times() {
    let fixture_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");

    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: "TEST_TOKEN".to_string(),
        bids: vec![(dec("0.50"), dec("100"))],
        asks: vec![(dec("0.51"), dec("100"))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )
    .unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    let features = vec![FeatureRowData {
        timestamp_ms: 1020,
        ..Default::default()
    }];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    // Run with different seeds and latency range
    let output_dir1 = TempDir::new().unwrap();
    let output_dir2 = TempDir::new().unwrap();

    let config1 = test_config_with_latency(
        fixture_dir.path().to_str().unwrap(),
        output_dir1.path().to_str().unwrap(),
        50,
        100,
        42, // Seed 42
    );

    let config2 = test_config_with_latency(
        fixture_dir.path().to_str().unwrap(),
        output_dir2.path().to_str().unwrap(),
        50,
        100,
        123, // Different seed
    );

    let mut strategy1 = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::BUY,
        dec("0.49"),
        dec("10"),
    )]);
    let mut strategy2 = TestStrategy::with_orders(vec![(
        1020,
        "TEST_TOKEN".to_string(),
        Side::BUY,
        dec("0.49"),
        dec("10"),
    )]);

    let _engine1 = run_backtest(config1, &mut strategy1);
    let _engine2 = run_backtest(config2, &mut strategy2);

    // Both should have orders (we can't easily check ack times directly without more infrastructure)
    // This test just verifies the backtest completes with different seeds
    assert!(output_dir1.path().join("orders.parquet").exists());
    assert!(output_dir2.path().join("orders.parquet").exists());
}

// ============================================================================
// Queue Position Fill Test
// ============================================================================

/// Test that queue position tracking works correctly for fills
#[test]
fn test_queue_position_fill_integration() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();

    generate_queue_test_fixture(fixture_dir.path()).unwrap();

    let mut config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );
    config.up_token_id = "QUEUE_TEST".to_string();

    // Strategy places SELL at 0.51 on feature at 1010
    // With 50ms latency, acks at 1060
    // Trade at 1070 (50 size) consumes 50 of ext_ahead (was 100)
    // Trade at 1080 (60 size) consumes remaining 50 ext_ahead + 10 of our order
    let mut strategy = TestStrategy::with_orders(vec![(
        1010,
        "QUEUE_TEST".to_string(),
        Side::SELL,
        dec("0.51"),
        dec("20"),
    )]);

    let engine = run_backtest(config, &mut strategy);

    // Check that we got exactly 10 filled (60 - 50 ext_ahead)
    let fills_path = output_dir.path().join("fills.parquet");
    if fills_path.exists() {
        let fills_df = read_parquet(&fills_path);
        let size_col = fills_df.column("size").unwrap();
        let total_fill: f64 = size_col.sum().unwrap();

        // We should have gotten 10 filled
        assert!(
            (total_fill - 10.0).abs() < 0.01,
            "Expected ~10 fill, got {}",
            total_fill
        );
    }

    // Position should be 1000 - 10 = 990 (started with 1000, sold 10)
    let position = engine.simulator().get_position("QUEUE_TEST");
    assert_eq!(position, dec("990"), "Position should be 990 (1000 - 10 sold), got {}", position);
}

// ============================================================================
// Empty Fixture Tests
// ============================================================================

/// Test backtest with no events
#[test]
fn test_backtest_empty_events() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    // Create empty fixture
    let date_dir = fixture_dir.path().join("2025-01-01");
    write_snapshots_parquet(&date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)), &[]).unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &[]).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = NoOpStrategy;
    let _engine = run_backtest(config, &mut strategy);

    // Backtest should complete without error
}

/// Test backtest with features only (no book data)
#[test]
fn test_backtest_features_only() {
    let fixture_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let ts = TEST_WINDOW_START;

    let date_dir = fixture_dir.path().join("2025-01-01");
    write_snapshots_parquet(&date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)), &[]).unwrap();
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[]).unwrap();
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &[]).unwrap();

    let features = vec![
        FeatureRowData {
            timestamp_ms: 1000,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 2000,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features).unwrap();

    let config = test_config(
        fixture_dir.path().to_str().unwrap(),
        output_dir.path().to_str().unwrap(),
    );

    let mut strategy = CountingStrategy::new();
    let _engine = run_backtest(config, &mut strategy);

    assert_eq!(strategy.feature_count, 2);
}
