//! Fixture generator for creating test parquet files

use polars::prelude::*;
use rust_decimal::Decimal;
use std::path::Path;

/// Snapshot row for fixture generation
#[derive(Debug, Clone)]
pub struct SnapshotRow {
    pub timestamp_ms: i64,
    pub token_id: String,
    pub bids: Vec<(Decimal, Decimal)>, // (price, size)
    pub asks: Vec<(Decimal, Decimal)>,
}

/// Delta row for fixture generation
#[derive(Debug, Clone)]
pub struct DeltaRow {
    pub timestamp_ms: i64,
    pub token_id: String,
    pub side: String, // "buy" or "sell"
    pub price: Decimal,
    pub size: Decimal,
}

/// Trade row for fixture generation
#[derive(Debug, Clone)]
pub struct TradeRow {
    pub timestamp_ms: i64,
    pub token_id: String,
    pub side: String, // "buy" or "sell" (taker side)
    pub price: Decimal,
    pub size: Decimal,
    pub trade_id: Option<String>,
}

/// Feature row for fixture generation
#[derive(Debug, Clone)]
pub struct FeatureRowData {
    pub timestamp_ms: i64,
    pub tau_secs: f64,
    pub oracle_price: f64,
    pub reference_price: f64,
    pub fair_up: f64,
    pub sigma: f64,
    pub coinbase_mid: f64,
    pub coinbase_microprice: f64,
    pub coinbase_spread: f64,
    pub coinbase_imb_1: f64,
    pub coinbase_imb_10: f64,
    pub coinbase_imb_20: f64,
    pub coinbase_imb_50: f64,
    pub coinbase_imb_100: f64,
    pub coinbase_liq_1bp: f64,
    pub coinbase_liq_2bp: f64,
    pub delta_microprice_1s: Option<f64>,
    pub delta_microprice_2s: Option<f64>,
    pub delta_microprice_5s: Option<f64>,
    pub delta_imb_1_1s: Option<f64>,
    pub delta_imb_1_2s: Option<f64>,
    pub delta_imb_1_5s: Option<f64>,
    pub bid_up: f64,
    pub ask_up: f64,
    pub bid_down: f64,
    pub ask_down: f64,
    pub up_mid: f64,
    pub blended_price: f64,
    pub basis: f64,
    pub blend_weight: f64,
    pub sigma_dyn: f64,
    pub ewma_variance: f64,
    pub alpha: f64,
}

impl Default for FeatureRowData {
    fn default() -> Self {
        Self {
            timestamp_ms: 0,
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
        }
    }
}

/// Write snapshots to a parquet file
pub fn write_snapshots_parquet(path: &Path, rows: &[SnapshotRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(path.parent().unwrap())?;

    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let token_ids: Vec<&str> = rows.iter().map(|r| r.token_id.as_str()).collect();

    // Build bid/ask level columns
    let mut columns: Vec<Series> = vec![
        Series::new("timestamp_ms".into(), &timestamps),
        Series::new("token_id".into(), &token_ids),
    ];

    // Add 20 bid levels
    for i in 0..20 {
        let prices: Vec<String> = rows
            .iter()
            .map(|r| {
                r.bids
                    .get(i)
                    .map(|(p, _)| p.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let sizes: Vec<String> = rows
            .iter()
            .map(|r| {
                r.bids
                    .get(i)
                    .map(|(_, s)| s.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let price_col_name = format!("bid_{}_price", i);
        let size_col_name = format!("bid_{}_size", i);
        columns.push(Series::new((&*price_col_name).into(), &prices));
        columns.push(Series::new((&*size_col_name).into(), &sizes));
    }

    // Add 20 ask levels
    for i in 0..20 {
        let prices: Vec<String> = rows
            .iter()
            .map(|r| {
                r.asks
                    .get(i)
                    .map(|(p, _)| p.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let sizes: Vec<String> = rows
            .iter()
            .map(|r| {
                r.asks
                    .get(i)
                    .map(|(_, s)| s.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let price_col_name = format!("ask_{}_price", i);
        let size_col_name = format!("ask_{}_size", i);
        columns.push(Series::new((&*price_col_name).into(), &prices));
        columns.push(Series::new((&*size_col_name).into(), &sizes));
    }

    let mut df = DataFrame::new(columns)?;

    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}

/// Write deltas to a parquet file
pub fn write_deltas_parquet(path: &Path, rows: &[DeltaRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(path.parent().unwrap())?;

    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let token_ids: Vec<&str> = rows.iter().map(|r| r.token_id.as_str()).collect();
    let sides: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    let prices: Vec<String> = rows.iter().map(|r| r.price.to_string()).collect();
    let sizes: Vec<String> = rows.iter().map(|r| r.size.to_string()).collect();

    let mut df = df! {
        "timestamp_ms" => &timestamps,
        "token_id" => &token_ids,
        "side" => &sides,
        "price" => &prices,
        "size" => &sizes,
    }?;

    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}

/// Write trades to a parquet file
pub fn write_trades_parquet(path: &Path, rows: &[TradeRow]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(path.parent().unwrap())?;

    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let token_ids: Vec<&str> = rows.iter().map(|r| r.token_id.as_str()).collect();
    let sides: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    let prices: Vec<String> = rows.iter().map(|r| r.price.to_string()).collect();
    let sizes: Vec<String> = rows.iter().map(|r| r.size.to_string()).collect();
    let trade_ids: Vec<Option<&str>> = rows
        .iter()
        .map(|r| r.trade_id.as_ref().map(|s| s.as_str()))
        .collect();

    let mut df = df! {
        "timestamp_ms" => &timestamps,
        "token_id" => &token_ids,
        "side" => &sides,
        "price" => &prices,
        "size" => &sizes,
        "trade_id" => &trade_ids,
    }?;

    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}

/// Write features to a parquet file
pub fn write_features_parquet(path: &Path, rows: &[FeatureRowData]) -> anyhow::Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(path.parent().unwrap())?;

    let timestamps: Vec<i64> = rows.iter().map(|r| r.timestamp_ms).collect();
    let tau_secs: Vec<f64> = rows.iter().map(|r| r.tau_secs).collect();
    let oracle_price: Vec<f64> = rows.iter().map(|r| r.oracle_price).collect();
    let reference_price: Vec<f64> = rows.iter().map(|r| r.reference_price).collect();
    let fair_up: Vec<f64> = rows.iter().map(|r| r.fair_up).collect();
    let sigma: Vec<f64> = rows.iter().map(|r| r.sigma).collect();
    let coinbase_mid: Vec<f64> = rows.iter().map(|r| r.coinbase_mid).collect();
    let coinbase_microprice: Vec<f64> = rows.iter().map(|r| r.coinbase_microprice).collect();
    let coinbase_spread: Vec<f64> = rows.iter().map(|r| r.coinbase_spread).collect();
    let coinbase_imb_1: Vec<f64> = rows.iter().map(|r| r.coinbase_imb_1).collect();
    let coinbase_imb_10: Vec<f64> = rows.iter().map(|r| r.coinbase_imb_10).collect();
    let coinbase_imb_20: Vec<f64> = rows.iter().map(|r| r.coinbase_imb_20).collect();
    let coinbase_imb_50: Vec<f64> = rows.iter().map(|r| r.coinbase_imb_50).collect();
    let coinbase_imb_100: Vec<f64> = rows.iter().map(|r| r.coinbase_imb_100).collect();
    let coinbase_liq_1bp: Vec<f64> = rows.iter().map(|r| r.coinbase_liq_1bp).collect();
    let coinbase_liq_2bp: Vec<f64> = rows.iter().map(|r| r.coinbase_liq_2bp).collect();
    let delta_microprice_1s: Vec<Option<f64>> =
        rows.iter().map(|r| r.delta_microprice_1s).collect();
    let delta_microprice_2s: Vec<Option<f64>> =
        rows.iter().map(|r| r.delta_microprice_2s).collect();
    let delta_microprice_5s: Vec<Option<f64>> =
        rows.iter().map(|r| r.delta_microprice_5s).collect();
    let delta_imb_1_1s: Vec<Option<f64>> = rows.iter().map(|r| r.delta_imb_1_1s).collect();
    let delta_imb_1_2s: Vec<Option<f64>> = rows.iter().map(|r| r.delta_imb_1_2s).collect();
    let delta_imb_1_5s: Vec<Option<f64>> = rows.iter().map(|r| r.delta_imb_1_5s).collect();
    let bid_up: Vec<f64> = rows.iter().map(|r| r.bid_up).collect();
    let ask_up: Vec<f64> = rows.iter().map(|r| r.ask_up).collect();
    let bid_down: Vec<f64> = rows.iter().map(|r| r.bid_down).collect();
    let ask_down: Vec<f64> = rows.iter().map(|r| r.ask_down).collect();
    let up_mid: Vec<f64> = rows.iter().map(|r| r.up_mid).collect();
    let blended_price: Vec<f64> = rows.iter().map(|r| r.blended_price).collect();
    let basis: Vec<f64> = rows.iter().map(|r| r.basis).collect();
    let blend_weight: Vec<f64> = rows.iter().map(|r| r.blend_weight).collect();
    let sigma_dyn: Vec<f64> = rows.iter().map(|r| r.sigma_dyn).collect();
    let ewma_variance: Vec<f64> = rows.iter().map(|r| r.ewma_variance).collect();
    let alpha: Vec<f64> = rows.iter().map(|r| r.alpha).collect();

    let mut df = df! {
        "timestamp_ms" => &timestamps,
        "tau_secs" => &tau_secs,
        "oracle_price" => &oracle_price,
        "reference_price" => &reference_price,
        "fair_up" => &fair_up,
        "sigma" => &sigma,
        "coinbase_mid" => &coinbase_mid,
        "coinbase_microprice" => &coinbase_microprice,
        "coinbase_spread" => &coinbase_spread,
        "coinbase_imb_1" => &coinbase_imb_1,
        "coinbase_imb_10" => &coinbase_imb_10,
        "coinbase_imb_20" => &coinbase_imb_20,
        "coinbase_imb_50" => &coinbase_imb_50,
        "coinbase_imb_100" => &coinbase_imb_100,
        "coinbase_liq_1bp" => &coinbase_liq_1bp,
        "coinbase_liq_2bp" => &coinbase_liq_2bp,
        "delta_microprice_1s" => &delta_microprice_1s,
        "delta_microprice_2s" => &delta_microprice_2s,
        "delta_microprice_5s" => &delta_microprice_5s,
        "delta_imb_1_1s" => &delta_imb_1_1s,
        "delta_imb_1_2s" => &delta_imb_1_2s,
        "delta_imb_1_5s" => &delta_imb_1_5s,
        "bid_up" => &bid_up,
        "ask_up" => &ask_up,
        "bid_down" => &bid_down,
        "ask_down" => &ask_down,
        "up_mid" => &up_mid,
        "blended_price" => &blended_price,
        "basis" => &basis,
        "blend_weight" => &blend_weight,
        "sigma_dyn" => &sigma_dyn,
        "ewma_variance" => &ewma_variance,
        "alpha" => &alpha,
    }?;

    let file = std::fs::File::create(path)?;
    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}

/// Test window timestamp (2025-01-01 00:00:00 UTC)
pub const TEST_WINDOW_START: i64 = 1735689600;

/// Generate a single-token test fixture
pub fn generate_single_token_fixture(base_dir: &Path) -> anyhow::Result<()> {
    let date = "2025-01-01";
    let token_id = "TEST_TOKEN";
    let ts = TEST_WINDOW_START;

    // Create directory structure
    let date_dir = base_dir.join(date);

    // Snapshots
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: token_id.to_string(),
        bids: vec![
            (Decimal::new(50, 2), Decimal::new(100, 0)),
            (Decimal::new(49, 2), Decimal::new(200, 0)),
        ],
        asks: vec![
            (Decimal::new(51, 2), Decimal::new(100, 0)),
            (Decimal::new(52, 2), Decimal::new(200, 0)),
        ],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )?;

    // Deltas
    let deltas = vec![
        DeltaRow {
            timestamp_ms: 1010,
            token_id: token_id.to_string(),
            side: "buy".to_string(),
            price: Decimal::new(50, 2),
            size: Decimal::new(150, 0),
        },
        DeltaRow {
            timestamp_ms: 1030,
            token_id: token_id.to_string(),
            side: "sell".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(80, 0),
        },
    ];
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &deltas)?;

    // Trades
    let trades = vec![
        TradeRow {
            timestamp_ms: 1040,
            token_id: token_id.to_string(),
            side: "buy".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(30, 0),
            trade_id: Some("trade_1".to_string()),
        },
        TradeRow {
            timestamp_ms: 1050,
            token_id: token_id.to_string(),
            side: "buy".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(60, 0),
            trade_id: Some("trade_2".to_string()),
        },
    ];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades)?;

    // Features (strategy triggers)
    let features = vec![
        FeatureRowData {
            timestamp_ms: 1020,
            bid_up: 0.50,
            ask_up: 0.51,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1100,
            bid_up: 0.50,
            ask_up: 0.51,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features)?;

    Ok(())
}

/// Generate a multi-token test fixture
pub fn generate_multi_token_fixture(base_dir: &Path) -> anyhow::Result<()> {
    let date = "2025-01-01";
    let ts = TEST_WINDOW_START;

    let date_dir = base_dir.join(date);

    // Snapshots for both tokens
    let snapshots = vec![
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_A".to_string(),
            bids: vec![(Decimal::new(50, 2), Decimal::new(100, 0))],
            asks: vec![(Decimal::new(51, 2), Decimal::new(100, 0))],
        },
        SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TOKEN_B".to_string(),
            bids: vec![(Decimal::new(45, 2), Decimal::new(50, 0))],
            asks: vec![(Decimal::new(46, 2), Decimal::new(50, 0))],
        },
    ];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )?;

    // Deltas for both tokens (interleaved)
    let deltas = vec![
        DeltaRow {
            timestamp_ms: 1010,
            token_id: "TOKEN_A".to_string(),
            side: "buy".to_string(),
            price: Decimal::new(50, 2),
            size: Decimal::new(150, 0),
        },
        DeltaRow {
            timestamp_ms: 1015,
            token_id: "TOKEN_B".to_string(),
            side: "sell".to_string(),
            price: Decimal::new(46, 2),
            size: Decimal::new(75, 0),
        },
    ];
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &deltas)?;

    // Trades
    let trades = vec![
        TradeRow {
            timestamp_ms: 1100,
            token_id: "TOKEN_A".to_string(),
            side: "buy".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(50, 0),
            trade_id: Some("trade_a_1".to_string()),
        },
        TradeRow {
            timestamp_ms: 1100,
            token_id: "TOKEN_B".to_string(),
            side: "sell".to_string(),
            price: Decimal::new(45, 2),
            size: Decimal::new(25, 0),
            trade_id: Some("trade_b_1".to_string()),
        },
    ];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades)?;

    // Features
    let features = vec![
        FeatureRowData {
            timestamp_ms: 1020,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1120,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features)?;

    Ok(())
}

/// Generate queue-specific test fixture for fill testing
pub fn generate_queue_test_fixture(base_dir: &Path) -> anyhow::Result<()> {
    let date = "2025-01-01";
    let token_id = "QUEUE_TEST";
    let ts = TEST_WINDOW_START;

    let date_dir = base_dir.join(date);

    // Initial snapshot with ask at 0.51 size 100
    let snapshots = vec![SnapshotRow {
        timestamp_ms: 1000,
        token_id: token_id.to_string(),
        bids: vec![(Decimal::new(50, 2), Decimal::new(100, 0))],
        asks: vec![(Decimal::new(51, 2), Decimal::new(100, 0))],
    }];
    write_snapshots_parquet(
        &date_dir.join(format!("orderbooks/snapshots_{}.parquet", ts)),
        &snapshots,
    )?;

    // No deltas needed for this test
    write_deltas_parquet(&date_dir.join(format!("orderbooks/deltas_{}.parquet", ts)), &[])?;

    // Trades that will consume external liquidity and then fill our order
    let trades = vec![
        TradeRow {
            timestamp_ms: 1070,
            token_id: token_id.to_string(),
            side: "buy".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(50, 0),
            trade_id: Some("trade_1".to_string()),
        },
        TradeRow {
            timestamp_ms: 1080,
            token_id: token_id.to_string(),
            side: "buy".to_string(),
            price: Decimal::new(51, 2),
            size: Decimal::new(60, 0),
            trade_id: Some("trade_2".to_string()),
        },
    ];
    write_trades_parquet(&date_dir.join(format!("trades/trades_{}.parquet", ts)), &trades)?;

    // Feature at 1010 triggers strategy to place order
    // With 50ms latency, order acks at 1060, before trades at 1070/1080
    let features = vec![
        FeatureRowData {
            timestamp_ms: 1010,
            bid_up: 0.50,
            ask_up: 0.51,
            ..Default::default()
        },
        FeatureRowData {
            timestamp_ms: 1100,
            bid_up: 0.50,
            ask_up: 0.51,
            ..Default::default()
        },
    ];
    write_features_parquet(&date_dir.join(format!("features/data_{}.parquet", ts)), &features)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_write_snapshots_parquet() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("snapshots.parquet");

        let rows = vec![SnapshotRow {
            timestamp_ms: 1000,
            token_id: "TEST".to_string(),
            bids: vec![(Decimal::new(50, 2), Decimal::new(100, 0))],
            asks: vec![(Decimal::new(51, 2), Decimal::new(100, 0))],
        }];

        write_snapshots_parquet(&path, &rows).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_write_deltas_parquet() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("deltas.parquet");

        let rows = vec![DeltaRow {
            timestamp_ms: 1000,
            token_id: "TEST".to_string(),
            side: "buy".to_string(),
            price: Decimal::new(50, 2),
            size: Decimal::new(100, 0),
        }];

        write_deltas_parquet(&path, &rows).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_write_trades_parquet() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("trades.parquet");

        let rows = vec![TradeRow {
            timestamp_ms: 1000,
            token_id: "TEST".to_string(),
            side: "buy".to_string(),
            price: Decimal::new(50, 2),
            size: Decimal::new(10, 0),
            trade_id: Some("trade_1".to_string()),
        }];

        write_trades_parquet(&path, &rows).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_write_features_parquet() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("features.parquet");

        let rows = vec![FeatureRowData {
            timestamp_ms: 1000,
            ..Default::default()
        }];

        write_features_parquet(&path, &rows).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_generate_single_token_fixture() {
        let tmp = TempDir::new().unwrap();
        generate_single_token_fixture(tmp.path()).unwrap();
        let ts = TEST_WINDOW_START;

        assert!(tmp
            .path()
            .join(format!("2025-01-01/orderbooks/snapshots_{}.parquet", ts))
            .exists());
        assert!(tmp
            .path()
            .join(format!("2025-01-01/orderbooks/deltas_{}.parquet", ts))
            .exists());
        assert!(tmp
            .path()
            .join(format!("2025-01-01/trades/trades_{}.parquet", ts))
            .exists());
        assert!(tmp
            .path()
            .join(format!("2025-01-01/features/data_{}.parquet", ts))
            .exists());
    }
}
