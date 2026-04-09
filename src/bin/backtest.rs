//! Backtest CLI entry point

use anyhow::Result;
use polymarket_mm::backtest::{BacktestConfig, BacktestEngine, EventLoader};
use polymarket_mm::strategies::BasicMarketMaker;
use std::env;

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();

    let config = if args.len() > 1 {
        // Load config from file
        let config_path = &args[1];
        tracing::info!("Loading config from: {}", config_path);
        BacktestConfig::from_file(config_path)?
    } else {
        // Use default config for testing
        tracing::info!("No config file specified, using defaults");
        tracing::info!("Usage: backtest <config.toml>");
        BacktestConfig::default()
    };

    tracing::info!(
        "Running backtest for window {} ({})",
        config.window_start,
        config.window_date()
    );
    tracing::info!("Data directory: {}", config.data_dir);
    tracing::info!("Output directory: {}", config.output_dir);
    tracing::info!(
        "Latency: {}ms - {}ms",
        config.latency_min_ms,
        config.latency_max_ms
    );
    tracing::info!("Seed: {}", config.seed);

    // Load events
    let loader = EventLoader::new(config.clone());
    let events = loader.load_events()?;

    // Create backtest engine so we can pass its trackers into the strategy
    let strategy_config = config.strategy.clone();
    let mut engine = BacktestEngine::new(config);
    let order_tracker = engine.order_tracker().clone();
    let position_tracker = engine.position_tracker().clone();

    let mut strategy = BasicMarketMaker::new(strategy_config, order_tracker, position_tracker);

    // Run backtest
    engine.run(&mut strategy, events)?;

    tracing::info!("Backtest complete!");

    Ok(())
}
