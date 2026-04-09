//! Backtest configuration

use serde::Deserialize;
use std::path::Path;

use crate::strategies::BasicMmConfig;

/// Delta mode for book reconstruction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaMode {
    #[default]
    Auto,
    Absolute,
    Incremental,
}

/// Backtest configuration
#[derive(Debug, Clone, Deserialize)]
pub struct BacktestConfig {
    /// Window start timestamp (unix seconds)
    pub window_start: i64,
    /// Optional filter for specific token_ids (None = all)
    #[serde(default)]
    pub token_filter: Option<Vec<String>>,
    /// Delta mode for book reconstruction
    #[serde(default)]
    pub delta_mode: DeltaMode,
    /// Whether new liquidity at a price goes ahead of us in queue
    #[serde(default)]
    pub queue_added_ahead: bool,
    /// Minimum latency in milliseconds
    #[serde(default = "default_min_latency")]
    pub latency_min_ms: u64,
    /// Maximum latency in milliseconds
    #[serde(default = "default_max_latency")]
    pub latency_max_ms: u64,
    /// Random seed for determinism
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Parquet files base directory
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// Output directory for results
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    /// Condition ID for the market (required for converting events)
    #[serde(default)]
    pub condition_id: String,
    /// UP token ID
    #[serde(default)]
    pub up_token_id: String,
    /// DOWN token ID
    #[serde(default)]
    pub down_token_id: String,
    /// Initial complete sets to seed (1 set = 1 UP + 1 DOWN)
    #[serde(default)]
    pub initial_complete_sets: u64,
    /// Cost per complete set (default $1.00)
    #[serde(default = "default_complete_set_price")]
    pub complete_set_price: f64,
    /// Initial cash balance (0 = no cash gate)
    #[serde(default)]
    pub initial_cash: f64,
    /// Strategy configuration
    #[serde(default)]
    pub strategy: BasicMmConfig,
}

fn default_min_latency() -> u64 {
    50
}

fn default_max_latency() -> u64 {
    100
}

fn default_seed() -> u64 {
    42
}

fn default_data_dir() -> String {
    "parquet_files".to_string()
}

fn default_output_dir() -> String {
    "backtest_results".to_string()
}

fn default_complete_set_price() -> f64 {
    1.0
}

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            window_start: 0,
            token_filter: None,
            delta_mode: DeltaMode::Auto,
            queue_added_ahead: false,
            latency_min_ms: default_min_latency(),
            latency_max_ms: default_max_latency(),
            seed: default_seed(),
            data_dir: default_data_dir(),
            output_dir: default_output_dir(),
            condition_id: String::new(),
            up_token_id: String::new(),
            down_token_id: String::new(),
            initial_complete_sets: 0,
            complete_set_price: default_complete_set_price(),
            initial_cash: 0.0,
            strategy: BasicMmConfig::default(),
        }
    }
}

impl BacktestConfig {
    /// Load config from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: BacktestConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Check if a token_id should be included
    pub fn should_include_token(&self, token_id: &str) -> bool {
        match &self.token_filter {
            Some(filter) => filter.iter().any(|t| t == token_id),
            None => true,
        }
    }

    /// Get the date string for the window (YYYY-MM-DD)
    pub fn window_date(&self) -> String {
        use chrono::{TimeZone, Utc};
        Utc.timestamp_opt(self.window_start, 0)
            .single()
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_default()
    }

    /// Get the window start timestamp as a string
    pub fn window_start_ts(&self) -> String {
        self.window_start.to_string()
    }
}
