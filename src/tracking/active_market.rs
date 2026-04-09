//! Shared active market state
//!
//! Tracks the currently active market (condition_id, token_ids).
//! Updated by the market lifecycle, read by reconciliation and other components.

use std::sync::Arc;
use tokio::sync::RwLock;

/// Active market info
#[derive(Debug, Clone)]
pub struct MarketInfo {
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
}

/// Shared active market state
pub struct ActiveMarket {
    current: Option<MarketInfo>,
}

impl ActiveMarket {
    pub fn new() -> Self {
        Self { current: None }
    }

    /// Set the active market
    pub fn set(&mut self, condition_id: String, up_token_id: String, down_token_id: String) {
        self.current = Some(MarketInfo {
            condition_id,
            up_token_id,
            down_token_id,
        });
    }

    /// Clear the active market
    pub fn clear(&mut self) {
        self.current = None;
    }

    /// Get the current condition_id if set
    pub fn condition_id(&self) -> Option<&str> {
        self.current.as_ref().map(|m| m.condition_id.as_str())
    }

    /// Get the full market info if set
    pub fn get(&self) -> Option<&MarketInfo> {
        self.current.as_ref()
    }

    /// Check if a condition_id matches the active market
    pub fn is_active(&self, condition_id: &str) -> bool {
        self.current
            .as_ref()
            .map(|m| m.condition_id == condition_id)
            .unwrap_or(false)
    }
}

impl Default for ActiveMarket {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared active market for concurrent access
pub type SharedActiveMarket = Arc<RwLock<ActiveMarket>>;
