//! Order tracking for submitted orders and their lifecycle

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Order status aligned with Polymarket API values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackedOrderStatus {
    /// Resting on book (status="live" or type=PLACEMENT)
    Live,
    /// Immediately matched on submission (status="matched")
    Matched,
    /// Has fills but still resting (type=UPDATE)
    PartiallyFilled,
    /// Fully filled
    Filled,
    /// User canceled (type=CANCELLATION)
    Canceled,
    /// Exchange rejected (errorMsg present)
    Rejected,
}

/// A tracked order with its current state
#[derive(Debug, Clone)]
pub struct TrackedOrder {
    /// Unique order ID from exchange
    pub order_id: String,
    /// Token ID (asset_id)
    pub asset_id: String,
    /// Market condition ID
    pub market: String,
    /// Order side
    pub side: Side,
    /// Limit price
    pub price: Decimal,
    /// Original order size
    pub original_size: Decimal,
    /// Amount filled (Polymarket field name)
    pub size_matched: Decimal,
    /// Current status
    pub status: TrackedOrderStatus,
    /// When order was created
    pub created_at: DateTime<Utc>,
    /// Last update time
    pub updated_at: DateTime<Utc>,
}

/// Tracks submitted orders and their lifecycle
pub struct OrderTracker {
    orders: HashMap<String, TrackedOrder>,
}

impl OrderTracker {
    /// Create a new empty order tracker
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
        }
    }

    /// Insert a new order
    pub fn insert(&mut self, order: TrackedOrder) {
        self.orders.insert(order.order_id.clone(), order);
    }

    /// Get order by ID
    pub fn get(&self, order_id: &str) -> Option<&TrackedOrder> {
        self.orders.get(order_id)
    }

    /// Get mutable reference to order by ID
    pub fn get_mut(&mut self, order_id: &str) -> Option<&mut TrackedOrder> {
        self.orders.get_mut(order_id)
    }

    /// Update order status
    pub fn update_status(&mut self, order_id: &str, status: TrackedOrderStatus) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.status = status;
            order.updated_at = Utc::now();
        }
    }

    /// Update from WebSocket ORDER message
    pub fn update_from_order_msg(
        &mut self,
        order_id: &str,
        msg_type: &str,
        size_matched: Decimal,
    ) -> Result<()> {
        let order = self
            .orders
            .get_mut(order_id)
            .ok_or_else(|| anyhow!("Unknown order_id in update_from_order_msg: {}", order_id))?;

        order.size_matched = size_matched;
        order.updated_at = Utc::now();

        let msg_type = msg_type.to_uppercase();
        order.status = match msg_type.as_str() {
            "PLACEMENT" => {
                if size_matched >= order.original_size {
                    TrackedOrderStatus::Filled
                } else if size_matched > Decimal::ZERO {
                    TrackedOrderStatus::PartiallyFilled
                } else {
                    TrackedOrderStatus::Live
                }
            }
            "UPDATE" => {
                if size_matched >= order.original_size {
                    TrackedOrderStatus::Filled
                } else {
                    TrackedOrderStatus::PartiallyFilled
                }
            }
            "CANCELLATION" => TrackedOrderStatus::Canceled,
            _ => {
                return Err(anyhow!("Unknown order msg_type: {}", msg_type));
            }
        };

        Ok(())
    }

    /// Apply a fill (increment size_matched)
    pub fn apply_fill(&mut self, order_id: &str, fill_size: Decimal) {
        if let Some(order) = self.orders.get_mut(order_id) {
            order.size_matched += fill_size;
            order.updated_at = Utc::now();

            if order.size_matched >= order.original_size {
                order.status = TrackedOrderStatus::Filled;
            } else {
                order.status = TrackedOrderStatus::PartiallyFilled;
            }
        }
    }

    /// Mark order as rejected
    pub fn mark_rejected(&mut self, order_id: &str) {
        self.update_status(order_id, TrackedOrderStatus::Rejected);
    }

    /// Get all open order IDs
    pub fn open_order_ids(&self) -> Vec<String> {
        self.orders
            .values()
            .filter(|o| {
                matches!(
                    o.status,
                    TrackedOrderStatus::Live | TrackedOrderStatus::PartiallyFilled
                )
            })
            .map(|o| o.order_id.clone())
            .collect()
    }

    /// Get all open orders (Live or PartiallyFilled)
    pub fn open_orders(&self) -> Vec<TrackedOrder> {
        self.orders
            .values()
            .filter(|o| {
                matches!(
                    o.status,
                    TrackedOrderStatus::Live | TrackedOrderStatus::PartiallyFilled
                )
            })
            .cloned()
            .collect()
    }

    /// Clear all orders for a market (called on MarketEnd)
    pub fn clear_market(&mut self, market: &str) {
        self.orders.retain(|_, order| order.market != market);
    }

    /// Sync orders from API response
    /// Replaces all orders with the API state (source of truth)
    pub fn sync_from_api(&mut self, api_orders: Vec<TrackedOrder>) {
        self.orders.clear();
        for order in api_orders {
            self.orders.insert(order.order_id.clone(), order);
        }
    }

    /// Sync API orders for a single market while preserving other markets.
    pub fn sync_market_from_api(&mut self, market: &str, api_orders: Vec<TrackedOrder>) {
        self.orders.retain(|_, order| order.market != market);
        for order in api_orders {
            self.orders.insert(order.order_id.clone(), order);
        }
    }

    /// Get number of tracked orders
    pub fn len(&self) -> usize {
        self.orders.len()
    }

    /// Check if tracker is empty
    pub fn is_empty(&self) -> bool {
        self.orders.is_empty()
    }
}

impl Default for OrderTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared order tracker for concurrent access
pub type SharedOrderTracker = Arc<RwLock<OrderTracker>>;
