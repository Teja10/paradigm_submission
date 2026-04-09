//! Queue position tracking for maker order fills

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};

use super::types::QueueEntry;

/// Key for queue lookup
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueKey {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
}

impl QueueKey {
    pub fn new(token_id: String, side: Side, price: Decimal) -> Self {
        Self {
            token_id,
            side,
            price,
        }
    }
}

/// Queue state at a single price level
#[derive(Debug, Clone, Default)]
pub struct QueueLevel {
    /// External liquidity ahead of all our orders
    pub ext_ahead: Decimal,
    /// Our orders in FIFO order
    pub orders: VecDeque<QueueEntry>,
    /// Trade volume at current timestamp (for avoiding double-counting)
    pub trade_vol_at_ts: Decimal,
    /// Current timestamp for trade volume tracking
    pub current_ts: i64,
}

impl QueueLevel {
    pub fn new() -> Self {
        Self::default()
    }

    /// Total size of our orders at this level
    pub fn our_size(&self) -> Decimal {
        self.orders.iter().map(|e| e.size).sum()
    }

    /// Reset trade volume tracking for new timestamp
    pub fn reset_trade_vol(&mut self, ts: i64) {
        if self.current_ts != ts {
            self.trade_vol_at_ts = Decimal::ZERO;
            self.current_ts = ts;
        }
    }

    /// Add trade volume at this timestamp
    pub fn add_trade_vol(&mut self, ts: i64, vol: Decimal) {
        self.reset_trade_vol(ts);
        self.trade_vol_at_ts += vol;
    }

    /// Get trade volume at timestamp (for double-counting avoidance)
    pub fn get_trade_vol(&self, ts: i64) -> Decimal {
        if self.current_ts == ts {
            self.trade_vol_at_ts
        } else {
            Decimal::ZERO
        }
    }
}

/// Full queue state across all levels
pub struct QueueModel {
    queues: HashMap<QueueKey, QueueLevel>,
    /// Whether new liquidity at a price goes ahead of us
    queue_added_ahead: bool,
}

impl QueueModel {
    pub fn new(queue_added_ahead: bool) -> Self {
        Self {
            queues: HashMap::new(),
            queue_added_ahead,
        }
    }

    /// Get or create a queue level
    pub fn get_or_create(&mut self, key: QueueKey) -> &mut QueueLevel {
        self.queues.entry(key).or_insert_with(QueueLevel::new)
    }

    /// Get a queue level (immutable)
    pub fn get(&self, key: &QueueKey) -> Option<&QueueLevel> {
        self.queues.get(key)
    }

    /// Get a queue level (mutable)
    pub fn get_mut(&mut self, key: &QueueKey) -> Option<&mut QueueLevel> {
        self.queues.get_mut(key)
    }

    /// Remove a specific order from a queue
    pub fn remove_order(&mut self, key: &QueueKey, order_id: u64) {
        if let Some(level) = self.queues.get_mut(key) {
            level.orders.retain(|e| e.order_id != order_id);
        }
    }

    /// Add an order to a queue level
    pub fn add_order(&mut self, key: QueueKey, order_id: u64, size: Decimal, book_size: Decimal) {
        let level = self.get_or_create(key);

        // If this is the first order at this level, set ext_ahead from book
        if level.orders.is_empty() {
            level.ext_ahead = book_size;
        }
        // Otherwise ext_ahead stays the same (we join behind existing external liquidity)

        level.orders.push_back(QueueEntry { order_id, size });
    }

    /// Handle a book size change at a level (from snapshot or delta)
    /// Returns the amount to reduce ext_ahead by (accounting for trades)
    pub fn handle_size_change(
        &mut self,
        key: &QueueKey,
        old_size: Decimal,
        new_size: Decimal,
        timestamp: i64,
    ) {
        let Some(level) = self.queues.get_mut(key) else {
            return;
        };

        if new_size < old_size {
            // Size decreased - could be cancels or trades
            let removed = old_size - new_size;
            let trade_vol = level.get_trade_vol(timestamp);

            // Cancel-like removal = removal not explained by trades
            let cancel_like = (removed - trade_vol).max(Decimal::ZERO);

            // Reduce ext_ahead by cancel-like amount only
            level.ext_ahead = (level.ext_ahead - cancel_like).max(Decimal::ZERO);
        } else if new_size > old_size && self.queue_added_ahead {
            // Size increased and we're in pessimistic mode - new liquidity goes ahead
            let added = new_size - old_size;
            level.ext_ahead += added;
        }
        // If size increased and !queue_added_ahead, ext_ahead unchanged
    }

    /// Update ext_ahead from a trade (before processing fills)
    pub fn record_trade_volume(&mut self, key: &QueueKey, timestamp: i64, trade_size: Decimal) {
        if let Some(level) = self.queues.get_mut(key) {
            level.add_trade_vol(timestamp, trade_size);
        }
    }

    /// Check if we have any orders at a given level
    pub fn has_orders_at(&self, key: &QueueKey) -> bool {
        self.queues
            .get(key)
            .map(|l| !l.orders.is_empty())
            .unwrap_or(false)
    }

    /// Get all queue keys for a token
    pub fn keys_for_token(&self, token_id: &str) -> Vec<QueueKey> {
        self.queues
            .keys()
            .filter(|k| k.token_id == token_id)
            .cloned()
            .collect()
    }
}
