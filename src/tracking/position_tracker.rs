//! Position tracking for inventory and cash across markets

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::TrackedOrder;

/// Position state for a single market
#[derive(Debug, Clone)]
pub struct MarketPosition {
    /// Market condition ID
    pub condition_id: String,
    /// UP token ID
    pub up_token_id: String,
    /// DOWN token ID
    pub down_token_id: String,
    /// Net UP token position (positive = long, negative = short)
    pub up_position: Decimal,
    /// Net DOWN token position (positive = long, negative = short)
    pub down_position: Decimal,
    /// Total cost basis for UP position (sum of price * size for buys)
    pub up_cost: Decimal,
    /// Total cost basis for DOWN position (sum of price * size for buys)
    pub down_cost: Decimal,
}

impl MarketPosition {
    /// Create a new market position with zero inventory
    pub fn new(condition_id: String, up_token_id: String, down_token_id: String) -> Self {
        Self {
            condition_id,
            up_token_id,
            down_token_id,
            up_position: Decimal::ZERO,
            down_position: Decimal::ZERO,
            up_cost: Decimal::ZERO,
            down_cost: Decimal::ZERO,
        }
    }

    /// Get average cost per share for UP token (returns None if no position)
    pub fn up_avg_cost(&self) -> Option<Decimal> {
        if self.up_position > Decimal::ZERO {
            Some(self.up_cost / self.up_position)
        } else {
            None
        }
    }

    /// Get average cost per share for DOWN token (returns None if no position)
    pub fn down_avg_cost(&self) -> Option<Decimal> {
        if self.down_position > Decimal::ZERO {
            Some(self.down_cost / self.down_position)
        } else {
            None
        }
    }
}

/// Tracks positions and cash across markets
pub struct PositionTracker {
    /// Positions by condition_id
    positions: HashMap<String, MarketPosition>,
    /// Reverse lookup: token_id -> condition_id
    token_to_market: HashMap<String, String>,
    /// Cash balance (updated by fills)
    cash: Decimal,
}

impl PositionTracker {
    /// Create a new empty position tracker
    pub fn new() -> Self {
        Self {
            positions: HashMap::new(),
            token_to_market: HashMap::new(),
            cash: Decimal::ZERO,
        }
    }

    /// Set initial cash balance
    pub fn set_cash(&mut self, amount: Decimal) {
        self.cash = amount;
    }

    /// Get current cash balance
    pub fn cash(&self) -> Decimal {
        self.cash
    }

    /// Apply cash delta from a fill (positive = received, negative = spent)
    pub fn apply_cash_delta(&mut self, delta: Decimal) {
        self.cash += delta;
    }

    /// Initialize a market with zero positions
    pub fn init_market(&mut self, condition_id: String, up_token_id: String, down_token_id: String) {
        self.token_to_market.insert(up_token_id.clone(), condition_id.clone());
        self.token_to_market.insert(down_token_id.clone(), condition_id.clone());
        self.positions.insert(
            condition_id.clone(),
            MarketPosition::new(condition_id, up_token_id, down_token_id),
        );
    }


    /// Apply a fill to update position (without price tracking)
    /// BUY adds to position, SELL subtracts
    pub fn apply_fill(&mut self, token_id: &str, side: Side, size: Decimal) {
        self.apply_fill_with_price(token_id, side, size, Decimal::ZERO);
    }

    /// Apply a fill to update position with price tracking for cost basis
    /// BUY adds to position and cost, SELL subtracts from position and cost
    pub fn apply_fill_with_price(
        &mut self,
        token_id: &str,
        side: Side,
        size: Decimal,
        price: Decimal,
    ) {
        let Some(condition_id) = self.token_to_market.get(token_id).cloned() else {
            return;
        };

        let Some(position) = self.positions.get_mut(&condition_id) else {
            return;
        };

        let (pos_delta, cost_delta) = match side {
            Side::BUY => (size, price * size),
            Side::SELL => (-size, -(price * size)),
        };

        if token_id == position.up_token_id {
            position.up_position += pos_delta;
            position.up_cost += cost_delta;
            // Clamp cost to zero if position goes to zero or negative
            if position.up_position <= Decimal::ZERO {
                position.up_cost = Decimal::ZERO;
            }
        } else if token_id == position.down_token_id {
            position.down_position += pos_delta;
            position.down_cost += cost_delta;
            if position.down_position <= Decimal::ZERO {
                position.down_cost = Decimal::ZERO;
            }
        }
    }

    /// Get position for a market by condition_id
    pub fn get_position(&self, condition_id: &str) -> Option<&MarketPosition> {
        self.positions.get(condition_id)
    }

    /// Get position by token_id, returns (position, is_up_token)
    pub fn get_position_by_token(&self, token_id: &str) -> Option<(Decimal, bool)> {
        let condition_id = self.token_to_market.get(token_id)?;
        let position = self.positions.get(condition_id)?;

        if token_id == position.up_token_id {
            Some((position.up_position, true))
        } else if token_id == position.down_token_id {
            Some((position.down_position, false))
        } else {
            None
        }
    }

    /// Set position for a token from API reconciliation
    /// Only updates if the market is already initialized
    pub fn set_position(&mut self, token_id: &str, position: Decimal) {
        let Some(condition_id) = self.token_to_market.get(token_id).cloned() else {
            return;
        };

        let Some(market_pos) = self.positions.get_mut(&condition_id) else {
            return;
        };

        if token_id == market_pos.up_token_id {
            market_pos.up_position = position;
        } else if token_id == market_pos.down_token_id {
            market_pos.down_position = position;
        }
    }

    /// Clear a market's position (on MarketEnd)
    pub fn clear_market(&mut self, condition_id: &str) {
        if let Some(position) = self.positions.remove(condition_id) {
            self.token_to_market.remove(&position.up_token_id);
            self.token_to_market.remove(&position.down_token_id);
        }
    }

    /// Get number of tracked markets
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Check if tracker is empty
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    /// Get available inventory for a token (position minus pending sell orders)
    pub fn available_inventory(&self, token_id: &str, open_orders: &[TrackedOrder]) -> Decimal {
        let position = self
            .get_position_by_token(token_id)
            .map(|(pos, _)| pos)
            .unwrap_or(Decimal::ZERO);

        // Sum up pending sell order sizes for this token
        let pending_sells: Decimal = open_orders
            .iter()
            .filter(|o| o.asset_id == token_id && o.side == Side::SELL)
            .map(|o| o.original_size - o.size_matched)
            .fold(Decimal::ZERO, |acc, size| acc + size);

        (position - pending_sells).max(Decimal::ZERO)
    }

    /// Get available cash (cash minus pending buy order costs)
    pub fn available_cash(&self, open_orders: &[TrackedOrder]) -> Decimal {
        // Sum up pending buy order costs
        let pending_buys: Decimal = open_orders
            .iter()
            .filter(|o| o.side == Side::BUY)
            .map(|o| {
                let remaining = o.original_size - o.size_matched;
                remaining * o.price
            })
            .fold(Decimal::ZERO, |acc, cost| acc + cost);

        (self.cash - pending_buys).max(Decimal::ZERO)
    }
}

impl Default for PositionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared position tracker for concurrent access
pub type SharedPositionTracker = Arc<RwLock<PositionTracker>>;
