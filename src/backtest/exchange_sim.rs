//! Exchange simulator with L2 book and order management

use polyfill_rs::Side;
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

use super::queue_model::{QueueKey, QueueModel};
use super::types::{Fill, SimOrder, SimOrderStatus};

/// Represents an inferred book change that could trigger a crossing fill
#[derive(Debug, Clone)]
pub struct ImpliedDelta {
    pub side: Side,
    pub price: Decimal,
    pub size_added: Decimal,
}

/// For a single delta: compute if new liquidity was added
pub fn implied_delta_from_delta(
    prev_size: Decimal,
    new_size: Decimal,
    side: Side,
    price: Decimal,
) -> Option<ImpliedDelta> {
    let size_added = new_size - prev_size;
    if size_added > Decimal::ZERO {
        Some(ImpliedDelta {
            side,
            price,
            size_added,
        })
    } else {
        None
    }
}

/// For a snapshot: diff current book vs new snapshot to get all implied deltas
pub fn implied_deltas_from_snapshot(
    current_book: &L2Book,
    new_bids: &[(Decimal, Decimal)],
    new_asks: &[(Decimal, Decimal)],
) -> Vec<ImpliedDelta> {
    let mut deltas = Vec::new();

    // Check bid side
    for (price, new_size) in new_bids {
        let prev_size = current_book
            .bids
            .get(price)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let size_added = *new_size - prev_size;
        if size_added > Decimal::ZERO {
            deltas.push(ImpliedDelta {
                side: Side::BUY,
                price: *price,
                size_added,
            });
        }
    }

    // Check ask side
    for (price, new_size) in new_asks {
        let prev_size = current_book
            .asks
            .get(price)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let size_added = *new_size - prev_size;
        if size_added > Decimal::ZERO {
            deltas.push(ImpliedDelta {
                side: Side::SELL,
                price: *price,
                size_added,
            });
        }
    }

    deltas
}

/// L2 order book for a single token
#[derive(Debug, Clone, Default)]
pub struct L2Book {
    /// Bids: price -> size (sorted descending, best bid = highest price)
    pub bids: BTreeMap<Decimal, Decimal>,
    /// Asks: price -> size (sorted ascending, best ask = lowest price)
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl L2Book {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, s)| (*p, *s))
    }

    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, s)| (*p, *s))
    }

    pub fn mid(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => Some((b + a) / Decimal::TWO),
            _ => None,
        }
    }

    pub fn size_at(&self, side: Side, price: Decimal) -> Decimal {
        match side {
            Side::BUY => self.bids.get(&price).copied().unwrap_or(Decimal::ZERO),
            Side::SELL => self.asks.get(&price).copied().unwrap_or(Decimal::ZERO),
        }
    }

    /// Apply a snapshot (overwrites all levels)
    pub fn apply_snapshot(
        &mut self,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
    ) -> Vec<(Side, Decimal, Decimal, Decimal)> {
        // Track changes: (side, price, old_size, new_size)
        let mut changes = Vec::new();

        // Record old bid levels that will be removed
        for (&price, &old_size) in &self.bids {
            changes.push((Side::BUY, price, old_size, Decimal::ZERO));
        }

        // Record old ask levels that will be removed
        for (&price, &old_size) in &self.asks {
            changes.push((Side::SELL, price, old_size, Decimal::ZERO));
        }

        self.bids.clear();
        self.asks.clear();

        for &(price, size) in bids {
            if size > Decimal::ZERO {
                self.bids.insert(price, size);
                // Find and update the change record, or add a new one
                if let Some(c) = changes
                    .iter_mut()
                    .find(|c| c.0 == Side::BUY && c.1 == price)
                {
                    c.3 = size;
                } else {
                    changes.push((Side::BUY, price, Decimal::ZERO, size));
                }
            }
        }

        for &(price, size) in asks {
            if size > Decimal::ZERO {
                self.asks.insert(price, size);
                if let Some(c) = changes
                    .iter_mut()
                    .find(|c| c.0 == Side::SELL && c.1 == price)
                {
                    c.3 = size;
                } else {
                    changes.push((Side::SELL, price, Decimal::ZERO, size));
                }
            }
        }

        // Filter to only actual changes
        changes.retain(|(_, _, old, new)| old != new);
        changes
    }

    /// Apply a delta (single level update)
    pub fn apply_delta(&mut self, side: Side, price: Decimal, size: Decimal) -> (Decimal, Decimal) {
        let book = match side {
            Side::BUY => &mut self.bids,
            Side::SELL => &mut self.asks,
        };

        let old_size = book.get(&price).copied().unwrap_or(Decimal::ZERO);

        if size == Decimal::ZERO {
            book.remove(&price);
        } else {
            book.insert(price, size);
        }

        (old_size, size)
    }
}

/// Exchange simulator managing books, orders, and fills
pub struct ExchangeSimulator {
    /// L2 books by token_id
    books: HashMap<String, L2Book>,
    /// All orders by order_id
    orders: HashMap<u64, SimOrder>,
    /// Queue model for fill simulation
    pub queues: QueueModel,
    /// Position by token_id
    positions: HashMap<String, Decimal>,
    /// Current cash balance
    cash: Decimal,
    /// Reserved cash for live BUY orders (order_id -> reserved amount)
    reserved_cash: HashMap<u64, Decimal>,
    /// Reserved inventory for live SELL orders (order_id -> (token_id, size))
    reserved_inventory: HashMap<u64, (String, Decimal)>,
    /// Next order ID
    next_order_id: u64,
    /// Next fill ID
    next_fill_id: u64,
}

impl ExchangeSimulator {
    pub fn new(queue_added_ahead: bool) -> Self {
        Self {
            books: HashMap::new(),
            orders: HashMap::new(),
            queues: QueueModel::new(queue_added_ahead),
            positions: HashMap::new(),
            cash: Decimal::ZERO,
            reserved_cash: HashMap::new(),
            reserved_inventory: HashMap::new(),
            next_order_id: 1,
            next_fill_id: 1,
        }
    }

    pub fn get_book(&self, token_id: &str) -> Option<&L2Book> {
        self.books.get(token_id)
    }

    pub fn get_book_mut(&mut self, token_id: &str) -> &mut L2Book {
        self.books.entry(token_id.to_string()).or_default()
    }

    pub fn get_order(&self, order_id: u64) -> Option<&SimOrder> {
        self.orders.get(&order_id)
    }

    pub fn get_order_mut(&mut self, order_id: u64) -> Option<&mut SimOrder> {
        self.orders.get_mut(&order_id)
    }

    pub fn get_position(&self, token_id: &str) -> Decimal {
        self.positions
            .get(token_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub fn set_initial_cash(&mut self, cash: Decimal) {
        self.cash = cash;
    }

    fn reserved_total(&self) -> Decimal {
        self.reserved_cash
            .values()
            .copied()
            .fold(Decimal::ZERO, |acc, v| acc + v)
    }

    fn available_cash(&self) -> Decimal {
        self.cash - self.reserved_total()
    }

    fn reserved_inventory_for_token(&self, token_id: &str) -> Decimal {
        self.reserved_inventory
            .values()
            .filter(|(tid, _)| tid == token_id)
            .map(|(_, size)| *size)
            .fold(Decimal::ZERO, |acc, v| acc + v)
    }

    fn available_inventory(&self, token_id: &str) -> Decimal {
        let position = self
            .positions
            .get(token_id)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let reserved = self.reserved_inventory_for_token(token_id);
        position - reserved
    }

    pub fn get_all_positions(&self) -> &HashMap<String, Decimal> {
        &self.positions
    }

    pub fn get_open_orders(&self) -> Vec<&SimOrder> {
        self.orders.values().filter(|o| o.is_open()).collect()
    }

    /// Available cash (net of reservations) — for taker order validation
    pub fn available_cash_for_taker(&self) -> Decimal {
        self.available_cash()
    }

    /// Available inventory for a token (net of reservations) — for taker order validation
    pub fn available_inventory_for_taker(&self, token_id: &str) -> Decimal {
        self.available_inventory(token_id)
    }

    /// Seed initial inventory from complete sets (1 UP + 1 DOWN) and record cash outflow.
    pub fn seed_complete_sets(
        &mut self,
        up_token_id: &str,
        down_token_id: &str,
        sets: Decimal,
        cost_per_set: Decimal,
    ) {
        if sets <= Decimal::ZERO {
            return;
        }
        *self.positions.entry(up_token_id.to_string()).or_default() += sets;
        *self.positions.entry(down_token_id.to_string()).or_default() += sets;
        self.cash -= cost_per_set * sets;
    }

    /// Submit a new order (returns order_id)
    pub fn submit_order(&mut self, mut order: SimOrder) -> u64 {
        let id = self.next_order_id;
        self.next_order_id += 1;
        order.order_id = id;
        order.status = SimOrderStatus::PendingNew;
        self.orders.insert(id, order);
        id
    }

    /// Process order ack - returns Ok(()) if accepted, Err(reason) if rejected
    pub fn process_order_ack(&mut self, order_id: u64) -> Result<(), String> {
        let (side, price, size, status, post_only, token_id) = {
            let order = self.orders.get(&order_id).ok_or("Order not found")?;
            (
                order.side,
                order.price,
                order.original_size,
                order.status,
                order.post_only,
                order.token_id.clone(),
            )
        };

        if status != SimOrderStatus::PendingNew {
            return Ok(()); // Already processed
        }

        // Post-only check: reject if would cross spread
        if post_only {
            let book = self.books.get(&token_id);
            let would_cross = match (side, book) {
                (Side::BUY, Some(b)) => b.best_ask().map(|(a, _)| price >= a).unwrap_or(false),
                (Side::SELL, Some(b)) => b.best_bid().map(|(b, _)| price <= b).unwrap_or(false),
                _ => false,
            };
            if would_cross {
                let order = self.orders.get_mut(&order_id).ok_or("Order not found")?;
                order.status = SimOrderStatus::Rejected;
                return Err("Post-only order would cross spread".to_string());
            }
        }

        // Cash gate for BUY orders (only if cash > 0)
        let mut reserved_cost = Decimal::ZERO;
        if side == Side::BUY && self.cash > Decimal::ZERO {
            let cost = price * size;
            if self.available_cash() < cost {
                let order = self.orders.get_mut(&order_id).ok_or("Order not found")?;
                order.status = SimOrderStatus::Rejected;
                return Err("Insufficient cash".to_string());
            }
            reserved_cost = cost;
        }

        // Inventory gate for SELL orders - can only sell what you own
        let mut reserved_shares = Decimal::ZERO;
        if side == Side::SELL {
            let available = self.available_inventory(&token_id);
            if available < size {
                let order = self.orders.get_mut(&order_id).ok_or("Order not found")?;
                order.status = SimOrderStatus::Rejected;
                return Err(format!(
                    "Insufficient inventory: have {}, need {}",
                    available, size
                ));
            }
            reserved_shares = size;
        }

        // Order becomes live
        let order = self.orders.get_mut(&order_id).ok_or("Order not found")?;
        order.status = SimOrderStatus::Live;

        // Get book size at this price for queue position
        let book_size = self
            .books
            .get(&token_id)
            .map(|b| b.size_at(side, price))
            .unwrap_or(Decimal::ZERO);

        // Add to queue
        let key = QueueKey::new(token_id.clone(), side, price);
        self.queues.add_order(key, order_id, size, book_size);

        if reserved_cost > Decimal::ZERO {
            self.reserved_cash.insert(order_id, reserved_cost);
        }
        if reserved_shares > Decimal::ZERO {
            self.reserved_inventory
                .insert(order_id, (token_id, reserved_shares));
        }

        Ok(())
    }

    /// Request cancel for an order
    pub fn request_cancel(&mut self, order_id: u64) -> bool {
        if let Some(order) = self.orders.get_mut(&order_id) {
            if order.is_open() {
                order.status = SimOrderStatus::PendingCancel;
                return true;
            }
        }
        false
    }

    /// Process cancel ack
    pub fn process_cancel_ack(&mut self, order_id: u64) {
        if let Some(order) = self.orders.get_mut(&order_id) {
            if order.status == SimOrderStatus::PendingCancel {
                order.status = SimOrderStatus::Canceled;

                // Remove from queue
                let key = QueueKey::new(order.token_id.clone(), order.side, order.price);
                self.queues.remove_order(&key, order_id);

                // Release reserved cash for BUY orders
                self.reserved_cash.remove(&order_id);
                // Release reserved inventory for SELL orders
                self.reserved_inventory.remove(&order_id);
            }
        }
    }

    /// Apply a book snapshot
    pub fn apply_snapshot(
        &mut self,
        token_id: &str,
        bids: &[(Decimal, Decimal)],
        asks: &[(Decimal, Decimal)],
        timestamp: i64,
    ) {
        let book = self.get_book_mut(token_id);
        let changes = book.apply_snapshot(bids, asks);

        // Update queue ext_ahead for changed levels
        for (side, price, old_size, new_size) in changes {
            let key = QueueKey::new(token_id.to_string(), side, price);
            self.queues
                .handle_size_change(&key, old_size, new_size, timestamp);
        }
    }

    /// Apply a book delta
    pub fn apply_delta(
        &mut self,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
        timestamp: i64,
    ) {
        let book = self.get_book_mut(token_id);
        let (old_size, new_size) = book.apply_delta(side, price, size);

        // Update queue ext_ahead
        let key = QueueKey::new(token_id.to_string(), side, price);
        self.queues
            .handle_size_change(&key, old_size, new_size, timestamp);
    }

    /// Process a trade - returns fills generated
    pub fn process_trade(
        &mut self,
        token_id: &str,
        taker_side: Side,
        price: Decimal,
        size: Decimal,
        timestamp: i64,
        trade_id: Option<String>,
    ) -> Vec<Fill> {
        // Maker side is opposite of taker side
        // BUY trade = taker bought = fills SELL orders (makers)
        // SELL trade = taker sold = fills BUY orders (makers)
        let maker_side = match taker_side {
            Side::BUY => Side::SELL,
            Side::SELL => Side::BUY,
        };

        let key = QueueKey::new(token_id.to_string(), maker_side, price);

        // Record trade volume for double-counting avoidance
        self.queues.record_trade_volume(&key, timestamp, size);

        let mut fills = Vec::new();
        let mut remaining = size;

        // Get queue level
        let Some(level) = self.queues.get_mut(&key) else {
            return fills;
        };

        // 1. Consume ext_ahead first
        let consume_ext = remaining.min(level.ext_ahead);
        level.ext_ahead -= consume_ext;
        remaining -= consume_ext;

        // 2. Fill our orders FIFO
        while remaining > Decimal::ZERO && !level.orders.is_empty() {
            let entry = level.orders.front_mut().unwrap();
            let order = self.orders.get(&entry.order_id).unwrap();

            // Skip if order not yet acked
            if order.status == SimOrderStatus::PendingNew {
                break;
            }

            let fill_size = remaining.min(entry.size);
            entry.size -= fill_size;
            remaining -= fill_size;

            // Apply fill to order
            let order = self.orders.get_mut(&entry.order_id).unwrap();
            order.filled_size += fill_size;

            if order.filled_size >= order.original_size {
                order.status = SimOrderStatus::Filled;
            } else if order.filled_size > Decimal::ZERO {
                order.status = SimOrderStatus::PartialFilled;
            }

            // Release reserved cash for BUY fills
            if maker_side == Side::BUY {
                if let Some(reserved) = self.reserved_cash.get_mut(&entry.order_id) {
                    let release = fill_size * price;
                    *reserved -= release;
                    if *reserved <= Decimal::ZERO {
                        self.reserved_cash.remove(&entry.order_id);
                    }
                }
            }

            // Release reserved inventory for SELL fills
            if maker_side == Side::SELL {
                if let Some((_, reserved)) = self.reserved_inventory.get_mut(&entry.order_id) {
                    *reserved -= fill_size;
                    if *reserved <= Decimal::ZERO {
                        self.reserved_inventory.remove(&entry.order_id);
                    }
                }
            }

            // Update position
            let position_delta = match maker_side {
                Side::BUY => fill_size,
                Side::SELL => -fill_size,
            };
            *self.positions.entry(token_id.to_string()).or_default() += position_delta;

            // Update cash: BUY = -cost, SELL = +revenue
            match maker_side {
                Side::BUY => self.cash -= price * fill_size,
                Side::SELL => self.cash += price * fill_size,
            }

            // Create fill record
            let fill = Fill {
                fill_id: self.next_fill_id,
                order_id: order.order_id,
                token_id: token_id.to_string(),
                side: maker_side,
                price,
                size: fill_size,
                fill_ts: timestamp,
                trade_id: trade_id.clone(),
            };
            self.next_fill_id += 1;
            fills.push(fill);

            // Remove from queue if fully consumed
            if entry.size == Decimal::ZERO {
                level.orders.pop_front();
            }
        }

        fills
    }

    /// Get book size at a specific price level
    pub fn book_size_at(&self, token_id: &str, side: Side, price: Decimal) -> Decimal {
        self.books
            .get(token_id)
            .map(|b| b.size_at(side, price))
            .unwrap_or(Decimal::ZERO)
    }

    /// Check if an implied delta crosses any resting orders and return crossing volume
    pub fn check_delta_crossing(&self, token_id: &str, delta: &ImpliedDelta) -> Decimal {
        // New ASK liquidity crosses BUY orders if ask_price <= buy_order_price
        // New BID liquidity crosses SELL orders if bid_price >= sell_order_price
        for order in self.orders.values() {
            if order.token_id != token_id {
                continue;
            }
            if !matches!(
                order.status,
                SimOrderStatus::Live | SimOrderStatus::PartialFilled
            ) {
                continue;
            }

            let crosses = match (delta.side, order.side) {
                // New ask crosses our buy
                (Side::SELL, Side::BUY) => delta.price <= order.price,
                // New bid crosses our sell
                (Side::BUY, Side::SELL) => delta.price >= order.price,
                _ => false, // Same side doesn't cross
            };

            if crosses {
                return delta.size_added;
            }
        }

        Decimal::ZERO
    }

    /// Execute a taker order (FOK or FAK) against the book immediately.
    /// Returns fills and the unfilled remainder. Does NOT add to queue.
    /// FOK: caller should reject if remainder > 0.
    /// FAK: caller accepts partial fills; remainder is killed.
    pub fn execute_taker_order(
        &mut self,
        token_id: &str,
        side: Side,
        price: Decimal,
        mut size: Decimal,
        timestamp: i64,
    ) -> Vec<Fill> {
        let maker_side = match side {
            Side::BUY => Side::SELL,
            Side::SELL => Side::BUY,
        };

        let book = match self.books.get_mut(token_id) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let mut fills = Vec::new();

        // Walk the book on the opposite side
        // BUY taker hits asks (ascending), SELL taker hits bids (descending)
        let levels: Vec<(Decimal, Decimal)> = match side {
            Side::BUY => book.asks.iter().map(|(&p, &s)| (p, s)).collect(),
            Side::SELL => book.bids.iter().rev().map(|(&p, &s)| (p, s)).collect(),
        };

        let mut consumed_levels: Vec<(Decimal, Decimal)> = Vec::new(); // (price, consumed)

        for (level_price, level_size) in levels {
            if size <= Decimal::ZERO {
                break;
            }
            // Check price limit: BUY up to price, SELL down to price
            let within_limit = match side {
                Side::BUY => level_price <= price,
                Side::SELL => level_price >= price,
            };
            if !within_limit {
                break;
            }

            let fill_size = size.min(level_size);
            size -= fill_size;
            consumed_levels.push((level_price, fill_size));

            fills.push(Fill {
                fill_id: self.next_fill_id,
                order_id: 0, // Will be set by caller
                token_id: token_id.to_string(),
                side,
                price: level_price,
                size: fill_size,
                fill_ts: timestamp,
                trade_id: None,
            });
            self.next_fill_id += 1;
        }

        // Update the book: remove consumed liquidity
        let book = self.books.get_mut(token_id).unwrap();
        for (level_price, consumed) in &consumed_levels {
            let book_side = match side {
                Side::BUY => &mut book.asks,
                Side::SELL => &mut book.bids,
            };
            if let Some(level) = book_side.get_mut(level_price) {
                *level -= consumed;
                if *level <= Decimal::ZERO {
                    book_side.remove(level_price);
                }
            }
        }

        // Consuming opposite-side book liquidity moves our same-side maker orders forward in queue.
        for (level_price, consumed) in &consumed_levels {
            let key = QueueKey::new(token_id.to_string(), maker_side, *level_price);
            if let Some(level) = self.queues.get_mut(&key) {
                level.ext_ahead = (level.ext_ahead - *consumed).max(Decimal::ZERO);
            }
        }

        // Update position and cash for fills
        for fill in &fills {
            let position_delta = match side {
                Side::BUY => fill.size,
                Side::SELL => -fill.size,
            };
            *self.positions.entry(token_id.to_string()).or_default() += position_delta;

            match side {
                Side::BUY => self.cash -= fill.price * fill.size,
                Side::SELL => self.cash += fill.price * fill.size,
            }
        }

        fills
    }

    /// Check the total fillable size for a taker order against the book (without executing).
    pub fn taker_fillable_size(
        &self,
        token_id: &str,
        side: Side,
        price: Decimal,
        size: Decimal,
    ) -> Decimal {
        let book = match self.books.get(token_id) {
            Some(b) => b,
            None => return Decimal::ZERO,
        };

        let levels: Vec<(Decimal, Decimal)> = match side {
            Side::BUY => book.asks.iter().map(|(&p, &s)| (p, s)).collect(),
            Side::SELL => book.bids.iter().rev().map(|(&p, &s)| (p, s)).collect(),
        };

        let mut remaining = size;
        for (level_price, level_size) in levels {
            if remaining <= Decimal::ZERO {
                break;
            }
            let within_limit = match side {
                Side::BUY => level_price <= price,
                Side::SELL => level_price >= price,
            };
            if !within_limit {
                break;
            }
            remaining -= remaining.min(level_size);
        }

        size - remaining
    }

    /// Check if any resting orders cross the current book and generate fills.
    /// Fill size = min(remaining_order_size, crossing_volume)
    /// Fill price = order's limit price (conservative)
    pub fn check_crossing_fills(
        &mut self,
        token_id: &str,
        crossing_volume: Decimal,
        timestamp: i64,
    ) -> Vec<Fill> {
        let book = match self.books.get(token_id) {
            Some(b) => b,
            None => return Vec::new(),
        };

        let best_ask = book.best_ask().map(|(p, _)| p);
        let best_bid = book.best_bid().map(|(p, _)| p);

        let mut fills = Vec::new();
        let mut remaining_vol = crossing_volume;

        // Collect order IDs that cross, to avoid borrow issues
        let crossing_orders: Vec<u64> = self
            .orders
            .values()
            .filter(|order| {
                order.token_id == token_id
                    && matches!(
                        order.status,
                        SimOrderStatus::Live | SimOrderStatus::PartialFilled
                    )
            })
            .filter(|order| match order.side {
                Side::BUY => best_ask.map(|a| a <= order.price).unwrap_or(false),
                Side::SELL => best_bid.map(|b| b >= order.price).unwrap_or(false),
            })
            .map(|o| o.order_id)
            .collect();

        for order_id in crossing_orders {
            if remaining_vol <= Decimal::ZERO {
                break;
            }

            let order = match self.orders.get_mut(&order_id) {
                Some(o) => o,
                None => continue,
            };

            let remaining_size = order.original_size - order.filled_size;
            let fill_size = remaining_vol.min(remaining_size);

            order.filled_size += fill_size;
            remaining_vol -= fill_size;

            // Update order status
            if order.filled_size >= order.original_size {
                order.status = SimOrderStatus::Filled;
            } else {
                order.status = SimOrderStatus::PartialFilled;
            }

            let fill_price = order.price;
            let order_side = order.side;

            // Release reserved cash for BUY fills
            if order_side == Side::BUY {
                if let Some(reserved) = self.reserved_cash.get_mut(&order_id) {
                    let release = fill_size * fill_price;
                    *reserved -= release;
                    if *reserved <= Decimal::ZERO {
                        self.reserved_cash.remove(&order_id);
                    }
                }
            }

            // Release reserved inventory for SELL fills
            if order_side == Side::SELL {
                if let Some((_, reserved)) = self.reserved_inventory.get_mut(&order_id) {
                    *reserved -= fill_size;
                    if *reserved <= Decimal::ZERO {
                        self.reserved_inventory.remove(&order_id);
                    }
                }
            }

            // Update position
            let position_delta = match order_side {
                Side::BUY => fill_size,
                Side::SELL => -fill_size,
            };
            *self.positions.entry(token_id.to_string()).or_default() += position_delta;

            // Update cash: BUY = -cost, SELL = +revenue
            match order_side {
                Side::BUY => self.cash -= fill_price * fill_size,
                Side::SELL => self.cash += fill_price * fill_size,
            }

            // Remove from queue if order fully filled
            if self
                .orders
                .get(&order_id)
                .map(|o| o.filled_size >= o.original_size)
                .unwrap_or(false)
            {
                if let Some(order) = self.orders.get(&order_id) {
                    let key = QueueKey::new(token_id.to_string(), order.side, order.price);
                    self.queues.remove_order(&key, order_id);
                }
            }

            // Create fill record
            fills.push(Fill {
                fill_id: self.next_fill_id,
                order_id,
                token_id: token_id.to_string(),
                side: order_side,
                price: fill_price,
                size: fill_size,
                fill_ts: timestamp,
                trade_id: None,
            });
            self.next_fill_id += 1;
        }

        fills
    }
}
