//! Ladder market maker - bid-only ladder strategy with cost basis constraint.
//!
//! Places post-only bid orders at multiple price levels around top of book on both
//! UP and DOWN token books. Pulls quotes when fills would violate the cost invariant:
//! avg_up_cost + avg_down_cost <= constraint_limit
//!
//! The constraint ensures we don't overpay for a complete UP+DOWN set (worth $1).

use anyhow::Result;
use async_trait::async_trait;
use polyfill_rs::{OrderBook, OrderType, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::engine::types::CancelType;
use crate::engine::{Action, CollectorMessageType, Event, PlaceOrderParams, Strategy};
use crate::tracking::{
    snapshot_market_account, SharedOrderTracker, SharedPositionTracker, TrackedOrder,
};

#[derive(Debug, Clone)]
pub struct LadderMmConfig {
    // Ladder structure
    /// Number of bid levels per side
    pub num_levels: usize,
    /// Spacing between levels in ticks (1 tick = 0.01)
    pub level_spacing_ticks: u32,
    /// Size per level in shares
    pub base_size: Decimal,
    /// Tick size (0.01 for 15-min markets)
    pub tick_size: Decimal,
    /// Negative risk flag (false for 15-min markets)
    pub neg_risk: bool,

    // Position limits
    /// Max position per token
    pub max_position: Decimal,

    // Constraint parameters
    /// Base constraint: avg_up_cost + avg_down_cost <= this (default: 1.00)
    pub base_constraint: Decimal,
    /// Relaxed constraint when both bids in [relaxation_lower, relaxation_upper]
    pub relaxed_constraint: Decimal,
    /// Lower bound for relaxation zone
    pub relaxation_lower: Decimal,
    /// Upper bound for relaxation zone
    pub relaxation_upper: Decimal,
}

impl Default for LadderMmConfig {
    fn default() -> Self {
        Self {
            num_levels: 3,
            level_spacing_ticks: 1,
            base_size: dec!(10),
            tick_size: dec!(0.01),
            neg_risk: false,
            max_position: dec!(100),
            base_constraint: dec!(1.00),
            relaxed_constraint: dec!(1.01),
            relaxation_lower: dec!(0.40),
            relaxation_upper: dec!(0.60),
        }
    }
}

/// A desired order in the ladder
#[derive(Debug, Clone, PartialEq)]
struct LadderLevel {
    price: Decimal,
    size: Decimal,
}

pub struct LadderMarketMaker {
    config: LadderMmConfig,
    order_tracker: SharedOrderTracker,
    position_tracker: SharedPositionTracker,
    current_condition_id: Option<String>,
}

impl LadderMarketMaker {
    pub fn new(
        config: LadderMmConfig,
        order_tracker: SharedOrderTracker,
        position_tracker: SharedPositionTracker,
    ) -> Self {
        Self {
            config,
            order_tracker,
            position_tracker,
            current_condition_id: None,
        }
    }

    /// Get best bid from orderbook
    fn best_bid(book: &OrderBook) -> Option<Decimal> {
        book.bids.first().map(|l| l.price)
    }

    /// Determine constraint limit based on best bid prices.
    /// Returns relaxed_constraint if both bids are in [relaxation_lower, relaxation_upper],
    /// otherwise returns base_constraint.
    fn get_constraint_limit(&self, up_bid: Option<Decimal>, down_bid: Option<Decimal>) -> Decimal {
        match (up_bid, down_bid) {
            (Some(ub), Some(db)) => {
                let in_range = |p: Decimal| {
                    p >= self.config.relaxation_lower && p <= self.config.relaxation_upper
                };
                if in_range(ub) && in_range(db) {
                    self.config.relaxed_constraint
                } else {
                    self.config.base_constraint
                }
            }
            _ => self.config.base_constraint,
        }
    }

    /// Check if buying at given price/size would violate the cost constraint.
    /// Simulates the fill and computes new average costs.
    ///
    /// Returns true if the constraint would be violated after the fill.
    fn would_violate_constraint(
        &self,
        is_up: bool,
        price: Decimal,
        size: Decimal,
        up_position: Decimal,
        up_cost: Decimal,
        down_position: Decimal,
        down_cost: Decimal,
        constraint_limit: Decimal,
    ) -> bool {
        // Simulate the fill
        let (new_up_pos, new_up_cost, new_down_pos, new_down_cost) = if is_up {
            (
                up_position + size,
                up_cost + price * size,
                down_position,
                down_cost,
            )
        } else {
            (
                up_position,
                up_cost,
                down_position + size,
                down_cost + price * size,
            )
        };

        // Compute average costs (0 if no position)
        let avg_up = if new_up_pos > Decimal::ZERO {
            new_up_cost / new_up_pos
        } else {
            Decimal::ZERO
        };

        let avg_down = if new_down_pos > Decimal::ZERO {
            new_down_cost / new_down_pos
        } else {
            Decimal::ZERO
        };

        // Check constraint
        avg_up + avg_down > constraint_limit
    }

    /// Generate a bid ladder for one side (UP or DOWN).
    /// Starts at best_bid and steps down by level_spacing_ticks.
    /// Stops adding levels when:
    /// - Position limit would be exceeded
    /// - Cost constraint would be violated
    fn generate_bid_ladder(
        &self,
        is_up: bool,
        best_bid: Option<Decimal>,
        up_position: Decimal,
        up_cost: Decimal,
        down_position: Decimal,
        down_cost: Decimal,
        constraint_limit: Decimal,
    ) -> Vec<LadderLevel> {
        let Some(top_price) = best_bid else {
            return vec![];
        };

        let mut ladder = Vec::new();
        let tick = self.config.tick_size;
        let spacing = Decimal::from(self.config.level_spacing_ticks) * tick;

        // Track simulated position/cost as we add levels
        let mut sim_up_pos = up_position;
        let mut sim_up_cost = up_cost;
        let mut sim_down_pos = down_position;
        let mut sim_down_cost = down_cost;

        for i in 0..self.config.num_levels {
            let price = top_price - Decimal::from(i as u32) * spacing;

            // Price must be positive
            if price <= Decimal::ZERO {
                break;
            }

            let size = self.config.base_size;

            // Check position limit
            let current_pos = if is_up { sim_up_pos } else { sim_down_pos };
            if current_pos + size > self.config.max_position {
                break;
            }

            // Check cost constraint
            if self.would_violate_constraint(
                is_up,
                price,
                size,
                sim_up_pos,
                sim_up_cost,
                sim_down_pos,
                sim_down_cost,
                constraint_limit,
            ) {
                break;
            }

            // Update simulated state for next level check
            if is_up {
                sim_up_pos += size;
                sim_up_cost += price * size;
            } else {
                sim_down_pos += size;
                sim_down_cost += price * size;
            }

            ladder.push(LadderLevel { price, size });
        }

        ladder
    }

    /// Reconcile existing orders with desired ladder.
    /// Returns actions to cancel stale orders and place missing ones.
    fn manage_ladder(
        &self,
        orders: &[TrackedOrder],
        condition_id: &str,
        token_id: &str,
        desired_ladder: &[LadderLevel],
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        // Get existing bid orders for this token
        let existing_bids: Vec<&TrackedOrder> = orders
            .iter()
            .filter(|o| o.asset_id == token_id && o.side == Side::BUY)
            .collect();

        // Find orders to cancel (not in desired ladder)
        let mut orders_to_cancel = Vec::new();
        for order in &existing_bids {
            let matches = desired_ladder
                .iter()
                .any(|l| l.price == order.price && l.size == order.original_size);
            if !matches {
                orders_to_cancel.push(order.order_id.clone());
            }
        }

        // Find levels to place (not already covered by existing order)
        let mut levels_to_place = Vec::new();
        for level in desired_ladder {
            let exists = existing_bids
                .iter()
                .any(|o| o.price == level.price && o.original_size == level.size);
            if !exists {
                levels_to_place.push(level.clone());
            }
        }

        // Cancel stale orders
        if !orders_to_cancel.is_empty() {
            if orders_to_cancel.len() == 1 {
                actions.push(Action::CancelOrder {
                    cancel_type: CancelType::Single {
                        order_id: orders_to_cancel.into_iter().next().unwrap(),
                    },
                });
            } else {
                actions.push(Action::CancelOrder {
                    cancel_type: CancelType::Multiple {
                        order_ids: orders_to_cancel,
                    },
                });
            }
        }

        // Place new orders
        for level in levels_to_place {
            actions.push(Action::PlaceOrder {
                token_id: token_id.to_string(),
                condition_id: condition_id.to_string(),
                side: Side::BUY,
                price: level.price,
                size: level.size,
                tick_size: self.config.tick_size,
                neg_risk: self.config.neg_risk,
                order_type: OrderType::GTC,
                post_only: true,
            });
        }

        actions
    }

    /// Main quoting logic - generate ladders for both UP and DOWN books.
    async fn quote_market(
        &self,
        condition_id: &str,
        up_book: &OrderBook,
        down_book: &OrderBook,
    ) -> Vec<Action> {
        let snapshot =
            snapshot_market_account(&self.order_tracker, &self.position_tracker, condition_id).await;
        let market_orders: Vec<TrackedOrder> = snapshot.open_orders;

        let (up_pos, down_pos, up_cost, down_cost) = snapshot
            .position
            .as_ref()
            .map(|p| (p.up_position, p.down_position, p.up_cost, p.down_cost))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));

        let up_token_id = &up_book.token_id;
        let down_token_id = &down_book.token_id;

        let up_best_bid = Self::best_bid(up_book);
        let down_best_bid = Self::best_bid(down_book);

        // Determine constraint limit
        let constraint_limit = self.get_constraint_limit(up_best_bid, down_best_bid);

        // Generate bid ladders
        let up_ladder = self.generate_bid_ladder(
            true,
            up_best_bid,
            up_pos,
            up_cost,
            down_pos,
            down_cost,
            constraint_limit,
        );

        let down_ladder = self.generate_bid_ladder(
            false,
            down_best_bid,
            up_pos,
            up_cost,
            down_pos,
            down_cost,
            constraint_limit,
        );

        // Manage ladders
        let mut actions = Vec::new();
        actions.extend(self.manage_ladder(&market_orders, condition_id, up_token_id, &up_ladder));
        actions.extend(self.manage_ladder(
            &market_orders,
            condition_id,
            down_token_id,
            &down_ladder,
        ));

        // Batch place orders for efficiency
        let (cancel_actions, place_orders): (Vec<_>, Vec<_>) = actions
            .into_iter()
            .partition(|a| matches!(a, Action::CancelOrder { .. }));

        let place_order_params: Vec<PlaceOrderParams> = place_orders
            .into_iter()
            .filter_map(|a| match a {
                Action::PlaceOrder {
                    token_id,
                    condition_id,
                    side,
                    price,
                    size,
                    tick_size,
                    neg_risk,
                    order_type,
                    post_only,
                } => Some(PlaceOrderParams {
                    token_id,
                    condition_id,
                    side,
                    price,
                    size,
                    tick_size,
                    neg_risk,
                    order_type,
                    post_only,
                }),
                _ => None,
            })
            .collect();

        let mut result = cancel_actions;
        match place_order_params.len() {
            0 => {}
            1 => {
                let p = place_order_params.into_iter().next().unwrap();
                result.push(Action::PlaceOrder {
                    token_id: p.token_id,
                    condition_id: p.condition_id,
                    side: p.side,
                    price: p.price,
                    size: p.size,
                    tick_size: p.tick_size,
                    neg_risk: p.neg_risk,
                    order_type: p.order_type,
                    post_only: p.post_only,
                });
            }
            _ => {
                result.push(Action::PlaceOrders {
                    orders: place_order_params,
                });
            }
        }

        result
    }
}

#[async_trait]
impl Strategy for LadderMarketMaker {
    fn name(&self) -> &str {
        "ladder-market-maker"
    }

    fn handles(&self) -> &[CollectorMessageType] {
        const HANDLES: [CollectorMessageType; 3] = [
            CollectorMessageType::FairValueUpdated,
            CollectorMessageType::MarketStart,
            CollectorMessageType::MarketEnd,
        ];
        &HANDLES
    }

    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Vec<Action> {
        if !self.should_handle(&event) {
            return vec![];
        }

        match event {
            Event::MarketStart { market } => {
                self.current_condition_id = Some(market.condition_id.clone());
                self.position_tracker.write().await.init_market(
                    market.condition_id,
                    market.up_token_id,
                    market.down_token_id,
                );
                vec![]
            }
            Event::MarketEnd => {
                if let Some(ref condition_id) = self.current_condition_id {
                    self.position_tracker
                        .write()
                        .await
                        .clear_market(condition_id);
                }
                self.current_condition_id = None;
                vec![Action::CancelOrder {
                    cancel_type: CancelType::All,
                }]
            }
            Event::FairValueUpdated {
                condition_id,
                up_book,
                down_book,
                ..
            } => {
                self.current_condition_id = Some(condition_id.clone());
                if self
                    .position_tracker
                    .read()
                    .await
                    .get_position(&condition_id)
                    .is_none()
                {
                    self.position_tracker.write().await.init_market(
                        condition_id.clone(),
                        up_book.token_id.clone(),
                        down_book.token_id.clone(),
                    );
                }
                self.quote_market(&condition_id, &up_book, &down_book).await
            }
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> LadderMmConfig {
        LadderMmConfig {
            num_levels: 3,
            level_spacing_ticks: 1,
            base_size: dec!(10),
            tick_size: dec!(0.01),
            neg_risk: false,
            max_position: dec!(100),
            base_constraint: dec!(1.00),
            relaxed_constraint: dec!(1.01),
            relaxation_lower: dec!(0.40),
            relaxation_upper: dec!(0.60),
        }
    }

    #[test]
    fn test_constraint_limit_base() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // Outside relaxation zone -> base constraint
        assert_eq!(
            mm.get_constraint_limit(Some(dec!(0.30)), Some(dec!(0.70))),
            dec!(1.00)
        );
        assert_eq!(
            mm.get_constraint_limit(Some(dec!(0.65)), Some(dec!(0.35))),
            dec!(1.00)
        );
    }

    #[test]
    fn test_constraint_limit_relaxed() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // Both in [0.40, 0.60] -> relaxed constraint
        assert_eq!(
            mm.get_constraint_limit(Some(dec!(0.50)), Some(dec!(0.50))),
            dec!(1.01)
        );
        assert_eq!(
            mm.get_constraint_limit(Some(dec!(0.40)), Some(dec!(0.60))),
            dec!(1.01)
        );
    }

    #[test]
    fn test_would_violate_no_position() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // No position: buying UP at 0.50 -> avg_up = 0.50, avg_down = 0 -> sum = 0.50 <= 1.00
        assert!(!mm.would_violate_constraint(
            true,
            dec!(0.50),
            dec!(10),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.00),
        ));
    }

    #[test]
    fn test_would_violate_with_position() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // Have UP at avg 0.60, buying DOWN at 0.50 -> 0.60 + 0.50 = 1.10 > 1.00
        assert!(mm.would_violate_constraint(
            false,
            dec!(0.50),
            dec!(10),
            dec!(10),  // up_pos
            dec!(6.0), // up_cost (avg 0.60)
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.00),
        ));

        // Same but with relaxed constraint -> 1.10 > 1.01
        assert!(mm.would_violate_constraint(
            false,
            dec!(0.50),
            dec!(10),
            dec!(10),
            dec!(6.0),
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.01),
        ));
    }

    #[test]
    fn test_would_not_violate() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // Have UP at avg 0.40, buying DOWN at 0.50 -> 0.40 + 0.50 = 0.90 <= 1.00
        assert!(!mm.would_violate_constraint(
            false,
            dec!(0.50),
            dec!(10),
            dec!(10),  // up_pos
            dec!(4.0), // up_cost (avg 0.40)
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.00),
        ));
    }

    #[test]
    fn test_generate_ladder_empty_book() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        let ladder = mm.generate_bid_ladder(
            true,
            None, // no best bid
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.00),
        );
        assert!(ladder.is_empty());
    }

    #[test]
    fn test_generate_ladder_basic() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        let ladder = mm.generate_bid_ladder(
            true,
            Some(dec!(0.50)),
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(1.00),
        );

        // Should generate 3 levels: 0.50, 0.49, 0.48
        assert_eq!(ladder.len(), 3);
        assert_eq!(ladder[0].price, dec!(0.50));
        assert_eq!(ladder[1].price, dec!(0.49));
        assert_eq!(ladder[2].price, dec!(0.48));
        assert!(ladder.iter().all(|l| l.size == dec!(10)));
    }

    #[test]
    fn test_generate_ladder_stops_at_constraint() {
        let config = make_config();
        let tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::OrderTracker::new(),
        ));
        let pos_tracker = std::sync::Arc::new(tokio::sync::RwLock::new(
            crate::tracking::PositionTracker::new(),
        ));
        let mm = LadderMarketMaker::new(config, tracker, pos_tracker);

        // Already have DOWN at avg 0.55, so UP bids must be < 0.45 to satisfy constraint
        // Best bid at 0.50 would make sum = 0.55 + 0.50 = 1.05 > 1.00
        let ladder = mm.generate_bid_ladder(
            true,
            Some(dec!(0.50)),
            Decimal::ZERO,
            Decimal::ZERO,
            dec!(10),   // down_pos
            dec!(5.50), // down_cost (avg 0.55)
            dec!(1.00),
        );

        // No levels should be generated since even best bid violates
        assert!(ladder.is_empty());
    }
}
