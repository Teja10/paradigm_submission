//! Touch market maker with momentum-based adverse selection protection.
//!
//! Key improvement: Pull quotes when momentum is running against our position.
//! Based on backtest analysis showing +9% P&L improvement.

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

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct BasicMmConfig {
    pub order_size: Decimal,
    pub max_position: Decimal,
    pub tick_size: Decimal,
    pub neg_risk: bool,
    pub invariant_buffer: Decimal,
    pub enforce_parity: bool,

    // === Fair-value quoting ===
    /// Offset from fair value in ticks for bid/ask placement.
    /// 0 = quote at fair, 1 = 1 tick outside fair, -1 = 1 tick inside fair.
    /// None/disabled when not set (uses TOB quoting).
    pub fair_value_offset: Option<i32>,

    // === Momentum filter ===
    /// Pull quotes when position_direction * delta_microprice_5s < threshold
    /// Default: -8.0
    pub momentum_threshold: f64,
    /// Tighter threshold when vol_ratio > vol_ratio_threshold
    /// Default: -5.0
    pub momentum_threshold_tight: f64,
    /// Vol ratio threshold: when sigma_dyn / sigma_5m > this, use tight threshold
    /// Default: 1.3
    pub vol_ratio_threshold: f64,
    /// Set to false to disable momentum filter
    pub enable_momentum_filter: bool,
}

impl Default for BasicMmConfig {
    fn default() -> Self {
        Self {
            order_size: dec!(10),
            max_position: dec!(100),
            tick_size: dec!(0.01),
            neg_risk: false,
            invariant_buffer: dec!(0.01),
            enforce_parity: true,
            fair_value_offset: None,
            momentum_threshold: -8.0,
            momentum_threshold_tight: -5.0,
            vol_ratio_threshold: 1.3,
            enable_momentum_filter: true,
        }
    }
}

pub struct BasicMarketMaker {
    config: BasicMmConfig,
    order_tracker: SharedOrderTracker,
    position_tracker: SharedPositionTracker,
    current_condition_id: Option<String>,
    /// Cached delta_microprice_5s from last update
    delta_microprice_5s: f64,
    /// Cached sigma_dyn (12s window) for vol ratio calculation
    sigma_dyn: f64,
    /// Cached sigma_5m (5-min window) for vol ratio calculation
    sigma_5m: f64,
    /// Cached fair value for UP token
    fair_up: f64,
    /// Cached fair value for DOWN token
    fair_down: f64,
}

impl BasicMarketMaker {
    pub fn new(
        config: BasicMmConfig,
        order_tracker: SharedOrderTracker,
        position_tracker: SharedPositionTracker,
    ) -> Self {
        Self {
            config,
            order_tracker,
            position_tracker,
            current_condition_id: None,
            delta_microprice_5s: 0.0,
            sigma_dyn: 0.0,
            sigma_5m: 1.0, // Default to 1.0 to avoid div by zero
            fair_up: 0.0,
            fair_down: 0.0,
        }
    }

    fn best_bid_ask(book: &OrderBook) -> (Option<Decimal>, Option<Decimal>) {
        let bid = book.bids.first().map(|l| l.price);
        let ask = book.asks.first().map(|l| l.price);
        (bid, ask)
    }

    /// Check if we should quote this side based on momentum alignment.
    /// Returns false if momentum is strongly against our would-be position.
    /// Uses tighter threshold when vol_ratio (sigma_dyn / sigma_5m) > vol_ratio_threshold.
    fn should_quote(&self, side: Side, is_up_token: bool) -> bool {
        if !self.config.enable_momentum_filter {
            return true;
        }

        // Position direction: +1 if we'd be long UP, -1 if we'd be short UP
        let position_direction: f64 = match (is_up_token, side) {
            (true, Side::BUY) => 1.0,   // buying UP = long UP
            (true, Side::SELL) => -1.0, // selling UP = short UP
            (false, Side::BUY) => -1.0, // buying DOWN = short UP
            (false, Side::SELL) => 1.0, // selling DOWN = long UP
        };

        let momentum_alignment = position_direction * self.delta_microprice_5s;

        // Vol-based threshold selection: use tighter threshold during high vol
        let vol_ratio = if self.sigma_5m > 0.0 {
            self.sigma_dyn / self.sigma_5m
        } else {
            1.0
        };
        let threshold = if vol_ratio > self.config.vol_ratio_threshold {
            self.config.momentum_threshold_tight
        } else {
            self.config.momentum_threshold
        };

        if momentum_alignment < threshold {
            tracing::debug!(
                side = ?side,
                is_up_token,
                delta_5s = self.delta_microprice_5s,
                momentum_alignment,
                vol_ratio,
                threshold,
                "Pulling quote: momentum against position"
            );
            return false;
        }

        true
    }

    /// Compute bid/ask prices from fair value + offset.
    /// Returns (bid_price, ask_price) for a given fair value.
    fn fair_value_prices(&self, fair: f64) -> (Option<Decimal>, Option<Decimal>) {
        let tick = self.config.tick_size;
        let offset = self.config.fair_value_offset.unwrap_or(0);
        let offset_dec = tick * Decimal::from(offset);

        // Convert fair to Decimal, round to tick grid
        let fair_dec = Decimal::from_f64_retain(fair).unwrap_or(Decimal::ZERO);
        // Bid: round down to tick, then subtract offset
        let bid_ticks = (fair_dec / tick).floor();
        let bid = bid_ticks * tick - offset_dec;
        // Ask: round up to tick, then add offset
        let ask_ticks = (fair_dec / tick).ceil();
        let ask = ask_ticks * tick + offset_dec;

        let min_price = dec!(0.01);
        let max_price = dec!(0.99);
        let bid = if bid >= min_price && bid <= max_price { Some(bid) } else { None };
        let ask = if ask >= min_price && ask <= max_price { Some(ask) } else { None };
        (bid, ask)
    }

    fn manage_side(
        &self,
        orders: &[TrackedOrder],
        condition_id: &str,
        token_id: &str,
        is_up_token: bool,
        side: Side,
        desired_price: Option<Decimal>,
        desired_size: Decimal,
    ) -> Vec<Action> {
        let mut actions = Vec::new();

        let matching: Vec<&TrackedOrder> = orders
            .iter()
            .filter(|o| o.asset_id == token_id && o.side == side)
            .collect();

        // If more than one order on the same side, nuke extras.
        if matching.len() > 1 {
            let order_ids: Vec<String> = matching.iter().map(|o| o.order_id.clone()).collect();
            actions.push(Action::CancelOrder {
                cancel_type: CancelType::Multiple { order_ids },
            });
            return actions;
        }

        let existing = matching.first().copied();

        // Check momentum filter
        let dominated = !self.should_quote(side, is_up_token);

        if dominated || desired_size <= Decimal::ZERO || desired_price.is_none() {
            if let Some(order) = existing {
                actions.push(Action::CancelOrder {
                    cancel_type: CancelType::Single {
                        order_id: order.order_id.clone(),
                    },
                });
            }
            return actions;
        }

        let desired_price = desired_price.unwrap();

        match existing {
            None => {
                actions.push(Action::PlaceOrder {
                    token_id: token_id.to_string(),
                    condition_id: condition_id.to_string(),
                    side,
                    price: desired_price,
                    size: desired_size,
                    tick_size: self.config.tick_size,
                    neg_risk: self.config.neg_risk,
                    order_type: OrderType::GTC,
                    post_only: true,
                });
            }
            Some(order) => {
                if order.price != desired_price || order.original_size != desired_size {
                    actions.push(Action::CancelOrder {
                        cancel_type: CancelType::Single {
                            order_id: order.order_id.clone(),
                        },
                    });
                }
            }
        }

        actions
    }

    fn clamp_parity(
        &self,
        up_bid: Option<Decimal>,
        down_bid: Option<Decimal>,
        up_ask: Option<Decimal>,
        down_ask: Option<Decimal>,
        up_bid_size: &mut Decimal,
        down_bid_size: &mut Decimal,
        up_ask_size: &mut Decimal,
        down_ask_size: &mut Decimal,
    ) {
        if !self.config.enforce_parity {
            return;
        }

        let buf = self.config.invariant_buffer;

        if let (Some(ub), Some(db)) = (up_bid, down_bid) {
            if ub + db > Decimal::ONE - buf {
                if ub >= db {
                    *up_bid_size = Decimal::ZERO;
                } else {
                    *down_bid_size = Decimal::ZERO;
                }
            }
        }

        if let (Some(ua), Some(da)) = (up_ask, down_ask) {
            if ua + da < Decimal::ONE + buf {
                if ua <= da {
                    *up_ask_size = Decimal::ZERO;
                } else {
                    *down_ask_size = Decimal::ZERO;
                }
            }
        }
    }

    async fn quote_market(
        &self,
        condition_id: &str,
        up_book: &OrderBook,
        down_book: &OrderBook,
    ) -> Vec<Action> {
        let snapshot =
            snapshot_market_account(&self.order_tracker, &self.position_tracker, condition_id).await;
        let market_orders: Vec<TrackedOrder> = snapshot.open_orders;

        let (up_pos, down_pos) = snapshot
            .position
            .as_ref()
            .map(|p| (p.up_position, p.down_position))
            .unwrap_or((Decimal::ZERO, Decimal::ZERO));

        let up_token_id = &up_book.token_id;
        let down_token_id = &down_book.token_id;

        let (up_best_bid, up_best_ask) = if self.config.fair_value_offset.is_some() {
            self.fair_value_prices(self.fair_up)
        } else {
            Self::best_bid_ask(up_book)
        };
        let (down_best_bid, down_best_ask) = if self.config.fair_value_offset.is_some() {
            self.fair_value_prices(self.fair_down)
        } else {
            Self::best_bid_ask(down_book)
        };

        let available_up = snapshot
            .position
            .as_ref()
            .map(|p| {
                let pending_sells: Decimal = market_orders
                    .iter()
                    .filter(|o| o.asset_id == *up_token_id && o.side == Side::SELL)
                    .map(|o| o.original_size - o.size_matched)
                    .sum();
                (p.up_position - pending_sells).max(Decimal::ZERO)
            })
            .unwrap_or(Decimal::ZERO);
        let available_down = snapshot
            .position
            .as_ref()
            .map(|p| {
                let pending_sells: Decimal = market_orders
                    .iter()
                    .filter(|o| o.asset_id == *down_token_id && o.side == Side::SELL)
                    .map(|o| o.original_size - o.size_matched)
                    .sum();
                (p.down_position - pending_sells).max(Decimal::ZERO)
            })
            .unwrap_or(Decimal::ZERO);
        let pending_buys: Decimal = market_orders
            .iter()
            .filter(|o| o.side == Side::BUY)
            .map(|o| (o.original_size - o.size_matched) * o.price)
            .sum();
        let available_cash = (snapshot.cash - pending_buys).max(Decimal::ZERO);

        let order_size = self.config.order_size;
        let max_pos = self.config.max_position;

        let mut up_bid_size = if up_pos >= max_pos {
            Decimal::ZERO
        } else if let Some(bid_price) = up_best_bid {
            let cost = order_size * bid_price;
            if available_cash >= cost { order_size } else { Decimal::ZERO }
        } else {
            Decimal::ZERO
        };

        let mut down_bid_size = if down_pos >= max_pos {
            Decimal::ZERO
        } else if let Some(bid_price) = down_best_bid {
            let cost = order_size * bid_price;
            if available_cash >= cost { order_size } else { Decimal::ZERO }
        } else {
            Decimal::ZERO
        };

        let mut up_ask_size = if available_up >= order_size {
            order_size
        } else {
            Decimal::ZERO
        };

        let mut down_ask_size = if available_down >= order_size {
            order_size
        } else {
            Decimal::ZERO
        };

        self.clamp_parity(
            up_best_bid,
            down_best_bid,
            up_best_ask,
            down_best_ask,
            &mut up_bid_size,
            &mut down_bid_size,
            &mut up_ask_size,
            &mut down_ask_size,
        );
        let mut actions = Vec::new();
        actions.extend(self.manage_side(
            &market_orders,
            condition_id,
            up_token_id,
            true,
            Side::BUY,
            up_best_bid,
            up_bid_size,
        ));
        actions.extend(self.manage_side(
            &market_orders,
            condition_id,
            up_token_id,
            true,
            Side::SELL,
            up_best_ask,
            up_ask_size,
        ));
        actions.extend(self.manage_side(
            &market_orders,
            condition_id,
            down_token_id,
            false,
            Side::BUY,
            down_best_bid,
            down_bid_size,
        ));
        actions.extend(self.manage_side(
            &market_orders,
            condition_id,
            down_token_id,
            false,
            Side::SELL,
            down_best_ask,
            down_ask_size,
        ));

        // Batch place orders
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
impl Strategy for BasicMarketMaker {
    fn name(&self) -> &str {
        "touch-market-maker"
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
                    self.position_tracker.write().await.clear_market(condition_id);
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
                delta_microprice_5s,
                volatility_features,
                fair_up,
                fair_down,
                ..
            } => {
                // Update cached features
                self.delta_microprice_5s = delta_microprice_5s.unwrap_or(0.0);
                self.sigma_dyn = volatility_features.sigma_dyn;
                self.sigma_5m = volatility_features.sigma_5m;
                self.fair_up = fair_up;
                self.fair_down = fair_down;

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
