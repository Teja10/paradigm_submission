//! Main backtest engine orchestrating the simulation

use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use polyfill_rs::types::{BookLevel, OrderBook};
use polyfill_rs::{OrderType, Side};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

use crate::engine::types::CancelType;
use crate::engine::{Action, Event, PlaceOrderParams, Strategy};
use crate::features::{CoinbaseFeatures, VolatilityFeatures};
use crate::tracking::{
    OrderTracker, PositionTracker, SharedOrderTracker, SharedPositionTracker, TrackedOrder,
    TrackedOrderStatus,
};

use super::config::{BacktestConfig, DeltaMode};
use super::exchange_sim::{
    implied_delta_from_delta, implied_deltas_from_snapshot, ExchangeSimulator,
};
use super::latency_model::LatencyModel;
use super::recorder::{make_order_event, Recorder};
use super::types::{
    BacktestEvent, EventPayload, FeatureRow, Fill, PendingAction, PendingActionType, SimOrder,
    SimOrderStatus,
};

/// Backtest engine that runs the simulation
pub struct BacktestEngine {
    config: BacktestConfig,
    sim: ExchangeSimulator,
    latency: LatencyModel,
    recorder: Recorder,
    /// Pending acks (min-heap by ack_ts)
    pending_acks: BinaryHeap<PendingAction>,
    /// Current simulation time
    current_ts: i64,
    /// Shared order tracker for strategy access
    order_tracker: SharedOrderTracker,
    /// Shared position tracker for strategy access
    position_tracker: SharedPositionTracker,
    /// All fills for history access
    fills: Vec<Fill>,
    /// Order type metadata for pending taker orders (consumed at ack time)
    pending_taker_order_types: HashMap<u64, OrderType>,
}

impl BacktestEngine {
    /// Create a new backtest engine (creates its own trackers internally)
    pub fn new(config: BacktestConfig) -> Self {
        let order_tracker = Arc::new(RwLock::new(OrderTracker::new()));
        let position_tracker = Arc::new(RwLock::new(PositionTracker::new()));

        // Initialize position tracker with market tokens
        {
            let mut pt = position_tracker.blocking_write();
            pt.init_market(
                config.condition_id.clone(),
                config.up_token_id.clone(),
                config.down_token_id.clone(),
            );
        }

        let mut engine = Self::new_with_trackers(config, order_tracker, position_tracker);
        engine.seed_initial_positions();
        engine
    }

    /// Create a new backtest engine with externally provided trackers
    pub fn new_with_trackers(
        config: BacktestConfig,
        order_tracker: SharedOrderTracker,
        position_tracker: SharedPositionTracker,
    ) -> Self {
        let latency = LatencyModel::new(config.seed, config.latency_min_ms, config.latency_max_ms);
        let sim = ExchangeSimulator::new(config.queue_added_ahead);
        let recorder = Recorder::new(config.output_dir.clone());

        Self {
            config,
            sim,
            latency,
            recorder,
            pending_acks: BinaryHeap::new(),
            current_ts: 0,
            order_tracker,
            position_tracker,
            fills: Vec::new(),
            pending_taker_order_types: HashMap::new(),
        }
    }

    fn seed_initial_positions(&mut self) {
        // Initialize cash in both simulator and position tracker
        let cash = Decimal::from_f64(self.config.initial_cash).unwrap_or(Decimal::ZERO);
        self.sim.set_initial_cash(cash);
        self.position_tracker.blocking_write().set_cash(cash);

        // Seed complete sets (1 UP + 1 DOWN per set)
        if self.config.initial_complete_sets == 0 {
            return;
        }
        if self.config.up_token_id.is_empty() || self.config.down_token_id.is_empty() {
            return;
        }

        let sets = Decimal::from(self.config.initial_complete_sets as i64);
        let cost_per_set =
            Decimal::from_f64(self.config.complete_set_price).unwrap_or(Decimal::ONE);

        // Seed simulator with inventory
        self.sim.seed_complete_sets(
            &self.config.up_token_id,
            &self.config.down_token_id,
            sets,
            cost_per_set,
        );

        // Seed position tracker with inventory
        let mut pt = self.position_tracker.blocking_write();
        pt.set_position(&self.config.up_token_id, sets);
        pt.set_position(&self.config.down_token_id, sets);
    }

    /// Run the backtest with the given strategy and events
    pub fn run<S, I>(&mut self, strategy: &mut S, events: I) -> Result<()>
    where
        S: Strategy,
        I: Iterator<Item = BacktestEvent>,
    {
        let rt = tokio::runtime::Runtime::new()?;

        for event in events {
            self.current_ts = event.timestamp_ms;

            // 1. Flush pending acks that are due
            self.flush_pending_acks();

            // 2. Apply event to exchange simulator
            match &event.payload {
                EventPayload::Snapshot {
                    token_id,
                    bids,
                    asks,
                } => {
                    // Get current book state before applying snapshot
                    let current_book = self.sim.get_book(token_id).cloned();

                    // Compute implied deltas from diff
                    let implied_deltas = if let Some(ref book) = current_book {
                        implied_deltas_from_snapshot(book, bids, asks)
                    } else {
                        Vec::new()
                    };

                    // Apply snapshot
                    self.sim
                        .apply_snapshot(token_id, bids, asks, self.current_ts);

                    // Process each implied delta for potential crossing fills
                    for implied in implied_deltas {
                        let crossing_vol = self.sim.check_delta_crossing(token_id, &implied);
                        if crossing_vol > Decimal::ZERO {
                            let fills = self.sim.check_crossing_fills(
                                token_id,
                                crossing_vol,
                                self.current_ts,
                            );
                            self.process_crossing_fills(fills);
                        }
                    }
                }
                EventPayload::Delta {
                    token_id,
                    side,
                    price,
                    size,
                } => {
                    // Get previous size before applying delta
                    let prev_size = self.sim.book_size_at(token_id, *side, *price);
                    let next_size = match self.config.delta_mode {
                        // AUTO currently follows ABSOLUTE semantics because production parquet
                        // deltas are absolute sizes for each level.
                        DeltaMode::Auto | DeltaMode::Absolute => (*size).max(Decimal::ZERO),
                        // INCREMENTAL treats incoming `size` as signed size change.
                        DeltaMode::Incremental => (prev_size + *size).max(Decimal::ZERO),
                    };
                    self.sim
                        .apply_delta(token_id, *side, *price, next_size, self.current_ts);

                    // Check for crossing fill from this delta
                    if let Some(implied) =
                        implied_delta_from_delta(prev_size, next_size, *side, *price)
                    {
                        let crossing_vol = self.sim.check_delta_crossing(token_id, &implied);
                        if crossing_vol > Decimal::ZERO {
                            let fills = self.sim.check_crossing_fills(
                                token_id,
                                crossing_vol,
                                self.current_ts,
                            );
                            self.process_crossing_fills(fills);
                        }
                    }
                }
                EventPayload::Trade {
                    token_id,
                    side,
                    price,
                    size,
                    trade_id,
                } => {
                    // Process fills from this trade
                    let fills = self.sim.process_trade(
                        token_id,
                        *side,
                        *price,
                        *size,
                        self.current_ts,
                        trade_id.clone(),
                    );

                    // Update trackers and record fills
                    for fill in fills {
                        // Update order tracker with fill
                        self.order_tracker
                            .blocking_write()
                            .apply_fill(&fill.order_id.to_string(), fill.size);

                        // Update position tracker with fill (including price for cost basis)
                        let mut pt = self.position_tracker.blocking_write();
                        pt.apply_fill_with_price(&fill.token_id, fill.side, fill.size, fill.price);
                        // Update cash: BUY = -cost, SELL = +revenue
                        let cash_delta = match fill.side {
                            Side::BUY => -(fill.price * fill.size),
                            Side::SELL => fill.price * fill.size,
                        };
                        pt.apply_cash_delta(cash_delta);
                        drop(pt);

                        // Store fill for history access
                        self.fills.push(fill.clone());

                        // Record to output
                        self.recorder.record_fill(fill);
                    }
                }
                EventPayload::Feature(feature) => {
                    // 3. Feature triggers strategy
                    // Convert to engine Event
                    if let Some(engine_event) = self.feature_to_event(feature) {
                        // Call strategy
                        let actions = rt.block_on(strategy.process_event(engine_event));

                        // Process returned actions
                        for action in actions {
                            self.process_action(action);
                        }
                    }
                }
            }
        }

        // Final flush: advance simulation clock to process any remaining acks.
        self.flush_all_pending_acks();

        // Write results
        self.recorder.finalize()?;

        Ok(())
    }

    /// Flush pending acks that are due at current_ts
    fn flush_pending_acks(&mut self) {
        while let Some(action) = self.pending_acks.peek() {
            if action.ack_ts > self.current_ts {
                break;
            }

            let action = self.pending_acks.pop().unwrap();

            match action.action_type {
                PendingActionType::OrderAck => {
                    let is_taker = self
                        .sim
                        .get_order(action.order_id)
                        .map(|o| o.is_taker)
                        .unwrap_or(false);

                    if is_taker {
                        self.process_taker_order_ack(action.order_id);
                    } else {
                        match self.sim.process_order_ack(action.order_id) {
                            Ok(()) => {
                                if let Some(order) = self.sim.get_order(action.order_id) {
                                    self.recorder.record_order_event(make_order_event(
                                        order,
                                        "ACK",
                                        self.current_ts,
                                    ));
                                }
                            }
                            Err(reason) => {
                                // Update order tracker with rejection
                                self.order_tracker
                                    .blocking_write()
                                    .mark_rejected(&action.order_id.to_string());

                                if let Some(order) = self.sim.get_order(action.order_id) {
                                    let event_type = format!("REJECT:{}", reason);
                                    self.recorder.record_order_event(make_order_event(
                                        order,
                                        &event_type,
                                        self.current_ts,
                                    ));
                                }
                            }
                        }
                    }
                }
                PendingActionType::CancelAck => {
                    let was_pending_cancel = self
                        .sim
                        .get_order(action.order_id)
                        .map(|o| o.status == SimOrderStatus::PendingCancel)
                        .unwrap_or(false);

                    self.sim.process_cancel_ack(action.order_id);

                    // Only mark canceled if cancel actually applied. If the order filled before
                    // cancel ack arrived, simulator keeps it as Filled/PartialFilled.
                    if was_pending_cancel {
                        if let Some(order) = self.sim.get_order(action.order_id) {
                            if order.status == SimOrderStatus::Canceled {
                                self.order_tracker.blocking_write().update_status(
                                    &action.order_id.to_string(),
                                    TrackedOrderStatus::Canceled,
                                );
                                self.recorder.record_order_event(make_order_event(
                                    order,
                                    "CANCEL_ACK",
                                    self.current_ts,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Drain all pending acks by advancing simulation time to the next ack timestamp.
    fn flush_all_pending_acks(&mut self) {
        while let Some(next_ack_ts) = self.pending_acks.peek().map(|a| a.ack_ts) {
            if next_ack_ts > self.current_ts {
                self.current_ts = next_ack_ts;
            }
            self.flush_pending_acks();
        }
    }

    /// Process fills generated from crossing checks on book updates
    fn process_crossing_fills(&mut self, fills: Vec<Fill>) {
        for fill in fills {
            // Update order tracker with fill
            self.order_tracker
                .blocking_write()
                .apply_fill(&fill.order_id.to_string(), fill.size);

            // Update position tracker with fill (including price for cost basis)
            let mut pt = self.position_tracker.blocking_write();
            pt.apply_fill_with_price(&fill.token_id, fill.side, fill.size, fill.price);
            // Update cash: BUY = -cost, SELL = +revenue
            let cash_delta = match fill.side {
                Side::BUY => -(fill.price * fill.size),
                Side::SELL => fill.price * fill.size,
            };
            pt.apply_cash_delta(cash_delta);
            drop(pt);

            // Store fill for history access
            self.fills.push(fill.clone());

            // Record to output
            self.recorder.record_fill(fill);
        }
    }

    /// Process a single order placement
    fn process_place_order(&mut self, params: &PlaceOrderParams) {
        match params.order_type {
            OrderType::FOK | OrderType::FAK => self.process_taker_order(params),
            _ => self.process_maker_order(params),
        }
    }

    /// Process a maker order (GTC/GTD): submit with latency, enter queue
    fn process_maker_order(&mut self, params: &PlaceOrderParams) {
        let latency = self.latency.sample() as i64;

        let order = SimOrder {
            order_id: 0, // Will be assigned by sim
            client_id: None,
            token_id: params.token_id.clone(),
            side: params.side,
            price: params.price,
            original_size: params.size,
            filled_size: Decimal::ZERO,
            status: SimOrderStatus::PendingNew,
            submit_ts: self.current_ts,
            ack_ts: self.current_ts + latency,
            cancel_req_ts: None,
            cancel_ack_ts: None,
            post_only: params.post_only,
            is_taker: false,
        };

        let order_id = self.sim.submit_order(order);

        // Insert into order tracker
        let tracked = TrackedOrder {
            order_id: order_id.to_string(),
            asset_id: params.token_id.clone(),
            market: self.config.condition_id.clone(),
            side: params.side,
            price: params.price,
            original_size: params.size,
            size_matched: Decimal::ZERO,
            status: TrackedOrderStatus::Live,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.order_tracker.blocking_write().insert(tracked);

        // Record submit event
        if let Some(order) = self.sim.get_order(order_id) {
            self.recorder
                .record_order_event(make_order_event(order, "SUBMIT", self.current_ts));
        }

        // Schedule ack
        self.pending_acks.push(PendingAction {
            ack_ts: self.current_ts + latency,
            order_id,
            action_type: PendingActionType::OrderAck,
        });
    }

    fn finalize_taker_order(
        &mut self,
        order_id: u64,
        filled_size: Decimal,
        sim_status: SimOrderStatus,
        tracked_status: TrackedOrderStatus,
        event_type: &str,
    ) {
        if let Some(order) = self.sim.get_order_mut(order_id) {
            order.status = sim_status;
            order.filled_size = filled_size;
        }

        if let Some(tracked) = self
            .order_tracker
            .blocking_write()
            .get_mut(&order_id.to_string())
        {
            tracked.size_matched = filled_size;
            tracked.status = tracked_status;
            tracked.updated_at = Utc::now();
        }

        if let Some(order) = self.sim.get_order(order_id) {
            self.recorder
                .record_order_event(make_order_event(order, event_type, self.current_ts));
        }
    }

    fn reject_taker_order(&mut self, order_id: u64, reason: &str) {
        let event_type = format!("REJECT:{reason}");
        self.finalize_taker_order(
            order_id,
            Decimal::ZERO,
            SimOrderStatus::Rejected,
            TrackedOrderStatus::Rejected,
            &event_type,
        );
    }

    /// Process a taker order submit (FOK/FAK): schedule ack-time execution with latency.
    fn process_taker_order(&mut self, params: &PlaceOrderParams) {
        let latency = self.latency.sample() as i64;

        let order = SimOrder {
            order_id: 0,
            client_id: None,
            token_id: params.token_id.clone(),
            side: params.side,
            price: params.price,
            original_size: params.size,
            filled_size: Decimal::ZERO,
            status: SimOrderStatus::PendingNew,
            submit_ts: self.current_ts,
            ack_ts: self.current_ts + latency,
            cancel_req_ts: None,
            cancel_ack_ts: None,
            post_only: false,
            is_taker: true,
        };
        let order_id = self.sim.submit_order(order);
        self.pending_taker_order_types
            .insert(order_id, params.order_type);

        let tracked = TrackedOrder {
            order_id: order_id.to_string(),
            asset_id: params.token_id.clone(),
            market: self.config.condition_id.clone(),
            side: params.side,
            price: params.price,
            original_size: params.size,
            size_matched: Decimal::ZERO,
            status: TrackedOrderStatus::Live,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        self.order_tracker.blocking_write().insert(tracked);

        if let Some(order) = self.sim.get_order(order_id) {
            self.recorder
                .record_order_event(make_order_event(order, "SUBMIT", self.current_ts));
        }

        self.pending_acks.push(PendingAction {
            ack_ts: self.current_ts + latency,
            order_id,
            action_type: PendingActionType::OrderAck,
        });
    }

    /// Execute a pending taker order at its ack time (after latency).
    fn process_taker_order_ack(&mut self, order_id: u64) {
        let order_type = match self.pending_taker_order_types.remove(&order_id) {
            Some(ot) => ot,
            None => return,
        };

        let (token_id, side, price, size, status) = match self.sim.get_order(order_id) {
            Some(order) => (
                order.token_id.clone(),
                order.side,
                order.price,
                order.original_size,
                order.status,
            ),
            None => return,
        };

        if status != SimOrderStatus::PendingNew {
            return;
        }

        if order_type == OrderType::FOK {
            let fillable = self.sim.taker_fillable_size(&token_id, side, price, size);
            if fillable < size {
                self.reject_taker_order(order_id, "FOK insufficient liquidity");
                return;
            }
        }

        if side == Side::BUY {
            let available_cash = self.sim.available_cash_for_taker();
            let max_cost = price * size;
            if available_cash > Decimal::ZERO && available_cash < max_cost {
                self.reject_taker_order(order_id, "Insufficient cash");
                return;
            }
        }

        if side == Side::SELL {
            let available = self.sim.available_inventory_for_taker(&token_id);
            if available < size {
                self.reject_taker_order(order_id, "Insufficient inventory");
                return;
            }
        }

        let mut fills = self
            .sim
            .execute_taker_order(&token_id, side, price, size, self.current_ts);
        let filled_size: Decimal = fills.iter().map(|f| f.size).sum();

        if filled_size <= Decimal::ZERO {
            self.reject_taker_order(order_id, "No fillable liquidity");
            return;
        }

        let event_type = match order_type {
            OrderType::FOK => "TAKER_FOK",
            OrderType::FAK => "TAKER_FAK",
            _ => "TAKER",
        };
        self.finalize_taker_order(
            order_id,
            filled_size,
            SimOrderStatus::Filled,
            TrackedOrderStatus::Filled,
            event_type,
        );

        // Stamp order_id onto fills.
        for fill in &mut fills {
            fill.order_id = order_id;
        }

        // Record fills and mirror them to position tracker.
        for fill in fills {
            let mut pt = self.position_tracker.blocking_write();
            pt.apply_fill_with_price(&fill.token_id, fill.side, fill.size, fill.price);
            let cash_delta = match fill.side {
                Side::BUY => -(fill.price * fill.size),
                Side::SELL => fill.price * fill.size,
            };
            pt.apply_cash_delta(cash_delta);
            drop(pt);

            self.fills.push(fill.clone());
            self.recorder.record_fill(fill);
        }
    }

    /// Process an action from the strategy
    fn process_action(&mut self, action: Action) {
        match action {
            Action::PlaceOrder {
                token_id,
                side,
                price,
                size,
                tick_size,
                neg_risk,
                order_type,
                post_only,
                condition_id,
            } => {
                // Convert to PlaceOrderParams and delegate
                let params = PlaceOrderParams {
                    token_id,
                    condition_id,
                    side,
                    price,
                    size,
                    tick_size,
                    neg_risk,
                    order_type,
                    post_only,
                };
                self.process_place_order(&params);
            }
            Action::PlaceOrders { orders } => {
                // Process each order in the batch
                for params in orders {
                    self.process_place_order(&params);
                }
            }
            Action::CancelOrder { cancel_type } => {
                let order_ids: Vec<u64> = match cancel_type {
                    CancelType::Single { order_id } => {
                        if let Ok(id) = order_id.parse::<u64>() {
                            vec![id]
                        } else {
                            vec![]
                        }
                    }
                    CancelType::Multiple { order_ids } => order_ids
                        .iter()
                        .filter_map(|id: &String| id.parse::<u64>().ok())
                        .collect(),
                    CancelType::All => self
                        .sim
                        .get_open_orders()
                        .iter()
                        .map(|o| o.order_id)
                        .collect(),
                };

                for order_id in order_ids {
                    if self.sim.request_cancel(order_id) {
                        let latency = self.latency.sample() as i64;

                        // Record cancel request
                        if let Some(order) = self.sim.get_order(order_id) {
                            self.recorder.record_order_event(make_order_event(
                                order,
                                "CANCEL_REQ",
                                self.current_ts,
                            ));
                        }

                        // Schedule cancel ack
                        self.pending_acks.push(PendingAction {
                            ack_ts: self.current_ts + latency,
                            order_id,
                            action_type: PendingActionType::CancelAck,
                        });
                    }
                }
            }
        }
    }

    /// Convert a FeatureRow to an engine Event
    fn feature_to_event(&self, feature: &FeatureRow) -> Option<Event> {
        let timestamp = Utc.timestamp_millis_opt(feature.timestamp_ms).single()?;

        // Compute window timing (15-minute windows)
        let window_duration_secs = 900i64;
        let window_start_secs =
            (feature.timestamp_ms / 1000 / window_duration_secs) * window_duration_secs;
        let window_start = Utc.timestamp_opt(window_start_secs, 0).single()?;
        let end_time = Utc
            .timestamp_opt(window_start_secs + window_duration_secs, 0)
            .single()?;

        // Build CoinbaseFeatures
        let coinbase_features = CoinbaseFeatures {
            mid: feature.coinbase_mid,
            microprice: feature.coinbase_microprice,
            spread: feature.coinbase_spread,
            imbalance_1: feature.coinbase_imb_1,
            imbalance_10: feature.coinbase_imb_10,
            imbalance_20: feature.coinbase_imb_20,
            imbalance_50: feature.coinbase_imb_50,
            imbalance_100: feature.coinbase_imb_100,
            liquidity_1bp: feature.coinbase_liq_1bp,
            liquidity_2bp: feature.coinbase_liq_2bp,
        };

        // Build Polymarket books from feature data (just best levels for context)
        let up_book = self.build_orderbook(&self.config.up_token_id);
        let down_book = self.build_orderbook(&self.config.down_token_id);

        // Use book_timestamp_ms from feature if available, otherwise fallback to timestamp
        let book_timestamp = feature
            .book_timestamp_ms
            .and_then(|ms| DateTime::from_timestamp_millis(ms))
            .unwrap_or(timestamp);

        // Construct volatility features from parquet data
        let volatility_features = VolatilityFeatures {
            sigma: feature.sigma,
            sigma_dyn: feature.sigma_dyn,
            ewma_variance: feature.ewma_variance,
            alpha: feature.alpha,
            sigma_1m: feature.sigma_1m.unwrap_or(feature.sigma),
            sigma_5m: feature.sigma_5m.unwrap_or(feature.sigma),
        };

        Some(Event::FairValueUpdated {
            condition_id: self.config.condition_id.clone(),
            timestamp,
            book_timestamp,
            window_start,
            end_time,
            tau_secs: feature.tau_secs,
            oracle_price: feature.oracle_price,
            reference_price: feature.reference_price,
            coinbase_features,
            delta_microprice_1s: feature.delta_microprice_1s,
            delta_microprice_2s: feature.delta_microprice_2s,
            delta_microprice_5s: feature.delta_microprice_5s,
            delta_imb_1_1s: feature.delta_imb_1_1s,
            delta_imb_1_2s: feature.delta_imb_1_2s,
            delta_imb_1_5s: feature.delta_imb_1_5s,
            volatility_features,
            blended_price: feature.blended_price,
            basis: feature.basis,
            blend_weight: feature.blend_weight,
            fair_up: feature.fair_up,
            fair_down: 1.0 - feature.fair_up,
            up_book,
            down_book,
        })
    }

    /// Build an OrderBook from the simulator
    fn build_orderbook(&self, token_id: &str) -> OrderBook {
        let sim_time = Utc
            .timestamp_millis_opt(self.current_ts)
            .single()
            .unwrap_or_else(Utc::now);

        let Some(book) = self.sim.get_book(token_id) else {
            warn!(
                token_id = token_id,
                timestamp_ms = self.current_ts,
                "No simulator book available for token"
            );
            return OrderBook {
                token_id: token_id.to_string(),
                timestamp: sim_time,
                bids: vec![],
                asks: vec![],
                sequence: 0,
            };
        };

        let bids: Vec<BookLevel> = book
            .bids
            .iter()
            .rev()
            .take(20)
            .map(|(&price, &size)| BookLevel { price, size })
            .collect();
        let asks: Vec<BookLevel> = book
            .asks
            .iter()
            .take(20)
            .map(|(&price, &size)| BookLevel { price, size })
            .collect();

        OrderBook {
            token_id: token_id.to_string(),
            timestamp: sim_time,
            bids,
            asks,
            sequence: 0,
        }
    }

    /// Get current simulation timestamp
    pub fn current_timestamp(&self) -> i64 {
        self.current_ts
    }

    /// Get reference to the exchange simulator
    pub fn simulator(&self) -> &ExchangeSimulator {
        &self.sim
    }

    /// Get reference to the shared order tracker
    pub fn order_tracker(&self) -> &SharedOrderTracker {
        &self.order_tracker
    }

    /// Get reference to the shared position tracker
    pub fn position_tracker(&self) -> &SharedPositionTracker {
        &self.position_tracker
    }

    /// Get all fills recorded during the backtest
    pub fn all_fills(&self) -> &[Fill] {
        &self.fills
    }
}
