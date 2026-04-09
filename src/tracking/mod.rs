//! Order and position tracking

mod active_market;
mod order_tracker;
mod position_tracker;

use rust_decimal::Decimal;

pub use active_market::{ActiveMarket, MarketInfo, SharedActiveMarket};
pub use order_tracker::{OrderTracker, SharedOrderTracker, TrackedOrder, TrackedOrderStatus};
pub use position_tracker::{MarketPosition, PositionTracker, SharedPositionTracker};

#[derive(Debug, Clone)]
pub struct MarketAccountSnapshot {
    pub open_orders: Vec<TrackedOrder>,
    pub position: Option<MarketPosition>,
    pub cash: Decimal,
}

/// Read the order and position trackers under a consistent lock order and
/// return a cloned market-level snapshot for quoting logic and tests.
pub async fn snapshot_market_account(
    order_tracker: &SharedOrderTracker,
    position_tracker: &SharedPositionTracker,
    condition_id: &str,
) -> MarketAccountSnapshot {
    let order_guard = order_tracker.read().await;
    let position_guard = position_tracker.read().await;

    let open_orders = order_guard
        .open_orders()
        .into_iter()
        .filter(|o| o.market == condition_id)
        .collect();

    let position = position_guard.get_position(condition_id).cloned();
    let cash = position_guard.cash();

    MarketAccountSnapshot {
        open_orders,
        position,
        cash,
    }
}
