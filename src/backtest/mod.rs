//! Backtest engine for simulating trading strategies on historical data

pub mod config;
pub mod engine;
pub mod event_loader;
pub mod exchange_sim;
pub mod latency_model;
pub mod queue_model;
pub mod recorder;
pub mod types;

#[cfg(test)]
mod tests;

// Re-export main types for convenience
pub use config::{BacktestConfig, DeltaMode};
pub use engine::BacktestEngine;
pub use event_loader::EventLoader;
pub use exchange_sim::{ExchangeSimulator, L2Book};
pub use latency_model::LatencyModel;
pub use queue_model::{QueueKey, QueueLevel, QueueModel};
pub use recorder::Recorder;
pub use types::{
    BacktestEvent, EventPayload, EventPriority, FeatureRow, Fill, OrderEvent, PendingAction,
    PendingActionType, QueueEntry, SimOrder, SimOrderStatus,
};
