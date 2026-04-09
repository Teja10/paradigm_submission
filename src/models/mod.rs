//! Models - process events and produce derived events
//!
//! Models sit between collectors and strategies, computing derived state
//! (like fair values) and emitting derived events that strategies can consume.

pub mod binary_option;
pub mod fair_value_logger;
pub mod backtest_logger;

pub use binary_option::BinaryOptionModel;
pub use fair_value_logger::FairValueLoggerModel;
pub use backtest_logger::BacktestLoggerModel;
