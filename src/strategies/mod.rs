//! Strategies - process events and produce actions
//!
//! Strategies consume events (including derived events like FairValueUpdated)
//! and emit trading actions (PlaceOrder, CancelOrder).
//!
//! Note: Pricing logic has been moved to models (BinaryOptionModel).

pub mod basic_mm;
pub mod ladder_mm;

pub use basic_mm::{BasicMarketMaker, BasicMmConfig};
pub use ladder_mm::{LadderMarketMaker, LadderMmConfig};

