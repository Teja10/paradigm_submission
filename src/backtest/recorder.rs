//! Recorder for writing backtest results to parquet files

use anyhow::Result;
use polars::prelude::*;
use polyfill_rs::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::path::Path;

use super::types::{Fill, OrderEvent};

/// Recorder for backtest results
pub struct Recorder {
    fills: Vec<Fill>,
    order_events: Vec<OrderEvent>,
    output_dir: String,
}

impl Recorder {
    pub fn new(output_dir: String) -> Self {
        std::fs::create_dir_all(&output_dir).ok();
        Self {
            fills: Vec::new(),
            order_events: Vec::new(),
            output_dir,
        }
    }

    pub fn record_fill(&mut self, fill: Fill) {
        self.fills.push(fill);
    }

    pub fn record_order_event(&mut self, event: OrderEvent) {
        self.order_events.push(event);
    }

    /// Finalize and write all results to parquet files
    pub fn finalize(&self) -> Result<()> {
        self.write_fills()?;
        self.write_orders()?;
        self.write_summary_json()?;
        self.print_summary();
        Ok(())
    }

    fn write_fills(&self) -> Result<()> {
        if self.fills.is_empty() {
            tracing::info!("No fills to write");
            return Ok(());
        }

        let timestamps: Vec<i64> = self.fills.iter().map(|f| f.fill_ts).collect();
        let fill_ids: Vec<u64> = self.fills.iter().map(|f| f.fill_id).collect();
        let order_ids: Vec<u64> = self.fills.iter().map(|f| f.order_id).collect();
        let token_ids: Vec<String> = self.fills.iter().map(|f| f.token_id.clone()).collect();
        let sides: Vec<String> = self
            .fills
            .iter()
            .map(|f| match f.side {
                Side::BUY => "buy".to_string(),
                Side::SELL => "sell".to_string(),
            })
            .collect();
        let prices: Vec<f64> = self
            .fills
            .iter()
            .map(|f| f.price.to_f64().unwrap_or(0.0))
            .collect();
        let sizes: Vec<f64> = self
            .fills
            .iter()
            .map(|f| f.size.to_f64().unwrap_or(0.0))
            .collect();
        let trade_ids: Vec<Option<String>> = self.fills.iter().map(|f| f.trade_id.clone()).collect();

        let mut df = df! {
            "timestamp_ms" => &timestamps,
            "fill_id" => &fill_ids,
            "order_id" => &order_ids,
            "token_id" => &token_ids,
            "side" => &sides,
            "price" => &prices,
            "size" => &sizes,
            "trade_id" => &trade_ids,
        }?;

        let path = Path::new(&self.output_dir).join("fills.parquet");
        let file = std::fs::File::create(&path)?;
        ParquetWriter::new(file).finish(&mut df)?;

        tracing::info!("Wrote {} fills to {:?}", self.fills.len(), path);
        Ok(())
    }

    fn write_orders(&self) -> Result<()> {
        if self.order_events.is_empty() {
            tracing::info!("No order events to write");
            return Ok(());
        }

        let timestamps: Vec<i64> = self.order_events.iter().map(|e| e.timestamp_ms).collect();
        let order_ids: Vec<u64> = self.order_events.iter().map(|e| e.order_id).collect();
        let event_types: Vec<String> = self.order_events.iter().map(|e| e.event_type.clone()).collect();
        let token_ids: Vec<String> = self.order_events.iter().map(|e| e.token_id.clone()).collect();
        let sides: Vec<String> = self
            .order_events
            .iter()
            .map(|e| match e.side {
                Side::BUY => "buy".to_string(),
                Side::SELL => "sell".to_string(),
            })
            .collect();
        let prices: Vec<f64> = self
            .order_events
            .iter()
            .map(|e| e.price.to_f64().unwrap_or(0.0))
            .collect();
        let sizes: Vec<f64> = self
            .order_events
            .iter()
            .map(|e| e.size.to_f64().unwrap_or(0.0))
            .collect();
        let statuses: Vec<String> = self
            .order_events
            .iter()
            .map(|e| format!("{:?}", e.status))
            .collect();

        let mut df = df! {
            "timestamp_ms" => &timestamps,
            "order_id" => &order_ids,
            "event_type" => &event_types,
            "token_id" => &token_ids,
            "side" => &sides,
            "price" => &prices,
            "size" => &sizes,
            "status" => &statuses,
        }?;

        let path = Path::new(&self.output_dir).join("orders.parquet");
        let file = std::fs::File::create(&path)?;
        ParquetWriter::new(file).finish(&mut df)?;

        tracing::info!("Wrote {} order events to {:?}", self.order_events.len(), path);
        Ok(())
    }

    fn write_summary_json(&self) -> Result<()> {
        let buy_fills = self.fills.iter().filter(|f| f.side == Side::BUY).count();
        let sell_fills = self.fills.len() - buy_fills;
        let total_volume: f64 = self
            .fills
            .iter()
            .map(|f| (f.size * f.price).to_f64().unwrap_or(0.0))
            .sum();
        let submits = self.order_events.iter().filter(|e| e.event_type == "SUBMIT").count();
        let acks = self.order_events.iter().filter(|e| e.event_type == "ACK").count();
        let rejects = self.order_events.iter().filter(|e| e.event_type.starts_with("REJECT")).count();
        let cancels = self.order_events.iter().filter(|e| e.event_type == "CANCEL_ACK").count();
        let fill_rate = if acks > 0 {
            self.fills.len() as f64 / acks as f64 * 100.0
        } else {
            0.0
        };

        let json = format!(
            r#"{{"fills":{},"buy_fills":{},"sell_fills":{},"volume":{:.2},"submits":{},"acks":{},"rejects":{},"cancels":{},"fill_rate":{:.1}}}"#,
            self.fills.len(), buy_fills, sell_fills, total_volume, submits, acks, rejects, cancels, fill_rate
        );

        let path = Path::new(&self.output_dir).join("summary.json");
        std::fs::write(&path, &json)?;
        Ok(())
    }

    fn print_summary(&self) {
        println!("\n=== Backtest Summary ===");
        println!("Total fills: {}", self.fills.len());
        println!("Total order events: {}", self.order_events.len());

        if !self.fills.is_empty() {
            let total_volume: Decimal = self.fills.iter().map(|f| f.size * f.price).sum();
            let buy_fills = self.fills.iter().filter(|f| f.side == Side::BUY).count();
            let sell_fills = self.fills.iter().filter(|f| f.side == Side::SELL).count();

            println!("Buy fills: {}", buy_fills);
            println!("Sell fills: {}", sell_fills);
            println!("Total volume: ${:.2}", total_volume.to_f64().unwrap_or(0.0));
        }

        // Order lifecycle stats
        let submits = self.order_events.iter().filter(|e| e.event_type == "SUBMIT").count();
        let acks = self.order_events.iter().filter(|e| e.event_type == "ACK").count();
        let rejects = self.order_events.iter().filter(|e| e.event_type.starts_with("REJECT")).count();
        let cancels = self.order_events.iter().filter(|e| e.event_type == "CANCEL_ACK").count();

        println!("\nOrder lifecycle:");
        println!("  Submitted: {}", submits);
        println!("  Acknowledged: {}", acks);
        println!("  Rejected: {}", rejects);
        println!("  Canceled: {}", cancels);

        if acks > 0 {
            let fill_rate = self.fills.len() as f64 / acks as f64 * 100.0;
            println!("  Fill rate: {:.1}% ({} fills / {} acks)", fill_rate, self.fills.len(), acks);
        }

        println!("========================\n");
    }

    /// Get current fill count
    pub fn fill_count(&self) -> usize {
        self.fills.len()
    }

    /// Get current order event count
    pub fn order_event_count(&self) -> usize {
        self.order_events.len()
    }
}

/// Helper to create an order event from a SimOrder
pub fn make_order_event(
    order: &super::types::SimOrder,
    event_type: &str,
    timestamp_ms: i64,
) -> OrderEvent {
    OrderEvent {
        timestamp_ms,
        order_id: order.order_id,
        event_type: event_type.to_string(),
        token_id: order.token_id.clone(),
        side: order.side,
        price: order.price,
        size: order.original_size,
        status: order.status,
    }
}
