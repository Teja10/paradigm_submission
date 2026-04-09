//! Market discovery for BTC 15-minute up/down markets

use crate::types::MarketState;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use polyfill_rs::{ClobClient, Market};
use regex::Regex;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Cache for markets that can be queried efficiently
pub struct MarketCache {
    markets: HashMap<String, Market>,
    last_full_fetch: Option<Instant>,
    last_incremental_fetch: Option<Instant>,
}

impl MarketCache {
    pub fn new() -> Self {
        Self {
            markets: HashMap::new(),
            last_full_fetch: None,
            last_incremental_fetch: None,
        }
    }

    /// Full fetch of all markets (expensive, do once at startup or daily)
    pub async fn full_fetch(&mut self, client: &ClobClient) -> Result<usize> {
        let start = Instant::now();
        let mut cursor: Option<String> = None;
        let mut count = 0;

        println!("Starting full market fetch...");

        loop {
            let response = client
                .get_markets(cursor.as_deref())
                .await
                .map_err(|e| anyhow!("Failed to fetch markets: {:?}", e))?;

            count += response.data.len();

            for market in response.data {
                self.markets.insert(market.condition_id.clone(), market);
            }

            if count % 50000 == 0 {
                println!("  Fetched {} markets...", count);
            }

            match response.next_cursor {
                Some(next) if next != "LTE=" => cursor = Some(next),
                _ => break,
            }
        }

        self.last_full_fetch = Some(Instant::now());
        println!(
            "Full fetch complete: {} markets in {:.1}s",
            count,
            start.elapsed().as_secs_f32()
        );

        Ok(count)
    }

    /// Incremental fetch using sampling-markets endpoint (returns active markets)
    pub async fn incremental_fetch(&mut self, client: &ClobClient) -> Result<usize> {
        let start = Instant::now();
        let mut cursor: Option<String> = None;
        let mut new_count = 0;
        let mut pages = 0;
        const MAX_PAGES: usize = 100; // Fetch more pages to find 15-min markets

        loop {
            pages += 1;
            // Use get_sampling_markets which returns active markets
            let response = client
                .get_sampling_markets(cursor.as_deref())
                .await
                .map_err(|e| anyhow!("Failed to fetch markets: {:?}", e))?;

            let batch_size = response.data.len();
            if batch_size == 0 {
                break;
            }

            let mut found_existing = false;
            for market in response.data {
                if self.markets.contains_key(&market.condition_id) {
                    found_existing = true;
                } else {
                    new_count += 1;
                }
                self.markets.insert(market.condition_id.clone(), market);
            }

            if found_existing || pages >= MAX_PAGES {
                break;
            }

            match response.next_cursor {
                Some(next) if next != "LTE=" && !next.is_empty() => cursor = Some(next),
                _ => break,
            }
        }

        self.last_incremental_fetch = Some(Instant::now());
        println!(
            "Fetched {} markets in {} pages ({:.1}s)",
            new_count,
            pages,
            start.elapsed().as_secs_f32()
        );

        Ok(new_count)
    }

    /// Check if cache needs refresh
    pub fn needs_refresh(&self, max_age: Duration) -> bool {
        match self.last_incremental_fetch.or(self.last_full_fetch) {
            Some(t) => t.elapsed() > max_age,
            None => true,
        }
    }

    /// Query markets with a filter function
    pub fn query<F>(&self, filter: F) -> Vec<&Market>
    where
        F: Fn(&Market) -> bool,
    {
        self.markets.values().filter(|m| filter(m)).collect()
    }

    /// Get a market by condition ID
    pub fn get(&self, condition_id: &str) -> Option<&Market> {
        self.markets.get(condition_id)
    }

    /// Get count of cached markets
    pub fn len(&self) -> usize {
        self.markets.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.markets.is_empty()
    }
}

/// Filter for Bitcoin 15-minute up/down markets
pub fn is_btc_15min_up_down(market: &Market) -> bool {
    let q = market.question.to_lowercase();
    let is_btc = q.contains("bitcoin");
    let is_up_down = q.contains("up or down");
    // 15-min markets have time ranges like "6:00AM-6:15AM ET"
    let is_15min = (q.contains(":00") && q.contains(":15"))
        || (q.contains(":15") && q.contains(":30"))
        || (q.contains(":30") && q.contains(":45"))
        || (q.contains(":45") && q.contains(":00"));

    market.active && !market.closed && is_btc && is_up_down && is_15min
}

/// Parse the reference price from a market question
/// Example: "Will Bitcoin be up or down from $94,450.79 between 6:00AM-6:15AM ET on December 29?"
pub fn parse_reference_price(question: &str) -> Option<f64> {
    let re = Regex::new(r"\$([0-9,]+\.?\d*)").ok()?;
    let caps = re.captures(question)?;
    let price_str = caps.get(1)?.as_str().replace(',', "");
    price_str.parse().ok()
}

/// Parse the end time from a market's end_date_iso field
pub fn parse_end_time(market: &Market) -> Option<DateTime<Utc>> {
    market
        .end_date_iso
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Gamma API response structures
#[derive(Debug, serde::Deserialize)]
struct GammaEvent {
    slug: String,
    title: String,
    markets: Vec<GammaMarket>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    question: String,
    condition_id: String,
    end_date: Option<String>,
    clob_token_ids: Option<String>,
    active: bool,
    closed: bool,
}

/// Fetch a specific 15-minute market window via Gamma API
///
/// # Arguments
/// * `window_start` - The start time of the 15-minute window (should be aligned to :00, :15, :30, :45)
///
/// # Returns
/// * `Some(MarketState)` if a valid market exists for this window
/// * `None` if no market found or market already ended
pub async fn fetch_market_for_window(window_start: DateTime<Utc>) -> Option<MarketState> {
    let now = Utc::now();
    let timestamp = window_start.timestamp();
    let slug = format!("btc-updown-15m-{}", timestamp);

    tracing::debug!("Fetching Gamma API for event: {}", slug);

    // Fetch from Gamma API
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let response = reqwest::get(&url).await.ok()?;
    let events: Vec<GammaEvent> = response.json().await.ok()?;

    if events.is_empty() {
        return None;
    }

    let event = &events[0];

    // Find active market in this event
    for market in &event.markets {
        if !market.active || market.closed {
            continue;
        }

        // Parse token IDs from clob_token_ids JSON string
        let token_ids: Vec<String> = market
            .clob_token_ids
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        if token_ids.len() < 2 {
            continue;
        }

        // Parse end time
        let end_time = market
            .end_date
            .as_ref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))?;

        // Check if market hasn't ended
        if end_time <= now {
            continue;
        }

        return Some(MarketState {
            condition_id: market.condition_id.clone(),
            up_token_id: token_ids[0].clone(),
            down_token_id: token_ids[1].clone(),
            reference_price: 0.0, // Captured from oracle at market start
            window_start,
            end_time,
            question: market.question.clone(),
        });
    }

    None
}

/// Align a datetime to the nearest 15-minute window start
pub fn align_to_15min_window(time: DateTime<Utc>) -> Option<DateTime<Utc>> {
    use chrono::Timelike;
    let minute = time.minute();
    let window_minute = (minute / 15) * 15;
    time.with_minute(window_minute)
        .and_then(|t| t.with_second(0))
        .and_then(|t| t.with_nanosecond(0))
}

/// Try to find BTC 15-minute market via Gamma API (searches current + next few windows)
async fn find_btc_15min_via_gamma() -> Option<MarketState> {
    let now = Utc::now();
    let current_window = align_to_15min_window(now)?;

    // Try current window and a few future windows
    for offset_mins in [0i64, 15, 30, 45] {
        let window = current_window + chrono::Duration::minutes(offset_mins);
        if let Some(market) = fetch_market_for_window(window).await {
            tracing::info!("Found market for window {}: {}", window, market.question);
            return Some(market);
        }
    }

    None
}

/// Find the current active 15-minute BTC market
pub async fn find_current_market(client: &ClobClient) -> Result<MarketState> {
    // First try to find via Gamma API for BTC updown events
    if let Some(market) = find_btc_15min_via_gamma().await {
        return Ok(market);
    }

    // Fallback to cache search
    let mut cache = MarketCache::new();
    cache.incremental_fetch(client).await?;

    // Debug: Show some sample markets to understand the format
    println!("Searching {} markets...", cache.markets.len());

    // Find all active 15-min BTC markets
    let markets = cache.query(is_btc_15min_up_down);

    // Debug: If none found, show some BTC markets to understand the format
    if markets.is_empty() {
        println!("No 15-min BTC markets matched. Looking for BTC markets...");

        // Show count of active markets
        let active_count = cache.markets.values().filter(|m| m.active && !m.closed).count();
        println!("  Active markets: {} out of {}", active_count, cache.markets.len());

        let btc_markets: Vec<_> = cache.markets.values()
            .filter(|m| m.question.to_lowercase().contains("bitcoin") && m.active && !m.closed)
            .take(10)
            .collect();
        println!("  Found {} active BTC markets", btc_markets.len());
        for m in btc_markets.iter().take(5) {
            println!("    {}", m.question);
        }

        // Also look for any "up or down" markets
        let up_down_markets: Vec<_> = cache.markets.values()
            .filter(|m| m.question.to_lowercase().contains("up or down") && m.active && !m.closed)
            .take(10)
            .collect();
        println!("  Found {} active 'up or down' markets:", up_down_markets.len());
        for m in up_down_markets.iter().take(5) {
            println!("    {}", m.question);
        }

        // Look for markets with btc/updown in slug
        let btc_slug_markets: Vec<_> = cache.markets.values()
            .filter(|m| {
                let slug = m.market_slug.to_lowercase();
                (slug.contains("btc") || slug.contains("bitcoin")) && m.active && !m.closed
            })
            .take(10)
            .collect();
        println!("  Found {} markets with btc in slug:", btc_slug_markets.len());
        for m in btc_slug_markets.iter().take(5) {
            println!("    {} -> {}", m.market_slug, m.question);
        }

        // Look for markets with 15m or updown in slug
        let updown_slug_markets: Vec<_> = cache.markets.values()
            .filter(|m| {
                let slug = m.market_slug.to_lowercase();
                (slug.contains("updown") || slug.contains("15m") || slug.contains("hourly")) && m.active && !m.closed
            })
            .take(10)
            .collect();
        println!("  Found {} markets with updown/15m/hourly in slug:", updown_slug_markets.len());
        for m in updown_slug_markets.iter().take(5) {
            println!("    {} -> {}", m.market_slug, m.question);
        }

        // Look for neg_risk markets (binary options often use this)
        let neg_risk_markets: Vec<_> = cache.markets.values()
            .filter(|m| m.neg_risk && m.active && !m.closed)
            .take(10)
            .collect();
        println!("  Found {} active neg_risk markets:", neg_risk_markets.len());
        for m in neg_risk_markets.iter().take(5) {
            println!("    {} -> {}", m.market_slug, m.question);
        }

        // Show a few random active markets to see what's there
        let any_active: Vec<_> = cache.markets.values()
            .filter(|m| m.active && !m.closed)
            .take(3)
            .collect();
        println!("  Sample active markets:");
        for m in &any_active {
            println!("    {}", m.question);
        }
    }

    if markets.is_empty() {
        return Err(anyhow!("No active BTC 15-minute markets found"));
    }

    // Find the market that ends soonest (current market)
    let now = Utc::now();
    let mut best_market: Option<(&Market, DateTime<Utc>)> = None;

    for market in markets {
        if let Some(end_time) = parse_end_time(market) {
            // Only consider markets that haven't ended yet
            if end_time > now {
                match &best_market {
                    None => best_market = Some((market, end_time)),
                    Some((_, best_end)) if end_time < *best_end => {
                        best_market = Some((market, end_time));
                    }
                    _ => {}
                }
            }
        }
    }

    let (market, end_time) = best_market.ok_or_else(|| anyhow!("No current market found"))?;

    // Parse reference price from question
    let reference_price = parse_reference_price(&market.question)
        .ok_or_else(|| anyhow!("Could not parse reference price from: {}", market.question))?;

    // Get UP/DOWN token IDs
    // tokens[0] is typically "Yes" (UP), tokens[1] is "No" (DOWN)
    let (up_token_id, down_token_id) = if market.tokens[0].outcome.to_lowercase().contains("yes")
        || market.tokens[0].outcome.to_lowercase().contains("up") {
        (market.tokens[0].token_id.clone(), market.tokens[1].token_id.clone())
    } else {
        (market.tokens[1].token_id.clone(), market.tokens[0].token_id.clone())
    };

    // window_start is 15 minutes before end_time
    let window_start = end_time - chrono::Duration::minutes(15);

    Ok(MarketState {
        condition_id: market.condition_id.clone(),
        up_token_id,
        down_token_id,
        reference_price,
        window_start,
        end_time,
        question: market.question.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_reference_price() {
        let q = "Will Bitcoin be up or down from $94,450.79 between 6:00AM-6:15AM ET on December 29?";
        assert_eq!(parse_reference_price(q), Some(94450.79));

        let q2 = "Will Bitcoin be up or down from $100,000.00 between 12:00PM-12:15PM ET?";
        assert_eq!(parse_reference_price(q2), Some(100000.00));
    }
}
