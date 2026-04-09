//! Binary option pricing for BTC 15-minute up/down markets
//!
//! Pricing model: P(S_T > K) = Φ(d) where d = ln(S/K) / (σ√τ)

use std::f64::consts::FRAC_1_SQRT_2;

/// Standard normal CDF using error function
fn normal_cdf(x: f64) -> f64 {
    0.5 * (1.0 + libm::erf(x * FRAC_1_SQRT_2))
}

/// Price UP share: P(S_T > K) = Φ(d)
/// where d = ln(S/K) / (σ√τ)
///
/// # Arguments
/// * `s` - Current oracle price
/// * `k` - Reference price (strike)
/// * `tau_secs` - Time remaining in seconds
/// * `sigma` - Annualized volatility (e.g., 0.60 for 60%)
pub fn price_up_share(s: f64, k: f64, tau_secs: f64, sigma: f64) -> f64 {
    // At expiry or past it, binary payout
    if tau_secs <= 0.0 {
        return if s > k { 1.0 } else { 0.0 };
    }

    // Edge case: K is 0 or negative (shouldn't happen in practice)
    if k <= 0.0 {
        return 0.5; // No valid reference price yet
    }

    // Convert seconds to years for volatility scaling
    let tau_years = tau_secs / (365.25 * 24.0 * 3600.0);
    let d = (s / k).ln() / (sigma * tau_years.sqrt());
    normal_cdf(d)
}

/// Price DOWN share: 1 - P(UP)
pub fn price_down_share(s: f64, k: f64, tau_secs: f64, sigma: f64) -> f64 {
    1.0 - price_up_share(s, k, tau_secs, sigma)
}

/// Result of price blending between oracle and Coinbase microprice
#[derive(Debug, Clone, Copy)]
pub struct BlendResult {
    /// Blended price S(t)
    pub blended_price: f64,
    /// Basis = coinbase_microprice - oracle_price
    pub basis: f64,
    /// Blend weight w (0 = all oracle, 1 = all coinbase)
    pub blend_weight: f64,
}

/// Blend oracle and Coinbase microprice using sigmoid weighting
///
/// Formula:
/// - basis = coinbase_microprice - oracle_price
/// - w = 1 / (1 + exp(-(-3.0 + 0.25 * |basis|)))
/// - S(t) = (1-w)*oracle_price + w*coinbase_microprice
///
/// Intuition:
/// - Small basis → mostly oracle (w close to 0.12)
/// - Large basis → shifts toward coinbase
pub fn blend_price(oracle_price: f64, coinbase_microprice: f64) -> BlendResult {
    let basis = coinbase_microprice - oracle_price;
    let w = 1.0 / (1.0 + (-(-3.0 + 0.25 * basis.abs())).exp());
    let blended_price = (1.0 - w) * oracle_price + w * coinbase_microprice;

    BlendResult {
        blended_price,
        basis,
        blend_weight: w,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_the_money() {
        // When S = K, P(UP) should be ~0.5
        let p = price_up_share(100.0, 100.0, 900.0, 0.60);
        assert!((p - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_deep_in_the_money() {
        // When S >> K, P(UP) should be close to 1
        let p = price_up_share(105.0, 100.0, 900.0, 0.60);
        assert!(p > 0.9);
    }

    #[test]
    fn test_deep_out_of_money() {
        // When S << K, P(UP) should be close to 0
        let p = price_up_share(95.0, 100.0, 900.0, 0.60);
        assert!(p < 0.1);
    }

    #[test]
    fn test_at_expiry() {
        // At expiry, binary payout
        assert_eq!(price_up_share(101.0, 100.0, 0.0, 0.60), 1.0);
        assert_eq!(price_up_share(99.0, 100.0, 0.0, 0.60), 0.0);
    }
}
