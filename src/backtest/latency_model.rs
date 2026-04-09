//! Latency model for simulating network delays

use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

/// Latency model with seeded RNG for determinism
pub struct LatencyModel {
    rng: StdRng,
    min_ms: u64,
    max_ms: u64,
}

impl LatencyModel {
    /// Create a new latency model with given seed and range
    pub fn new(seed: u64, min_ms: u64, max_ms: u64) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            min_ms,
            max_ms,
        }
    }

    /// Sample a random latency in milliseconds
    pub fn sample(&mut self) -> u64 {
        self.rng.gen_range(self.min_ms..=self.max_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determinism() {
        let mut model1 = LatencyModel::new(42, 40, 100);
        let mut model2 = LatencyModel::new(42, 40, 100);

        for _ in 0..100 {
            assert_eq!(model1.sample(), model2.sample());
        }
    }

    #[test]
    fn test_range() {
        let mut model = LatencyModel::new(123, 40, 100);

        for _ in 0..1000 {
            let sample = model.sample();
            assert!(sample >= 40 && sample <= 100);
        }
    }
}
