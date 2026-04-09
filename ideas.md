# Ideas Scratchpad

## Brainstormed Ideas

### Fair-value quoting
**Hypothesis**: Quoting at `fair_up`/`fair_down` instead of TOB should reduce adverse selection — the model's fair value incorporates oracle+coinbase information that pure TOB quoting ignores.
**Changes**: In `quote_market()`, compute desired price from `fair_up`/`fair_down` (rounded to tick) instead of `best_bid`/`best_ask`. Need to pass fair values into the strategy.
**Param grid**: Sweep offset from fair value in ticks (-2 to +2).

### Multi-timeframe momentum
**Hypothesis**: Using only `delta_microprice_5s` misses fast momentum shifts. Combining 1s+2s+5s signals (e.g., weighted sum) should catch adverse moves earlier.
**Changes**: Modify `should_quote()` to use a weighted combination of all three delta signals.
**Param grid**: Sweep weights for 1s/2s/5s contributions.

### Inventory skew
**Hypothesis**: When holding inventory, skew quotes toward reducing position — tighter ask when long, tighter bid when short. This should improve position turnover and reduce inventory risk.
**Changes**: In `quote_market()`, adjust bid/ask prices based on current position relative to max_position.
**Param grid**: Sweep skew_factor (how aggressively to skew per unit of inventory).

### Coinbase imbalance filter
**Hypothesis**: Coinbase orderbook imbalance (imb_1, imb_10) should predict short-term direction. Pulling quotes when imbalance is strongly against us should reduce adverse selection.
**Changes**: Add coinbase imbalance fields to strategy state, add an imbalance-based filter similar to momentum filter.
**Param grid**: Sweep imbalance thresholds.

### Volatility-scaled momentum threshold
**Hypothesis**: A fixed momentum threshold doesn't account for varying volatility regimes. Scaling the threshold by `sigma_dyn` should make the filter adaptive — more permissive in calm markets, stricter in volatile ones.
**Changes**: Replace fixed `momentum_threshold` with `momentum_threshold * sigma_dyn / sigma_baseline`.
**Param grid**: Sweep sigma_baseline values.

## In Progress

### Fair-value quoting
**Hypothesis**: Quoting at `fair_up`/`fair_down` instead of TOB should reduce adverse selection — the model's fair value incorporates oracle+coinbase information that pure TOB quoting ignores.
**Changes**: In `quote_market()`, compute desired price from `fair_up`/`fair_down` (rounded to tick) instead of `best_bid`/`best_ask`. Add `fair_value_offset` config param (ticks to offset from fair). Need to pass fair values into the strategy and cache them.
**Param grid**: Sweep `fair_value_offset` in [-2, -1, 0, 1, 2] ticks, combined with momentum thresholds.

## Completed (Kept)

(none)

## Failed Ideas

(none)
