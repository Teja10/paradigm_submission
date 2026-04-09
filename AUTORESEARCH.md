# Autoresearch: Basic MM Strategy

Autonomous experimentation loop for improving the `BasicMarketMaker` strategy via backtesting. The LLM acts as a researcher: proposing ideas, running experiments, keeping what works, discarding what doesn't.

## Setup

To set up a new autoresearch session:

1. **Agree on a run tag**: propose a tag based on today's date (e.g. `apr9`). The branch `autoresearch/<tag>` must not already exist.
2. **Create the branch**: `git checkout -b autoresearch/<tag>` from `main`.
3. **Read the in-scope files** for full context:

   Modifiable files:

   | File | Purpose |
   |------|---------|
   | `src/strategies/basic_mm.rs` | Strategy logic (quoting, momentum filter, parity enforcement) |
   | `src/backtest/config.rs` | Backtest + strategy config (modify only if adding new strategy params) |
   | `analysis/sweep.py` | Param grid definition and sweep runner |

   Read-only context:

   | File | Purpose |
   |------|---------|
   | `src/backtest/engine.rs` | Backtest simulation engine |
   | `src/backtest/types.rs` | Event types, FeatureRow (available data fields) |
   | `src/engine/types.rs` | Event, Action, PlaceOrderParams definitions |
   | `src/backtest/exchange_sim.rs` | Exchange simulator (order matching) |
   | `src/backtest/recorder.rs` | Results recording (fills, orders, summary.json) |
   | `src/tracking/` | Order and position tracking |
   | `analysis/backtest_runner.py` | Python analysis utilities |

4. **Read the ideas scratchpad**: Read `ideas.md`.
5. **Build**: `cargo build --release`
6. **Run the baseline**: `python analysis/sweep.py --parallel 4 --cleanup` (uses sample_data or parquet_files)
7. **Record baseline results**: Note the top bps_per_dollar from `results.tsv`.
8. **Confirm and go**: Kick off the experimentation loop.

## Strategy Context

`BasicMarketMaker` is a **touch market maker** for Polymarket prediction markets:

1. **Quote at TOB**: Place POST_ONLY bid at best_bid and ask at best_ask for both UP and DOWN tokens.
2. **Momentum filter**: Pull quotes when `position_direction * delta_microprice_5s < threshold`. Uses tighter threshold during high volatility (`sigma_dyn / sigma_5m > vol_ratio_threshold`).
3. **Parity enforcement**: Prevents quoting both sides when UP+DOWN prices would violate the $1.00 complete-set constraint.
4. **Position limits**: Respects `max_position` per token side.

### Available Features (from FairValueUpdated event)

These fields are available to the strategy on every feature update:

| Feature | Description |
|---------|-------------|
| `delta_microprice_1s` | Microprice change over 1 second |
| `delta_microprice_2s` | Microprice change over 2 seconds |
| `delta_microprice_5s` | Microprice change over 5 seconds |
| `delta_imb_1_1s/2s/5s` | Imbalance-1 change over 1/2/5 seconds |
| `sigma_dyn` | Dynamic volatility (12s window) |
| `sigma_5m` | 5-minute window volatility |
| `sigma_1m` | 1-minute window volatility |
| `sigma` | Base volatility |
| `fair_up` / `fair_down` | Model fair values for UP/DOWN tokens |
| `blended_price` | Blended oracle/coinbase price |
| `basis` | Basis between oracle and market |
| `blend_weight` | Weight given to coinbase in blend |
| `coinbase_features` | Coinbase mid, microprice, spread, imbalances (1/10/20/50/100), liquidity (1bp/2bp) |
| `oracle_price` | Current oracle price |
| `reference_price` | Strike/reference price |
| `up_book` / `down_book` | Full orderbooks (20 levels each) |

### Key Research Directions

The strategy currently only uses `delta_microprice_5s` for a binary pull/no-pull decision. Potential improvements:

- **Use fair_up/fair_down**: Quote at fair value instead of TOB
- **Multi-timeframe momentum**: Combine 1s/2s/5s signals
- **Volatility regime**: Adjust quoting aggressiveness based on sigma levels
- **Coinbase imbalance signals**: Use external exchange data for positioning
- **Basis trading**: Exploit oracle-market basis
- **Position-aware pricing**: Skew quotes based on current inventory
- **Dynamic order sizing**: Size based on signal strength
- **Spread management**: Quote inside/outside TOB based on conditions

## Parameters

### Fixed (do not change)

| Parameter | Value | Reason |
|-----------|-------|--------|
| `order_size` | 10 | Fixed order size |
| `max_position` | 100 | Fixed: backtest assumes infinite bankroll |
| `tick_size` | 0.01 | Fixed: Polymarket tick size |
| `neg_risk` | false | Fixed: not a neg-risk market |

### Variable (experiment with these)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `momentum_threshold` | -8.0 | Pull quotes when `position_dir * delta_5s < threshold` |
| `momentum_threshold_tight` | -5.0 | Tighter threshold during high vol |
| `vol_ratio_threshold` | 1.3 | `sigma_dyn / sigma_5m` threshold for tight mode |
| `enable_momentum_filter` | true | Master toggle for momentum filter |
| `enforce_parity` | true | UP+DOWN pricing constraint |

## Data

### Sample data (committed to git)

5 windows from 2026-02-05 in `sample_data/`. Pre-built TOML configs in `sample_configs/`:

| Window | Config | UP Token (prefix) | DOWN Token (prefix) |
|--------|--------|-------------------|---------------------|
| 1770262200 | `sample_configs/window_1770262200.toml` | `11389088756...` | `21127618355...` |
| 1770273000 | `sample_configs/window_1770273000.toml` | `11570952299...` | `62942697878...` |
| 1770282000 | `sample_configs/window_1770282000.toml` | `11090031365...` | `94517312629...` |
| 1770290100 | `sample_configs/window_1770290100.toml` | `58994366588...` | `19183535154...` |
| 1770298200 | `sample_configs/window_1770298200.toml` | `65522822842...` | `68770803580...` |

Run a single window manually:
```bash
cargo run --release --bin backtest -- sample_configs/window_1770262200.toml
```

### Full data (local only, gitignored)

If `parquet_files/` exists (place full dataset there), the sweep script uses it automatically (41 dates, ~1000+ windows). Token IDs are auto-discovered and cached in `analysis/token_cache.json`.

Use `--windows N` for quick iteration (5-10 windows), full sweep for validation.

## Experimentation

### What you CAN modify

The 3 modifiable files. Focus areas:

- **Strategy logic** in `basic_mm.rs`: How quotes are placed, when to pull/skew, how features influence decisions
- **New parameters** in `config.rs`: Add new params to `BasicMmConfig` if your strategy changes need them
- **Param grid** in `sweep.py`: Adjust `PARAM_GRID` dict to sweep different values. Aim for 20-50 combos per experiment

### What you CANNOT modify

Everything not in the 3 modifiable files. The backtest engine, exchange simulator, event types, and analysis utilities are read-only.

### Running an experiment

1. **Ideate**: Add an entry under **Brainstormed Ideas** in `ideas.md` with:
   - Short title
   - `**Hypothesis**`: Why this might improve bps_per_dollar
   - `**Changes**`: What code/param changes needed
   - `**Param grid**`: What values to sweep

2. **Move to In Progress**: Cut from Brainstormed, paste under In Progress.

3. **Implement**: Edit the modifiable files surgically.

4. **Build**: `cargo build --release 2>&1 | tail -5`

5. **Sweep**: `python analysis/sweep.py --parallel 4 --cleanup 2>&1 | tail -20`

6. **Evaluate**: Read `results.tsv` (sorted by bps_per_dollar descending).

7. **Commit**: `git add <modified files> && git commit -m "<description>"`

8. **Update ideas.md**: Move to Completed (Kept) or Failed Ideas with analysis.

9. **Keep or discard** (see git workflow below).

### Keep/discard decision

Evaluate the best config using three metrics:
- **`bps_per_dollar`** (primary) — profit efficiency: `(pnl / volume) * 10000`
- **`pnl`** — absolute profit
- **`volume`** — trading activity (reject high bps from tiny volume)

- **Keep** if bps_per_dollar improves while maintaining reasonable volume and pnl.
- **Discard** if metrics are equal or worse.

Simplicity criterion: A tiny improvement with significant complexity is not worth keeping.

### Git workflow

Every experiment gets its own commit. The branch only advances on keeps.

**When kept** (bps_per_dollar improved):
1. Commit stays. Branch has advanced.
2. Update `ideas.md` and commit.
3. Continue to next experiment.

**When discarded** (worse or equal):
1. Update `ideas.md` and amend current commit.
2. Create throwaway branch: `git branch discard/<tag>/<short-description>`
3. Reset: `git reset --hard HEAD~1`
4. Prune: `git branch -D discard/<tag>/<short-description>`
5. Continue from last good state.

### Validation

When an experiment is kept, validate on more windows:
```bash
python analysis/sweep.py --windows 50 --parallel 4 --cleanup
```

Check that the improvement holds across a larger sample.

## Results

Results are written to `results.tsv` (overwritten each sweep). Each row has: window_id, date, param values, fills, volume, fill_rate, pnl, bps_per_dollar.

### Logging

Always redirect backtest output:
```bash
python analysis/sweep.py --windows 10 --parallel 4 --cleanup 2>&1 | tail -20
```

Read results from `results.tsv`, not stdout.

## Never Stop

Once the experiment loop begins, do NOT pause to ask "should I continue?". If you run out of ideas:

- Re-read `ideas.md` for brainstormed ideas
- Re-read `basic_mm.rs` for new angles
- Examine `results.tsv` for patterns (which params correlate with high bps_per_dollar)
- Try combining elements from near-miss experiments
- Consider radical changes to quoting logic
- Think about what features are available but unused

The loop runs until the human manually interrupts.
