# Polymarket MM Backtester + Autoresearch Agent

An autonomous research agent loop that proposes, implements, backtests, and evaluates market making strategies on Polymarket's 15-minute BTC prediction markets.

Built for a hackathon showcasing **autoresearch** — an LLM agent that continuously experiments with trading strategy improvements without human intervention, using a tight feedback loop of hypothesis generation, code modification, backtesting, and metric evaluation.

## How It Works

The system has three layers:

```
┌─────────────────────────────────────────────────────┐
│  LLM Autoresearch Agent                             │
│  Proposes ideas → edits strategy code →             │
│  runs backtests → evaluates results → keeps/discards│
│  Loops until manually stopped                       │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Python Sweep Harness (analysis/sweep.py)           │
│  Parameter grid generation, parallel backtest       │
│  execution, PnL aggregation → results.tsv           │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Rust Backtest Engine (src/backtest/)                │
│  High-fidelity exchange simulator with latency      │
│  modeling, POST_ONLY semantics, queue priority       │
└─────────────────────────────────────────────────────┘
```

**The autoresearch loop:**
1. **Ideate** — Agent reads strategy code and past results, proposes a hypothesis
2. **Implement** — Agent edits strategy logic, config, and parameter grid (only 3 files are modifiable)
3. **Build** — `cargo build --release`
4. **Sweep** — Runs parameter grid across multiple market windows in parallel
5. **Evaluate** — Reads `results.tsv`, checks if `bps_per_dollar` improved
6. **Keep/Discard** — Commits if better, reverts if not. Repeats forever

## Architecture

The live trading system follows the [Artemis](https://github.com/paradigmxyz/artemis) architecture — a pipeline of **Collectors**, **Models**, **Strategies**, and **Executors** connected via broadcast channels:

```
Collectors              Models              Strategies           Executors
(produce events)     (derive signals)     (decide actions)     (submit orders)
┌──────────────┐    ┌───────────────┐    ┌───────────────┐    ┌──────────────┐
│ Oracle Price │    │ BinaryOption  │    │ BasicMarket   │    │ Polymarket   │
│ Polymarket WS│─►  │ FairValue     │─►  │ Maker         │─►  │ CLOB         │
│ Coinbase WS  │    │ Logger        │    │               │    │              │
└──────────────┘    └───────────────┘    └───────────────┘    └──────────────┘
     Events ──────────► Events ──────────► Actions ──────────► Execution
```

- **Collectors** stream market data (oracle prices, orderbook updates, Coinbase feed)
- **Models** compute derived signals (fair values, volatility, blended prices)
- **Strategies** consume events and emit order actions (place, cancel)
- **Executors** submit actions to the exchange

The backtest engine replays the same event types from parquet files through the same strategy interface, so strategies developed in backtesting work identically in live trading.

## Quick Start

### Prerequisites

- Rust 1.70+
- Python 3.12+

### Build and run

```bash
# Build the backtest engine
cargo build --release

# Run a single backtest on sample data
cargo run --release --bin backtest -- sample_configs/window_1770262200.toml

# Run a parameter sweep (5 windows, 4 parallel workers)
python analysis/sweep.py --windows 5 --parallel 4 --cleanup

# View top results
head -20 results.tsv
```

### Start autoresearch

Point Claude Code at `AUTORESEARCH.md` to kick off the autonomous loop:

```bash
claude "Read AUTORESEARCH.md and begin the autoresearch loop"
```

The agent will create a branch, run a baseline sweep, and start experimenting.

## The Market

Polymarket runs 15-minute BTC prediction markets: *"Will BTC be above $X at HH:MM?"*

Each market has two tokens (UP and DOWN) that form a complete set summing to $1.00. The strategy makes markets on both tokens, profiting from the bid-ask spread while managing adverse selection risk.

## The Strategy

`BasicMarketMaker` is a touch market maker that:

- **Quotes at TOB** (top of book) on both UP and DOWN tokens
- **Momentum filter** — pulls quotes when short-term microprice movement is against the position
- **Volatility regime** — tightens the momentum filter during high-volatility periods
- **Parity enforcement** — prevents quoting when UP+DOWN prices would violate the $1.00 constraint

The strategy only uses `delta_microprice_5s` for filtering. Many features are available but unused — coinbase orderbook data, multi-timeframe momentum, fair value estimates, basis signals — leaving significant room for the autoresearch agent to find improvements.

## Data

Order book snapshots, deltas, trades, and computed features for 41 dates (~1000+ market windows) stored as parquet files. Sample data (5 windows from 2026-02-05) is committed to git in `sample_data/`.

## Project Structure

```
src/
  engine/                  # Artemis-style engine (Collector, Model, Strategy, Executor traits)
  bin/backtest.rs          # Entry point
  strategies/basic_mm.rs   # Strategy logic (modifiable)
  backtest/
    config.rs              # Config schema (modifiable)
    engine.rs              # Simulation loop
    exchange_sim.rs        # Order matching
    event_loader.rs        # Parquet ingestion
    recorder.rs            # Results output
  tracking/                # Order & position tracking
  models/                  # Fair value pricing
analysis/
  sweep.py                 # Parameter sweep runner (modifiable)
  backtest_runner.py       # Config generation, PnL computation
sample_configs/            # TOML configs for sample windows
sample_data/               # Committed sample parquet data
parquet_files/             # Full dataset (gitignored)
AUTORESEARCH.md            # Agent instructions
ideas.md                   # Research hypothesis scratchpad
results.tsv                # Sweep output (sorted by bps_per_dollar)
```

## Key Metric

**`bps_per_dollar`** = `(pnl / volume) * 10000`

Profit efficiency: how many basis points of profit per dollar of volume traded. This is the primary optimization target for the autoresearch agent.
