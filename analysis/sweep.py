"""Parameter sweep for basic_mm strategy backtesting.

Discovers windows, runs backtests across a param grid, aggregates results into results.tsv.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import product
from pathlib import Path

import polars as pl

from backtest_runner import (
    compute_pnl,
    discover_dates,
    discover_windows,
    identify_tokens,
    load_window_data,
    make_config_toml,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Use parquet_files/ if available (full dataset), otherwise sample_data/
_full_data = PROJECT_ROOT / "parquet_files"
_sample_data = PROJECT_ROOT / "sample_data"
PARQUET_DIR = _full_data if _full_data.exists() else _sample_data
TOKEN_CACHE_PATH = PROJECT_ROOT / "analysis" / "token_cache.json"
RESULTS_PATH = PROJECT_ROOT / "results.tsv"
BACKTEST_BIN = PROJECT_ROOT / "target" / "release" / "backtest"

# --- Param grid ---

PARAM_GRID: dict[str, list[object]] = {
    "momentum_threshold": [-10.0, -8.0, -6.0, -4.0],
    "momentum_threshold_tight": [-7.0, -5.0, -3.0],
    "vol_ratio_threshold": [1.0, 1.3, 1.6],
}

FIXED_PARAMS: dict[str, object] = {
    "order_size": 10.0,
    "max_position": 100.0,
    "enable_momentum_filter": True,
    "enforce_parity": True,
}


def build_param_combos() -> list[dict[str, object]]:
    keys = list(PARAM_GRID.keys())
    combos = []
    for values in product(*PARAM_GRID.values()):
        combo = dict(zip(keys, values))
        combo.update(FIXED_PARAMS)
        combos.append(combo)
    return combos


# --- Token discovery ---


def build_token_cache() -> dict[str, dict[str, str]]:
    """Discover UP/DOWN token IDs for all windows, return {window_id: {up, down}}."""
    if TOKEN_CACHE_PATH.exists():
        return json.loads(TOKEN_CACHE_PATH.read_text())

    print("Building token cache (first run)...")
    cache: dict[str, dict[str, str]] = {}
    dates = discover_dates(PARQUET_DIR)
    for date in dates:
        windows = discover_windows(PARQUET_DIR, date)
        for window_id in windows:
            try:
                data = load_window_data(PARQUET_DIR, date, window_id)
                snaps = data.get("snapshots_df")
                feats = data.get("features_df")
                if snaps is None or feats is None:
                    continue
                up_token, down_token = identify_tokens(snaps, feats)
                cache[window_id] = {"up": up_token, "down": down_token, "date": date}
            except Exception as e:
                print(f"  Skip {date}/{window_id}: {e}", file=sys.stderr)

    TOKEN_CACHE_PATH.write_text(json.dumps(cache, indent=2))
    print(f"Cached {len(cache)} windows to {TOKEN_CACHE_PATH}")
    return cache


# --- Single backtest run ---


def run_single(
    window_id: str,
    up_token: str,
    down_token: str,
    strategy_params: dict[str, object],
    run_id: int,
    cleanup: bool,
) -> dict[str, object] | None:
    output_dir = f"backtest_results/sweep/{window_id}_{run_id}"

    config_text = make_config_toml(
        window_id=window_id,
        up_token=up_token,
        down_token=down_token,
        initial_cash=100.0,
        initial_sets=50,
        output_dir=output_dir,
        data_dir=str(PARQUET_DIR.relative_to(PROJECT_ROOT)),
        strategy_params=strategy_params,
    )

    config_path = PROJECT_ROOT / f"sweep_config_{window_id}_{run_id}.toml"
    config_path.write_text(config_text)

    result = subprocess.run(
        [str(BACKTEST_BIN), str(config_path)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        timeout=120,
    )
    config_path.unlink(missing_ok=True)

    if result.returncode != 0:
        print(f"  FAIL window={window_id} run={run_id}: {result.stderr[-200:]}", file=sys.stderr)
        return None

    # Read summary.json
    summary_path = PROJECT_ROOT / output_dir / "summary.json"
    if not summary_path.exists():
        return None
    summary = json.loads(summary_path.read_text())

    # Compute PnL
    fills_path = PROJECT_ROOT / output_dir / "fills.parquet"
    pnl_value = 0.0
    volume = summary.get("volume", 0.0)
    bps_per_dollar = 0.0

    if fills_path.exists():
        fills_df = pl.read_parquet(fills_path)
        # Load features for PnL computation
        date = token_cache_global.get(window_id, {}).get("date", "")
        if date:
            data = load_window_data(PARQUET_DIR, date, window_id)
            features_df = data.get("features_df")
            if features_df is not None and fills_df.height > 0:
                pnl_result = compute_pnl(fills_df, features_df, up_token, down_token, 50, 100.0)
                pnl_value = pnl_result.pnl
                if volume > 0:
                    bps_per_dollar = (pnl_value / volume) * 10000

    row: dict[str, object] = {
        "window_id": window_id,
        "date": token_cache_global.get(window_id, {}).get("date", ""),
        "run_id": run_id,
        **{k: v for k, v in strategy_params.items() if k not in FIXED_PARAMS},
        "fills": summary.get("fills", 0),
        "buy_fills": summary.get("buy_fills", 0),
        "sell_fills": summary.get("sell_fills", 0),
        "volume": volume,
        "fill_rate": summary.get("fill_rate", 0.0),
        "pnl": round(pnl_value, 4),
        "bps_per_dollar": round(bps_per_dollar, 2),
    }

    if cleanup:
        shutil.rmtree(PROJECT_ROOT / output_dir, ignore_errors=True)

    return row


# Global token cache for subprocess access
token_cache_global: dict[str, dict[str, str]] = {}


def main() -> None:
    global token_cache_global

    parser = argparse.ArgumentParser(description="Parameter sweep for basic_mm backtest")
    parser.add_argument("--windows", type=int, help="Limit to N random windows")
    parser.add_argument("--dates", type=str, help="Comma-separated dates to include")
    parser.add_argument("--parallel", type=int, default=4, help="Parallel workers")
    parser.add_argument("--cleanup", action="store_true", help="Delete parquet output after metrics")
    parser.add_argument("--rebuild-cache", action="store_true", help="Force rebuild token cache")
    args = parser.parse_args()

    if args.rebuild_cache and TOKEN_CACHE_PATH.exists():
        TOKEN_CACHE_PATH.unlink()

    # Ensure binary is built
    if not BACKTEST_BIN.exists():
        print("Building release binary...")
        subprocess.run(["cargo", "build", "--release"], cwd=str(PROJECT_ROOT), check=True)

    token_cache = build_token_cache()
    token_cache_global = token_cache

    # Select windows
    window_ids = list(token_cache.keys())
    if args.dates:
        allowed_dates = set(args.dates.split(","))
        window_ids = [w for w in window_ids if token_cache[w].get("date") in allowed_dates]
    if args.windows:
        random.seed(42)
        window_ids = random.sample(window_ids, min(args.windows, len(window_ids)))

    param_combos = build_param_combos()
    total_runs = len(window_ids) * len(param_combos)
    print(f"Sweep: {len(window_ids)} windows x {len(param_combos)} param combos = {total_runs} runs")

    # Create output dir
    (PROJECT_ROOT / "backtest_results" / "sweep").mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    completed = 0

    if args.parallel <= 1:
        for window_id in window_ids:
            info = token_cache[window_id]
            for run_id, params in enumerate(param_combos):
                row = run_single(window_id, info["up"], info["down"], params, run_id, args.cleanup)
                if row:
                    results.append(row)
                completed += 1
                if completed % 10 == 0:
                    print(f"  {completed}/{total_runs} complete")
    else:
        with ProcessPoolExecutor(max_workers=args.parallel) as executor:
            futures = {}
            for window_id in window_ids:
                info = token_cache[window_id]
                for run_id, params in enumerate(param_combos):
                    fut = executor.submit(
                        run_single, window_id, info["up"], info["down"], params, run_id, args.cleanup
                    )
                    futures[fut] = (window_id, run_id)

            for fut in as_completed(futures):
                row = fut.result()
                if row:
                    results.append(row)
                completed += 1
                if completed % 50 == 0:
                    print(f"  {completed}/{total_runs} complete")

    if not results:
        print("No results collected.")
        return

    # Write results.tsv
    df = pl.DataFrame(results)
    df = df.sort("bps_per_dollar", descending=True)
    df.write_csv(str(RESULTS_PATH), separator="\t")
    print(f"\nWrote {len(results)} results to {RESULTS_PATH}")

    # Print top 10
    print("\nTop 10 by bps_per_dollar:")
    print(df.head(10))


if __name__ == "__main__":
    main()
