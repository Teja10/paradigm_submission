"""Shared backtest analysis library.

Building blocks for data discovery, backtest execution, PnL computation,
orderbook reconstruction, and adverse selection analysis. Used by the
marimo notebook and (eventually) AI agent tooling.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

import polars as pl


# ---------------------------------------------------------------------------
# Data discovery
# ---------------------------------------------------------------------------


def discover_dates(parquet_dir: Path) -> list[str]:
    """Return available date directory names, sorted descending."""
    return sorted(
        [d.name for d in parquet_dir.iterdir() if d.is_dir() and not d.name.startswith(".")],
        reverse=True,
    )


def discover_windows(parquet_dir: Path, date: str) -> list[str]:
    """Return window_id strings for a date, sorted ascending."""
    features_dir = parquet_dir / date / "features"
    if not features_dir.exists():
        return []
    return sorted(f.stem.replace("data_", "") for f in features_dir.glob("data_*.parquet"))


def discover_results(results_dir: Path) -> list[str]:
    """Return result window IDs that have fills.parquet, sorted descending."""
    if not results_dir.exists():
        return []
    return sorted(
        [d.name for d in results_dir.iterdir() if d.is_dir() and (d / "fills.parquet").exists()],
        reverse=True,
    )


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_window_data(
    parquet_dir: Path, date: str, window_id: str
) -> dict[str, pl.DataFrame | None]:
    """Load parquet data for a window.

    Returns dict with keys: features_df, snapshots_df, deltas_df, trades_df.
    Missing files result in None.
    """
    paths = {
        "features_df": parquet_dir / date / "features" / f"data_{window_id}.parquet",
        "snapshots_df": parquet_dir / date / "orderbooks" / f"snapshots_{window_id}.parquet",
        "deltas_df": parquet_dir / date / "orderbooks" / f"deltas_{window_id}.parquet",
        "trades_df": parquet_dir / date / "trades" / f"trades_{window_id}.parquet",
    }
    return {key: pl.read_parquet(p) if p.exists() else None for key, p in paths.items()}


def load_results(results_dir: Path, window_id: str) -> dict[str, pl.DataFrame | None]:
    """Load backtest result parquets. Returns dict with keys: fills_df, orders_df, pnl_df."""
    base = results_dir / window_id
    paths = {
        "fills_df": base / "fills.parquet",
        "orders_df": base / "orders.parquet",
        "pnl_df": base / "pnl.parquet",
    }
    return {key: pl.read_parquet(p) if p.exists() else None for key, p in paths.items()}


# ---------------------------------------------------------------------------
# Token identification
# ---------------------------------------------------------------------------


def identify_tokens(snapshots_df: pl.DataFrame, features_df: pl.DataFrame) -> tuple[str, str]:
    """Identify UP and DOWN token IDs from snapshots and features.

    Returns (up_token, down_token). UP token is the one whose initial mid
    is closest to the features up_mid reference price.
    """
    first_snaps = snapshots_df.group_by("token_id").first()
    first_snaps = first_snaps.with_columns(
        ((pl.col("bid_0_price").cast(pl.Float64) + pl.col("ask_0_price").cast(pl.Float64)) / 2).alias("mid")
    )

    up_mid_ref = features_df.filter(pl.col("up_mid").is_not_null())["up_mid"][0]

    first_snaps = first_snaps.with_columns(
        (pl.col("mid") - up_mid_ref).abs().alias("dist")
    ).sort("dist")

    return first_snaps["token_id"][0], first_snaps["token_id"][1]


# ---------------------------------------------------------------------------
# Orderbook reconstruction
# ---------------------------------------------------------------------------


def reconstruct_orderbook(
    snapshots_df: pl.DataFrame, deltas_df: pl.DataFrame, token_id: str
) -> pl.DataFrame:
    """Reconstruct orderbook mid prices from snapshots and deltas for a token.

    Returns DataFrame with columns: timestamp_ms, mid, best_bid, best_ask.
    """
    token_snaps = snapshots_df.filter(pl.col("token_id") == token_id).sort("timestamp_ms")
    token_deltas = deltas_df.filter(pl.col("token_id") == token_id).sort("timestamp_ms")

    snap_events = [
        {"timestamp_ms": row["timestamp_ms"], "type": "snapshot", "data": row}
        for row in token_snaps.iter_rows(named=True)
    ]
    delta_events = [
        {"timestamp_ms": row["timestamp_ms"], "type": "delta", "data": row}
        for row in token_deltas.iter_rows(named=True)
    ]

    all_events = snap_events + delta_events
    all_events.sort(key=lambda x: (x["timestamp_ms"], 0 if x["type"] == "snapshot" else 1))

    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    results: list[dict] = []

    for event in all_events:
        ts = event["timestamp_ms"]
        if event["type"] == "snapshot":
            bids = {}
            asks = {}
            data = event["data"]
            for i in range(20):
                bid_price = data.get(f"bid_{i}_price", "")
                bid_size = data.get(f"bid_{i}_size", "")
                if bid_price and bid_size:
                    try:
                        p, s = float(bid_price), float(bid_size)
                        if s > 0:
                            bids[p] = s
                    except (ValueError, TypeError):
                        pass
                ask_price = data.get(f"ask_{i}_price", "")
                ask_size = data.get(f"ask_{i}_size", "")
                if ask_price and ask_size:
                    try:
                        p, s = float(ask_price), float(ask_size)
                        if s > 0:
                            asks[p] = s
                    except (ValueError, TypeError):
                        pass
        else:
            data = event["data"]
            side = data["side"]
            try:
                price = float(data["price"])
                size = float(data["size"])
            except (ValueError, TypeError):
                continue

            if side == "buy":
                if size > 0:
                    bids[price] = size
                elif price in bids:
                    del bids[price]
            else:
                if size > 0:
                    asks[price] = size
                elif price in asks:
                    del asks[price]

        if bids and asks:
            best_bid = max(bids.keys())
            best_ask = min(asks.keys())
            mid = (best_bid + best_ask) / 2
            results.append({"timestamp_ms": ts, "mid": mid, "best_bid": best_bid, "best_ask": best_ask})

    return pl.DataFrame(results)


# ---------------------------------------------------------------------------
# Config generation + execution
# ---------------------------------------------------------------------------


def make_config_toml(
    window_id: str,
    up_token: str,
    down_token: str,
    latency_min: int = 15,
    latency_max: int = 40,
    seed: int = 42,
    fee_bps: int = 0,
    initial_cash: float = 100.0,
    initial_sets: int = 0,
    data_dir: str = "parquet_files",
    output_dir: str | None = None,
    strategy_params: dict[str, object] | None = None,
) -> str:
    """Generate a TOML config string for the backtest binary."""
    if output_dir is None:
        output_dir = f"backtest_results/{window_id}"
    toml = f"""\
window_start = {window_id}
delta_mode = "absolute"
queue_added_ahead = false
latency_min_ms = {latency_min}
latency_max_ms = {latency_max}
seed = {seed}
fee_bps = {fee_bps}
data_dir = "{data_dir}"
output_dir = "{output_dir}"
condition_id = "window_{window_id}"
up_token_id = "{up_token}"
down_token_id = "{down_token}"
token_filter = ["{up_token}", "{down_token}"]
initial_complete_sets = {initial_sets}
complete_set_price = 1.0
initial_cash = {initial_cash}
"""
    if strategy_params:
        toml += "\n[strategy]\n"
        for k, v in strategy_params.items():
            if isinstance(v, bool):
                toml += f"{k} = {str(v).lower()}\n"
            elif isinstance(v, str):
                toml += f'{k} = "{v}"\n'
            else:
                toml += f"{k} = {v}\n"
    return toml


def run_backtest(config_text: str, window_id: str, cwd: str | Path) -> tuple[str, bool]:
    """Write config to temp file, run cargo backtest, clean up.

    Returns (output_text, success).
    """
    cwd = Path(cwd)
    config_path = cwd / f"backtest_config_{window_id}.toml"
    config_path.write_text(config_text)
    try:
        result = subprocess.run(
            ["cargo", "run", "--release", "--bin", "backtest", "--", str(config_path)],
            capture_output=True,
            text=True,
            cwd=str(cwd),
        )
        output = result.stdout + result.stderr
        return output, result.returncode == 0
    finally:
        config_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# PnL computation
# ---------------------------------------------------------------------------


@dataclass
class PnlResult:
    """Breakdown of PnL computation at market resolution."""

    initial_cash: float
    initial_sets: int
    initial_portfolio: float
    total_cash_flow: float
    final_cash: float
    up_final_inventory: float
    down_final_inventory: float
    oracle_price: float | None
    reference_price: float | None
    up_wins: bool | None
    resolution_label: str
    up_payout: float
    down_payout: float
    final_inventory_value: float
    final_portfolio: float
    pnl: float


def compute_pnl(
    fills_df: pl.DataFrame,
    features_df: pl.DataFrame,
    up_token: str,
    down_token: str,
    initial_sets: int,
    initial_cash: float,
) -> PnlResult:
    """Compute PnL breakdown from fills and features data."""
    fills = fills_df.sort("timestamp_ms").with_columns([
        pl.when(pl.col("side") == "buy")
        .then(pl.col("size"))
        .otherwise(-pl.col("size"))
        .alias("inv_delta"),
        pl.when(pl.col("side") == "buy")
        .then(-pl.col("price") * pl.col("size"))
        .otherwise(pl.col("price") * pl.col("size"))
        .alias("cash_delta"),
    ])

    up_fills = fills.filter(pl.col("token_id") == up_token)
    down_fills = fills.filter(pl.col("token_id") == down_token)

    up_final = (up_fills["inv_delta"].sum() + initial_sets) if up_fills.height > 0 else float(initial_sets)
    down_final = (down_fills["inv_delta"].sum() + initial_sets) if down_fills.height > 0 else float(initial_sets)

    total_cash_flow = fills["cash_delta"].sum()
    final_cash = initial_cash + total_cash_flow

    # Determine resolution
    oracle_price = None
    reference_price = None
    up_wins = None
    resolution_label = "Unknown"

    if features_df is not None and features_df.height > 0:
        last_row = features_df.sort("timestamp_ms").tail(1)
        oracle_price = last_row["oracle_price"][0]
        reference_price = last_row["reference_price"][0]
        if oracle_price is not None and reference_price is not None:
            up_wins = oracle_price > reference_price
            if up_wins:
                resolution_label = f"UP (oracle ${oracle_price:,.0f} > strike ${reference_price:,.0f})"
            else:
                resolution_label = f"DOWN (oracle ${oracle_price:,.0f} < strike ${reference_price:,.0f})"

    up_payout = 1.0 if up_wins else 0.0
    down_payout = 0.0 if up_wins else 1.0
    if up_wins is None:
        up_payout = 0.0
        down_payout = 0.0

    final_inv_value = up_final * up_payout + down_final * down_payout
    initial_portfolio = initial_cash + initial_sets * 1.0
    final_portfolio = final_cash + final_inv_value
    pnl = final_portfolio - initial_portfolio

    return PnlResult(
        initial_cash=initial_cash,
        initial_sets=initial_sets,
        initial_portfolio=initial_portfolio,
        total_cash_flow=total_cash_flow,
        final_cash=final_cash,
        up_final_inventory=float(up_final),
        down_final_inventory=float(down_final),
        oracle_price=oracle_price,
        reference_price=reference_price,
        up_wins=up_wins,
        resolution_label=resolution_label,
        up_payout=up_payout,
        down_payout=down_payout,
        final_inventory_value=final_inv_value,
        final_portfolio=final_portfolio,
        pnl=pnl,
    )


# ---------------------------------------------------------------------------
# Adverse selection analysis
# ---------------------------------------------------------------------------


def _get_mid_at_time(token_id: str, target_ts: int, mids_df: pl.DataFrame) -> float | None:
    """Get the mid price for a token at or just before the target timestamp."""
    filtered = mids_df.filter(
        (pl.col("token_id") == token_id) & (pl.col("timestamp_ms") <= target_ts)
    ).sort("timestamp_ms", descending=True)
    if filtered.height > 0:
        return filtered["mid_price"][0]
    return None


def _get_mid_after_time(token_id: str, target_ts: int, mids_df: pl.DataFrame) -> float | None:
    """Get the mid price for a token at or just after the target timestamp."""
    filtered = mids_df.filter(
        (pl.col("token_id") == token_id) & (pl.col("timestamp_ms") >= target_ts)
    ).sort("timestamp_ms")
    if filtered.height > 0:
        return filtered["mid_price"][0]
    return None


def analyze_adverse_selection(
    fills_df: pl.DataFrame,
    reconstructed_up: pl.DataFrame,
    reconstructed_down: pl.DataFrame,
    up_token: str,
    down_token: str,
    horizon_ms: int,
) -> pl.DataFrame | None:
    """Analyze adverse selection for each fill.

    Returns DataFrame with per-fill metrics, or None if no fills can be analyzed.
    """
    up_mid = reconstructed_up.select([
        pl.col("timestamp_ms"),
        pl.col("mid").alias("mid_price"),
        pl.lit(up_token).alias("token_id"),
    ])
    down_mid = reconstructed_down.select([
        pl.col("timestamp_ms"),
        pl.col("mid").alias("mid_price"),
        pl.lit(down_token).alias("token_id"),
    ])
    all_mids = pl.concat([up_mid, down_mid])

    fill_analysis = []
    for row in fills_df.iter_rows(named=True):
        fill_ts = row["timestamp_ms"]
        token_id = row["token_id"]
        side = row["side"]
        price = row["price"]
        size = row["size"]

        mid_at_fill = _get_mid_at_time(token_id, fill_ts, all_mids)
        mid_at_horizon = _get_mid_after_time(token_id, fill_ts + horizon_ms, all_mids)

        if mid_at_fill is not None and mid_at_horizon is not None:
            price_change = mid_at_horizon - mid_at_fill

            if side == "buy":
                pnl_direction = price_change
                edge_at_fill = mid_at_fill - price
            else:
                pnl_direction = -price_change
                edge_at_fill = price - mid_at_fill

            mtm_pnl = (edge_at_fill + pnl_direction) * size

            fill_analysis.append({
                "timestamp_ms": fill_ts,
                "token_id": token_id,
                "token": "UP" if token_id == up_token else "DOWN",
                "side": side,
                "price": price,
                "size": size,
                "mid_at_fill": mid_at_fill,
                "mid_at_horizon": mid_at_horizon,
                "price_change": price_change,
                "edge_at_fill": edge_at_fill,
                "pnl_direction": pnl_direction,
                "mtm_pnl": mtm_pnl,
                "is_adverse": pnl_direction < 0,
            })

    return pl.DataFrame(fill_analysis) if fill_analysis else None


def summarize_adverse_selection(fills_analyzed: pl.DataFrame) -> dict:
    """Compute summary statistics from analyzed fills."""
    total = fills_analyzed.height
    adverse_count = fills_analyzed.filter(pl.col("is_adverse")).height
    favorable_count = total - adverse_count

    adverse_pnl = fills_analyzed.filter(pl.col("is_adverse"))["mtm_pnl"].sum()
    favorable_pnl = fills_analyzed.filter(~pl.col("is_adverse"))["mtm_pnl"].sum()
    total_pnl = fills_analyzed["mtm_pnl"].sum()

    buy_fills = fills_analyzed.filter(pl.col("side") == "buy")
    sell_fills = fills_analyzed.filter(pl.col("side") == "sell")

    return {
        "total": total,
        "adverse_count": adverse_count,
        "favorable_count": favorable_count,
        "adverse_pct": 100 * adverse_count / total if total > 0 else 0,
        "adverse_pnl": adverse_pnl,
        "favorable_pnl": favorable_pnl,
        "total_pnl": total_pnl,
        "avg_adverse_pnl": adverse_pnl / adverse_count if adverse_count > 0 else 0,
        "avg_favorable_pnl": favorable_pnl / favorable_count if favorable_count > 0 else 0,
        "buy_count": buy_fills.height,
        "sell_count": sell_fills.height,
        "buy_adverse": buy_fills.filter(pl.col("is_adverse")).height,
        "sell_adverse": sell_fills.filter(pl.col("is_adverse")).height,
        "avg_edge": fills_analyzed["edge_at_fill"].mean(),
        "avg_price_move": fills_analyzed["pnl_direction"].mean(),
    }
