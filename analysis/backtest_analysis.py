import marimo

__generated_with = "0.19.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import sys
    import marimo as mo
    import polars as pl
    from pathlib import Path
    import altair as alt
    sys.path.insert(0, str(Path(__file__).parent))
    import backtest_runner as br
    return Path, alt, br, mo, pl


@app.cell
def _(Path, br, mo):
    parquet_dir = Path("parquet_files")
    available_dates = br.discover_dates(parquet_dir)
    date_dropdown = mo.ui.dropdown(
        options=available_dates,
        value=available_dates[0] if available_dates else None,
        label="Select Date",
    )
    date_dropdown
    return date_dropdown, parquet_dir


@app.cell
def _(br, date_dropdown, mo, parquet_dir):
    selected_date = date_dropdown.value
    window_ids = br.discover_windows(parquet_dir, selected_date) if selected_date else []
    window_dropdown = mo.ui.dropdown(
        options=window_ids,
        value=window_ids[0] if window_ids else None,
        label="Select Window",
    )
    window_dropdown
    return (window_dropdown,)


@app.cell
def _(br, date_dropdown, mo, parquet_dir, window_dropdown):
    _date = date_dropdown.value
    _window_id = window_dropdown.value

    _data = br.load_window_data(parquet_dir, _date, _window_id) if _date and _window_id else {}
    features_df = _data.get("features_df")
    snapshots_df = _data.get("snapshots_df")
    deltas_df = _data.get("deltas_df")
    trades_df = _data.get("trades_df")

    mo.md(f"""
    **Loaded data for {_date} / window {_window_id}:**
    - Features: {features_df.shape if features_df is not None else 'Not found'}
    - Snapshots: {snapshots_df.shape if snapshots_df is not None else 'Not found'}
    - Deltas: {deltas_df.shape if deltas_df is not None else 'Not found'}
    - Trades: {trades_df.shape if trades_df is not None else 'Not found'}
    """)
    return deltas_df, features_df, snapshots_df, trades_df


@app.cell
def _(br, features_df, mo, snapshots_df):
    mo.stop(snapshots_df is None or features_df is None, mo.md("No data loaded"))

    up_token, down_token = br.identify_tokens(snapshots_df, features_df)

    mo.md(f"""
    **Token Identification:**
    - UP token: `{up_token[:20]}...`
    - DOWN token: `{down_token[:20]}...`
    """)
    return down_token, up_token


@app.cell
def _(alt, down_token, mo, pl, trades_df, up_token):
    mo.stop(trades_df is None, mo.md("No trades data loaded"))

    _trades = trades_df.filter(
        pl.col("token_id").is_in([up_token, down_token])
    ).with_columns([
        pl.when(pl.col("token_id") == up_token)
        .then(pl.lit("UP"))
        .otherwise(pl.lit("DOWN"))
        .alias("token_side"),
        (pl.col("timestamp_ms") // 15000 * 15000).alias("bucket_ms")
    ])

    _start_ms = _trades["bucket_ms"].min()

    _volume_by_bucket = _trades.group_by(["bucket_ms", "token_side"]).agg(
        pl.col("size").cast(pl.Float64).sum().alias("volume")
    ).with_columns([
        ((pl.col("bucket_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.when(pl.col("token_side") == "DOWN")
        .then(-pl.col("volume"))
        .otherwise(pl.col("volume"))
        .alias("volume_signed")
    ]).sort("bucket_ms")

    _chart = alt.Chart(_volume_by_bucket).mark_bar(opacity=0.8).encode(
        x=alt.X("time_sec:Q", title="Time (seconds)"),
        y=alt.Y("volume_signed:Q", title="Trade Volume"),
        color=alt.Color("token_side:N", title="Token", scale=alt.Scale(
            domain=["UP", "DOWN"],
            range=["#1f77b4", "#ff7f0e"]
        )),
        tooltip=["time_sec:Q", "token_side:N", "volume:Q"]
    ).properties(
        width=800,
        height=300,
        title="Trade Volume per 15-Second Interval (UP positive, DOWN negative)"
    ).interactive()

    _chart
    return


@app.cell
def _(br, deltas_df, down_token, mo, snapshots_df, up_token):
    mo.stop(snapshots_df is None or deltas_df is None, mo.md("No data loaded"))

    reconstructed_up = br.reconstruct_orderbook(snapshots_df, deltas_df, up_token)
    reconstructed_down = br.reconstruct_orderbook(snapshots_df, deltas_df, down_token)

    mo.md(f"""
    **Reconstructed Orderbooks:**
    - UP token: {reconstructed_up.shape[0]} mid price points
    - DOWN token: {reconstructed_down.shape[0]} mid price points
    """)
    return reconstructed_down, reconstructed_up


@app.cell
def _(alt, features_df, mo, pl, reconstructed_down, reconstructed_up):
    mo.stop(reconstructed_up is None or features_df is None, mo.md("No data to plot"))

    _recon_up = reconstructed_up.rename({"mid": "reconstructed_mid_up"}).select(["timestamp_ms", "reconstructed_mid_up"])
    _recon_down = reconstructed_down.rename({"mid": "reconstructed_mid_down"}).select(["timestamp_ms", "reconstructed_mid_down"])

    _features_subset = features_df.select([
        "timestamp_ms",
        pl.col("up_mid").alias("features_up_mid"),
    ]).with_columns(
        (1 - pl.col("features_up_mid")).alias("features_down_mid")
    )

    _plot_data = _recon_up.join(_recon_down, on="timestamp_ms", how="full", coalesce=True)
    _plot_data = _plot_data.join(_features_subset, on="timestamp_ms", how="full", coalesce=True)
    _plot_data = _plot_data.sort("timestamp_ms")

    _start_ts = _plot_data["timestamp_ms"].min()
    _plot_data = _plot_data.with_columns(
        ((pl.col("timestamp_ms") - _start_ts) / 1000).alias("time_sec")
    )

    _melted = _plot_data.unpivot(
        index=["timestamp_ms", "time_sec"],
        on=["reconstructed_mid_up", "reconstructed_mid_down", "features_up_mid", "features_down_mid"],
        variable_name="series",
        value_name="price"
    ).filter(pl.col("price").is_not_null())

    _melted = _melted.with_columns(
        pl.when(pl.col("series").str.starts_with("features"))
        .then(pl.lit("dashed"))
        .otherwise(pl.lit("solid"))
        .alias("line_type")
    )

    _total_rows = _melted.height
    _max_rows = 15000
    _sample_rate = max(1, _total_rows // _max_rows)
    _melted_sampled = _melted.gather_every(_sample_rate)

    _chart = alt.Chart(_melted_sampled).mark_line(opacity=0.8).encode(
        x=alt.X("time_sec:Q", title="Time (seconds)"),
        y=alt.Y("price:Q", title="Mid Price", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Series", scale=alt.Scale(
            domain=["reconstructed_mid_up", "reconstructed_mid_down", "features_up_mid", "features_down_mid"],
            range=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
        )),
        strokeDash=alt.StrokeDash("line_type:N", scale=alt.Scale(
            domain=["solid", "dashed"],
            range=[[0], [5, 5]]
        ), legend=None)
    ).properties(
        width=800,
        height=400,
        title=f"Reconstructed vs Features Mid Prices (sampled 1:{_sample_rate}, {_melted_sampled.height} points)"
    ).interactive()

    _chart
    return


@app.cell
def _(mo):
    mo.md("""
    ## Backtest Configuration
    """)
    return


@app.cell
def _(mo):
    latency_min = mo.ui.slider(10, 200, value=15, label="Min Latency (ms)")
    latency_max = mo.ui.slider(40, 500, value=40, label="Max Latency (ms)")
    seed = mo.ui.number(value=42, label="Random Seed")
    initial_cash = mo.ui.number(value=100.0, label="Initial Cash ($)")
    initial_sets = mo.ui.number(value=100, label="Initial Complete Sets")
    mo.vstack([latency_min, latency_max, seed, initial_cash, initial_sets])
    return initial_cash, initial_sets, latency_max, latency_min, seed


@app.cell
def _(
    br,
    down_token,
    initial_cash,
    initial_sets,
    latency_max,
    latency_min,
    mo,
    seed,
    up_token,
    window_dropdown,
):
    _window_id = window_dropdown.value

    config_text = ""
    if _window_id and up_token and down_token:
        config_text = br.make_config_toml(
            window_id=_window_id,
            up_token=up_token,
            down_token=down_token,
            latency_min=int(latency_min.value),
            latency_max=int(latency_max.value),
            seed=int(seed.value),
            initial_cash=float(initial_cash.value),
            initial_sets=int(initial_sets.value),
        )

    mo.md(f"""
    **Generated Config:**
    ```toml
    {config_text}
    ```
    """)
    return (config_text,)


@app.cell
def _(mo):
    run_button = mo.ui.run_button(label="Run Backtest")
    run_button
    return (run_button,)


@app.cell
def _(br, config_text, mo, run_button, window_dropdown):
    mo.stop(not run_button.value, mo.md("Click 'Run Backtest' to execute"))
    mo.stop(not config_text, mo.md("No config generated - select a window first"))

    _output, _success = br.run_backtest(
        config_text, window_dropdown.value, cwd="/Users/tejaaluru/crypto/polymarket_mm"
    )

    mo.md(f"""
    **Backtest Output:**
    ```
    {_output[-3000:] if len(_output) > 3000 else _output}
    ```
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## Load Backtest Fills
    """)
    return


@app.cell
def _(Path, br, mo):
    results_dir = Path("backtest_results")
    available_results = br.discover_results(results_dir)
    results_dropdown = mo.ui.dropdown(
        options=available_results,
        value=available_results[0] if available_results else None,
        label="Select Backtest Result",
    )
    results_dropdown
    return (results_dropdown,)


@app.cell
def _(Path, br, mo, results_dropdown):
    selected_result = results_dropdown.value
    _data = br.load_results(Path("backtest_results"), selected_result) if selected_result else {}
    fills_df = _data.get("fills_df")
    orders_df = _data.get("orders_df")
    pnl_df = _data.get("pnl_df")

    mo.md(f"""
    **Loaded backtest results for {selected_result}:**
    - Fills: {fills_df.shape if fills_df is not None else 'Not found'}
    - Orders: {orders_df.shape if orders_df is not None else 'Not found'}
    - PnL: {pnl_df.shape if pnl_df is not None else 'Not found'}
    """)
    return (fills_df,)


@app.cell
def _(fills_df, mo, pl):
    mo.stop(fills_df is None or fills_df.is_empty(), mo.md("No fills data loaded"))

    _buy_fills = fills_df.filter(pl.col("side") == "buy")
    _sell_fills = fills_df.filter(pl.col("side") == "sell")

    _buy_volume = (_buy_fills["price"] * _buy_fills["size"]).sum()
    _sell_volume = (_sell_fills["price"] * _sell_fills["size"]).sum()

    _unique_tokens = fills_df["token_id"].n_unique()

    mo.md(f"""
    **Fill Summary:**
    - Total fills: {fills_df.height}
    - Buy fills: {_buy_fills.height} (volume: ${_buy_volume:.2f})
    - Sell fills: {_sell_fills.height} (volume: ${_sell_volume:.2f})
    - Unique tokens: {_unique_tokens}
    """)
    return


@app.cell
def _(
    alt,
    down_token,
    fills_df,
    mo,
    pl,
    reconstructed_down,
    reconstructed_up,
    up_token,
):
    mo.stop(
        fills_df is None or fills_df.is_empty() or reconstructed_up is None,
        mo.md("Need both fills and reconstructed orderbook data"),
    )

    _start_ms = reconstructed_up["timestamp_ms"].min()

    # Prepare reconstructed mid prices
    _recon_up = reconstructed_up.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.lit("UP Mid").alias("series"),
    ]).select(["time_sec", "mid", "series"])

    _recon_down = reconstructed_down.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.lit("DOWN Mid").alias("series"),
    ]).select(["time_sec", "mid", "series"])

    _mid_data = pl.concat([_recon_up, _recon_down])

    # Sample if too many points
    _sample_rate = max(1, _mid_data.height // 10000)
    _mid_sampled = _mid_data.gather_every(_sample_rate)

    # Prepare fills with token labels
    _fills = fills_df.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.when(pl.col("token_id") == up_token)
        .then(pl.lit("UP"))
        .when(pl.col("token_id") == down_token)
        .then(pl.lit("DOWN"))
        .otherwise(pl.lit("OTHER"))
        .alias("token_side"),
    ])

    # Mid price line chart
    _mid_chart = alt.Chart(_mid_sampled).mark_line(opacity=0.6, strokeWidth=2).encode(
        x=alt.X("time_sec:Q", title="Time (seconds)"),
        y=alt.Y("mid:Q", title="Price", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Orderbook", scale=alt.Scale(
            domain=["UP Mid", "DOWN Mid"],
            range=["#1f77b4", "#ff7f0e"]
        ), legend=alt.Legend(orient="right")),
    )

    # Fills scatter chart
    _fills_chart = alt.Chart(_fills).mark_circle(size=80, opacity=0.9).encode(
        x=alt.X("time_sec:Q"),
        y=alt.Y("price:Q"),
        color=alt.Color("side:N", scale=alt.Scale(
            domain=["buy", "sell"],
            range=["#2ca02c", "#d62728"]
        ), title="Fill Side", legend=alt.Legend(orient="right")),
        shape=alt.Shape("token_side:N", scale=alt.Scale(
            domain=["UP", "DOWN", "OTHER"],
            range=["circle", "square", "triangle-up"]
        ), title="Token", legend=alt.Legend(orient="right")),
        tooltip=["time_sec:Q", "token_side:N", "side:N", "price:Q", "size:Q"],
    )

    _combined = (_mid_chart + _fills_chart).properties(
        width=800,
        height=400,
        title="Fills Over Reconstructed Orderbook Mid"
    ).resolve_scale(
        color="independent"
    ).interactive()

    _combined
    return


@app.cell
def _(
    alt,
    br,
    down_token,
    features_df,
    fills_df,
    initial_cash,
    initial_sets,
    mo,
    pl,
    up_token,
):
    mo.stop(fills_df is None or fills_df.is_empty(), mo.md("No fills data for inventory"))

    _initial = int(initial_sets.value) if initial_sets.value else 0
    _init_cash = float(initial_cash.value) if initial_cash.value else 0.0

    # Compute PnL via library
    _pnl = br.compute_pnl(fills_df, features_df, up_token, down_token, _initial, _init_cash)

    # Build inventory chart data (visualization-specific)
    _start_ms = fills_df["timestamp_ms"].min()
    _fills = fills_df.sort("timestamp_ms").with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.when(pl.col("token_id") == up_token)
        .then(pl.lit("UP"))
        .when(pl.col("token_id") == down_token)
        .then(pl.lit("DOWN"))
        .otherwise(pl.lit("OTHER"))
        .alias("token"),
        pl.when(pl.col("side") == "buy")
        .then(pl.col("size"))
        .otherwise(-pl.col("size"))
        .alias("inv_delta"),
    ])

    _up_inv = _fills.filter(pl.col("token") == "UP").with_columns(
        (pl.col("inv_delta").cum_sum() + _initial).alias("inventory")
    ).select(["time_sec", "inventory", "token"])

    _down_inv = _fills.filter(pl.col("token") == "DOWN").with_columns(
        (pl.col("inv_delta").cum_sum() + _initial).alias("inventory")
    ).select(["time_sec", "inventory", "token"])

    _inv_data = pl.concat([_up_inv, _down_inv])

    _pnl_text = ""
    if _pnl.up_wins is not None:
        _pnl_text = f"""
    **PnL Breakdown:**
    - Initial: ${_pnl.initial_cash:.2f} cash + {_pnl.initial_sets} sets @ $1 = **${_pnl.initial_portfolio:.2f}**
    - Cash from trades: ${_pnl.total_cash_flow:+.2f}
    - Final cash: ${_pnl.final_cash:.2f}
    - Final inventory payout: {_pnl.up_final_inventory:.1f} UP × ${_pnl.up_payout:.0f} + {_pnl.down_final_inventory:.1f} DOWN × ${_pnl.down_payout:.0f} = **${_pnl.final_inventory_value:.2f}**
    - Final portfolio: ${_pnl.final_portfolio:.2f}
    - **PnL: ${_pnl.pnl:+.2f}**
    """

    _inv_chart = alt.Chart(_inv_data).mark_line(strokeWidth=2).encode(
        x=alt.X("time_sec:Q", title="Time (seconds)"),
        y=alt.Y("inventory:Q", title="Inventory (tokens)"),
        color=alt.Color("token:N", title="Token", scale=alt.Scale(
            domain=["UP", "DOWN"],
            range=["#1f77b4", "#ff7f0e"]
        ), legend=alt.Legend(orient="right")),
    ).properties(
        width=800,
        height=300,
        title=f"Inventory Over Time (initial: {_initial} each)"
    ).interactive()

    mo.vstack([
        _inv_chart,
        mo.md(f"""
    **Market Resolution:** {_pnl.resolution_label}

    **Final Inventory:** {_pnl.up_final_inventory:.1f} UP, {_pnl.down_final_inventory:.1f} DOWN (net: {_pnl.up_final_inventory - _pnl.down_final_inventory:+.1f})
    {_pnl_text}
        """)
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ## Adverse Selection Analysis

    Adverse selection occurs when you trade against informed flow - buying just before price drops, or selling just before price rises.

    For each fill, we measure the mid price movement over various horizons to determine if the fill was:
    - **Adversely selected**: Price moved against you (bought and price fell, or sold and price rose)
    - **Spread captured**: Price moved in your favor or stayed flat
    """)
    return


@app.cell
def _(mo):
    horizon_ms = mo.ui.slider(500, 30000, value=5000, step=500, label="Horizon (ms)")
    horizon_ms
    return (horizon_ms,)


@app.cell
def _(
    br,
    down_token,
    fills_df,
    horizon_ms,
    mo,
    reconstructed_down,
    reconstructed_up,
    up_token,
):
    mo.stop(
        fills_df is None or fills_df.is_empty() or reconstructed_up is None,
        mo.md("Need fills and reconstructed orderbook data"),
    )

    fills_analyzed = br.analyze_adverse_selection(
        fills_df, reconstructed_up, reconstructed_down,
        up_token, down_token, horizon_ms.value,
    )
    fills_analyzed
    return (fills_analyzed,)


@app.cell
def _(br, fills_analyzed, horizon_ms, mo):
    mo.stop(fills_analyzed is None or fills_analyzed.is_empty(), mo.md("No analyzed fills"))

    _s = br.summarize_adverse_selection(fills_analyzed)
    _horizon = horizon_ms.value

    mo.md(f"""
    ### Summary at {_horizon}ms Horizon

    | Metric | Value |
    |--------|-------|
    | Total fills | {_s['total']} |
    | Adverse fills | {_s['adverse_count']} ({_s['adverse_pct']:.1f}%) |
    | Favorable fills | {_s['favorable_count']} ({100 - _s['adverse_pct']:.1f}%) |
    | Adverse fill PnL | ${_s['adverse_pnl']:.2f} (avg ${_s['avg_adverse_pnl']:.4f}/fill) |
    | Favorable fill PnL | ${_s['favorable_pnl']:.2f} (avg ${_s['avg_favorable_pnl']:.4f}/fill) |
    | **Total MTM PnL** | **${_s['total_pnl']:.2f}** |

    **By Side:**
    - Buys: {_s['buy_count']} fills, {_s['buy_adverse']} adverse ({100*_s['buy_adverse']/_s['buy_count']:.1f}%)
    - Sells: {_s['sell_count']} fills, {_s['sell_adverse']} adverse ({100*_s['sell_adverse']/_s['sell_count']:.1f}%)

    **Edge Analysis:**
    - Avg edge at fill: ${_s['avg_edge']:.4f} (positive = executed better than mid)
    - Avg price movement (in our favor): ${_s['avg_price_move']:.4f}
    """)
    return


@app.cell
def _(alt, fills_analyzed, horizon_ms, mo, pl):
    mo.stop(fills_analyzed is None, mo.md("No data"))

    _horizon = horizon_ms.value
    _start_ms = fills_analyzed["timestamp_ms"].min()

    _plot_data = fills_analyzed.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.when(pl.col("is_adverse"))
        .then(pl.lit("Adverse"))
        .otherwise(pl.lit("Favorable"))
        .alias("selection_type"),
    ])

    # Scatter plot of MTM PnL per fill
    _scatter = alt.Chart(_plot_data).mark_circle(size=60, opacity=0.7).encode(
        x=alt.X("time_sec:Q", title="Time (seconds)"),
        y=alt.Y("mtm_pnl:Q", title=f"MTM PnL at {_horizon}ms ($)"),
        color=alt.Color("selection_type:N", scale=alt.Scale(
            domain=["Adverse", "Favorable"],
            range=["#d62728", "#2ca02c"]
        ), title="Selection"),
        shape=alt.Shape("side:N", scale=alt.Scale(
            domain=["buy", "sell"],
            range=["circle", "triangle-up"]
        ), title="Side"),
        tooltip=["time_sec:Q", "side:N", "token:N", "price:Q", "size:Q", "mtm_pnl:Q", "edge_at_fill:Q", "price_change:Q"],
    ).properties(
        width=800,
        height=350,
        title=f"Fill-by-Fill MTM PnL ({_horizon}ms horizon)"
    ).interactive()

    _scatter
    return


@app.cell
def _(alt, fills_analyzed, mo, pl):
    mo.stop(fills_analyzed is None, mo.md("No data"))

    # Histogram of price movements
    _hist_data = fills_analyzed.select([
        pl.col("pnl_direction").alias("price_move"),
        pl.col("side"),
        pl.col("is_adverse"),
    ])

    _hist = alt.Chart(_hist_data).mark_bar(opacity=0.7).encode(
        x=alt.X("price_move:Q", bin=alt.Bin(maxbins=50), title="Price Move in Our Favor ($)"),
        y=alt.Y("count():Q", title="Count"),
        color=alt.Color("side:N", scale=alt.Scale(
            domain=["buy", "sell"],
            range=["#1f77b4", "#ff7f0e"]
        ), title="Side"),
    ).properties(
        width=800,
        height=250,
        title="Distribution of Price Movements After Fills"
    )

    _hist
    return


@app.cell
def _(fills_analyzed, mo, pl):
    mo.stop(fills_analyzed is None, mo.md("No data"))

    # Analyze patterns: adverse selection by price level
    _by_price = fills_analyzed.with_columns([
        (pl.col("price") * 100).round().alias("price_cents"),
    ]).group_by(["price_cents", "side"]).agg([
        pl.len().alias("count"),
        pl.col("is_adverse").sum().alias("adverse_count"),
        pl.col("mtm_pnl").sum().alias("total_pnl"),
        pl.col("mtm_pnl").mean().alias("avg_pnl"),
    ]).with_columns([
        (pl.col("adverse_count") / pl.col("count") * 100).alias("adverse_pct"),
    ]).sort("price_cents")

    # By token
    _by_token = fills_analyzed.group_by(["token", "side"]).agg([
        pl.len().alias("count"),
        pl.col("is_adverse").sum().alias("adverse_count"),
        pl.col("mtm_pnl").sum().alias("total_pnl"),
        pl.col("mtm_pnl").mean().alias("avg_pnl"),
        pl.col("edge_at_fill").mean().alias("avg_edge"),
    ]).with_columns([
        (pl.col("adverse_count") / pl.col("count") * 100).alias("adverse_pct"),
    ]).sort(["token", "side"])

    mo.vstack([
        mo.md("### Adverse Selection by Token and Side"),
        _by_token,
        mo.md("### Adverse Selection by Price Level (cents)"),
        _by_price,
    ])
    return


@app.cell
def _(alt, fills_analyzed, mo, pl):
    mo.stop(fills_analyzed is None, mo.md("No data"))

    # Adverse % by price bucket
    _price_buckets = fills_analyzed.with_columns([
        (pl.col("price") * 20).round().alias("price_bucket"),  # 5 cent buckets
    ]).group_by(["price_bucket", "token"]).agg([
        pl.len().alias("count"),
        (pl.col("is_adverse").sum() / pl.len() * 100).alias("adverse_pct"),
        pl.col("mtm_pnl").sum().alias("total_pnl"),
    ]).with_columns([
        (pl.col("price_bucket") / 20).alias("price_level"),
    ]).filter(pl.col("count") >= 3)  # Only buckets with enough samples

    _chart = alt.Chart(_price_buckets).mark_bar(opacity=0.8).encode(
        x=alt.X("price_level:Q", title="Price Level ($)", scale=alt.Scale(zero=False)),
        y=alt.Y("adverse_pct:Q", title="Adverse Selection %"),
        color=alt.Color("token:N", scale=alt.Scale(
            domain=["UP", "DOWN"],
            range=["#1f77b4", "#ff7f0e"]
        )),
        column=alt.Column("token:N", title="Token"),
        tooltip=["price_level:Q", "count:Q", "adverse_pct:Q", "total_pnl:Q"],
    ).properties(
        width=350,
        height=250,
        title="Adverse Selection % by Price Level"
    )

    _chart
    return


@app.cell
def _(fills_analyzed, mo, pl):
    mo.stop(fills_analyzed is None, mo.md("No data"))

    # Cumulative PnL over time
    _start_ms = fills_analyzed["timestamp_ms"].min()
    _cum_pnl = fills_analyzed.sort("timestamp_ms").with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.col("mtm_pnl").cum_sum().alias("cumulative_pnl"),
    ])

    # Split by adverse vs favorable
    _adverse_fills = fills_analyzed.filter(pl.col("is_adverse")).sort("timestamp_ms")
    _favorable_fills = fills_analyzed.filter(~pl.col("is_adverse")).sort("timestamp_ms")

    _adverse_cum = _adverse_fills.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.col("mtm_pnl").cum_sum().alias("cumulative_pnl"),
        pl.lit("Adverse").alias("type"),
    ]).select(["time_sec", "cumulative_pnl", "type"]) if _adverse_fills.height > 0 else None

    _favorable_cum = _favorable_fills.with_columns([
        ((pl.col("timestamp_ms") - _start_ms) / 1000).alias("time_sec"),
        pl.col("mtm_pnl").cum_sum().alias("cumulative_pnl"),
        pl.lit("Favorable").alias("type"),
    ]).select(["time_sec", "cumulative_pnl", "type"]) if _favorable_fills.height > 0 else None

    mo.md(f"""
    ### Cumulative PnL Attribution

    - Adverse fills cumulative: ${_adverse_fills['mtm_pnl'].sum():.2f}
    - Favorable fills cumulative: ${_favorable_fills['mtm_pnl'].sum():.2f}
    - Net: ${fills_analyzed['mtm_pnl'].sum():.2f}

    If adverse selection is the main driver of losses, consider:
    1. **Widening spreads** at price levels with high adverse selection %
    2. **Reducing size** on the side (buy/sell) that gets picked off more
    3. **Faster cancellation** when detecting informed flow
    4. **Skewing quotes** based on recent trade flow direction
    """)
    return


if __name__ == "__main__":
    app.run()
