# `backtest` — agent context

## `run_backtest`

Inputs: `Strategy`, sorted bars `DataFrame`, options chain `DataFrame`, `Preferences`.

- Cash starts at `preferences.defaults.starting_capital`.
- Options grouped by `asof` (string key); chain for bar date passed into `StrategyContext`.
- **Buy**: `price * qty + commission(qty) + slippage` from `slippage_bps`.
- **Sell**: min(requested qty, position); proceeds minus commission and slippage.
- Equity each bar: `cash + position * close`.

Returns `RunResult`: `trades`, `equity_curve`, `final_cash`, `final_position`.

## `write_run_outputs`

Writes under caller-provided `run_dir`:

- `trades.csv` (empty schema if no trades)
- `equity.parquet`

CLI additionally writes `metrics.json`, `config.snapshot.toml`, `run.log`, optional `report.html` — see `cli.py` `run` command.

Run directory layout: `runs/<YYYY-MM>/<strategy.name>/<run_id>/` (month from `ensure_month_dir` at run time).

Parent: [AGENTS.md](../../../AGENTS.md).
