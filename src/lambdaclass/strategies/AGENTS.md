# `strategies` — agent context

## Base API

`Strategy` (`base.py`): `name`, `params`, `on_bar(context: StrategyContext) -> StrategyDecision`.

`StrategyContext`: bar row, `cash`, `position`, optional `options_chain` for that bar date.

## Scaffolding

`scaffolder.py`:

- `ensure_month_dir(strategies_dir, now)` → `strategies/<YYYY-MM>/`, ensures `_template.py` exists.
- `scaffold_strategy(strategies_dir, strategy_name, now)` → writes `<YYYY-MM>/<strategy_name>.py` from `TEMPLATE` if missing.

Strategy files must define `StrategyImpl` with `name` and `params` dict; default template is a minimal buy/hold example.

## CLI alignment

- Strategy **name** validated: `^[A-Za-z][A-Za-z0-9_]*$`.
- Lookup: `strategies_dir.glob(f"*/{strategy_name}.py")` — monthly folders; latest match wins.
- Resolved file must remain under `strategies_dir` (path confinement).

Parent: [AGENTS.md](../../../AGENTS.md).
