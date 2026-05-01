# `storage` — agent context

## `DuckDBStore`

Root: `Preferences.paths.data_dir` (default `data/`).

| Method | Parquet path | Dedupe key |
|--------|----------------|------------|
| `write_bars` / `read_bars` | `data/stocks/<SYMBOL>.parquet` | `(symbol, date)` — `keep="last"` |
| `write_chain` / `read_chain` | `data/options/<SYMBOL>.parquet` | `(symbol, contract_symbol, asof)` — `keep="last"` |

On append: read existing if present, `concat`, `drop_duplicates`, sort (`date` for bars; `asof`, `contract_symbol` for options), write.

Reads use ephemeral `duckdb.connect()` + `read_parquet(?)` with optional `WHERE` on `date` or `asof`.

Dirs created in ctor: `stocks/`, `options/`, `cache/`.

OptionsDX **normalized** Parquet layout is **not** under this class — it lives under `data/optionsdx/normalized/...` (see `data_adapters/optionsdx_normalize.py` and root AGENTS).

Parent: [AGENTS.md](../../../AGENTS.md).
