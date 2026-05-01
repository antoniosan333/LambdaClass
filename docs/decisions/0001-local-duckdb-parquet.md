# ADR-0001: Local DuckDB + Parquet for market data

**Status:** Accepted

## Context

LambdaClass is a backtesting-first toolkit: users run it locally, fetch history (e.g. via yfinance), and iterate on strategies. The project needs durable, queryable storage without operating a separate database server.

## Decision

Use **Parquet files** on disk for OHLCV bars and option chains under `data/stocks/` and `data/options/`, with **DuckDB** used ephemerally (`duckdb.connect()`) only to run `read_parquet` SQL for filtered reads (`DuckDBStore` in `src/lambdaclass/storage/duckdb_store.py`).

Dependencies are declared in `pyproject.toml`: `duckdb>=1.1.3`, `pyarrow>=18.0.0`, `pandas>=2.2.0`.

OptionsDX normalization outputs additional Parquet under `data/optionsdx/normalized/` (partitioned by symbol and calendar month), separate from the yfinance-backed `DuckDBStore` paths.

## Consequences

- **Pros:** No network or DB ops for core backtests; easy backup/copy of `data/`; fits reproducible local workflows.
- **Cons:** Concurrent writers to the same Parquet file are not a design goal; large universes may need sharding or external stores later.
- **Follow-up:** If a hosted DB is adopted, treat it as a new ADR and preserve Parquet as an export or cache if needed.
