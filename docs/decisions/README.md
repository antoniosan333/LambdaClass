# Architecture Decision Records (ADRs)

Short, append-only records of **why** LambdaClass chose a path — not user guides (see root `README.md` and `AGENTS.md`).

## When to add an ADR

Write a new ADR when you change or lock in:

- Storage or on-disk layout (Parquet paths, DuckDB usage, normalization output)
- CLI surface or default behavior users rely on
- OptionsDX normalization rules or quality semantics
- A significant dependency or integration choice

Skip ADRs for one-line bugfixes unless they reverse a prior decision.

## Naming

`NNNN-kebab-title.md` — four-digit sequence (zero-padded), then a short kebab-case slug. Next free number after the highest existing file.

## Status

Use in the document frontmatter or first line:

- **Proposed** — under discussion
- **Accepted** — current truth for the codebase
- **Superseded** — link to the replacing ADR

## Template

Copy [0000-template.md](0000-template.md) and fill in sections.

## Index

| ADR | Title |
|-----|--------|
| [0001](0001-local-duckdb-parquet.md) | Local DuckDB + Parquet for market data |
| [0002](0002-options-chain-source.md) | Options chain source for backtests (`yfinance` vs `optionsdx`) |
