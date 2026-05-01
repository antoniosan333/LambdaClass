# `reporting` — agent context

## `metrics.compute_metrics`

Input: equity curve `DataFrame` with `equity` column.

Output dict keys: `cagr`, `max_drawdown`, `sharpe`, `hit_rate`, `total_return`. Returns zeros if empty or missing column.

## `tearsheet.write_tearsheet`

Plotly line chart (`date` vs `equity`), `fig.update_layout(template=theme)`, `write_html(..., include_plotlyjs="cdn")`.

Theme default from prefs: `preferences.reporting.plot_theme` (e.g. `plotly_dark`). HTML written only when `preferences.reporting.save_html` is true (`cli` `run`).

Parent: [AGENTS.md](../../../AGENTS.md).
