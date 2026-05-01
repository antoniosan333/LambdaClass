from __future__ import annotations

from pathlib import Path

import plotly.express as px
import pandas as pd


def write_tearsheet(equity_curve: pd.DataFrame, destination: Path, theme: str = "plotly_dark") -> Path:
    if equity_curve.empty:
        fig = px.line(pd.DataFrame({"date": [], "equity": []}), x="date", y="equity", title="Equity Curve")
    else:
        fig = px.line(equity_curve, x="date", y="equity", title="Equity Curve")
    fig.update_layout(template=theme)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(destination), include_plotlyjs="cdn")
    return destination
