from __future__ import annotations

import math

import pandas as pd


def compute_metrics(equity_curve: pd.DataFrame) -> dict[str, float]:
    if equity_curve.empty or "equity" not in equity_curve:
        return {
            "cagr": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "hit_rate": 0.0,
            "total_return": 0.0,
        }
    returns = equity_curve["equity"].pct_change().fillna(0.0)
    total_return = float((equity_curve["equity"].iloc[-1] / equity_curve["equity"].iloc[0]) - 1.0)
    periods = max(len(equity_curve), 1)
    cagr = float((1.0 + total_return) ** (252 / periods) - 1.0) if periods > 1 else 0.0
    volatility = float(returns.std(ddof=0))
    sharpe = float((returns.mean() / volatility) * math.sqrt(252)) if volatility > 0 else 0.0
    rolling_max = equity_curve["equity"].cummax()
    drawdowns = (equity_curve["equity"] - rolling_max) / rolling_max.replace(0, pd.NA)
    max_drawdown = float(drawdowns.fillna(0.0).min())
    hit_rate = float((returns > 0).sum() / max(len(returns), 1))
    return {
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "hit_rate": hit_rate,
        "total_return": total_return,
    }
