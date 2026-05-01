from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from lambdaclass.config import Preferences
from lambdaclass.strategies.base import Strategy, StrategyContext


@dataclass
class RunResult:
    trades: pd.DataFrame
    equity_curve: pd.DataFrame
    final_cash: float
    final_position: int


def _commission(decision_qty: int, prefs: Preferences) -> float:
    return abs(decision_qty) * prefs.defaults.commission_per_contract


def run_backtest(
    strategy: Strategy,
    bars: pd.DataFrame,
    options_chain: pd.DataFrame,
    preferences: Preferences,
) -> RunResult:
    bars_sorted = bars.sort_values("date").reset_index(drop=True)
    chain_by_date = {
        str(key): frame.reset_index(drop=True)
        for key, frame in options_chain.groupby("asof")
    } if not options_chain.empty else {}
    cash = float(preferences.defaults.starting_capital)
    position = 0
    trades: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    for _, row in bars_sorted.iterrows():
        date_key = str(row["date"])
        chain = chain_by_date.get(date_key)
        context = StrategyContext(
            row=row,
            cash=cash,
            position=position,
            options_chain=chain,
        )
        decision = strategy.on_bar(context)
        price = float(row["close"])
        qty = int(decision.quantity)
        if decision.action == "buy" and qty > 0:
            total_cost = (price * qty) + _commission(qty, preferences)
            slippage = price * (preferences.defaults.slippage_bps / 10_000.0) * qty
            cash -= total_cost + slippage
            position += qty
            trades.append(
                {
                    "date": date_key,
                    "action": "buy",
                    "quantity": qty,
                    "price": price,
                    "cash_after": cash,
                }
            )
        elif decision.action == "sell" and qty > 0 and position > 0:
            executed = min(qty, position)
            proceeds = (price * executed) - _commission(executed, preferences)
            slippage = price * (preferences.defaults.slippage_bps / 10_000.0) * executed
            cash += proceeds - slippage
            position -= executed
            trades.append(
                {
                    "date": date_key,
                    "action": "sell",
                    "quantity": executed,
                    "price": price,
                    "cash_after": cash,
                }
            )
        equity = cash + (position * price)
        equity_records.append(
            {
                "date": date_key,
                "cash": cash,
                "position": position,
                "close": price,
                "equity": equity,
            }
        )
    return RunResult(
        trades=pd.DataFrame(trades),
        equity_curve=pd.DataFrame(equity_records),
        final_cash=cash,
        final_position=position,
    )


def write_run_outputs(run_result: RunResult, run_dir: Path) -> tuple[Path, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    trades_path = run_dir / "trades.csv"
    equity_path = run_dir / "equity.parquet"
    if run_result.trades.empty:
        pd.DataFrame(columns=["date", "action", "quantity", "price", "cash_after"]).to_csv(
            trades_path, index=False
        )
    else:
        run_result.trades.to_csv(trades_path, index=False)
    run_result.equity_curve.to_parquet(equity_path, index=False)
    return trades_path, equity_path
