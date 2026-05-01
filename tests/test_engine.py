import pandas as pd

from lambdaclass.backtest.engine import run_backtest
from lambdaclass.config import Preferences
from lambdaclass.strategies.base import Strategy, StrategyContext, StrategyDecision


class BuyOnceStrategy(Strategy):
    name = "buy_once"
    params = {"units": 1}

    def on_bar(self, context: StrategyContext) -> StrategyDecision:
        if context.position == 0:
            return StrategyDecision(action="buy", quantity=1)
        return StrategyDecision(action="hold", quantity=0)


def test_engine_is_deterministic() -> None:
    bars = pd.DataFrame(
        [
            {"date": "2026-01-01", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1_000},
            {"date": "2026-01-02", "open": 102, "high": 103, "low": 101, "close": 102, "volume": 1_000},
        ]
    )
    options = pd.DataFrame()
    prefs = Preferences()
    strategy = BuyOnceStrategy()
    first = run_backtest(strategy, bars, options, prefs)
    second = run_backtest(strategy, bars, options, prefs)
    assert first.equity_curve.equals(second.equity_curve)
    assert first.trades.equals(second.trades)
