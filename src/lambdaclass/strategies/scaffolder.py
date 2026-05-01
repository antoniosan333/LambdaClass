from __future__ import annotations

from datetime import datetime
from pathlib import Path

TEMPLATE = """from lambdaclass.strategies.base import Strategy, StrategyContext, StrategyDecision


class StrategyImpl(Strategy):
    name = "{strategy_name}"
    params = {{
        "units": 1,
    }}

    def on_bar(self, context: StrategyContext) -> StrategyDecision:
        if context.position == 0:
            return StrategyDecision(action="buy", quantity=self.params["units"])
        return StrategyDecision(action="hold", quantity=0)
"""


def ensure_month_dir(strategies_dir: Path, now: datetime | None = None) -> Path:
    timestamp = now or datetime.now()
    month_dir = strategies_dir / timestamp.strftime("%Y-%m")
    month_dir.mkdir(parents=True, exist_ok=True)
    template_file = month_dir / "_template.py"
    if not template_file.exists():
        template_file.write_text(TEMPLATE.format(strategy_name="template_strategy"), encoding="utf-8")
    return month_dir


def scaffold_strategy(
    strategies_dir: Path,
    strategy_name: str,
    now: datetime | None = None,
) -> Path:
    month_dir = ensure_month_dir(strategies_dir, now=now)
    strategy_file = month_dir / f"{strategy_name}.py"
    if strategy_file.exists():
        return strategy_file
    strategy_file.write_text(TEMPLATE.format(strategy_name=strategy_name), encoding="utf-8")
    return strategy_file
