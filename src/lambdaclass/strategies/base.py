from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class StrategyContext:
    row: pd.Series
    cash: float
    position: int
    options_chain: pd.DataFrame | None = None


@dataclass
class StrategyDecision:
    action: str = "hold"
    quantity: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class Strategy(ABC):
    name: str = "base"
    params: dict[str, Any] = {}

    @abstractmethod
    def on_bar(self, context: StrategyContext) -> StrategyDecision:
        raise NotImplementedError

    def on_chain(self, context: StrategyContext) -> StrategyDecision:
        return StrategyDecision()
