from lambdaclass.strategies.base import Strategy, StrategyContext, StrategyDecision


class StrategyImpl(Strategy):
    name = "demo"
    params = {
        "units": 1,
    }

    def on_bar(self, context: StrategyContext) -> StrategyDecision:
        if context.position == 0:
            return StrategyDecision(action="buy", quantity=self.params["units"])
        return StrategyDecision(action="hold", quantity=0)
