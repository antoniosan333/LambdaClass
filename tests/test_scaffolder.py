from datetime import datetime
from pathlib import Path

from lambdaclass.strategies.scaffolder import ensure_month_dir, scaffold_strategy


def test_ensure_month_dir_creates_expected_path(tmp_path: Path) -> None:
    now = datetime(2026, 4, 30, 12, 0, 0)
    month_dir = ensure_month_dir(tmp_path, now=now)
    assert month_dir.exists()
    assert month_dir.name == "2026-04"
    assert (month_dir / "_template.py").exists()


def test_scaffold_strategy_creates_file(tmp_path: Path) -> None:
    now = datetime(2026, 4, 30, 12, 0, 0)
    strategy_file = scaffold_strategy(tmp_path, "demo_strategy", now=now)
    assert strategy_file.exists()
    content = strategy_file.read_text(encoding="utf-8")
    assert "class StrategyImpl" in content
    assert 'name = "demo_strategy"' in content
