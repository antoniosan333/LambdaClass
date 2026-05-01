from pathlib import Path

import pytest
import typer

from lambdaclass.cli import _validate_strategy_name
from lambdaclass.config import Preferences, snapshot_preferences


def test_strategy_name_validation_rejects_path_characters() -> None:
    with pytest.raises(typer.BadParameter):
        _validate_strategy_name("../escape")


def test_snapshot_redacts_sensitive_keys(tmp_path: Path) -> None:
    destination = tmp_path / "config.snapshot.toml"
    snapshot_preferences(
        preferences=Preferences(),
        strategy_params={"api_key": "secret-123", "units": 2},
        cli_overrides={"password": "pw", "start": "2020-01-01"},
        output_path=destination,
    )
    content = destination.read_text(encoding="utf-8")
    assert "***REDACTED***" in content
    assert "secret-123" not in content
    assert "pw" not in content
