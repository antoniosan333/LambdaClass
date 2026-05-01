from __future__ import annotations

import json
from pathlib import Path

from lambdaclass.data_adapters.optionsdx_normalize import NormalizeOptions, run_normalize


def test_run_normalize_writes_parquet_and_reports(tmp_path: Path) -> None:
    fixture_root = Path(__file__).parent / "fixtures" / "optionsdx"
    out = tmp_path / "normalized"
    rep = tmp_path / "reports"
    state = tmp_path / "state" / "optionsdx_normalize_state.json"
    state.parent.mkdir(parents=True, exist_ok=True)
    opts = NormalizeOptions(
        input_root=fixture_root,
        output_root=out,
        reports_dir=rep,
        state_path=state,
        dry_run=False,
        fail_on_errors=False,
    )
    summary = run_normalize(opts)
    assert summary["totals"]["files"] == 2
    assert summary["totals"]["output_rows"] == 3
    assert summary["totals"]["parse_errors"] == 0
    assert (rep / "runs").exists()
    run_files = list((rep / "runs").glob("*.json"))
    assert len(run_files) == 1
    run_payload = json.loads(run_files[0].read_text(encoding="utf-8"))
    assert run_payload["totals"]["schema_33_files"] == 1
    assert run_payload["totals"]["schema_28_files"] == 1
    pq = list(out.rglob("*.parquet"))
    assert len(pq) == 2
    assert state.exists()


def test_run_normalize_dry_run_skips_writes(tmp_path: Path) -> None:
    fixture_root = Path(__file__).parent / "fixtures" / "optionsdx"
    out = tmp_path / "normalized2"
    rep = tmp_path / "reports2"
    state = tmp_path / "state2" / "optionsdx_normalize_state.json"
    state.parent.mkdir(parents=True, exist_ok=True)
    opts = NormalizeOptions(
        input_root=fixture_root,
        output_root=out,
        reports_dir=rep,
        state_path=state,
        dry_run=True,
    )
    summary = run_normalize(opts)
    assert summary["totals"]["files"] == 2
    assert not out.exists() or not any(out.rglob("*.parquet"))
    assert not rep.exists()
