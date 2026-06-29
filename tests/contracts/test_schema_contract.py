from __future__ import annotations

import polars as pl

from tools.validation.checks import schema_contract
from tools.validation.findings import CheckContext, Severity


def _ctx(**kw):
    base = dict(
        domain="nfl",
        dataset="nfl_pbp",
        schema={"game_id": "Int64", "epa": "Float64"},
        required_columns=("game_id",),
        join_keys=("game_id",),
    )
    base.update(kw)
    return CheckContext(**base)


def test_join_key_dtype_mismatch_is_error():
    frame = pl.DataFrame({"game_id": ["1", "2"], "epa": [0.1, 0.2]})  # str, expected Int64
    findings = schema_contract.run("nfl_pbp", frame, _ctx())
    hits = [f for f in findings if f.locator.get("is_join_key")]
    assert len(hits) == 1
    assert hits[0].severity is Severity.ERROR
    assert hits[0].expected == "Int64" and hits[0].actual == "String"


def test_missing_column_is_error():
    frame = pl.DataFrame({"epa": [0.1, 0.2]})  # game_id absent
    findings = schema_contract.run("nfl_pbp", frame, _ctx())
    assert any(f.severity is Severity.ERROR and "missing" in f.message for f in findings)


def test_extra_column_is_error():
    frame = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2], "extra": [0, 1]})
    findings = schema_contract.run("nfl_pbp", frame, _ctx())
    assert any(f.severity is Severity.ERROR and "unexpected" in f.message for f in findings)


def test_null_in_required_column_is_error():
    frame = pl.DataFrame({"game_id": [1, None], "epa": [0.1, 0.2]})
    findings = schema_contract.run("nfl_pbp", frame, _ctx())
    assert any(
        f.severity is Severity.ERROR and "null" in f.message and f.locator.get("column") == "game_id" for f in findings
    )


def test_clean_frame_yields_no_findings():
    frame = pl.DataFrame({"game_id": [1, 2], "epa": [0.1, 0.2]})
    assert schema_contract.run("nfl_pbp", frame, _ctx()) == []
