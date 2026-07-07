from __future__ import annotations

import polars as pl

from tools.validation.checks import constant_column
from tools.validation.findings import CheckContext, Severity


def _ctx(**kw) -> CheckContext:
    base = dict(domain="cfb", dataset="d", schema={})
    base.update(kw)
    return CheckContext(**base)


def test_all_null_column_flagged() -> None:
    frame = pl.DataFrame({"a": [1, 2, 3], "dead": [None, None, None]})
    findings = constant_column.run("d", frame, _ctx())
    assert len(findings) == 1
    f = findings[0]
    assert f.check == "constant_column" and f.severity is Severity.WARN and f.needs_judgment
    assert f.locator == {"column": "dead", "kind": "all_null"}


def test_all_zero_numeric_flagged() -> None:
    frame = pl.DataFrame({"a": [1, 2, 3], "sacked": [0.0, 0.0, 0.0]})
    findings = constant_column.run("d", frame, _ctx())
    assert [f.locator["column"] for f in findings] == ["sacked"]
    assert findings[0].locator["kind"] == "constant"


def test_single_value_string_flagged() -> None:
    frame = pl.DataFrame({"a": [1, 2, 3], "division": ["fbs", "fbs", "fbs"]})
    findings = constant_column.run("d", frame, _ctx())
    assert [f.locator["column"] for f in findings] == ["division"]


def test_varying_column_is_clean() -> None:
    frame = pl.DataFrame({"a": [1, 2, 3], "b": [0.0, 1.0, 0.0]})
    assert constant_column.run("d", frame, _ctx()) == []


def test_allowlist_excludes_column() -> None:
    frame = pl.DataFrame({"season": [2024, 2024, 2024], "dead": [None, None, None]})
    ctx = _ctx(expected_constant_columns=("season", "dead"))
    assert constant_column.run("d", frame, ctx) == []


def test_all_null_and_constant_reports_once_as_all_null() -> None:
    frame = pl.DataFrame({"dead": [None, None, None]}, schema={"dead": pl.Float64})
    findings = constant_column.run("d", frame, _ctx())
    assert len(findings) == 1 and findings[0].locator["kind"] == "all_null"


def test_empty_frame_returns_no_findings() -> None:
    frame = pl.DataFrame({"a": []}, schema={"a": pl.Float64})
    assert constant_column.run("d", frame, _ctx()) == []


def test_all_nan_float_column_flagged_as_all_null() -> None:
    frame = pl.DataFrame(
        {"a": [1.0, 2.0, 3.0], "dead": [float("nan"), float("nan"), float("nan")]},
        schema={"a": pl.Float64, "dead": pl.Float64},
    )
    findings = constant_column.run("d", frame, _ctx())
    assert len(findings) == 1
    assert findings[0].locator["column"] == "dead"
    assert findings[0].locator["kind"] == "all_null"


def test_partial_nan_float_column_is_judged_on_real_values() -> None:
    # [1.0, nan, 1.0] — single real value → flagged as constant
    frame_const = pl.DataFrame(
        {"col": [1.0, float("nan"), 1.0]},
        schema={"col": pl.Float64},
    )
    findings_const = constant_column.run("d", frame_const, _ctx())
    assert len(findings_const) == 1
    assert findings_const[0].locator["kind"] == "constant"

    # [1.0, nan, 2.0] — two distinct real values → NOT flagged
    frame_vary = pl.DataFrame(
        {"col": [1.0, float("nan"), 2.0]},
        schema={"col": pl.Float64},
    )
    findings_vary = constant_column.run("d", frame_vary, _ctx())
    assert findings_vary == []
