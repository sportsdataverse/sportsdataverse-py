from __future__ import annotations

import polars as pl

from tools.validation.checks import extraction
from tools.validation.findings import CheckContext, Severity


def _ctx(**kw):
    base = dict(domain="cfb", dataset="espn_cfb_pbp", schema={})
    base.update(kw)
    return CheckContext(**base)


def test_totally_null_extraction_is_error():
    frame = pl.DataFrame(
        {
            "cleaned_text": ["Smith run for 3", "Jones pass complete"],
            "rush_player_name": [None, None],
        }
    )
    findings = extraction.run("espn_cfb_pbp", frame, _ctx())
    assert any(f.severity is Severity.ERROR and "100% null" in f.message for f in findings)


def test_low_coverage_is_warn_needs_judgment_with_sample():
    frame = pl.DataFrame(
        {
            "cleaned_text": ["a", "b", "c", "d"],
            "rush_player_name": ["X", None, None, None],  # 25% coverage
        }
    )
    findings = extraction.run("espn_cfb_pbp", frame, _ctx(thresholds={"extraction_coverage_floor": 0.95}))
    warns = [f for f in findings if f.severity is Severity.WARN]
    assert warns and warns[0].needs_judgment is True
    assert warns[0].sample  # example rows attached for the semantics agent
