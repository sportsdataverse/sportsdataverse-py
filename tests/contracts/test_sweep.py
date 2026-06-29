from __future__ import annotations

import polars as pl

from tools.validation.checks import sweep
from tools.validation.findings import CheckContext, Severity


def test_duplicate_join_keys_is_error():
    frame = pl.DataFrame({"game_id": [1, 1, 2], "play_id": [1, 1, 1]})
    ctx = CheckContext(domain="nfl", dataset="nfl_pbp", schema={}, join_keys=("game_id", "play_id"))
    findings = sweep.run("nfl_pbp", frame, ctx)
    assert any(f.severity is Severity.ERROR and "duplicate" in f.message for f in findings)


def test_mean_shift_vs_prior_release_is_warn():
    prior = pl.DataFrame({"game_id": [1, 2], "epa": [0.0, 0.0]})
    frame = pl.DataFrame({"game_id": [3, 4], "epa": [5.0, 5.0]})  # large shift
    ctx = CheckContext(
        domain="nfl",
        dataset="nfl_pbp",
        schema={},
        join_keys=("game_id",),
        prior_frame=prior,
        thresholds={"mean_shift_warn": 0.10},
    )
    findings = sweep.run("nfl_pbp", frame, ctx)
    assert any(f.severity is Severity.WARN and "shifted" in f.message and f.needs_judgment for f in findings)
