from pathlib import Path

import polars as pl

from tools.validation.checks import boundary_leakage
from tools.validation.findings import CheckContext, Severity

_FIX = Path(__file__).parent / "fixtures"


def _ctx(**kw):
    base = dict(domain="nfl", dataset="nfl_pbp", schema={}, group_key="game_id", lag_columns=("prev_ep",))
    base.update(kw)
    return CheckContext(**base)


def test_lag_nonnull_on_first_of_game_is_error():
    # game 1 first row has a carried prev_ep (leak); proper resets are null on row 0
    frame = pl.DataFrame(
        {
            "game_id": [1, 1, 2, 2],
            "prev_ep": [3.5, 1.0, None, 2.0],  # game 1 row0 = 3.5 (LEAK); game 2 row0 = None (ok)
        }
    )
    findings = boundary_leakage.run("nfl_pbp", frame, _ctx())
    assert any(f.severity is Severity.ERROR and "prev_ep" in f.message for f in findings)


def test_clean_lag_resets_yield_no_findings():
    frame = pl.DataFrame(
        {
            "game_id": [1, 1, 2, 2],
            "prev_ep": [None, 1.0, None, 2.0],  # both games reset to null on row 0
        }
    )
    assert boundary_leakage.run("nfl_pbp", frame, _ctx()) == []


def test_no_group_key_column_skips():
    frame = pl.DataFrame({"prev_ep": [None, 1.0]})
    assert boundary_leakage.run("nfl_pbp", frame, _ctx()) == []


def test_lag_column_absent_from_frame_skips():
    frame = pl.DataFrame({"game_id": [1, 1]})  # prev_ep absent
    assert boundary_leakage.run("nfl_pbp", frame, _ctx()) == []


def test_cumulative_reset_is_clean():
    frame = pl.read_parquet(_FIX / "boundary_cumulative_clean.parquet")
    ctx = CheckContext(
        domain="cfb", dataset="d", schema={}, group_key="game_id", cumulative_columns=("game_play_number",)
    )
    assert boundary_leakage.run("d", frame, ctx) == []


def test_cumulative_nonreset_is_warn_needs_judgment():
    frame = pl.read_parquet(_FIX / "boundary_cumulative_leak.parquet")
    ctx = CheckContext(
        domain="cfb", dataset="d", schema={}, group_key="game_id", cumulative_columns=("game_play_number",)
    )
    findings = boundary_leakage.run("d", frame, ctx)
    assert len(findings) == 1
    assert findings[0].severity is Severity.WARN and findings[0].needs_judgment
    assert findings[0].locator["column"] == "game_play_number"
