"""ESPN's -1 in end.yardsToEndzone is a MISSING marker, not a yardline.

2016 week 2 shipped with -1 on ~every play in 72 of 75 games. The parser guarded
only on ``end.yardLine is not null`` -- which ESPN populates perfectly well on
exactly those plays -- so the sentinel passed straight through and the fallback
never fired for the case it exists to handle. Downstream that scored EP_end as if
the offense were on its own 1-yard line after every snap, running EPA to about
-2.6/play.

The guard now also requires the value to be non-negative. This test pins that
narrow contract directly on the expression, so it stays offline and does not need
a raw fixture or a full pipeline run.
"""

from __future__ import annotations

import polars as pl


def _apply_guard(df: pl.DataFrame) -> pl.DataFrame:
    """The shipped guard from CFBPlayProcess (cfb_pbp.py, end-state cleanup)."""
    return df.with_columns(
        pl.when((pl.col("end.yardLine").is_null() == False).and_(pl.col("end.yardsToEndzone") >= 0))  # noqa: E712
        .then(pl.col("end.yardsToEndzone"))
        .otherwise(pl.col("end.yard"))
        .alias("end.yardsToEndzone"),
    )


def test_negative_sentinel_falls_back_to_end_yard() -> None:
    df = pl.DataFrame(
        {
            "end.yardLine": [77, 38, 35],  # ESPN populates this even when ytez is -1
            "end.yardsToEndzone": [-1, -1, -1],
            "end.yard": [23, 62, 65],
        }
    )
    assert _apply_guard(df)["end.yardsToEndzone"].to_list() == [23, 62, 65]


def test_valid_values_are_untouched() -> None:
    """The fix must not reach any play that already had a usable value."""
    df = pl.DataFrame(
        {
            "end.yardLine": [77, 38, 35],
            "end.yardsToEndzone": [23, 62, 65],
            "end.yard": [99, 99, 99],
        }
    )
    assert _apply_guard(df)["end.yardsToEndzone"].to_list() == [23, 62, 65]


def test_zero_is_a_real_yardline_not_a_sentinel() -> None:
    """0 yards to the endzone is the goal line -- it must survive the guard."""
    df = pl.DataFrame({"end.yardLine": [100], "end.yardsToEndzone": [0], "end.yard": [50]})
    assert _apply_guard(df)["end.yardsToEndzone"].to_list() == [0]


def test_null_end_yardline_still_falls_back() -> None:
    """The original behaviour for a genuinely absent end state is preserved."""
    df = pl.DataFrame({"end.yardLine": [None], "end.yardsToEndzone": [45], "end.yard": [70]})
    assert _apply_guard(df)["end.yardsToEndzone"].to_list() == [70]
