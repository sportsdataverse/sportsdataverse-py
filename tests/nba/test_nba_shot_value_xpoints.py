"""Tests for the xPoints baseline table + per-shot scorer."""

import polars as pl

from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, xpoints_baseline


def _league_avgs():
    return pl.DataFrame(
        {
            "shot_zone_basic": ["Restricted Area", "Above the Break 3", "Mid-Range"],
            "shot_zone_area": ["Center(C)", "Center(C)", "Left Side(L)"],
            "shot_zone_range": ["Less Than 8 ft.", "24+ ft.", "16-24 ft."],
            "fga": [1000, 800, 500],
            "fgm": [640, 288, 200],
            "fg_pct": [0.640, 0.360, 0.400],
        }
    )


def _shots():
    return pl.DataFrame(
        {
            "game_id": ["0022200001", "0022200001"],
            "player_id": [201939, 201939],
            "shot_type": ["3PT Field Goal", "2PT Field Goal"],
            "shot_zone_basic": ["Above the Break 3", "Restricted Area"],
            "shot_zone_area": ["Center(C)", "Center(C)"],
            "shot_zone_range": ["24+ ft.", "Less Than 8 ft."],
            "shot_made_flag": [1, 0],
        }
    )


def test_baseline_marks_threes_and_carries_pct():
    base = xpoints_baseline(_league_avgs())
    ra = base.filter(pl.col("shot_zone_basic") == "Restricted Area").row(0, named=True)
    atb = base.filter(pl.col("shot_zone_basic") == "Above the Break 3").row(0, named=True)
    assert ra["is_three"] is False and abs(ra["base_fg_pct"] - 0.640) < 1e-9
    assert atb["is_three"] is True


def test_baseline_empty_schema():
    out = xpoints_baseline(
        pl.DataFrame(
            schema={
                "shot_zone_basic": pl.Utf8,
                "shot_zone_area": pl.Utf8,
                "shot_zone_range": pl.Utf8,
                "fga": pl.Int64,
                "fgm": pl.Int64,
                "fg_pct": pl.Float64,
            }
        )
    )
    assert out.height == 0 and "base_fg_pct" in out.columns and "is_three" in out.columns


def test_score_xpoints_math():
    out = score_shot_xpoints(_shots(), _league_avgs())
    three = out.filter(pl.col("shot_type") == "3PT Field Goal").row(0, named=True)
    two = out.filter(pl.col("shot_type") == "2PT Field Goal").row(0, named=True)
    assert three["shot_value"] == 3 and abs(three["xpoints"] - 0.360 * 3) < 1e-9
    assert abs(three["actual_points"] - 3.0) < 1e-9  # made 3
    assert two["shot_value"] == 2 and abs(two["xpoints"] - 0.640 * 2) < 1e-9
    assert abs(two["actual_points"] - 0.0) < 1e-9  # missed 2


def test_score_xpoints_dtype_mismatch_raises():
    import pytest

    # cast a zone key on the SHOTS side so the baseline builds cleanly and the
    # join-key dtype guard is what fires (not xpoints_baseline's str ops)
    bad = _shots().with_columns(pl.col("shot_zone_basic").cast(pl.Categorical))
    with pytest.raises(ValueError):
        score_shot_xpoints(bad, _league_avgs())


def test_score_xpoints_empty_schema():
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "shot_type": pl.Utf8,
            "shot_zone_basic": pl.Utf8,
            "shot_zone_area": pl.Utf8,
            "shot_zone_range": pl.Utf8,
            "shot_made_flag": pl.Int64,
        }
    )
    out = score_shot_xpoints(empty, _league_avgs())
    assert out.height == 0 and "xpoints" in out.columns and "actual_points" in out.columns
