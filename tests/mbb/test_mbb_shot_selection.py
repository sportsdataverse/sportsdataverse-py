"""Tests for shot-selection value (``mbb_shot_selection``)."""

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_shot_selection import mbb_shot_selection


def _scored(shooter, xp, n):
    return pl.DataFrame(
        {
            "shooter_id": [shooter] * n,
            "team_id": ["T"] * n,
            "xpoints": [xp] * n,
            "point_value": [2] * n,
            "made": [True] * n,
        }
    )


def test_selection_value_sign_and_zero_sum():
    df = pl.concat([_scored("A", 1.30, 50), _scored("B", 0.70, 50)])
    out = mbb_shot_selection(df, group="shooter_id")
    a = out.filter(pl.col("shooter_id") == "A").row(0, named=True)
    b = out.filter(pl.col("shooter_id") == "B").row(0, named=True)
    assert a["selection_value"] > 0 > b["selection_value"]
    assert abs((out["selection_value"] * out["n_shots"]).sum()) < 1e-6
    assert a["n_shots"] == 50 and abs(a["xppp"] - 1.30) < 1e-9
    assert abs(a["actual_ppp"] - 2.0) < 1e-9
    assert abs(a["selection_value_total"] - a["selection_value"] * 50) < 1e-9


def test_selection_group_team_and_pandas():
    df = pl.concat([_scored("A", 1.30, 10), _scored("B", 0.70, 10)])
    out = mbb_shot_selection(df, group="team_id")
    assert out.height == 1 and out.schema["team_id"] == pl.Utf8
    pdf = mbb_shot_selection(df, group="shooter_id", return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")


def test_selection_unknown_group_raises():
    with pytest.raises(ValueError):
        mbb_shot_selection(_scored("A", 1.0, 5), group="conference_id")


def test_selection_empty_input():
    out = mbb_shot_selection(pl.DataFrame(schema={"shooter_id": pl.Utf8, "xpoints": pl.Float64}))
    assert out.height == 0
    assert out.columns == ["shooter_id", "n_shots", "xppp", "actual_ppp", "selection_value", "selection_value_total"]
