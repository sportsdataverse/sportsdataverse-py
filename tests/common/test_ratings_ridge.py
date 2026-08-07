"""Unit tests for the two shared ridge engines in ``sportsdataverse._common.ratings``.

These lock in the pure-function behavior directly (the per-league
byte-for-byte regression against real data lives in each league's own
``test_*_ratings*`` suite, which re-runs unchanged after the T7.2 retarget).
"""

import numpy as np
import polars as pl
import pytest

from sportsdataverse._common.ratings import dropped_level_ridge, opponent_adjusted_ridge


def test_opponent_adjusted_ridge_empty_returns_schema() -> None:
    frame, intercept, home = opponent_adjusted_ridge(
        pl.DataFrame(schema={"o": pl.Utf8, "d": pl.Utf8, "h": pl.Utf8, "y": pl.Float64}),
        off_col="o",
        def_col="d",
        home_col="h",
        resp_col="y",
        lam=10.0,
    )
    assert frame.columns == ["team_id", "off_coef", "def_coef"]
    assert frame.height == 0
    assert intercept == 0.0 and home == 0.0


def test_opponent_adjusted_ridge_recovers_injected_offense_ordering() -> None:
    rng = np.random.default_rng(0)
    true_off = {"A": 0.20, "B": 0.0, "C": -0.20}
    true_def = {"A": 0.0, "B": 0.05, "C": -0.05}
    rows = []
    for o in "ABC":
        for d in "ABC":
            if o == d:
                continue
            for h in (o, d):  # alternate home team so `h` isn't collinear with the intercept
                for _ in range(40):
                    rows.append((o, d, h, true_off[o] - true_def[d] + rng.normal(0, 0.02)))
    plays = pl.DataFrame(rows, schema=["o", "d", "h", "y"], orient="row")
    frame, _intercept, _home = opponent_adjusted_ridge(
        plays, off_col="o", def_col="d", home_col="h", resp_col="y", lam=1.0
    )
    out = frame.sort("off_coef", descending=True)
    assert out["team_id"].to_list()[0] == "A"
    a_net = (
        out.filter(pl.col("team_id") == "A")["off_coef"].item()
        - out.filter(pl.col("team_id") == "A")["def_coef"].item()
    )
    c_net = (
        out.filter(pl.col("team_id") == "C")["off_coef"].item()
        - out.filter(pl.col("team_id") == "C")["def_coef"].item()
    )
    assert a_net > c_net


def test_opponent_adjusted_ridge_home_edge_positive() -> None:
    rows = []
    for o in "AB":
        for d in "AB":
            if o == d:
                continue
            for h in (o, d):  # h==o -> offense is home; h==d -> defense is home
                for _ in range(50):
                    rows.append((o, d, h, 0.03 if h == o else 0.0))
    plays = pl.DataFrame(rows, schema=["o", "d", "h", "y"], orient="row")
    _frame, _intercept, home_coef = opponent_adjusted_ridge(
        plays, off_col="o", def_col="d", home_col="h", resp_col="y", lam=1.0
    )
    assert 0.0 < home_coef < 0.06


def test_dropped_level_ridge_emits_the_reference_team_at_the_intercept() -> None:
    """The reference level has no design-matrix column, but it IS a team.

    This test previously asserted the opposite -- that the first-sorted team was
    absent from the output -- which encoded the bug db4361a4 fixed. Emitting only
    the dummy columns silently dropped one team per side from every fit, and the
    season path joins opponent strength with ``fill_strength=None``, so that
    team's opponents got a null adjustment and were filtered out downstream too.
    Which team it hit was arbitrary: the sort runs on the STRING id, so it was
    whichever sorted first lexicographically ("100" < "1005" < "101").

    Under model.matrix encoding the reference effect is 0 and lives in the
    intercept, so its strength is exactly ``intercept`` -- asserted below rather
    than merely checking presence, because a reference row carrying some other
    value would be a different bug wearing the same shape.
    """
    rng = np.random.default_rng(1)
    rows = []
    teams = ["Alpha", "Beta", "Gamma"]
    strength = {"Alpha": 0.15, "Beta": 0.0, "Gamma": -0.15}
    for o in teams:
        for d in teams:
            if o == d:
                continue
            for _ in range(30):
                rows.append((o, d, o, "N", strength[o] - strength[d] + rng.normal(0, 0.02), 1, 0))
    clean = pl.DataFrame(
        rows, schema=["pos_team_id", "def_pos_team_id", "pos_team", "home", "EPA", "pass", "rush"], orient="row"
    ).with_columns(hfa=pl.lit(0))
    offense, defense, intercept = dropped_level_ridge(clean, ridge_lambda=5.0)

    # No team may be missing from either side -- that is the regression.
    assert sorted(offense["team_id"].to_list()) == teams
    assert sorted(defense["team_id"].to_list()) == teams

    # "Alpha" sorts first, so it is the reference level on BOTH sides: effect 0,
    # strength == intercept. Checked on offense and defense separately -- the two
    # sides are built from separate coefficient blocks, so a wrong value on one
    # would otherwise ride through on the other's assertion.
    alpha_off = offense.filter(pl.col("team_id") == "Alpha")["adjmodelOff"].item()
    alpha_def = defense.filter(pl.col("team_id") == "Alpha")["adjmodelDef"].item()
    assert alpha_off == pytest.approx(intercept, abs=1e-12)
    assert alpha_def == pytest.approx(intercept, abs=1e-12)

    # The fit still recovers the injected ordering (Alpha 0.15 > Beta 0.0 > Gamma -0.15),
    # so keeping the reference row has not distorted the ratings it sits alongside.
    beta_off = offense.filter(pl.col("team_id") == "Beta")["adjmodelOff"].item()
    gamma_off = offense.filter(pl.col("team_id") == "Gamma")["adjmodelOff"].item()
    assert alpha_off > beta_off > gamma_off
    assert isinstance(intercept, float)
