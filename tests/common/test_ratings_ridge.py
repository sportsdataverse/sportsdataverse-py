"""Unit tests for the two shared ridge engines in ``sportsdataverse._common.ratings``.

These lock in the pure-function behavior directly (the per-league
byte-for-byte regression against real data lives in each league's own
``test_*_ratings*`` suite, which re-runs unchanged after the T7.2 retarget).
"""

import numpy as np
import polars as pl

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


def test_dropped_level_ridge_drops_first_team_alphabetically() -> None:
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
    # "Alpha" is the first sorted team_id -> dropped as the reference level.
    assert "Alpha" not in offense["team_id"].to_list()
    assert "Beta" in offense["team_id"].to_list() and "Gamma" in offense["team_id"].to_list()
    beta_off = offense.filter(pl.col("team_id") == "Beta")["adjmodelOff"].item()
    gamma_off = offense.filter(pl.col("team_id") == "Gamma")["adjmodelOff"].item()
    assert beta_off > gamma_off
    assert isinstance(intercept, float)
