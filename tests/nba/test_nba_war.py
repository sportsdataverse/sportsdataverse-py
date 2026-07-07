"""Tests for the WP4 WAR layer: calibration helpers + nba_war scoring."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from sportsdataverse.nba.nba_war import WAR_SCHEMA, calibrate_pts_per_win, calibrate_replacement_level, nba_war


def test_calibrate_pts_per_win_recovers_planted_noiseless_relationship():
    # planted: wins = 41 + total_margin / 250  (K=250 points of margin per marginal win)
    margins = np.array([-400.0, -200.0, -100.0, 0.0, 100.0, 200.0, 300.0, 400.0, 500.0, -300.0])
    wins = 41.0 + margins / 250.0
    team_season = pl.DataFrame({"team_id": list(range(1, 11)), "wins": wins.tolist(), "total_margin": margins.tolist()})
    pts_per_win = calibrate_pts_per_win(team_season)
    assert abs(pts_per_win - 250.0) < 1e-6


def test_calibrate_pts_per_win_tolerates_noise():
    rng = np.random.default_rng(0)
    margins = np.linspace(-500.0, 500.0, 30)
    wins = 41.0 + margins / 260.0 + rng.normal(0, 1.0, size=30)
    team_season = pl.DataFrame({"team_id": list(range(1, 31)), "wins": wins.tolist(), "total_margin": margins.tolist()})
    pts_per_win = calibrate_pts_per_win(team_season)
    assert abs(pts_per_win - 260.0) / 260.0 < 0.2  # within 20%


def test_calibrate_pts_per_win_raises_on_too_few_rows():
    team_season = pl.DataFrame({"team_id": [1, 2], "wins": [40.0, 42.0], "total_margin": [-10.0, 10.0]})
    with pytest.raises(ValueError, match="team-season"):
        calibrate_pts_per_win(team_season)


def test_calibrate_pts_per_win_raises_on_zero_variance_margin():
    team_season = pl.DataFrame({"team_id": [1, 2, 3], "wins": [40.0, 41.0, 42.0], "total_margin": [0.0, 0.0, 0.0]})
    with pytest.raises(ValueError, match="zero variance"):
        calibrate_pts_per_win(team_season)


def test_calibrate_replacement_level_solves_the_target_equation():
    ratings = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "rating": [3.0, 1.0, 0.0, -1.0, -3.0]})
    poss = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "poss": [1000, 1000, 1000, 1000, 1000]})
    repl = calibrate_replacement_level(ratings, poss, pts_per_win=250.0, target_total_war=10.0)
    assert abs(repl - (-50.0)) < 1e-9


def test_calibrate_replacement_level_raises_on_empty_join():
    ratings = pl.DataFrame({"player_id": [1], "rating": [3.0]})
    poss = pl.DataFrame({"player_id": [2], "poss": [1000]})
    with pytest.raises(ValueError, match="no shared player_id"):
        calibrate_replacement_level(ratings, poss, pts_per_win=250.0, target_total_war=10.0)


# ---------------------------------------------------------------------------
# Task 4: nba_war scoring function
# ---------------------------------------------------------------------------


def test_nba_war_matches_hand_computed_values():
    ratings = pl.DataFrame({"player_id": [1, 2], "rating": [5.0, -5.0]})
    poss = pl.DataFrame({"player_id": [1, 2], "poss": [800, 800]})
    out = nba_war(ratings, poss, replacement_level=0.0, pts_per_win=200.0)
    assert dict(out.schema) == WAR_SCHEMA
    got = dict(zip(out["player_id"], out["war"]))
    assert abs(got[1] - 0.2) < 1e-9
    assert abs(got[2] - (-0.2)) < 1e-9


def test_nba_war_consistent_with_calibrate_replacement_level():
    """Closed-loop check: calibrate a replacement level for a target, then
    verify nba_war's own summed output actually hits that target."""
    ratings = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "rating": [3.0, 1.0, 0.0, -1.0, -3.0]})
    poss = pl.DataFrame({"player_id": [1, 2, 3, 4, 5], "poss": [1000, 1000, 1000, 1000, 1000]})
    repl = calibrate_replacement_level(ratings, poss, pts_per_win=250.0, target_total_war=10.0)
    out = nba_war(ratings, poss, replacement_level=repl, pts_per_win=250.0)
    assert abs(out["war"].sum() - 10.0) < 1e-9


def test_nba_war_custom_column_names():
    ratings = pl.DataFrame({"player_id": [1], "my_rating": [10.0]})
    poss = pl.DataFrame({"player_id": [1], "my_poss": [500]})
    out = nba_war(ratings, poss, replacement_level=0.0, pts_per_win=100.0, rating_col="my_rating", poss_col="my_poss")
    assert abs(out["war"][0] - (10.0 * 500 / 100 / 100.0)) < 1e-9


def test_nba_war_empty_ratings_returns_documented_schema():
    empty = pl.DataFrame(schema={"player_id": pl.Int64, "rating": pl.Float64})
    poss = pl.DataFrame({"player_id": [1], "poss": [500]})
    out = nba_war(empty, poss, replacement_level=0.0, pts_per_win=100.0)
    assert out.height == 0
    assert dict(out.schema) == WAR_SCHEMA


def test_nba_war_disjoint_nonempty_inputs_raises():
    ratings = pl.DataFrame({"player_id": [1], "rating": [5.0]})
    poss = pl.DataFrame({"player_id": [2], "poss": [500]})
    with pytest.raises(ValueError, match="no shared player_id"):
        nba_war(ratings, poss, replacement_level=0.0, pts_per_win=100.0)


def test_nba_war_return_as_pandas():
    import pandas as pd

    ratings = pl.DataFrame({"player_id": [1], "rating": [5.0]})
    poss = pl.DataFrame({"player_id": [1], "poss": [500]})
    out = nba_war(ratings, poss, replacement_level=0.0, pts_per_win=100.0, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
