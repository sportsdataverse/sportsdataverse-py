"""Tests for the WP4 WAR layer: calibration helpers + nba_war scoring."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
from sportsdataverse.nba.nba_war import calibrate_pts_per_win, calibrate_replacement_level


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
