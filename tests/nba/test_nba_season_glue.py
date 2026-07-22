"""Gates for the season-to-date glue + walk-forward calibration harness.

Offline: the leaguegamelog pivot on a committed real sample. Live
(``SDV_PY_LIVE_TESTS=1``): the full-season path — one release parquet
feeds the whole fitted tree — and a real walk-forward window with the
leakage assertion. Live thresholds are deliberately loose (the harness is
resilient to upstream data movement); observed 2026-07-22 (calibrated
pace anchor): season shelf sims 174.7 vs realized 173.3; walk-forward
07-08..07-21 total coverage .632, margin coverage .868, total bias -4.6
(train-anchored level lags a hot recent stretch — recency-weighted
anchoring is the recorded refinement).
"""

from __future__ import annotations

import datetime as dt
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import games_from_leaguegamelog
from tests.conftest import skip_if_no_live

SAMPLE = pathlib.Path("tests/fixtures/wnba_season/leaguegamelog_2026_sample.parquet")


def test_leaguegamelog_pivot_on_real_sample() -> None:
    log = pl.read_parquet(SAMPLE)
    games = games_from_leaguegamelog(log)
    assert games.height == 3
    assert games["game_id"].n_unique() == 3
    for row in games.iter_rows(named=True):
        assert row["home_team_id"] != row["away_team_id"]
        assert row["home_pts"] > 0 and row["away_pts"] > 0
        assert isinstance(row["game_date"], dt.date)
        assert row["completed"] is True
    # the pivot reconstructs each team's own PTS on the correct side
    raw = {(str(r["GAME_ID"]), int(r["TEAM_ID"])): int(r["PTS"]) for r in log.iter_rows(named=True)}
    for row in games.iter_rows(named=True):
        assert raw[(row["game_id"], row["home_team_id"])] == row["home_pts"]
        assert raw[(row["game_id"], row["away_team_id"])] == row["away_pts"]
    with pytest.raises(ValueError, match="missing columns"):
        games_from_leaguegamelog(log.drop("MATCHUP"))


def test_team_ratings_mechanics_on_real_sample() -> None:
    from sportsdataverse.nba.nba_possession_sim import fit_team_ratings, matchup_targets

    games = games_from_leaguegamelog(pl.read_parquet(SAMPLE))
    ratings = fit_team_ratings(games, shrinkage=2.0)
    assert set(ratings["off"]) == set(ratings["def"])
    assert len(ratings["off"]) == 6  # three games, six teams
    for factor in [*ratings["off"].values(), *ratings["def"].values()]:
        assert 0.7 < factor < 1.3  # one-game teams shrink hard toward league
    assert ratings["home_edge"] > 0 and ratings["team_mean"] > 60
    rated_home = next(iter(ratings["off"]))
    targets = matchup_targets(ratings, rated_home, 999_999)  # unrated away -> league prior
    assert targets["home"] > 0 and targets["away"] > 0
    with pytest.raises(ValueError, match="no completed"):
        fit_team_ratings(games.with_columns(pl.lit(False).alias("completed")))


@skip_if_no_live
def test_season_data_respects_the_cutoff() -> None:
    from sportsdataverse.nba.nba_possession_sim import season_data

    through = dt.date(2026, 7, 1)
    data = season_data("wnba", [2026], through=through)
    assert data["schedule"]["game_date"].max() <= through  # the leakage boundary
    assert data["events"].height > 20_000
    assert data["logs"].height > 1_500
    assert int(data["logs"]["ftm"].sum()) > 1_000
    included = set(data["schedule"].filter(pl.col("completed") == True)["game_id"].to_list())  # noqa: E712
    assert set(data["pbp"]["game_id"].unique().to_list()) <= included


@skip_if_no_live
def test_season_shelf_is_fully_fitted() -> None:
    from sportsdataverse.nba.nba_possession_sim import season_shelf

    shelf = season_shelf("wnba", [2026], through=dt.date(2026, 7, 1))
    assert len(shelf.outcome_pmfs) == 144
    assert shelf.pace_rates and len(shelf.pace_rates) >= 100
    assert shelf.aux_rates and len(shelf.aux_rates) == 144
    assert 8.0 < shelf.mean_possession_seconds < 20.0


@skip_if_no_live
def test_walk_forward_window_is_leakage_safe_and_calibrated() -> None:
    from sportsdataverse.nba.nba_possession_sim import walk_forward_backtest

    result = walk_forward_backtest(
        "wnba", 2026, start=dt.date(2026, 7, 15), end=dt.date(2026, 7, 21), n_sim=150, min_train_games=40
    )
    summary = result["summary"]
    assert summary["n_games"] > 0 and summary["n_refits"] > 0
    assert summary["max_train_date_lt_eval"] is True  # never trains on the eval date
    assert 0.4 <= summary["total_coverage_80"] <= 1.0  # observed .71 at nominal .80
    assert 0.4 <= summary["margin_coverage_80"] <= 1.0  # observed .84
    assert abs(summary["total_bias"]) < 15.0  # observed -5.4
    # team factors give real discrimination: full 07-08..07-21 window
    # observed winner brier .1999 vs .25 baseline (favorites 28/38); the
    # short CI window gets a loose absolute bound for resilience
    assert summary["winner_baseline_brier"] == pytest.approx(0.25, abs=0.02)
    assert summary["winner_brier"] < 0.30
    games = result["games"]
    assert (games["total_p10"] <= games["total_p90"]).all()
    assert ((games["p_home"] >= 0.0) & (games["p_home"] <= 1.0)).all()
