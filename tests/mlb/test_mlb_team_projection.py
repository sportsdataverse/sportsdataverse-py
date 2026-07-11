"""Tests for pythagenpat + as-of-date Elo team projection."""

import datetime as dt
import math

import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import ELO_INIT
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat, mlb_team_elo


def test_pythagenpat_symmetric_is_half():
    assert abs(mlb_pythagenpat(700, 700, 162) - 0.5) < 1e-9


def test_pythagenpat_monotone():
    assert mlb_pythagenpat(800, 600, 162) > 0.5 > mlb_pythagenpat(600, 800, 162)


def test_pythagenpat_exponent_value():
    # x = ((rs+ra)/g)^0.287 ; rs=750 ra=650 g=162
    x = ((750 + 650) / 162) ** 0.287
    assert abs(mlb_pythagenpat(750, 650, 162) - 750**x / (750**x + 650**x)) < 1e-12


def test_pythagenpat_zero_games_guard():
    assert mlb_pythagenpat(0, 0, 0) == 0.5


def _dominant_vs_weak_results(n_games: int = 20) -> pl.DataFrame:
    # Team A hosts and crushes Team B every game; alternate home/away so
    # both the home and away rating-update paths get exercised.
    rows = []
    for i in range(n_games):
        d = dt.date(2024, 4, 1) + dt.timedelta(days=i)
        if i % 2 == 0:
            rows.append(
                {
                    "game_id": f"G{i}",
                    "date": d,
                    "home_team_id": "A",
                    "away_team_id": "B",
                    "home_score": 8,
                    "away_score": 1,
                }
            )
        else:
            rows.append(
                {
                    "game_id": f"G{i}",
                    "date": d,
                    "home_team_id": "B",
                    "away_team_id": "A",
                    "home_score": 1,
                    "away_score": 8,
                }
            )
    return pl.DataFrame(rows)


def test_elo_dominant_team_ends_above_init_and_above_weak_team():
    results = _dominant_vs_weak_results()
    elo = mlb_team_elo(results)
    assert elo.height == results.height
    assert elo["home_win_prob_elo"].min() > 0.0 and elo["home_win_prob_elo"].max() < 1.0

    a_final = elo.filter(pl.col("home_team_id") == "A")["home_rating_post"].to_list()
    a_final += elo.filter(pl.col("away_team_id") == "A")["away_rating_post"].to_list()
    b_final = elo.filter(pl.col("home_team_id") == "B")["home_rating_post"].to_list()
    b_final += elo.filter(pl.col("away_team_id") == "B")["away_rating_post"].to_list()
    assert max(a_final) > ELO_INIT
    assert max(a_final) > max(b_final)


def test_elo_as_of_date_no_leakage():
    results = _dominant_vs_weak_results()
    elo = elo_first = mlb_team_elo(results).sort(["date", "game_id"])
    # First game's pre-game ratings must equal init -- no leakage from later games.
    first = elo_first.row(0, named=True)
    assert math.isclose(first["home_rating"], ELO_INIT) and math.isclose(first["away_rating"], ELO_INIT)
    # Game i's pre-game rating equals game (i-1)'s post-game rating for that same team.
    rows = elo.to_dicts()
    ratings_seen: dict = {}
    for row in rows:
        if row["home_team_id"] in ratings_seen:
            assert math.isclose(row["home_rating"], ratings_seen[row["home_team_id"]])
        if row["away_team_id"] in ratings_seen:
            assert math.isclose(row["away_rating"], ratings_seen[row["away_team_id"]])
        ratings_seen[row["home_team_id"]] = row["home_rating_post"]
        ratings_seen[row["away_team_id"]] = row["away_rating_post"]
