"""WNBA props for the 2026 expansion teams, Portland Fire (132052) and Toronto Tempo (131935).

Neither team has a prior season, and on its first game neither has a game before the cut. The
fixtures are the real opening week of the 2026 season (``wnba_*_2026_opening.parquet``, see
``tests/fixtures/nba_prediction/README.md``); the whole projection path runs on them: the box
loader and the ratings engine's schedule / team box loaders are stubbed with the fixtures, and
everything downstream (as-of cut, ``player_rates``, ``nba_team_ratings``, pace) is the real code.
"""

import importlib
import math

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wnba.wnba_player_props import wnba_player_props

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"
props = importlib.import_module("sportsdataverse.nba.nba_player_props")
ratings_mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")
POR, TOR = "132052", "131935"


@pytest.fixture
def opening_week(monkeypatch):
    box = pl.read_parquet(FIXTURES / "wnba_player_box_2026_opening.parquet")
    team_box = pl.read_parquet(FIXTURES / "wnba_team_box_2026_opening.parquet")
    sched = pl.read_parquet(FIXTURES / "wnba_schedule_2026_opening.parquet")
    monkeypatch.setattr(
        importlib.import_module("sportsdataverse.wnba.wnba_loaders"), "load_wnba_player_boxscore", lambda s: box
    )
    monkeypatch.setattr(ratings_mod, "_league_loaders", lambda lg: (lambda s: sched, lambda s: team_box))
    return box


@pytest.mark.parametrize(
    ("game_id", "home", "away"),
    [("401856891", TOR, "16"), ("401856895", POR, "19")],  # TOR's opener (night one); POR's opener (night two)
)
def test_an_expansion_teams_first_game_returns_the_empty_schema(opening_week, game_id, home, away):
    out = wnba_player_props(2026, game_id, home, away)
    assert out.height == 0
    assert dict(out.schema) == props._PROPS_SCHEMA


def test_a_team_missing_from_the_ratings_gets_the_league_pace():
    # night two: the ratings know the six teams that played night one, not Portland or Chicago
    ratings = pl.DataFrame({"team_id": ["9", "18"], "adj_pace": [96.0, 94.0]})
    avg = importlib.import_module("sportsdataverse.nba.nba_prediction_constants").get_constants("10").avg_pace
    assert props.team_pace_projection(POR, "19", ratings, league_id="10") == pytest.approx(avg)
    assert props.team_pace_projection(POR, "9", ratings, league_id="10") == pytest.approx(96.0)


@pytest.mark.parametrize(
    ("game_id", "home", "away", "team"),
    [("401856903", POR, "9", POR), ("401856904", TOR, "14", TOR), ("401856912", "6", TOR, TOR)],
)
def test_an_expansion_team_with_one_or_two_games_projects_finite_lines(opening_week, game_id, home, away, team):
    out = wnba_player_props(2026, game_id, home, away)
    mine = out.filter(pl.col("team_id") == team)
    assert mine.height >= 8  # its rotation from the games it has played
    for col in ("stat_pts_exp", "stat_reb_exp", "stat_ast_exp", "stat_fg3m_exp", "minutes_exp", "pace_proj"):
        assert out[col].null_count() == 0, col
        assert all(math.isfinite(v) and v >= 0 for v in out[col].to_list()), col
    assert mine["minutes_exp"].sum() == pytest.approx(200.0)
    assert 0 < mine["stat_pts_exp"].sum() < 200  # a WNBA team scores 60-120 a game
