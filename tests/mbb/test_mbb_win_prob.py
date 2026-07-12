"""Tests for the season win-probability compile helper (``mbb_win_prob``).

The compile helper is an *identity* transform on the per-play ``home_win_prob``
column: :func:`mbb_in_game_win_prob` already produces the WP, and the helper
only attaches display metadata (ids, names, scores, play sequence) plus a
per-game pregame anchor. The **reproduction gate** below (helper output ==
``mbb_in_game_win_prob`` for a game, byte parity) therefore transitively
guarantees the decile-calibration gate already proven on the model in
``test_mbb_prediction_backtest.test_in_game_wp_decile_calibration``.
"""

from __future__ import annotations

import datetime

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob, predict_margin, win_prob_from_margin
from sportsdataverse.mbb.mbb_win_prob import _compile_season_wp, _WP_SCHEMA, build_mbb_season_wp

_MON1 = datetime.date(2024, 1, 1)  # Monday
_MON3 = datetime.date(2024, 1, 15)  # two weeks later (Monday)


def _schedule() -> pl.DataFrame:
    """Two week-1 games (rating source) + one week-3 game (gets an as-of prob).

    Ids are ``Int32`` to mirror the raw ``load_mbb_schedule`` dtype (the trap
    ``_pregame_probs`` must cast past before calling ``mbb_predict_games``).
    """
    return pl.DataFrame(
        {
            "game_id": [1, 2, 3],
            "season": [2024, 2024, 2024],
            "date": [_MON1, _MON1, _MON3],
            "home_team_id": [1, 3, 1],
            "away_team_id": [2, 4, 3],
            "home_score": [75, 60, 80],
            "away_score": [70, 66, 62],
            "neutral_site": [False, False, False],
        },
        schema_overrides={
            "game_id": pl.Int32,
            "season": pl.Int32,
            "home_team_id": pl.Int32,
            "away_team_id": pl.Int32,
            "home_score": pl.Int32,
            "away_score": pl.Int32,
        },
    )


def _team_box() -> pl.DataFrame:
    """Two team rows per week-1 game (efficiency source for the as-of ratings)."""
    rows = []
    for gid, (h, a) in ((1, (1, 2)), (2, (3, 4))):
        for tid, sc, opp in (
            (h, 75 if gid == 1 else 60, 70 if gid == 1 else 66),
            (a, 70 if gid == 1 else 66, 75 if gid == 1 else 60),
        ):
            rows.append(
                {
                    "game_id": gid,
                    "season": 2024,
                    "game_date": _MON1,
                    "team_id": tid,
                    "field_goals_attempted": 60.0,
                    "offensive_rebounds": 10.0,
                    "turnovers": 12.0,
                    "free_throws_attempted": 20.0,
                    "team_score": float(sc),
                }
            )
    return pl.DataFrame(rows, schema_overrides={"game_id": pl.Int32, "season": pl.Int32, "team_id": pl.Int32})


def _pbp_game(gid: int, home: int, away: int, hname: str, aname: str, date: datetime.date) -> pl.DataFrame:
    """A tiny 3-play pbp frame in the ``load_mbb_pbp`` schema (Int32 ids)."""
    return pl.DataFrame(
        {
            "game_id": [gid, gid, gid],
            "season": [2024, 2024, 2024],
            "game_play_number": [1, 2, 3],
            "game_date": [date, date, date],
            "home_team_name": [hname, hname, hname],
            "away_team_name": [aname, aname, aname],
            "home_score": [0, 40, 80],
            "away_score": [0, 38, 62],
            "start_game_seconds_remaining": [2400, 1200, 20],
            "team_id": [home, away, home],
            "home_team_id": [home, home, home],
        },
        schema_overrides={
            "game_id": pl.Int32,
            "season": pl.Int32,
            "game_play_number": pl.Int32,
            "home_score": pl.Int32,
            "away_score": pl.Int32,
            "start_game_seconds_remaining": pl.Int32,
            "team_id": pl.Int32,
            "home_team_id": pl.Int32,
        },
    )


def _pbp() -> pl.DataFrame:
    # game 1 (week 1, no prior -> fallback anchor), game 3 (week 3, real as-of prob)
    return pl.concat([_pbp_game(1, 1, 2, "Aggies", "Bruins", _MON1), _pbp_game(3, 1, 3, "Aggies", "Cougars", _MON3)])


def test_output_schema_and_dtypes():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    assert out.columns == list(_WP_SCHEMA)
    assert {k: out.schema[k] for k in _WP_SCHEMA} == _WP_SCHEMA
    # game_id emitted as Utf8 (join-key discipline), not the Int32 it arrives as
    assert out.schema["game_id"] == pl.Utf8


def test_sorted_by_game_then_play():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    assert out.select("game_id", "game_play_number").rows() == sorted(out.select("game_id", "game_play_number").rows())


def test_reproduces_in_game_win_prob_for_a_game():
    """GATE: per-play home_win_prob is exactly mbb_in_game_win_prob's output."""
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    g = out.filter(pl.col("game_id") == "3").sort("game_play_number")
    p0 = g.get_column("pregame_home_prob")[0]
    expected = mbb_in_game_win_prob(_pbp_game(3, 1, 3, "Aggies", "Cougars", _MON3), p0)
    assert g.get_column("home_win_prob").to_list() == expected.get_column("home_win_prob").to_list()


def test_week3_game_gets_real_as_of_prob_not_fallback():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    fallback = win_prob_from_margin(predict_margin(0.0, 0.0, neutral=False), league="mens")
    p3 = out.filter(pl.col("game_id") == "3").get_column("pregame_home_prob")[0]
    assert abs(p3 - fallback) > 1e-9, "week-3 game should have an as-of rating-based prob, not the flat anchor"


def test_opening_week_game_uses_fallback_anchor():
    out = _compile_season_wp(_pbp(), _schedule(), _team_box())
    fallback = win_prob_from_margin(predict_margin(0.0, 0.0, neutral=False), league="mens")
    p1 = out.filter(pl.col("game_id") == "1").get_column("pregame_home_prob")[0]
    assert abs(p1 - fallback) < 1e-9, "opening-week game has no prior -> HFA-only fallback anchor"


def test_empty_pbp_returns_zero_row_schema():
    out = _compile_season_wp(pl.DataFrame(), _schedule(), _team_box())
    assert out.height == 0
    assert out.columns == list(_WP_SCHEMA)


def test_return_as_pandas(monkeypatch):
    monkeypatch.setattr(
        "sportsdataverse.mbb.mbb_win_prob._load_league_frames",
        lambda season, league: (_pbp(), _schedule(), _team_box()),
    )
    out = build_mbb_season_wp(2024, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns) == list(_WP_SCHEMA)
