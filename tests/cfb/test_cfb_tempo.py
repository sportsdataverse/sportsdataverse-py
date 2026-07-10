"""Tests for cfb_adjusted_tempo (synthetic, offline)."""

import polars as pl

import importlib

m = importlib.import_module("sportsdataverse.cfb.cfb_tempo")


def _synthetic_pbp() -> pl.DataFrame:
    """Round-robin A/B/C/D. A is fast (62 plays/game, 25 s/play); everyone
    else runs 45 in mutual games and 60 against A's defense (30 s/play).
    A's opponents are slow, so A's adjusted pace should exceed its raw pace.
    """
    teams = {"A": 1, "B": 2, "C": 3, "D": 4}
    games = [("A", "B"), ("A", "C"), ("A", "D"), ("B", "C"), ("B", "D"), ("C", "D")]
    rows = []
    for gi, (h, a) in enumerate(games):
        gid = 401000000 + gi
        for off, dfn in ((h, a), (a, h)):
            if off == "A":
                plays, sec = 62, 25
            elif dfn == "A":
                plays, sec = 60, 30
            else:
                plays, sec = 45, 30
            for p in range(plays):
                rows.append(
                    {
                        "season": 2021,
                        "game_id": gid,
                        "period": 1,
                        "pos_team_id": teams[off],
                        "def_pos_team_id": teams[dfn],
                        "scrimmage_play": True,
                        "play_type": "Rush",
                        "pos_team_score": 0,
                        "def_pos_team_score": 0,
                        "start_time_secs_rem": 1800 - p,
                        "end_time_secs_rem": 1800 - p - sec,
                    }
                )
    # one kneel + one garbage-time play for A: both must be excluded
    rows.append(
        {
            "season": 2021,
            "game_id": 401000000,
            "period": 1,
            "pos_team_id": 1,
            "def_pos_team_id": 2,
            "scrimmage_play": True,
            "play_type": "Kneel",
            "pos_team_score": 0,
            "def_pos_team_score": 0,
            "start_time_secs_rem": 10,
            "end_time_secs_rem": 0,
        }
    )
    rows.append(
        {
            "season": 2021,
            "game_id": 401000000,
            "period": 4,
            "pos_team_id": 1,
            "def_pos_team_id": 2,
            "scrimmage_play": True,
            "play_type": "Rush",
            "pos_team_score": 50,
            "def_pos_team_score": 0,
            "start_time_secs_rem": 5,
            "end_time_secs_rem": 0,
        }
    )
    return pl.DataFrame(rows)


def test_adjusted_tempo_math_and_ordering(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: _synthetic_pbp())
    out = m.cfb_adjusted_tempo([2021])
    assert out.schema["team_id"] == pl.Utf8
    a = out.filter(pl.col("team_id") == "1").row(0, named=True)
    assert a["games"] == 3
    # kneel + garbage plays excluded -> exactly 62/game
    assert abs(a["raw_plays_game"] - 62.0) < 1e-9
    assert abs(a["raw_sec_play"] - 25.0) < 1e-9
    # A's opponents are slow -> adjustment credits A with more pace
    assert a["adj_plays_game"] > a["raw_plays_game"]
    assert a["pace_rank"] == 1
    pdf = m.cfb_adjusted_tempo([2021], return_as_pandas=True)
    assert pdf.__class__.__module__.startswith("pandas")


def test_adjusted_tempo_empty(monkeypatch):
    monkeypatch.setattr(m, "load_cfb_pbp", lambda s, **k: pl.DataFrame())
    out = m.cfb_adjusted_tempo([1999])
    assert out.height == 0 and "pace_rank" in out.columns
