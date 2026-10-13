"""The generated ``lba_*`` wrappers: URL, wire parameters and the parser each one feeds (transport mocked)."""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.basketball import lba
from sportsdataverse.basketball.lba import legabasket as api

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "legabasket"
BASE = "https://www.legabasket.it/api"


def _load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


@pytest.fixture
def seen(monkeypatch):
    calls = []

    def fake(fixture):
        def _get(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return _load(fixture)

        monkeypatch.setattr(api, "_get", _get)
        return calls

    return fake


def test_nineteen_wrappers_are_exported_with_the_parsers():
    names = [n for n in lba.__all__ if n.startswith("lba_")]
    assert len(names) == 19 and all(callable(getattr(lba, n)) for n in names)
    assert sum(n.startswith("parse_lba_") for n in lba.__all__) == 19


WRAPPERS = [
    # name, kwargs, fixture, route, wire params the call must send
    (
        "lba_championships",
        {"season": 2027},
        "full/championships__s2026.json",
        "/championships/get-championships",
        {"cs_id": 1, "items": 1000, "s": 2027},
    ),
    (
        "lba_calendar",
        {"championship_id": 602, "round": 1},
        "full/calendar__602__d1.json",
        "/championships/get-championships-calendar-by-id",
        {"id": 602, "d": 1},
    ),
    (
        "lba_game",
        {"game_id": 25602},
        "full/matches__25602.json",
        "/championships/get-championships-matches-by-id",
        {"id": 25602, "sc": 1},
    ),
    (
        "lba_box_score",
        {"game_id": 25602},
        "full/matches__25602.json",
        "/championships/get-championships-matches-scores-by-id",
        {"id": 25602, "info": "true"},
    ),
    (
        "lba_team_box_score",
        {"game_id": 25602},
        "full/matches__25602.json",
        "/championships/get-championships-matches-scores-by-id",
        {"id": 25602, "info": "true"},
    ),
    (
        "lba_pbp",
        {"game_id": 25602},
        "full/pbp__25602.json",
        "/championships/get-championships-matches-play-by-play",
        {"id": 25602, "info": "true", "sort": "asc"},
    ),
    (
        "lba_pbp_legend",
        {"game_id": 25602},
        "full/pbp__25602.json",
        "/championships/get-championships-matches-play-by-play",
        {"id": 25602, "info": "true", "sort": "asc"},
    ),
    (
        "lba_standings",
        {"championship_id": 602, "round": 3},
        "full/ranking__602__d3.json",
        "/championships/get-championship-ranking",
        {"id": 602, "d": 3},
    ),
    ("lba_teams", {"season": 2027}, "full/teams__2026.json", "/teams/get-teams", {"year": 2027, "items": 50}),
    (
        "lba_team_players_stats",
        {"team_id": 1764, "season": 2027},
        "full/team_players_stats__1764.json",
        "/teams/get-team-players-stats",
        {"id": 1764, "s": 2027, "cs_id": 1},
    ),
    (
        "lba_game_log",
        {"player_id": 969, "season": 2027},
        "full/player_matches__969.json",
        "/players/get-player-matches",
        {"id": 969, "s": 2027, "cs_id": 1},
    ),
    (
        "lba_players_leaderboard",
        {"season": 2027, "category": "points"},
        "full/players_statistics__points.json",
        "/statistics/get-players-statistics",
        {"s": 2027, "cs_id": 1, "cat": "points"},
    ),
    (
        "lba_teams_leaderboard",
        {"board": "teams_offensive", "season": 2027},
        "full/teams_statistics__offensive.json",
        "/statistics/get-teams-statistics-by-type",
        {"type": "teams_offensive", "s": 2027, "cs_id": 1},
    ),
    (
        "lba_roster",
        {"team_id": 1764},
        "captures/2026/teams__get-team-roster.json",
        "/teams/get-team-roster",
        {"id": 1764},
    ),
    (
        "lba_roster_coach",
        {"team_id": 1764},
        "captures/2026/teams__get-team-roster.json",
        "/teams/get-team-roster",
        {"id": 1764},
    ),
    (
        "lba_players",
        {"season": 2027, "page": 2},
        "captures/2026/players__get-players.json",
        "/players/get-players",
        {"year": 2027, "page": 2, "items": 50},
    ),
    (
        "lba_player",
        {"player_id": 969},
        "captures/2026/players__get-player-by-id.json",
        "/players/get-player-by-id",
        {"id": 969, "stats": 1},
    ),
    (
        "lba_player_stats",
        {"player_id": 969},
        "captures/2026/players__get-player-stats.json",
        "/players/get-player-stats",
        {"id": 969},
    ),
    (
        "lba_shots",
        {"game_id": 25602},
        "captures/2026/matches__get-championship-matches-shots-by-id.json",
        "/matches/get-championship-matches-shots-by-id",
        {"id": 25602, "info": 1},
    ),
]


@pytest.mark.parametrize(("name", "kwargs", "fixture", "route", "wire"), WRAPPERS)
def test_wrapper_builds_the_url_and_wire_params_and_returns_a_frame(name, kwargs, fixture, route, wire, seen):
    calls = seen(fixture)
    df = getattr(lba, name)(**kwargs)
    assert calls[0][0] == BASE + route
    assert {k: v for k, v in calls[0][1].items() if v is not None} == wire  # the public END year goes through as is
    assert isinstance(df, pl.DataFrame) and df.height > 0
    raw = getattr(lba, name)(**kwargs, return_parsed=False)
    assert isinstance(raw, dict)


def test_all_nineteen_wrappers_are_covered_above():
    assert {w[0] for w in WRAPPERS} == {n for n in lba.__all__ if n.startswith("lba_")}


def test_required_parameters_and_choices():
    sig = inspect.signature(lba.lba_calendar)
    assert sig.parameters["championship_id"].default is inspect.Parameter.empty
    assert sig.parameters["round"].default is None
    with pytest.raises(ValueError, match="board"):
        lba.lba_teams_leaderboard(board="teams_neutral")
    with pytest.raises(TypeError):
        lba.lba_game()  # game_id is required


def test_docstring_notes_carry_the_legal_notice_and_the_crawl_delay():
    doc = lba.lba_pbp.__doc__
    assert "Non è consentito copiare" in doc and "Crawl-delay: 2" in doc
    assert "Example:" in doc and "Raises:" in doc
