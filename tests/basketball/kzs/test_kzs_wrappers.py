"""The generated ``kzs_*`` wrappers and their endpoint YAML: URL, wire parameters, the parser each one feeds.

Reads only committed files (CI has no sdv-internal-refs checkout): the YAML, the spec-path snapshot written by
``tools/codegen/gen_kzs.py`` and the fixtures.
"""

from __future__ import annotations

import gzip
import inspect
import json
import re
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import kzs
from sportsdataverse.basketball.kzs import kzs as api
from sportsdataverse.basketball.kzs import kzs_runtime as rt

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "kzs"
BASE = "https://api.kzs.si/api/v1/public"
DOC = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "kzs.yaml").read_text(encoding="utf-8"))


def _snake(path: str) -> str:
    return re.sub(r"\{([^}]+)\}", lambda m: "{" + re.sub(r"(?<!^)(?=[A-Z])", "_", m.group(1)).lower() + "}", path)


def test_the_yaml_lists_every_route_of_the_spec():
    paths = json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(paths) == 18
    assert {_snake(p) for p in paths} == {e["path"] for e in DOC["endpoints"]}
    assert len(DOC["endpoints"]) == 23 and len({e["short"] for e in DOC["endpoints"]}) == 23


def test_no_path_token_named_league_or_sport_and_the_tokens_are_the_path_params():
    for e in DOC["endpoints"]:
        tokens = re.findall(r"\{([^}]+)\}", e["path"])
        assert not {"league", "sport"} & set(tokens), e["short"]
        assert tokens == [p["name"] for p in e.get("path_params", [])], e["short"]
        assert all(p["type"] == "int" and p["required"] for p in e.get("path_params", []))


def test_the_family_header_and_every_endpoint_is_documented():
    assert DOC["api"] == "kzs" and DOC["host"] == BASE and DOC["name_pattern"] == "kzs_{short}"
    assert DOC["getter_module"] == "sportsdataverse.basketball.kzs.kzs_runtime"
    notes = " ".join(DOC["docstring"]["notes"])
    for needle in ("robots.txt", "Personal data", "HTTP 400", "extra, cached request", "END years", "Unofficial"):
        assert needle in notes, needle
    for e in DOC["endpoints"]:
        assert e["parser"].startswith("parse_kzs_") and e["returns_schema"] == f"native/kzs/{e['short']}"
        assert (ROOT / "tools" / "codegen" / "schemas" / "native" / "kzs" / f"{e['short']}.yaml").exists()


def test_every_description_note_is_free_of_a_blank_and_the_snippet_covers_every_column():
    snippet = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "notes" / "kzs_descriptions.yaml").read_text(encoding="utf-8")
    )
    for e in DOC["endpoints"]:
        schema = yaml.safe_load(
            (ROOT / "tools" / "codegen" / "schemas" / "native" / "kzs" / f"{e['short']}.yaml").read_text(
                encoding="utf-8"
            )
        )
        assert all(c["description"] == "" for c in schema["columns"])  # descriptions live in the snippet only
        rows = snippet[f"kzs_{e['short']}"]
        assert {c["name"] for c in schema["columns"]} == set(rows)
        for name, text in rows.items():
            assert len(text) >= 15 and text.strip().lower() != name, (e["short"], name, text)


WRAPPERS = [
    # name, kwargs, fixture, route, wire params
    ("kzs_seasons", {}, "full/seasons.json", "/seasons/", {"limit": 1000}),
    (
        "kzs_competitions",
        {"season": 2026},
        "full/competitions__season26.json",
        "/competitions/",
        {"limit": 10000, "season": 2026},
    ),
    (
        "kzs_competition",
        {"competition_id": 579},
        "captures/competitions__competitionId__579.json",
        "/competitions/579",
        {},
    ),
    ("kzs_phases", {"competition_id": 579}, "captures/competitions__competitionId__579.json", "/competitions/579", {}),
    (
        "kzs_rounds",
        {"competition_id": 579},
        "captures/competitions__competitionId__rounds__579.json",
        "/competitions/579/rounds",
        {},
    ),
    (
        "kzs_player_leaderboard",
        {"competition_id": 579, "stats_category": "R", "sort": "ASC", "sort_by": "sum", "order_type": "SUM"},
        "full/playerStats__579.json",
        "/competitions/579/playerStats",
        {"limit": 1000, "statsCategory": "R", "orderType": "SUM", "sortBy": "sum", "sort": "ASC"},
    ),
    (
        "kzs_team_leaderboard",
        {"competition_id": 579},
        "full/teamStats__579.json",
        "/competitions/579/teamStats",
        {"limit": 1000, "statsCategory": "PTS", "orderType": "AVERAGE", "sortBy": "avg", "sort": "DESC"},
    ),
    (
        "kzs_matches",
        {"competition_id": 579, "date_from": "2025-10-01T00:00:00.000Z", "date_to": "2025-10-31T23:59:59.000Z"},
        "full/matches__579_2025-10.json",
        "/matches/",
        {"limit": 1000, "competitionId": 579, "from": "2025-10-01T00:00:00.000Z", "to": "2025-10-31T23:59:59.000Z"},
    ),
    ("kzs_match", {"match_id": 101839}, "full/match__101839.json", "/matches/101839", {}),
    ("kzs_period_scores", {"match_id": 101839}, "full/match__101839.json", "/matches/101839", {}),
    ("kzs_pbp", {"match_id": 101839}, "full/playByPlay__101839.json.gz", "/matches/101839/playByPlay", {}),
    ("kzs_pbp_players", {"match_id": 101839}, "full/playByPlay__101839.json.gz", "/matches/101839/playByPlay", {}),
    ("kzs_box_score", {"match_id": 101839}, "full/stats__101839.json", "/matches/101839/stats", {}),
    ("kzs_team_box_score", {"match_id": 101839}, "full/stats__101839.json", "/matches/101839/stats", {}),
    ("kzs_score_changes", {"match_id": 101839}, "full/stats__101839.json", "/matches/101839/stats", {}),
    (
        "kzs_standings",
        {"competition_id": 579},
        "full/standings__579.json",
        "/matches/standings",
        {"competitionId": 579},
    ),
    (
        "kzs_playing_days",
        {"competition_id": 579},
        "captures/matches__competitionId__playingDays__579.json",
        "/matches/579/playingDays",
        {},
    ),
    (
        "kzs_teams",
        {"competition_id": 579},
        "captures/teams__579.json",
        "/teams/",
        {"limit": 1000, "competitionId": 579},
    ),
    ("kzs_team", {"team_id": 17551}, "captures/teams__teamId__17551.json", "/teams/17551", {}),
    (
        "kzs_team_general",
        {"team_id": 17551, "competition_id": 579},
        "captures/teams__teamId__general__17551_579.json",
        "/teams/17551/general",
        {"competitionId": 579},
    ),
    (
        "kzs_roster",
        {"team_id": 17551, "competition_id": 579},
        "captures/teamPlayers__17551_579.json",
        "/teamPlayers/",
        {"limit": 1000, "teamId": 17551, "competitionId": 579},
    ),
    ("kzs_player", {"player_id": 106285}, "captures/players__playerId__106285.json", "/players/106285", {}),
    (
        "kzs_player_stats",
        {"player_id": 106285, "season": 2026},
        "full/player_stats__106285_26.json",
        "/players/106285/stats",
        {"season": 2026},
    ),
]


@pytest.fixture
def seen(monkeypatch):
    calls = []

    def fake(fixture):
        path = FIX / fixture
        text = gzip.open(path, "rt", encoding="utf-8").read() if fixture.endswith(".gz") else path.read_text("utf-8")
        data = json.loads(text)["data"]
        data = dict(data) if isinstance(data, dict) else {"items": data}

        def _get(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return {**data, "_request": {}}

        monkeypatch.setattr(api, "_get", _get)
        return calls

    return fake


def test_twenty_three_wrappers_are_exported_with_the_parsers():
    names = [n for n in kzs.__all__ if n.startswith("kzs_")]
    assert len(names) == 23 and all(callable(getattr(kzs, n)) for n in names)
    assert sum(n.startswith("parse_kzs_") for n in kzs.__all__) == 23
    assert {w[0] for w in WRAPPERS} == set(names)


@pytest.mark.parametrize("name,kwargs,fixture,route,wire", WRAPPERS, ids=[w[0] for w in WRAPPERS])
def test_each_wrapper_sends_its_route_and_wire_params_and_returns_a_frame(name, kwargs, fixture, route, wire, seen):
    calls = seen(fixture)
    df = getattr(kzs, name)(**kwargs)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    url, params = calls[0]
    assert url == BASE + route and params == wire, (url, params)
    assert not isinstance(getattr(kzs, name)(**kwargs, return_as_pandas=True), pl.DataFrame)
    raw = getattr(kzs, name)(**kwargs, return_parsed=False)
    assert isinstance(raw, dict) and "_request" in raw


def test_a_caller_can_override_a_wire_param_through_params(seen):
    calls = seen("full/competitions__season26.json")
    kzs.kzs_competitions(params={"seasonId": 26, "season": None, "limit": 5})
    assert calls[0][1] == {"limit": 5, "season": None, "seasonId": 26}


def test_required_arguments_come_first_and_the_season_is_documented_as_an_end_year():
    sig = inspect.signature(kzs.kzs_player_stats)
    assert list(sig.parameters)[:2] == ["player_id", "season"] and sig.parameters["season"].default is inspect._empty
    assert "END year" in inspect.getdoc(kzs.kzs_player_stats) and "opaque" in inspect.getdoc(kzs.kzs_player_stats)
    board = inspect.signature(kzs.kzs_player_leaderboard).parameters
    assert board["stats_category"].default == "PTS" and board["sort"].default == "DESC"


def test_a_bad_choice_is_a_value_error_before_any_request(seen):
    calls = seen("full/playerStats__579.json")
    with pytest.raises(ValueError, match="stats_category"):
        kzs.kzs_player_leaderboard(competition_id=579, stats_category="XYZ")
    with pytest.raises(ValueError, match="sort_by"):
        kzs.kzs_team_leaderboard(competition_id=579, sort_by="name")
    assert calls == []


def test_the_wrappers_use_the_kzs_runtime_getter():
    assert api._get is rt._get
