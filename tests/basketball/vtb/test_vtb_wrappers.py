"""The generated ``vtb_league_*`` and ``vtb_infobasket_*`` wrappers and their endpoint YAML: URL, wire parameters, parsers.

Reads only committed files (CI has no sdv-internal-refs checkout): the YAML, the descriptions snippet, the spec-path
snapshots written by ``tools/codegen/gen_vtb.py`` and the fixtures.
"""

from __future__ import annotations

import inspect
import json
import re

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import vtb
from sportsdataverse.basketball.vtb import vtb_infobasket as ib_api
from sportsdataverse.basketball.vtb import vtb_infobasket_runtime as ib_rt
from sportsdataverse.basketball.vtb import vtb_league as lg_api
from sportsdataverse.basketball.vtb import vtb_league_runtime as lg_rt
from tests.basketball.vtb._fx import CASES, FIXTURES, ROOT, body

HOSTS = {"vtb_league": "https://api.vtb-league.com", "vtb_infobasket": "https://org.infobasket.su"}
DOCS = {
    fam: yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / f"{fam}.yaml").read_text(encoding="utf-8"))
    for fam in HOSTS
}
FIRST = {f"{fam}_{short}": (fam, fixtures[0]) for fam, short, fixtures in CASES}

# wrapper, kwargs, route, wire params (verified by hand against vtb-internal-refs' capture.py SAMPLES)
WRAPPERS = [
    ("vtb_league_leagues", {}, "/v2/leagues", {}),
    ("vtb_league_seasons", {"league_slug": "vtb"}, "/v2/leagues/vtb/seasons", {}),
    ("vtb_league_matches", {"league_slug": "vtb", "season": 2026}, "/v2/leagues/vtb/seasons/2026/matches", {"limit": 1000}),
    ("vtb_infobasket_seasons", {}, "/Comp/GetSeasonsForTag", {"format": "json", "tag": "vtb"}),
    ("vtb_infobasket_calendar", {"season": 2026}, "/Comp/GetCalendar/", {"format": "json", "season": 2026, "tag": "vtb"}),
    ("vtb_infobasket_team_stats", {"season": 2026}, "/Comp/GetTeamStats/2026", {"page": 0, "tag": "vtb"}),
    ("vtb_infobasket_team_stats_players", {"season": 2026}, "/Comp/GetTeamStats/2026", {"page": 0, "tag": "vtb"}),
    ("vtb_infobasket_team_stats_coaches", {"season": 2026}, "/Comp/GetTeamStats/2026", {"page": 0, "tag": "vtb"}),
    ("vtb_infobasket_best_players", {"season": 2026}, "/Comp/GetBestPlayers/2026", {"count": 5, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_best_teams", {"season": 2026}, "/Comp/GetBestTeams/2026", {"count": 5, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_preview", {"game_id": 921391, "season": 2026}, "/Comp/GetTeamStatsForPreview/921391", {"season": 2026, "tag": "vtb"}),
    ("vtb_infobasket_game", {"game_id": 921391}, "/Widget/GetOnline/921391", {"format": "json"}),
    ("vtb_infobasket_box_score", {"game_id": 921391}, "/Widget/GetOnline/921391", {"format": "json"}),
    ("vtb_infobasket_team_box_score", {"game_id": 921391}, "/Widget/GetOnline/921391", {"format": "json"}),
    ("vtb_infobasket_pbp", {"game_id": 921391}, "/Widget/GetOnline/921391", {"format": "json"}),
    ("vtb_infobasket_game_staff", {"game_id": 921391}, "/Widget/GetOnline/921391", {"format": "json"}),
    ("vtb_infobasket_game_page", {"game_id": 921391}, "/Widget/GamePage/921391", {"format": "json"}),
    ("vtb_infobasket_comp_issue", {"season": 2026}, "/Widget/CompIssue/2026", {"format": "json", "tag": "vtb"}),
    ("vtb_infobasket_standings", {"comp_id": 50720}, "/Widget/CompTeamResults/50720", {"format": "json"}),
    ("vtb_infobasket_cross_table", {"comp_id": 50720}, "/Widget/CrossTable/50720", {"format": "json"}),
    ("vtb_infobasket_playoff", {"comp_id": 54281}, "/Widget/PlayOff/54281", {"format": "json"}),
    ("vtb_infobasket_playoff_games", {"comp_id": 54281}, "/Widget/PlayOff/54281", {"format": "json"}),
    ("vtb_infobasket_best_player_parameters", {}, "/Widget/GetBestPlayerParameters/", {"format": "json", "lang": "en"}),
    ("vtb_infobasket_best_team_parameters", {}, "/Widget/GetBestTeamParameters/", {"format": "json", "lang": "en"}),
    ("vtb_infobasket_player", {"person_id": 423011}, "/Widget/PlayerPage/423011", {"format": "json", "tag": "vtb"}),
    ("vtb_infobasket_player_season_stats", {"person_id": 423011}, "/Widget/PlayerSeasonStats/423011", {"format": "json", "tag": "vtb"}),
    ("vtb_infobasket_player_career_stats", {"person_id": 423011}, "/Widget/PlayerSeasonStats/423011", {"format": "json", "tag": "vtb"}),
    (
        "vtb_infobasket_player_game_log",
        {"person_id": 423011, "season": 2026, "team": 2650},
        "/Widget/PlayerStats/423011",
        {"format": "json", "season": 2026, "team": 2650, "filter": 1, "tag": "vtb"},
    ),
    ("vtb_infobasket_coach", {"person_id": 131047}, "/Widget/CoachPage/131047", {"format": "json"}),
    ("vtb_infobasket_team", {"team_id": 2650}, "/Widget/TeamPage/2650", {"format": "json", "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_seasons", {"team_id": 2650}, "/Widget/GetTeamSeasons/2650", {"format": "json", "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_comps", {"team_id": 2650, "season": 2026}, "/Widget/GetTeamComps/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_roster", {"team_id": 2650, "season": 2026}, "/Widget/TeamRoster/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_roster_coaches", {"team_id": 2650, "season": 2026}, "/Widget/TeamRoster/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_games", {"team_id": 2650, "season": 2026}, "/Widget/TeamGames/2650", {"format": "json", "season": 2026, "tag": "vtb"}),
    ("vtb_infobasket_team_season_stats", {"team_id": 2650, "season": 2026}, "/Widget/TeamStats/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_player_stats", {"team_id": 2650, "season": 2026}, "/Widget/TeamStats/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
    ("vtb_infobasket_team_coach_stats", {"team_id": 2650, "season": 2026}, "/Widget/TeamStats/2650", {"format": "json", "season": 2026, "lang": "en", "tag": "vtb"}),
]  # fmt: skip


def _norm(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", path)


@pytest.mark.parametrize("fam,count", [("vtb_league", 3), ("vtb_infobasket", 24)])
def test_the_yaml_lists_every_route_of_the_spec(fam, count):
    paths = json.loads((FIXTURES / fam / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(paths) == count
    assert {_norm(p) for p in paths} == {_norm(e["path"]) for e in DOCS[fam]["endpoints"]}


def test_thirty_eight_wrappers_and_every_one_has_a_case_a_schema_and_a_parser():
    assert sum(len(d["endpoints"]) for d in DOCS.values()) == 38 == len(WRAPPERS) == len(CASES)
    names = [n for n in vtb.__all__ if n.startswith("vtb_")]
    assert len(names) == 38 and {w[0] for w in WRAPPERS} == set(names)
    for fam, doc in DOCS.items():
        assert len({e["short"] for e in doc["endpoints"]}) == len(doc["endpoints"])
        for e in doc["endpoints"]:
            assert e["parser"] == f"parse_{fam}_{e['short']}" and e["returns_schema"] == f"native/{fam}/{e['short']}"
            assert (ROOT / "tools" / "codegen" / "schemas" / "native" / fam / f"{e['short']}.yaml").exists()
            assert callable(getattr(vtb, e["parser"]))


def test_no_path_token_named_league_or_sport_and_the_tokens_are_the_path_params():
    for doc in DOCS.values():
        for e in doc["endpoints"]:
            tokens = re.findall(r"\{([^}]+)\}", e["path"])
            assert not {"league", "sport"} & set(tokens), e["short"]
            assert tokens == [p["name"] for p in e.get("path_params", [])], e["short"]
            assert all(p["required"] for p in e.get("path_params", []))


def test_the_family_headers_carry_the_owner_flag_and_the_refusal_vocabulary():
    for fam, doc in DOCS.items():
        assert doc["api"] == fam and doc["host"] == HOSTS[fam] and doc["name_pattern"] == f"{fam}_{{short}}"
        assert doc["getter_module"] == f"sportsdataverse.basketball.vtb.{fam}_runtime"
        assert doc["module"] == fam and doc["parser_module"] == f"basketball.vtb.{fam}_parsers"
        notes = " ".join(doc["docstring"]["notes"])
        for needle in ("Owner flag", "sanction", "sign-off", "Russia", "END years", "at least 1 s", "Unofficial"):
            assert needle in notes, (fam, needle)
        raises = " ".join(doc["docstring"]["raises"])
        assert "NoDataError" in raises and "AssetFetchError" in raises
    assert "BadRequest" in " ".join(DOCS["vtb_infobasket"]["docstring"]["notes"])


def test_every_description_is_in_the_snippet_never_in_the_schema_and_is_not_filler():
    snippet = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "notes" / "vtb_descriptions.yaml").read_text(encoding="utf-8")
    )
    total = 0
    for fam, doc in DOCS.items():
        for e in doc["endpoints"]:
            schema = yaml.safe_load(
                (ROOT / "tools" / "codegen" / "schemas" / "native" / fam / f"{e['short']}.yaml").read_text(
                    encoding="utf-8"
                )
            )
            assert all(c["description"] == "" for c in schema["columns"])  # descriptions live in the snippet only
            rows = snippet[f"{fam}_{e['short']}"]
            assert {c["name"] for c in schema["columns"]} == set(rows)
            for name, text in rows.items():
                total += 1
                assert len(text) >= 15 and text.strip().lower() != name, (fam, e["short"], name, text)
    assert total > 1000


@pytest.fixture
def seen(monkeypatch):
    calls = []

    def fake(name):
        fam, (rel, ctx) = FIRST[name]
        data = body(fam, rel, ctx)

        def _get(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return data

        monkeypatch.setattr(lg_api if fam == "vtb_league" else ib_api, "_get", _get)
        return calls

    return fake


@pytest.mark.parametrize("name,kwargs,route,wire", WRAPPERS, ids=[w[0] for w in WRAPPERS])
def test_each_wrapper_sends_its_route_and_wire_params_and_returns_a_frame(name, kwargs, route, wire, seen):
    calls = seen(name)
    df = getattr(vtb, name)(**kwargs)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    url, params = calls[0]
    fam = FIRST[name][0]
    assert url == HOSTS[fam] + route and params == wire, (url, params)
    assert not isinstance(getattr(vtb, name)(**kwargs, return_as_pandas=True), pl.DataFrame)
    raw = getattr(vtb, name)(**kwargs, return_parsed=False)
    assert isinstance(raw, dict) and "_request" in raw


def test_a_caller_can_override_a_wire_param_through_params(seen):
    calls = seen("vtb_infobasket_roster")
    vtb.vtb_infobasket_roster(team_id=2650, season=2026, params={"compId": 50720, "season": None})
    assert calls[0][1]["compId"] == 50720 and calls[0][1]["season"] is None  # the runtime drops the None


def test_required_arguments_come_first_and_seasons_are_documented_as_end_years():
    sig = inspect.signature(vtb.vtb_infobasket_player_game_log)
    assert list(sig.parameters)[:3] == ["person_id", "season", "team"]
    assert all(sig.parameters[p].default is inspect._empty for p in ("person_id", "season", "team"))
    assert sig.parameters["tag"].default == "vtb" and sig.parameters["filter"].default == 1
    assert "END year" in inspect.getdoc(vtb.vtb_infobasket_calendar) and "END year" in inspect.getdoc(
        vtb.vtb_league_matches
    )
    assert list(inspect.signature(vtb.vtb_league_matches).parameters)[:2] == ["league_slug", "season"]


def test_a_bad_choice_is_a_value_error_before_any_request(seen):
    calls = seen("vtb_infobasket_best_players")
    with pytest.raises(ValueError, match="lang"):
        vtb.vtb_infobasket_best_players(season=2026, lang="de")
    assert calls == []


def test_the_wrappers_use_their_family_runtime_getter():
    assert ib_api._get is ib_rt._get and lg_api._get is lg_rt._get


def test_the_home_package_documents_the_owner_flag():
    doc = inspect.getdoc(vtb)
    assert "sanction" in doc and "sign-off" in doc and "END years" in doc
