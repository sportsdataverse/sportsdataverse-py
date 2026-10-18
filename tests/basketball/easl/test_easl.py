"""EASL: parsers over committed fixtures, the runtime's season mapping, freshness, politeness and refusals (mocked transport).

Reads only ``tests/fixtures/easl`` (never the private refs checkout).
"""

from __future__ import annotations

import copy
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest
import requests
import yaml

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.easl import easl_parsers as P
from sportsdataverse.basketball.easl import easl_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "easl"
API = "https://easl.joshricha.com/api/v1"
G = "10af8fd4-106e-11f1-960b-5798087c7aa3"  # the 2025-26 final
T = "865489e8-b0f5-11ed-8b3f-c199a9dea184"
PL = "85c99dab-9f67-11f0-8780-a731edb5aad5"
S24, S25, S26, S27 = (
    "c24ce68a-6420-11ee-9daf-6b6188281bd0",
    "57cefc32-5ace-11ef-81a0-3d1f6018efb7",
    "77441bb3-9759-11f0-b659-0561c09a0222",
    "1409787d-a683-11f1-aea7-b596f55be5ed",
)


def _load(rel: str) -> Any:
    return json.loads((FIX / rel).read_text(encoding="utf-8"))


def _wrap(body: Any, **req: Any) -> Dict[str, Any]:
    out = dict(body) if isinstance(body, dict) else {"items": body}
    out["_request"] = req
    return out


# --- parsers --------------------------------------------------------------------------------------------------------

_GM = {"fixture_id": G}
_SE = {"season": 2026, "season_id": S26}
CASES = [
    ("parse_easl_health", "captures/health.json", {}, 1),
    ("parse_easl_seasons", "full/metadata__seasons.json", {}, 4),
    ("parse_easl_schedule", "full/matches@2025.json", _SE, 42),
    ("parse_easl_schedule", "captures/matches.json", {"season": 2027, "season_id": S27}, 3),
    ("parse_easl_schedule", "captures/matches@2023.json", {"season": 2024, "season_id": S24}, 3),
    ("parse_easl_game", "captures/matches__fixtureId@2025.json", _GM, 1),
    ("parse_easl_game", "captures/matches__fixtureId@2023.json", _GM, 1),
    ("parse_easl_player_box", "full/matches__fixtureId__boxscore@2025.json", _GM, 24),
    ("parse_easl_player_box", "captures/matches__fixtureId__boxscore.json", _GM, 3),
    ("parse_easl_pbp", "full/matches__fixtureId__playbyplay@2025.json", _GM, 495),
    ("parse_easl_pbp", "captures/matches__fixtureId__playbyplay@2023.json", _GM, 3),
    ("parse_easl_team_box", "full/matches__fixtureId__teamstats@2025.json", _GM, 2),
    ("parse_easl_team_box", "captures/matches__fixtureId__teamstats@2023.json", _GM, 2),
    ("parse_easl_team_box_by_period", "full/matches__fixtureId__teamstats__by__period@2025.json", _GM, 8),
    ("parse_easl_team_box_by_period", "captures/matches__fixtureId__teamstats__by__period.json", _GM, 8),
    ("parse_easl_standings", "full/standings@2025.json", _SE, 12),
    ("parse_easl_standings", "captures/standings@2023.json", {"season": 2024, "season_id": S24}, 3),
    ("parse_easl_leaders", "full/leaders@2025.json", _SE, 45),
    ("parse_easl_leaders", "captures/leaders.json", {"season": 2027, "season_id": S27}, 27),
    ("parse_easl_teams", "full/teams.json", {}, 12),
    ("parse_easl_team", "captures/teams__teamId.json", {"team_id": T}, 1),
    ("parse_easl_team_schedule", "captures/teams__teamId__matches@2025.json", {"team_id": T, **_SE}, 3),
    ("parse_easl_team_stats", "captures/teams__teamId__stats@2025.json", {"team_id": T, **_SE}, 1),
    ("parse_easl_team_leaders", "captures/teams__teamId__leaders@2025.json", {"team_id": T, **_SE}, 27),
    ("parse_easl_players", "full/players@roster.json", {**_SE, "team_id": T}, 17),
    ("parse_easl_players", "captures/players.json", {}, 3),
    ("parse_easl_player", "captures/players__playerId.json", {"player_id": PL}, 1),
    ("parse_easl_player_stats", "captures/players__playerId__stats.json", {"player_id": PL}, 1),
]
ID_COLUMNS = re.compile(r"(^|_)id$")


@pytest.mark.parametrize(("name", "rel", "req", "rows"), CASES)
def test_parser_over_fixture(name, rel, req, rows):
    df = getattr(P, name)(_wrap(_load(rel), **req))
    assert isinstance(df, pl.DataFrame) and df.height == rows
    assert dict(df.schema) == {c: d for c, _w, d in P.SPECS[name]}
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", c) for c in df.columns)
    # every id is one dtype: a string (a DataCore UUID), never a float or an int
    assert all(
        df.schema[c] == pl.String
        for c in df.columns
        if ID_COLUMNS.search(c) and not re.search(r"row_id$|^period_id$", c)
    )


@pytest.mark.parametrize("name", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", {"items": []}, {"_request": {"season": 2026}}])
def test_empty_or_malformed_is_a_typed_zero_row_frame_that_stacks(name, bad):
    fn = getattr(P, name)
    empty = fn(bad)
    assert empty.height == 0 and dict(empty.schema) == {c: d for c, _w, d in P.SPECS[name]}
    assert pl.concat([empty, empty]).height == 0
    assert fn(bad, return_as_pandas=True).shape[0] == 0


def test_all_public_parsers_are_listed_and_have_examples():
    assert sorted(P.__all__) == sorted(P.SPECS)
    for n in P.__all__:
        doc = getattr(P, n).__doc__ or ""
        assert "Example:" in doc and "Raises:" in doc and ">>>" not in doc


def test_return_as_pandas():
    import pandas as pd

    pdf = P.parse_easl_standings(_wrap(_load("full/standings@2025.json"), **_SE), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) == 12


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_easl_player_box(_wrap(_load("full/matches__fixtureId__boxscore@2025.json"), **_GM))
    assert box.schema["points"] == pl.Int64 and box.schema["minutes"] == pl.String
    assert box.schema["assists_turnover_ratio"] == pl.Float64 and box.schema["usage_rate"] == pl.Float64
    st = P.parse_easl_standings(_wrap(_load("full/standings@2025.json"), **_SE))
    assert st.schema["win_percentage"] == pl.Float64 and st.schema["wins"] == pl.Int64
    # a whole-number rate in the sampled body is still a rate
    ts = P.parse_easl_team_stats(_wrap(_load("captures/teams__teamId__stats@2025.json"), **_SE))
    assert ts.schema["points_per_game"] == pl.Float64 and ts.schema["games"] == pl.Int64
    ps = P.parse_easl_player_stats(_wrap(_load("captures/players__playerId__stats.json"), player_id=PL))
    assert ps.schema["minutes_per_game"] == pl.String and ps.schema["steals_per_game"] == pl.Float64
    lead = P.parse_easl_leaders(_wrap(_load("full/leaders@2025.json"), **_SE))
    assert lead.schema["value"] == pl.Float64 and lead.schema["rank"] == pl.Int64


def test_fixture_sides_come_from_ishome_not_from_list_order():
    game = _load("captures/matches__fixtureId@2025.json")
    away_first = game["competitors"][0]
    assert away_first["isHome"] is False  # the 2025-26 final lists the away team first
    row = P.parse_easl_game(_wrap(game, **_GM)).row(0, named=True)
    assert row["away_team_id"] == away_first["teamId"] == T and row["away_score"] == 90 and row["home_score"] == 81
    assert row["home_team_id"] != row["away_team_id"] and row["fixture_type"] == "FINAL"
    assert row["season"] is None  # the game route carries no season


def test_fixture_number_is_null_on_2023_24_and_season_comes_from_the_request():
    old = P.parse_easl_game(_wrap(_load("captures/matches__fixtureId@2023.json"), **_GM))
    assert old["fixture_number"][0] is None
    new = P.parse_easl_schedule(_wrap(_load("full/matches@2025.json"), **_SE))
    assert set(new["season"]) == {2026} and set(new["season_id"]) == {S26} and new["fixture_number"].null_count() == 0
    assert new.schema["home_score"] == pl.Int64 and new["home_score"].null_count() == 0


def test_team_schedule_takes_the_season_from_the_request_because_rows_have_no_season_id():
    raw = _load("captures/teams__teamId__matches@2025.json")
    assert all("seasonId" not in r for r in raw)
    df = P.parse_easl_team_schedule(_wrap(raw, team_id=T, **_SE))
    assert set(df["season_id"]) == {S26} and all(s.startswith(S26 + ":") for s in df["stage_id"])


def test_pbp_points_come_from_the_running_score_not_the_text():
    body = _load("full/matches__fixtureId__playbyplay@2025.json")
    df = P.parse_easl_pbp(_wrap(body, **_GM))
    by_team = {
        r["team_id"]: r["pts"]
        for r in df.group_by("team_id").agg(pl.col("points_scored").sum().alias("pts")).to_dicts()
    }
    assert by_team[T] == 90 and by_team["f170040e-5ad9-11ef-b4d4-070129f618c6"] == 81
    assert df["event_number"].to_list() == list(range(1, 496)) and set(df["period_label"]) == {"Q1", "Q2", "Q3", "Q4"}
    shot = df.filter(pl.col("description").str.starts_with("3pt") & (pl.col("points_scored") == 3)).row(0, named=True)
    assert "missed" in shot["description"] and shot["team_score"] >= 3
    last = df.row(-1, named=True)
    assert {last["team1_score"], last["team2_score"]} == {90, 81}


def test_box_marks_players_who_did_not_play():
    df = P.parse_easl_player_box(_wrap(_load("full/matches__fixtureId__boxscore@2025.json"), **_GM))
    dnp = df.filter(pl.col("played") == False)  # noqa: E712
    assert dnp.height == 7 and dnp["minutes_played"].null_count() == 7 and dnp["assists"].null_count() == 7
    played = df.filter(pl.col("played") == True)  # noqa: E712
    assert played["minutes_played"].min() > 0 and set(played["team_name"]) == {
        "Utsunomiya Brex",
        "Taoyuan Pauian Pilots",
    }
    assert df.filter(pl.col("team_id") == T)["points"].sum() == 90


def test_period_table_sums_to_the_team_totals():
    per = P.parse_easl_team_box_by_period(
        _wrap(_load("full/matches__fixtureId__teamstats__by__period@2025.json"), **_GM)
    )
    tot = P.parse_easl_team_box(_wrap(_load("full/matches__fixtureId__teamstats@2025.json"), **_GM))
    assert sorted(set(per["period"])) == [1, 2, 3, 4] and set(per["period_label"]) == {"Q1", "Q2", "Q3", "Q4"}
    sums = per.group_by("team_id").agg(pl.col("points").sum())
    assert dict(zip(sums["team_id"], sums["points"])) == dict(zip(tot["team_id"], tot["points"]))
    assert tot.schema["points_against"] == pl.Int64


def test_the_2023_24_team_totals_lack_most_of_the_against_family_but_keep_the_schema():
    old = P.parse_easl_team_box(_wrap(_load("captures/matches__fixtureId__teamstats@2023.json"), **_GM))
    assert (
        old["points_against"].null_count() == 0
        and old["rebounds_against"].null_count() == 2
        and old.schema == P.parse_easl_team_box({}).schema
    )


def test_leaders_are_long_with_nine_categories():
    df = P.parse_easl_leaders(_wrap(_load("full/leaders@2025.json"), **_SE))
    assert df["category"].n_unique() == 9 and sorted(set(df["rank"])) == [1, 2, 3, 4, 5]
    assert "player_season_id" not in df.columns  # the sync server's integer is not carried
    top = df.filter((pl.col("category") == "pointsPerGame") & (pl.col("rank") == 1)).row(0, named=True)
    assert top["player_name"] == "Archie GOODWIN III" and top["team_id"] == "86515908-b0f5-11ed-ac6b-c199a9dea184"


def test_standings_keep_the_proxy_spelling_and_last_five():
    df = P.parse_easl_standings(_wrap(_load("full/standings@2025.json"), **_SE))
    assert "steak" in df.columns and df["last_five"].str.len_chars().min() > 0
    assert set(df["season_id"]) == {S26}


def test_seasons_end_year_is_start_plus_one():
    df = P.parse_easl_seasons(_wrap(_load("full/metadata__seasons.json")))
    assert dict(zip(df["year"], df["season"])) == {2023: 2024, 2024: 2025, 2025: 2026, 2026: 2027}
    assert df.filter(pl.col("status") == "ACTIVE")["season"].to_list() == [2027]


def test_players_drop_social_handles_and_roster_carries_position():
    roster = P.parse_easl_players(_wrap(_load("full/players@roster.json"), **_SE, team_id=T))
    assert not [c for c in roster.columns if "username" in c] and roster["position"].null_count() == 0
    assert roster.schema["dob"] == pl.Date and set(roster["season_id"]) == {S26}
    directory = P.parse_easl_players(_wrap(_load("captures/players.json")))
    assert directory["position"].null_count() == directory.height and directory["season"].null_count() == 3


def test_no_personal_contact_columns_in_any_schema():
    banned = re.compile(r"email|phone|address_(line|street)|username|mobile")
    for name, spec in P.SPECS.items():
        for col, _w, _d in spec:
            if "team" in name or "teams" in name:
                continue  # a team's own public social links are organisational, not personal
            assert not banned.search(col), (name, col)


# --- generated artefacts --------------------------------------------------------------------------------------------

YAML = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "easl.yaml").read_text(encoding="utf-8"))


def test_yaml_lists_every_route_and_wrapper():
    snap = json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(snap["paths"]) == 18 and len(YAML["endpoints"]) == 18 and len(snap["wrappers"]) == 18
    norm = lambda p: re.sub(r"\{[^}]+\}", "{}", p)  # noqa: E731
    assert {norm(p) for p in snap["paths"]} == {norm(e["path"]) for e in YAML["endpoints"]}
    assert {e["parser"] for e in YAML["endpoints"]} == set(P.SPECS)
    assert YAML["host"] == API and YAML["getter_module"].endswith("easl_runtime")
    assert not [e for e in YAML["endpoints"] if {"league", "sport"} & set(re.findall(r"\{(\w+)\}", e["path"]))]


def test_docstring_notes_carry_the_third_party_warning():
    notes = " ".join(YAML["docstring"]["notes"])
    assert "UNOFFICIAL THIRD-PARTY INTERMEDIARY" in notes and "can change or vanish" in notes
    assert "first-party DataCore feed needs credentials" in notes
    assert "Unofficial" in (ROOT / "sportsdataverse" / "basketball" / "easl" / "__init__.py").read_text(
        encoding="utf-8"
    )


def test_descriptions_snippet_is_complete_and_schemas_are_blank():
    snippet = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "notes" / "easl_descriptions.yaml").read_text(encoding="utf-8")
    )
    for e in YAML["endpoints"]:
        sch = yaml.safe_load(
            (ROOT / "tools/codegen/schemas" / f"{e['returns_schema']}.yaml").read_text(encoding="utf-8")
        )
        cols = [c["name"] for c in sch["columns"]]
        assert cols == [c for c, _w, _d in P.SPECS[e["parser"]]]
        assert all(c["description"] == "" for c in sch["columns"])
        assert set(snippet[sch["schema"]]) == set(cols)
        for col, desc in snippet[sch["schema"]].items():
            assert len(desc) >= 15 and desc.lower().rstrip(".") != col.replace("_", " "), (sch["schema"], col)


# --- runtime --------------------------------------------------------------------------------------------------------


def _resp(status=200, body=None, headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body).encode()
    r.headers.update(headers or {"Content-Type": "application/json"})
    r.url = API
    return r


class Router:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def __call__(self, method, url, *a, **kw):
        route = url.replace(API, "")
        self.calls.append({"route": route, "params": dict(kw.get("params") or {})})
        ans = self.routes[route]
        if isinstance(ans, list):
            ans = ans.pop(0) if len(ans) > 1 else ans[0]
        if isinstance(ans, Exception):
            raise ans
        return ans

    def routes_called(self) -> List[str]:
        return [c["route"] for c in self.calls]


NOW = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def transport(monkeypatch):
    def install(routes):
        t = Router(routes)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(rt, "_now", lambda: NOW)
    rt.clear_cache()
    yield install
    rt.clear_cache()


def _health(ts="2026-10-13T11:00:00.000Z"):
    return _resp(body={"status": "OK", "message": "EASL Basketball API is running", "timestamp": ts})


def _base(**over):
    routes = {"/metadata/seasons": _resp(body=_load("full/metadata__seasons.json")), "/health": _health()}
    routes.update(over)
    return routes


def _game_routes(**over):
    routes = _base(
        **{
            f"/matches/{G}": _resp(body=_load("captures/matches__fixtureId@2025.json")),
            f"/matches/{G}/boxscore": _resp(body=_load("full/matches__fixtureId__boxscore@2025.json")),
            f"/matches/{G}/playbyplay": _resp(body=_load("full/matches__fixtureId__playbyplay@2025.json")),
            f"/matches/{G}/teamstats": _resp(body=_load("full/matches__fixtureId__teamstats@2025.json")),
            f"/matches/{G}/teamstats/by/period": _resp(
                body=_load("full/matches__fixtureId__teamstats__by__period@2025.json")
            ),
        }
    )
    routes.update(over)
    return routes


@pytest.mark.parametrize(("season", "sid"), [(2024, S24), (2025, S25), (2026, S26), (2027, S27), (None, S27)])
def test_end_year_maps_to_the_season_id_and_none_is_the_active_season(transport, season, sid):
    t = transport(
        _base(**{"/matches": _resp(body=[dict(r, seasonId=sid) for r in _load("captures/matches@2025.json")])})
    )
    out = rt._get(f"{API}/matches", {"season": season} if season is not None else {})
    assert [c["params"] for c in t.calls if c["route"] == "/matches"] == [{"seasonId": sid}]
    assert out["_request"] == {"season": season or 2027, "season_id": sid}


def test_the_seasons_list_is_fetched_once(transport):
    t = transport(
        _base(
            **{
                "/matches": _resp(body=_load("full/matches@2025.json")),
                "/standings": _resp(body=_load("full/standings@2025.json")),
            }
        )
    )
    rt._get(f"{API}/matches", {"season": 2026})
    rt._get(f"{API}/standings", {"season": 2026})
    assert t.routes_called().count("/metadata/seasons") == 1


@pytest.mark.parametrize("bad", [2022, 2028, "x", True])
def test_unknown_or_malformed_season_is_a_value_error(transport, bad):
    t = transport(_base())
    with pytest.raises(ValueError, match="season"):
        rt._get(f"{API}/standings", {"season": bad})
    assert "/standings" not in t.routes_called()


def test_schedule_of_another_season_is_refused(transport):
    rows = _load("full/matches@2025.json")
    rows[3] = dict(rows[3], seasonId=S25)
    transport(_base(**{"/matches": _resp(body=rows)}))
    with pytest.raises(AssetFetchError, match="another season"):
        rt._get(f"{API}/matches", {"season": 2026})


def _active_rows(**first):
    rows = [dict(r, seasonId=S27) for r in _load("captures/matches.json")]
    rows[1].update(first)
    return rows


def test_active_season_is_checked_against_the_health_clock(transport):
    t = transport(_base(**{"/matches": _resp(body=_active_rows())}))
    out = rt._get(f"{API}/matches")
    assert len(out["items"]) == 3 and "/health" in t.routes_called()


def test_a_stalled_mirror_on_the_active_season_is_refused(transport, monkeypatch):
    # server and local clocks agree on 2026-11-07; the SCHEDULED game tipped off on 2026-11-05
    monkeypatch.setattr(rt, "_now", lambda: datetime(2026, 11, 7, 1, tzinfo=timezone.utc))
    transport(_base(**{"/matches": _resp(body=_active_rows()), "/health": _health("2026-11-07T00:00:00.000Z")}))
    with pytest.raises(AssetFetchError, match="stalled"):
        rt._get(f"{API}/matches")
    # the same list one hour after tip-off is fine
    monkeypatch.setattr(rt, "_now", lambda: datetime(2026, 11, 5, 11, 10, tzinfo=timezone.utc))
    transport(_base(**{"/matches": _resp(body=_active_rows()), "/health": _health("2026-11-05T11:10:00.000Z")}))
    assert len(rt._get(f"{API}/matches")["items"]) == 3


def test_a_server_clock_far_from_ours_is_refused(transport):
    transport(_base(**{"/matches": _resp(body=_active_rows()), "/health": _health("2026-09-01T00:00:00.000Z")}))
    with pytest.raises(AssetFetchError, match="more than a day"):
        rt._get(f"{API}/matches")


def test_a_health_that_is_not_ok_is_refused(transport):
    transport(
        _base(**{"/matches": _resp(body=_active_rows()), "/health": _resp(body={"status": "DOWN", "timestamp": "x"})})
    )
    with pytest.raises(AssetFetchError, match="not OK"):
        rt._get(f"{API}/matches")


def test_past_seasons_skip_the_freshness_check(transport):
    t = transport(_base(**{"/matches": _resp(body=_load("full/matches@2025.json"))}))
    rt._get(f"{API}/matches", {"season": 2026})
    assert "/health" not in t.routes_called()


def test_a_game_reads_the_header_then_the_route_and_is_stamped(transport):
    t = transport(_game_routes())
    box = rt._get(f"{API}/matches/{G}/boxscore")
    assert box["_request"] == {"fixture_id": G} and len(box["boxScore"]) == 24
    assert t.routes_called() == [f"/matches/{G}", f"/matches/{G}/boxscore"]
    for route, key in [("playbyplay", "playbyplay"), ("teamstats", "items"), ("teamstats/by/period", "items")]:
        assert key in rt._get(f"{API}/matches/{G}/{route}")
    assert rt._get(f"{API}/matches/{G}")["fixtureId"] == G
    assert t.routes_called().count(f"/matches/{G}") == 1  # the header is cached for the process


@pytest.mark.parametrize(
    "patch",
    [{"status": "SCHEDULED"}, {"status": "DRAFT"}, {"fixtureId": "bde3aaa1-da50-11ef-97fd-97d4eecf418b"}],
)
def test_an_unfinished_or_foreign_header_is_refused(transport, patch):
    transport(
        _game_routes(**{f"/matches/{G}": _resp(body=dict(_load("captures/matches__fixtureId@2025.json"), **patch))})
    )
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/matches/{G}/boxscore")


def test_a_header_without_two_scores_is_refused(transport):
    h = copy.deepcopy(_load("captures/matches__fixtureId@2025.json"))
    h["competitors"][0]["score"] = None
    transport(_game_routes(**{f"/matches/{G}": _resp(body=h)}))
    with pytest.raises(AssetFetchError, match="two scored"):
        rt._get(f"{API}/matches/{G}")


def test_box_points_must_match_the_header(transport):
    box = copy.deepcopy(_load("full/matches__fixtureId__boxscore@2025.json"))
    box["boxScore"][0]["statistics"]["points"] += 2
    transport(_game_routes(**{f"/matches/{G}/boxscore": _resp(body=box)}))
    with pytest.raises(AssetFetchError, match="player points"):
        rt._get(f"{API}/matches/{G}/boxscore")


def test_an_incomplete_one_team_box_is_refused(transport):
    box = copy.deepcopy(_load("full/matches__fixtureId__boxscore@2025.json"))
    box["boxScore"] = [r for r in box["boxScore"] if r["entityId"] == T]
    transport(_game_routes(**{f"/matches/{G}/boxscore": _resp(body=box)}))
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/matches/{G}/boxscore")


def test_a_box_of_another_fixture_is_refused(transport):
    box = copy.deepcopy(_load("full/matches__fixtureId__boxscore@2025.json"))
    box["boxScore"][2]["fixtureId"] = "bde3aaa1-da50-11ef-97fd-97d4eecf418b"
    transport(_game_routes(**{f"/matches/{G}/boxscore": _resp(body=box)}))
    with pytest.raises(AssetFetchError, match="another fixture"):
        rt._get(f"{API}/matches/{G}/boxscore")


def test_a_play_by_play_cut_short_is_refused(transport):
    pbp = copy.deepcopy(_load("full/matches__fixtureId__playbyplay@2025.json"))
    pbp["playbyplay"] = pbp["playbyplay"][:-6]
    transport(_game_routes(**{f"/matches/{G}/playbyplay": _resp(body=pbp)}))
    with pytest.raises(AssetFetchError, match="final score"):
        rt._get(f"{API}/matches/{G}/playbyplay")


def test_a_play_by_play_that_ends_before_period_four_is_refused(transport):
    pbp = copy.deepcopy(_load("full/matches__fixtureId__playbyplay@2025.json"))
    pbp["playbyplay"][-1]["periodId"] = 3
    transport(_game_routes(**{f"/matches/{G}/playbyplay": _resp(body=pbp)}))
    with pytest.raises(AssetFetchError, match="final period"):
        rt._get(f"{API}/matches/{G}/playbyplay")


def test_a_play_by_play_must_end_in_the_last_period_of_the_period_table(transport):
    per = copy.deepcopy(_load("full/matches__fixtureId__teamstats__by__period@2025.json"))
    for r in per:
        r["period_5"] = {"points": 0}
    transport(_game_routes(**{f"/matches/{G}/teamstats/by/period": _resp(body=per)}))
    with pytest.raises(AssetFetchError, match="period table"):
        rt._get(f"{API}/matches/{G}/playbyplay")


def test_a_play_by_play_with_foreign_teams_is_refused(transport):
    pbp = copy.deepcopy(_load("full/matches__fixtureId__playbyplay@2025.json"))
    pbp["teams"][0]["teamId"] = "00000000-0000-1000-8000-000000000000"
    transport(_game_routes(**{f"/matches/{G}/playbyplay": _resp(body=pbp)}))
    with pytest.raises(AssetFetchError, match="not this fixture"):
        rt._get(f"{API}/matches/{G}/playbyplay")


def test_team_totals_and_period_table_must_add_up(transport):
    ts = copy.deepcopy(_load("full/matches__fixtureId__teamstats@2025.json"))
    ts[0]["statistics"]["points"] += 1
    per = copy.deepcopy(_load("full/matches__fixtureId__teamstats__by__period@2025.json"))
    per[0]["period_2"]["points"] += 1
    transport(
        _game_routes(
            **{f"/matches/{G}/teamstats": _resp(body=ts), f"/matches/{G}/teamstats/by/period": _resp(body=per)}
        )
    )
    with pytest.raises(AssetFetchError, match="team points"):
        rt._get(f"{API}/matches/{G}/teamstats")
    with pytest.raises(AssetFetchError, match="period points"):
        rt._get(f"{API}/matches/{G}/teamstats/by/period")


def test_malformed_ids_are_value_errors_before_any_request(transport):
    t = transport({})
    for url in (f"{API}/matches/not-a-uuid/boxscore", f"{API}/teams/123", f"{API}/players/x/stats"):
        with pytest.raises(ValueError, match="UUID"):
            rt._get(url)
    with pytest.raises(ValueError, match="not an EASL route"):
        rt._get(f"{API}/auth/login")
    assert t.calls == []


ENVELOPE = {"error": "Internal server error"}


@pytest.mark.parametrize("status", [200, 400, 500, 403])
def test_error_envelopes_and_error_statuses_are_asset_fetch_errors(transport, status):
    transport({"/health": _resp(status, ENVELOPE)})
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/health")


def test_a_500_is_not_retried(transport):
    t = transport(_game_routes(**{f"/matches/{G}": _resp(500, ENVELOPE)}))
    with pytest.raises(AssetFetchError):
        rt._get(f"{API}/matches/{G}")
    assert t.routes_called().count(f"/matches/{G}") == 1


def test_a_404_is_no_data_and_a_pager_key_is_refused(transport):
    transport(
        {
            "/standings": _resp(404, {"message": "not found"}),
            "/metadata/seasons": _resp(body=_load("full/metadata__seasons.json")),
        }
    )
    with pytest.raises(NoDataError):
        rt._get(f"{API}/standings", {"season": 2026})
    transport({"/teams": _resp(body={"data": [], "next": "x"})})
    with pytest.raises(AssetFetchError, match="paging"):
        rt._get(f"{API}/teams")


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, *a, **k: waits.append(url))
    monkeypatch.setattr("time.sleep", lambda s: None)
    answers = [_resp(503, {}), _resp(502, {}), _resp(200, _load("full/teams.json"))]
    monkeypatch.setattr(requests.Session, "request", lambda self, m, u, *a, **k: answers.pop(0))
    assert len(rt._get(f"{API}/teams")["items"]) == 12
    assert len(waits) == 3 and all(w.startswith(API) for w in waits)


def test_retry_after_is_honoured(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", slept.append)
    answers = [_resp(429, {}, {"Retry-After": "7"}), _resp(200, _load("full/teams.json"))]
    monkeypatch.setattr(requests.Session, "request", lambda self, m, u, *a, **k: answers.pop(0))
    rt._get(f"{API}/teams")
    assert any(s >= 7 for s in slept)


def test_the_real_session_throttles_every_request():
    assert isinstance(bb.SESSION, bb.PoliteSession) and bb.MIN_INTERVAL >= 1.0


def test_standings_last_five_outside_the_season_are_refused(transport):
    rows = copy.deepcopy(_load("full/standings@2025.json"))
    rows[0]["lastFiveMatches"][0]["startTimeUTC"] = "2024-01-01T10:00:00"
    transport(_base(**{"/standings": _resp(body=rows)}))
    with pytest.raises(AssetFetchError, match="outside the season"):
        rt._get(f"{API}/standings", {"season": 2026})


def test_leaders_of_another_season_or_team_are_refused(transport):
    body = copy.deepcopy(_load("full/leaders@2025.json"))
    body["pointsPerGame"][1]["seasonId"] = S25
    team = copy.deepcopy(_load("captures/teams__teamId__leaders@2025.json"))
    transport(_base(**{"/leaders": _resp(body=body), f"/teams/{T}/leaders": _resp(body=team)}))
    with pytest.raises(AssetFetchError, match="another season"):
        rt._get(f"{API}/leaders", {"season": 2026})
    with pytest.raises(AssetFetchError, match="another season or team"):
        rt._get(f"{API}/teams/{T}/leaders", {"season": 2025})  # the 2025-26 leaders sent for 2024-25
    other = "00000000-0000-1000-8000-000000000000"
    transport(
        _base(**{f"/teams/{other}/leaders": _resp(body=team)})
    )  # another team's id answered with this team's rows
    with pytest.raises(AssetFetchError, match="another season or team"):
        rt._get(f"{API}/teams/{other}/leaders", {"season": 2026})


def test_empty_leaders_are_no_data(transport):
    transport(_base(**{"/leaders": _resp(body={"pointsPerGame": []})}))
    with pytest.raises(NoDataError):
        rt._get(f"{API}/leaders", {"season": 2026})


def test_team_fixtures_use_the_rows_own_season_or_the_stage_prefix_and_fail_closed(transport):
    rows = _load("captures/teams__teamId__matches@2025.json")
    transport(_base(**{f"/teams/{T}/matches": _resp(body=rows)}))
    assert len(rt._get(f"{API}/teams/{T}/matches", {"season": 2026})["items"]) == 3
    with pytest.raises(AssetFetchError, match="neither|carries"):
        rt._get(f"{API}/teams/{T}/matches", {"season": 2025})  # stage ids carry the 2025-26 season
    neither = copy.deepcopy(rows)
    neither[0]["stage"] = {}
    transport(_base(**{f"/teams/{T}/matches": _resp(body=neither)}))
    with pytest.raises(AssetFetchError, match="neither"):
        rt._get(f"{API}/teams/{T}/matches", {"season": 2026})
    own = copy.deepcopy(rows)
    for r in own:
        r["seasonId"] = S26
        r["stage"] = {}
    transport(_base(**{f"/teams/{T}/matches": _resp(body=own)}))
    assert len(rt._get(f"{API}/teams/{T}/matches", {"season": 2026})["items"]) == 3
    foreign = copy.deepcopy(rows)
    foreign[0]["competitors"][0]["teamId"] = foreign[0]["competitors"][1]["teamId"]
    transport(_base(**{f"/teams/{T}/matches": _resp(body=foreign)}))
    with pytest.raises(AssetFetchError, match="does not involve"):
        rt._get(f"{API}/teams/{T}/matches", {"season": 2026})


def test_team_stats_are_tied_by_games_count(transport):
    body = _load("captures/teams__teamId__stats@2025.json")
    transport(_base(**{f"/teams/{T}/stats": _resp(body=body)}))
    out = rt._get(f"{API}/teams/{T}/stats", {"season": 2026})
    assert out["_request"] == {"season": 2026, "season_id": S26, "team_id": T} and out["games"] == 9
    transport(_base(**{f"/teams/{T}/stats": _resp(body=dict(body, games=0))}))
    with pytest.raises(NoDataError):
        rt._get(f"{API}/teams/{T}/stats", {"season": 2026})
    transport(_base(**{f"/teams/{T}/stats": _resp(body={k: v for k, v in body.items() if k != "games"})}))
    with pytest.raises(AssetFetchError, match="games"):
        rt._get(f"{API}/teams/{T}/stats", {"season": 2026})


def test_player_stats_ignore_the_season_and_need_a_positive_games_count(transport):
    body = _load("captures/players__playerId__stats.json")
    t = transport({f"/players/{PL}/stats": _resp(body=body)})
    out = rt._get(f"{API}/players/{PL}/stats")
    assert out["_request"] == {"player_id": PL} and t.calls[0]["params"] == {}
    transport({f"/players/{PL}/stats": _resp(body=dict(body, games=0))})
    with pytest.raises(NoDataError):
        rt._get(f"{API}/players/{PL}/stats")
    transport({f"/players/{PL}/stats": _resp(body={"unrelated": 1})})
    with pytest.raises(AssetFetchError, match="games"):
        rt._get(f"{API}/players/{PL}/stats")


def test_entity_routes_check_the_id_they_echo(transport):
    transport(
        {
            f"/teams/{T}": _resp(body=_load("captures/teams__teamId.json")),
            f"/players/{PL}": _resp(body=_load("captures/players__playerId.json")),
        }
    )
    assert rt._get(f"{API}/teams/{T}")["_request"] == {"team_id": T}
    assert rt._get(f"{API}/players/{PL}")["_request"] == {"player_id": PL}
    other = "00000000-0000-1000-8000-000000000000"
    transport(
        {
            f"/teams/{other}": _resp(body=_load("captures/teams__teamId.json")),
            f"/players/{other}": _resp(body=_load("captures/players__playerId.json")),
        }
    )
    with pytest.raises(AssetFetchError, match="teamId"):
        rt._get(f"{API}/teams/{other}")
    with pytest.raises(AssetFetchError, match="playerId"):
        rt._get(f"{API}/players/{other}")


def test_players_filter_params_and_validation(transport):
    t = transport(_base(**{"/players": _resp(body=_load("full/players@roster.json"))}))
    out = rt._get(f"{API}/players", {"season": 2026, "teamId": T})
    assert t.calls[-1]["params"] == {"seasonId": S26, "teamId": T}
    assert out["_request"] == {"season": 2026, "season_id": S26, "team_id": T}
    with pytest.raises(ValueError, match="team_id"):
        rt._get(f"{API}/players", {"teamId": "nope"})
    transport({"/players": _resp(body=[{"playerId": None}])})
    with pytest.raises(AssetFetchError, match="playerId"):
        rt._get(f"{API}/players")


def test_teams_directory_must_be_the_easl_organisation(transport):
    rows = _load("full/teams.json")
    rows[0]["organizationId"] = "zzzzz"
    transport({"/teams": _resp(body=rows)})
    with pytest.raises(AssetFetchError, match="organisation"):
        rt._get(f"{API}/teams")


def test_seasons_need_an_active_season(transport):
    rows = [dict(r, status="COMPLETE") for r in _load("full/metadata__seasons.json")]
    transport({"/metadata/seasons": _resp(body=rows)})
    with pytest.raises(AssetFetchError, match="ACTIVE"):
        rt._get(f"{API}/metadata/seasons")


def test_wrappers_end_to_end_over_a_mocked_transport(transport):
    transport(_game_routes(**{"/matches": _resp(body=_load("full/matches@2025.json"))}))
    from sportsdataverse.basketball import easl

    games = easl.easl_schedule(season=2026)
    assert games.height == 42 and set(games["season"]) == {2026}
    box = easl.easl_player_box(fixture_id=G)
    assert box.height == 24 and box.filter(pl.col("team_id") == T)["points"].sum() == 90
    assert easl.easl_pbp(fixture_id=G).height == 495
    assert easl.easl_team_box_by_period(fixture_id=G).height == 8
    raw = easl.easl_player_box(fixture_id=G, return_parsed=False)
    assert raw["_request"] == {"fixture_id": G}
