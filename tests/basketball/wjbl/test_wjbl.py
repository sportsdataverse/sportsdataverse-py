"""The W League (``wjbl``) family: parsers, runtime refusals, politeness, season mapping, and the generated wrappers.

Reads only committed files (CI has no sdv-internal-refs checkout): ``tests/fixtures/wjbl`` (the FULL bodies of game
7987 and the refs captures), the endpoint YAML, its schemas, the description snippet and the spec-route snapshot.
The transport is mocked.
"""

from __future__ import annotations

import copy
import inspect
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import polars as pl
import pytest
import requests
import yaml

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball import wjbl
from sportsdataverse.basketball.wjbl import wjbl as api
from sportsdataverse.basketball.wjbl import wjbl_parsers as P
from sportsdataverse.basketball.wjbl import wjbl_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[3]
FIX = ROOT / "tests" / "fixtures" / "wjbl"
BASE = "https://api.wjbl.01core.app/api"
DOC = yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "wjbl.yaml").read_text(encoding="utf-8"))
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "wjbl"

GID = 7987


def _body(name: str) -> Any:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


# parser -> the fixtures it must parse to a non-empty frame (the FULL bodies first, then the refs captures)
_DETAIL = [
    "full/play-schedule__detail@7987.json",
    *[f"captures/play-schedule__detail{s}.json" for s in ("", "@4243", "@5533", "@7360")],
]
_BOX = ["full/boxscore@7987.json", *[f"captures/boxscore{s}.json" for s in ("", "@4243", "@5533", "@7360")]]
_PBP = [
    "full/pbp__play-by-play@7987.json",
    "captures/pbp__play-by-play.json",
    "captures/pbp__play-by-play@5533.json",
    "captures/pbp__play-by-play@7360.json",
]
_TEAMSTATS = [
    "full/pbp__team-stats@7987.json",
    "captures/pbp__team-stats.json",
    "captures/pbp__team-stats@5533.json",
    "captures/pbp__team-stats@7360.json",
]
_PLAYERSTATS = [
    "full/pbp__player-stats@7987.json",
    "captures/pbp__player-stats.json",
    "captures/pbp__player-stats@5533.json",
    "captures/pbp__player-stats@7360.json",
]
_INFO = ["full/play-schedule@7987.json", *[f"captures/play-schedule{s}.json" for s in ("", "@4243", "@5533", "@7360")]]
_SCHED = [
    "full/play-schedules@45_2026-04.json",
    "captures/play-schedules.json",
    "captures/play-schedules@20_2014-04.json",
    "captures/play-schedules@28_2018-10.json",
    "captures/play-schedules@45_2025-10_team13.json",
]
FIXTURES: Dict[str, List[str]] = {
    "parse_wjbl_leagues": ["full/leagues.json", "captures/leagues.json"],
    "parse_wjbl_game_months": ["captures/play-schedule-months.json", "captures/play-schedule-months@20.json"],
    "parse_wjbl_schedule": _SCHED,
    "parse_wjbl_game": _DETAIL,
    "parse_wjbl_period_scores": _DETAIL,
    "parse_wjbl_game_info": _INFO,
    "parse_wjbl_box_score": _BOX,
    "parse_wjbl_team_box_score": _BOX,
    "parse_wjbl_pbp": _PBP,
    "parse_wjbl_team_period_stats": _TEAMSTATS,
    "parse_wjbl_player_period_leaders": _PLAYERSTATS,
    "parse_wjbl_standing_seasons": ["full/seasons__standings.json", "captures/seasons__standings.json"],
    "parse_wjbl_team_standings": ["captures/team-standings.json"],
    "parse_wjbl_head_to_head": ["captures/team-standings.json"],
    "parse_wjbl_team_ranking": ["captures/team-ranking.json"],
    "parse_wjbl_teams": ["full/teams__base.json", "captures/teams__base.json"],
    "parse_wjbl_roster": ["captures/team__roster.json"],
    "parse_wjbl_staff": ["captures/team__roster.json"],
    "parse_wjbl_player": ["captures/player.json"],
}
CASES = [(name, rel) for name, rels in FIXTURES.items() for rel in rels]
_ID_COL = re.compile(r"(^|_)id\d?$")
_SNAKE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")
_PHONE = re.compile(
    r"(?<![0-9０-９])[(（]?[0０][0-9０-９]{1,4}[)）]?[-－‐ー−― 　][0-9０-９]{1,4}[-－‐ー−― 　][0-9０-９]{3,4}(?![0-9０-９])"
)
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")


# --- parsers ------------------------------------------------------------------------------------------------------


def test_the_fixture_table_covers_every_parser():
    assert set(FIXTURES) == set(P.SCHEMAS) == {name for name in P.__all__}
    assert len(P.__all__) == 19


@pytest.mark.parametrize("parser,rel", CASES, ids=[f"{p[11:]}:{r}" for p, r in CASES])
def test_every_fixture_parses_to_a_typed_non_empty_frame(parser, rel):
    df = getattr(P, parser)(_body(rel))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert df.schema == pl.Schema(P.SCHEMAS[parser])
    assert all(_SNAKE.match(c) for c in df.columns)
    for col, dtype in df.schema.items():
        if _ID_COL.search(col):
            assert dtype == pl.Int64, col


@pytest.mark.parametrize("parser,rel", CASES, ids=[f"{p[11:]}:{r}" for p, r in CASES])
def test_the_wrapped_and_the_bare_body_parse_alike(parser, rel):
    raw = _body(rel)
    wrapped = {"items": raw, "_request": {}} if isinstance(raw, list) else {**raw, "_request": {}}
    assert getattr(P, parser)(wrapped).equals(getattr(P, parser)(raw))


@pytest.mark.parametrize("parser", sorted(FIXTURES))
@pytest.mark.parametrize("bad", [{}, [], None, "x", 3, {"items": []}, [1, "a"]])
def test_an_empty_or_malformed_body_is_a_typed_zero_row_frame_that_stacks(parser, bad):
    fn = getattr(P, parser)
    empty = fn(bad)
    assert empty.height == 0 and empty.schema == pl.Schema(P.SCHEMAS[parser])
    full = fn(_body(FIXTURES[parser][0]))
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([empty, fn(bad)]).height == 0


@pytest.mark.parametrize("parser", sorted(FIXTURES))
def test_return_as_pandas(parser):
    import pandas as pd

    df = getattr(P, parser)(_body(FIXTURES[parser][0]), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) > 0
    assert list(getattr(P, parser)({}, return_as_pandas=True).columns) == list(P.SCHEMAS[parser])


def test_every_public_parser_documents_args_returns_raises_and_an_example():
    for name in P.__all__:
        doc = inspect.getdoc(getattr(P, name))
        for head in ("Args:", "Returns:", "Raises:", "Example:"):
            assert head in doc, (name, head)
        assert ">>>" not in doc


def test_the_game_header_full_body():
    g = P.parse_wjbl_game(_body("full/play-schedule__detail@7987.json"))
    assert g.height == 1
    row = g.row(0, named=True)
    assert (row["play_schedule_id"], row["game_status"], row["home_team_id"], row["away_team_id"]) == (
        GID,
        "fixed",
        13,
        12,
    )
    assert (row["home_score"], row["away_score"], row["home_winner_flg"], row["away_winner_flg"]) == (66, 75, -1, 1)
    assert row["home_score_ot1"] is None
    ps = P.parse_wjbl_period_scores(_body("full/play-schedule__detail@7987.json"))
    assert ps.height == 8 and set(ps["period_label"]) == {"Q1", "Q2", "Q3", "Q4"}
    assert ps.group_by("team_side").agg(pl.col("score").sum()).sort("team_side")["score"].to_list() == [75, 66]


def test_an_overtime_game_has_overtime_period_rows():
    ps = P.parse_wjbl_period_scores(_body("captures/play-schedule__detail@7360.json"))
    assert ps.filter(pl.col("period") == 5)["period_label"].unique().to_list() == ["OT1"]
    g = P.parse_wjbl_game(_body("captures/play-schedule__detail@7360.json")).row(0, named=True)
    total = [ps.filter(pl.col("team_side") == s)["score"].sum() for s in ("home", "away")]
    assert total == [g["home_score"], g["away_score"]]


def test_the_schedule_full_month():
    df = P.parse_wjbl_schedule(_body("full/play-schedules@45_2026-04.json"))
    assert df.height == 4 and set(df["season"]) == {2026} and set(df["league_id"]) == {45}
    assert df["game_day"].to_list()[0] == "2026-04-04" and df["game_year"].dtype == pl.Int64
    assert (df["home_team_score"].to_list()[0], df["away_team_score"].to_list()[0]) == (66, 75)
    assert df["play_schedule_id"].is_unique().all()


def test_the_game_info_drops_the_contact_and_keeps_the_public_fields():
    raw = _body("full/play-schedule@7987.json")
    assert "contact" in raw  # the committed body is redacted, the key is still there
    df = P.parse_wjbl_game_info(raw)
    assert not [c for c in df.columns if "contact" in c]
    row = df.row(0, named=True)
    assert row["game_day"] == "2026-04-04" and row["season"] == 2026 and row["referee_name"] == "須黒 祥子"
    assert row["home_team_id"] == 13 and row["away_team_id"] == 12 and row["game_status"] == "fixed"


def test_the_box_score_full_body_adds_up():
    raw = _body("full/boxscore@7987.json")
    players = P.parse_wjbl_box_score(raw)
    teams = P.parse_wjbl_team_box_score(raw)
    assert players.height == 27 and teams.height == 4 and set(teams["row_type"]) == {"team", "total"}
    for side in ("home", "away"):
        total = teams.filter((pl.col("team_side") == side) & (pl.col("row_type") == "total")).row(0, named=True)
        mine = players.filter(pl.col("team_side") == side)
        assert mine["pts"].sum() == total["pts"] and mine["p3m"].sum() == total["p3m"]
        team_line = teams.filter((pl.col("team_side") == side) & (pl.col("row_type") == "team")).row(0, named=True)
        assert team_line["pts"] == 0 and team_line["p2p"] is None
    assert players["min_seconds"].null_count() == 0
    assert players.filter(pl.col("player_id") == 9799)["min_seconds"][0] == 22 * 60 + 19
    assert teams.filter(pl.col("row_type") == "total")["fgp"].dtype == pl.Float64
    assert teams.filter((pl.col("team_side") == "home") & (pl.col("row_type") == "total"))["fgp"][0] == 39.3
    assert teams.filter(pl.col("row_type") == "total")["min_seconds"].to_list() == [200 * 60, 200 * 60]


def test_the_play_by_play_full_body():
    df = P.parse_wjbl_pbp(_body("full/pbp__play-by-play@7987.json"))
    assert df.height == 439 and df["no"].is_unique().all() and df["play_schedule_id"].unique().to_list() == [GID]
    assert df["end_game_flg"].to_list()[-1] == 1 and df["end_game_flg"].sum() == 1
    assert (
        set(df["team_side"].drop_nulls()) == {"home", "away"}
        and df["home_away"].null_count() == df["team_side"].null_count()
    )
    first = df.row(0, named=True)
    assert first["rest_time"] == "10:00" and first["seconds_remaining"] == 600
    scored = df.filter(pl.col("score") != "")
    assert (
        scored["home_score"].null_count() == 0
        and scored["home_score"].to_list()[-1] == 66
        and scored["away_score"].to_list()[-1] == 75
    )
    assert df.filter(pl.col("score") == "")["home_score"].null_count() == df.filter(pl.col("score") == "").height
    subs = df.filter(pl.col("action1") == "プレイヤーインアウト")
    assert subs.height == 89 and subs["player_id1"].null_count() == 0 and subs["player_id2"].null_count() == 0
    assert df["player_id1"].dtype == pl.Int64 and df["player_id2"].dtype == pl.Int64


def test_the_team_period_stats_full_body():
    df = P.parse_wjbl_team_period_stats(_body("full/pbp__team-stats@7987.json"))
    assert df["period_category"].to_list() == [1, 2, 3, 4, 15, 16, 18]
    assert df["period_label"].to_list() == ["Q1", "Q2", "Q3", "Q4", "H1", "H2", "GAME"]
    q1 = df.row(0, named=True)
    assert (q1["home_rebound_off"], q1["home_rebound_def"], q1["home_rebound_tot"]) == (3, 4, 7)
    assert q1["home_turnovers"] == 6 and q1["home_max_difference"] == 2 and q1["away_max_difference"] == 8
    assert q1["tie_count"] == 4 and q1["lead_count"] == 0
    assert df["home_ptr"].dtype == pl.Float64 and q1["home_ptr"] == 61.5 and q1["home_ftr"] == 100.0
    game = df.filter(pl.col("period_category") == 18).row(0, named=True)
    assert game["home_rebound_tot"] == df.filter(pl.col("period_category") < 5)["home_rebound_tot"].sum()
    assert game["home_assist"] == df.filter(pl.col("period_category") < 5)["home_assist"].sum()


def test_the_player_period_leaders_full_body():
    df = P.parse_wjbl_player_period_leaders(_body("full/pbp__player-stats@7987.json"))
    assert df.height == 7 and df["period_category"].to_list() == [1, 2, 3, 4, 15, 16, 18]
    assert "#8 高田 真希 , #28" in df["away_point_player"][0] and df["away_point_value"][0] == "6点 , 6点"
    assert df["home_team_id"].unique().to_list() == [13] and df["away_team_id"].unique().to_list() == [12]


def test_the_standings_grid_and_the_head_to_head():
    raw = _body("captures/team-standings.json")
    table = P.parse_wjbl_team_standings(raw)
    assert table.select("wins", "losses").row(0) == (23, 5) and table["position"].to_list() == [1, 2, 3]
    h2h = P.parse_wjbl_head_to_head(raw)
    assert set(h2h.columns) == set(P.SCHEMAS["parse_wjbl_head_to_head"]) and h2h["win"].dtype == pl.Int64
    first = h2h.row(0, named=True)
    assert (first["team_id"], first["opponent_team_id"], first["win"], first["losg"], first["result"]) == (
        13,
        12,
        63,
        56,
        "○ 63-56",
    )
    assert not h2h.filter(pl.col("team_id") == pl.col("opponent_team_id")).height


def test_the_ranking_numbers_are_floats_without_the_percent_sign():
    df = P.parse_wjbl_team_ranking(_body("captures/team-ranking.json"))
    row = df.row(0, named=True)
    assert (row["season"], row["season_id"], row["win"], row["lost"]) == (2026, 169, 23, 5)
    assert (row["fg"], row["opp_fg"], row["ppg"], row["gpd"], row["winning_rate"]) == (
        40.92,
        37.72,
        63.11,
        283.0,
        0.821,
    )
    assert set(df["season_id"]) == {169, 170}


def test_the_standing_seasons_carry_the_end_year():
    df = P.parse_wjbl_standing_seasons(_body("full/seasons__standings.json"))
    assert df["season_id"].is_unique().all() and {169, 170} <= set(df["season_id"])
    assert set(df.filter(pl.col("season_id") == 169)["season"]) == {2026}


def test_roster_staff_and_player():
    raw = _body("captures/team__roster.json")
    roster, staff = P.parse_wjbl_roster(raw), P.parse_wjbl_staff(raw)
    assert (
        set(roster["season_team_id"]) == {1343} and set(roster["team_id"]) == {13} and set(roster["season"]) == {2026}
    )
    assert roster["player_number"].dtype == pl.String and set(roster["coach_id"]) == {175}
    assert staff["staff_name"].null_count() == 0 and set(staff["season_id"]) == {169}
    player = P.parse_wjbl_player(_body("captures/player.json")).row(0, named=True)
    assert (player["player_height_cm"], player["player_weight_kg"], player["player_id"]) == (179, 77, 9799)
    assert not [c for c in player if "blood" in c]


def test_no_parsed_frame_carries_a_phone_number_or_an_email():
    for parser, rel in CASES:
        df = getattr(P, parser)(_body(rel))
        for col in [c for c, d in df.schema.items() if d == pl.String]:
            for value in df[col].drop_nulls().to_list():
                assert not _PHONE.search(value) and not _EMAIL.search(value), (parser, rel, col, value[:40])


@pytest.mark.parametrize(
    "text,out",
    [
        ("TEL 03-1234-5678 まで", "TEL [redacted-phone] まで"),
        ("０３－１２３４－５６７８", "[redacted-phone]"),
        ("090 1234 5678", "[redacted-phone]"),
        ("info@example.co.jp へ", "[redacted-email] へ"),
        ("2Pシュート (2点) 10-0", "2Pシュート (2点) 10-0"),
        (None, None),
    ],
)
def test_free_text_is_cleaned(text, out):
    assert P._clean(text) == out


def test_a_phone_in_a_free_text_field_is_replaced_and_a_contact_key_dropped():
    raw = copy.deepcopy(_body("full/play-schedule@7987.json"))
    raw["ticket_info"] = "お問合せ 03-1234-5678 / a@b.jp"
    raw["contact_tel"] = "03-1111-2222"
    row = P.parse_wjbl_game_info(raw).row(0, named=True)
    assert row["ticket_info"] == "お問合せ [redacted-phone] / [redacted-email]"
    assert not [c for c in row if "contact" in c]
    p = copy.deepcopy(_body("captures/player.json"))
    p["player_appeal"] = "tel 090-1234-5678"
    assert P.parse_wjbl_player(p)["player_appeal"][0] == "tel [redacted-phone]"


def test_ids_are_never_float_strings():
    for parser, rel in CASES:
        df = getattr(P, parser)(_body(rel))
        for col in [c for c in df.columns if _ID_COL.search(c)]:
            assert df[col].dtype == pl.Int64 and not (df[col].cast(pl.String).str.contains(r"\.").any()), (parser, col)


def test_the_league_title_gives_the_end_year():
    cases = {
        "25-26 Wリーグ": 2026,
        "99-00 Wリーグ": 2000,
        "00-01 Wリーグ": 2001,
        "04-05 Wリーグ": 2005,
        "26-27 Wリーグ 大樹生命 Wリーグ ユナイテッド": 2027,
        "Wリーグオールスター2025-26": None,
        "Ｗリーグオールスター": None,
        "オータムカップ2021": None,
        "サマーキャンプ2019 ": None,
        "WリーグSUPERGAMES": None,
        "25-27 Wリーグ": None,
        None: None,
    }
    for title, end in cases.items():
        assert rt.season_of_title(title) == end, title
    df = P.parse_wjbl_leagues(_body("full/leagues.json"))
    assert df.height == 37 and df["season"].drop_nulls().n_unique() == df["season"].drop_nulls().len()
    assert df.filter(pl.col("league_id") == 45)["season"][0] == 2026


# --- runtime ------------------------------------------------------------------------------------------------------


def _response(status: int = 200, body: Any = None, *, text: Optional[str] = None, headers: Optional[dict] = None):
    resp = requests.Response()
    resp.status_code = status
    resp._content = (text if text is not None else json.dumps(body, ensure_ascii=False)).encode()
    resp.headers.update(headers or {"Content-Type": "application/json"})
    resp.url = BASE
    return resp


def _ok(name: str, mutate: Optional[Callable[[Any], None]] = None) -> requests.Response:
    body = copy.deepcopy(_body(name))
    if mutate:
        mutate(body)
    return _response(body=body)


class Router:
    """Replaces ``requests.Session.request``: answers by route (the path after ``/api``) and records every attempt."""

    def __init__(self, routes: Dict[str, Any]):
        self.routes = routes
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, method, url, *args, **kwargs):
        route = url.split(".app/api", 1)[1]
        self.calls.append({"route": route, "params": dict(kwargs.get("params") or {})})
        answer = self.routes[route]
        if isinstance(answer, list):
            answer = answer.pop(0) if len(answer) > 1 else answer[0]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.fixture
def transport(monkeypatch):
    """Install a router under the real session; no polite wait, no retry sleep. The dependency cache starts empty."""

    def install(routes: Dict[str, Any]) -> Router:
        t = Router(routes)
        monkeypatch.setattr(requests.Session, "request", t)
        return t

    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: None)
    rt.clear_cache()
    yield install
    rt.clear_cache()


def _get(route: str, params: Optional[dict] = None):
    return rt._get(f"{BASE}{route}", params)


def _game_routes(**over: Any) -> Dict[str, Any]:
    routes = {
        "/play-schedule/detail": _ok("full/play-schedule__detail@7987.json"),
        "/play-schedule": _ok("full/play-schedule@7987.json"),
        "/boxscore": _ok("full/boxscore@7987.json"),
        "/pbp/play-by-play": _ok("full/pbp__play-by-play@7987.json"),
        "/pbp/team-stats": _ok("full/pbp__team-stats@7987.json"),
        "/pbp/player-stats": _ok("full/pbp__player-stats@7987.json"),
    }
    routes.update(over)
    return routes


def test_the_season_maps_to_the_league_edition_through_the_live_league_list(transport):
    t = transport(
        {"/leagues": _ok("full/leagues.json"), "/play-schedule-months": _ok("captures/play-schedule-months.json")}
    )
    out = _get("/play-schedule-months", {"season": 2026})
    assert [c["route"] for c in t.calls] == ["/leagues", "/play-schedule-months"]
    assert t.calls[0]["params"] == {"target": "play_schedule"} and t.calls[1]["params"] == {"league_id": 45}
    assert out["_request"] == {"league_id": 45, "season": 2026}
    assert rt.league_id(2000) == 4 and rt.league_id("2027") == 47 and rt.league_id(2014) == 20
    assert len([c for c in t.calls if c["route"] == "/leagues"]) == 1  # cached


def test_every_league_season_maps_to_one_edition(transport):
    transport({"/leagues": _ok("full/leagues.json")})
    rows = _body("full/leagues.json")
    ends = {
        rt.season_of_title(r["league_title"]): r["league_id"] for r in rows if rt.season_of_title(r["league_title"])
    }
    assert len(ends) == 28 and min(ends) == 2000 and max(ends) == 2027
    for end, lid in ends.items():
        assert rt.league_id(end) == lid


@pytest.mark.parametrize("bad", [True, "abc", "2026.5", 1999, 2028, 0, 20.5])
def test_a_season_that_is_not_a_league_season_is_a_value_error(bad, transport):
    transport({"/leagues": _ok("full/leagues.json")})
    with pytest.raises(ValueError, match="END year|no W League edition"):
        _get("/play-schedule-months", {"season": bad})


def test_an_explicit_league_id_passes_through_untouched(transport):
    t = transport({"/play-schedule-months": _ok("captures/play-schedule-months.json")})
    out = _get("/play-schedule-months", {"league_id": 45})
    assert [c["route"] for c in t.calls] == ["/play-schedule-months"] and out["_request"] == {"league_id": 45}


def test_the_months_echo_must_be_the_league_asked_for(transport):
    transport({"/play-schedule-months": _ok("captures/play-schedule-months.json")})
    with pytest.raises(AssetFetchError, match="league_id"):
        _get("/play-schedule-months", {"league_id": 20})
    transport({"/play-schedule-months": _ok("captures/play-schedule-months.json", lambda b: b.update(league_id=45))})
    with pytest.raises(AssetFetchError):  # the echo is a string; an integer echo is another API
        _get("/play-schedule-months", {"league_id": 45})


def test_an_unknown_league_has_no_months(transport):
    transport({"/play-schedule-months": _response(body={"league_id": "999", "game_months": []})})
    with pytest.raises(NoDataError):
        _get("/play-schedule-months", {"league_id": 999})


def test_the_schedule_page_is_checked_against_league_month_and_team(transport):
    page = "full/play-schedules@45_2026-04.json"
    t = transport({"/play-schedules": _ok(page)})
    out = _get("/play-schedules", {"league_id": 45, "game_month": "2026-04"})
    assert t.calls[0]["params"] == {"league_id": 45, "game_month": "2026-04"} and "seasons" in out
    with pytest.raises(AssetFetchError, match="asked for 2026-03"):
        _get("/play-schedules", {"league_id": 45, "game_month": "2026-03"})
    with pytest.raises(AssetFetchError, match="league_id"):
        _get("/play-schedules", {"league_id": 20, "game_month": "2026-04"})
    with pytest.raises(AssetFetchError, match="team 99"):
        _get("/play-schedules", {"league_id": 45, "game_month": "2026-04", "team_id": 99})
    out = _get("/play-schedules", {"league_id": 45, "game_month": "2026-04", "team_id": 13})
    assert out["_request"]["team_id"] == 13


def test_an_empty_schedule_month_is_no_data(transport):
    for empty in ({"league_id": 45, "league_title": "25-26 Wリーグ", "seasons": {}}, [], {}):
        transport({"/play-schedules": _response(body=empty)})
        with pytest.raises(NoDataError):
            _get("/play-schedules", {"league_id": 45, "game_month": "2026-04"})


def test_the_schedule_needs_a_season_and_a_month_must_be_yyyy_mm(transport):
    t = transport({"/play-schedules": _ok("full/play-schedules@45_2026-04.json")})
    with pytest.raises(ValueError, match="season"):
        _get("/play-schedules", {})
    with pytest.raises(ValueError, match="YYYY-MM"):
        _get("/play-schedules", {"league_id": 45, "game_month": "2026-4"})
    assert t.calls == []


def test_a_game_route_fetches_the_header_first_and_caches_a_final_one(transport):
    t = transport(_game_routes())
    out = _get("/boxscore", {"play_schedule_id": GID})
    assert [c["route"] for c in t.calls] == ["/play-schedule/detail", "/boxscore"]
    assert out["_request"] == {"play_schedule_id": GID}
    _get("/pbp/play-by-play", {"play_schedule_id": str(GID)})
    assert [c["route"] for c in t.calls].count("/play-schedule/detail") == 1


@pytest.mark.parametrize("route", sorted(rt._GAME_ROUTES))
def test_each_game_route_returns_its_body_with_the_request_stamped(route, transport):
    transport(_game_routes())
    out = _get(route, {"play_schedule_id": GID})
    assert out["play_schedule_id"] == GID and out["_request"] == {"play_schedule_id": GID}


def test_a_game_that_is_not_final_is_refused_on_every_game_route(transport):
    live = _ok("full/play-schedule__detail@7987.json", lambda b: b.update(game_status="scheduled"))
    for route in ("/play-schedule/detail", *sorted(rt._GAME_ROUTES)):
        transport(_game_routes(**{"/play-schedule/detail": live}))
        rt.clear_cache()
        with pytest.raises(AssetFetchError, match="not final"):
            _get(route, {"play_schedule_id": GID})


def test_a_not_final_header_is_never_cached(transport):
    t = transport(
        _game_routes(
            **{
                "/play-schedule/detail": [
                    _ok("full/play-schedule__detail@7987.json", lambda b: b.update(game_status="live")),
                    _ok("full/play-schedule__detail@7987.json"),
                ]
            }
        )
    )
    with pytest.raises(AssetFetchError):
        _get("/boxscore", {"play_schedule_id": GID})
    assert _get("/boxscore", {"play_schedule_id": GID})["play_schedule_id"] == GID
    assert [c["route"] for c in t.calls].count("/play-schedule/detail") == 2


def test_a_body_for_another_game_is_refused_not_returned(transport):
    other = lambda b: b.update(play_schedule_id=GID + 1)  # noqa: E731
    for route, fix in (
        ("/boxscore", "full/boxscore@7987.json"),
        ("/pbp/play-by-play", "full/pbp__play-by-play@7987.json"),
        ("/pbp/team-stats", "full/pbp__team-stats@7987.json"),
        ("/pbp/player-stats", "full/pbp__player-stats@7987.json"),
        ("/play-schedule", "full/play-schedule@7987.json"),
    ):
        transport(_game_routes(**{route: _ok(fix, other)}))
        rt.clear_cache()
        with pytest.raises(AssetFetchError, match="asked for game 7987"):
            _get(route, {"play_schedule_id": GID})
    transport(_game_routes(**{"/play-schedule/detail": _ok("full/play-schedule__detail@7987.json", other)}))
    rt.clear_cache()
    with pytest.raises(AssetFetchError, match="asked for game 7987"):
        _get("/play-schedule/detail", {"play_schedule_id": GID})


def test_the_teams_of_every_game_route_must_be_the_headers_in_order(transport):
    def swap(b):
        b["home"], b["away"] = b["away"], b["home"]

    def swap_ids(b):
        b["home_team_id"], b["away_team_id"] = b["away_team_id"], b["home_team_id"]

    for route, fix, mut in (
        ("/boxscore", "full/boxscore@7987.json", swap),
        ("/play-schedule", "full/play-schedule@7987.json", swap),
        ("/pbp/team-stats", "full/pbp__team-stats@7987.json", swap_ids),
        ("/pbp/player-stats", "full/pbp__player-stats@7987.json", swap_ids),
    ):
        transport(_game_routes(**{route: _ok(fix, mut)}))
        rt.clear_cache()
        with pytest.raises(AssetFetchError, match="home/away order"):
            _get(route, {"play_schedule_id": GID})


def test_a_play_that_contradicts_its_side_is_refused(transport):
    def flip(b):
        b["play_by_plays"][5]["home_away"] = 2 if b["play_by_plays"][5]["home_away"] == 1 else 1

    transport(_game_routes(**{"/pbp/play-by-play": _ok("full/pbp__play-by-play@7987.json", flip)}))
    with pytest.raises(AssetFetchError, match="side"):
        _get("/pbp/play-by-play", {"play_schedule_id": GID})


def test_a_truncated_play_by_play_is_refused(transport):
    transport(
        _game_routes(
            **{"/pbp/play-by-play": _ok("full/pbp__play-by-play@7987.json", lambda b: b["play_by_plays"].pop())}
        )
    )
    with pytest.raises(AssetFetchError, match="truncated"):
        _get("/pbp/play-by-play", {"play_schedule_id": GID})


@pytest.mark.parametrize("route", sorted(rt._PBP_ROUTES))
def test_a_game_without_play_by_play_is_no_data(route, transport):
    transport(_game_routes(**{route: _response(body=[])}))
    with pytest.raises(NoDataError, match="2018-19"):
        _get(route, {"play_schedule_id": GID})


def test_an_unknown_game_is_no_data_and_an_empty_box_score_too(transport):
    transport({"/play-schedule/detail": _response(body=[])})
    with pytest.raises(NoDataError, match="no game"):
        _get("/boxscore", {"play_schedule_id": 999999})
    transport(_game_routes(**{"/boxscore": _response(body=[])}))
    rt.clear_cache()
    with pytest.raises(NoDataError):
        _get("/boxscore", {"play_schedule_id": GID})


def test_the_error_envelope_is_an_asset_fetch_error_even_at_http_200(transport):
    envelope = {"error": {"error_code": "E003", "error_message": "パラメータに誤りがあります。：play_schedule_id"}}
    transport({"/play-schedule/detail": _response(body=envelope)})
    with pytest.raises(AssetFetchError, match="E003") as exc:
        _get("/play-schedule/detail", {"play_schedule_id": GID})
    assert "パラメータ" not in str(exc.value) and "?" not in str(exc.value)
    transport({"/teams/base": _response(body=envelope)})
    with pytest.raises(AssetFetchError, match="E003"):
        _get("/teams/base")


def test_season_ids_must_come_back_as_asked(transport):
    t = transport(
        {"/team-standings": _ok("captures/team-standings.json"), "/team-ranking": _ok("captures/team-ranking.json")}
    )
    out = _get("/team-standings", {"season_ids": [169]})
    assert t.calls[0]["params"] == {"season_ids": "169"} and out["_request"] == {"season_ids": "169"}
    assert _get("/team-ranking", {"season_ids": "169,170"})["league_id"] == 45
    assert _get("/team-ranking", {"season_ids": (169, 170)})["_request"]["season_ids"] == "169,170"
    with pytest.raises(AssetFetchError, match="asked for season_ids"):
        _get("/team-standings", {"season_ids": "169,170"})  # a partial answer
    with pytest.raises(AssetFetchError, match="asked for season_ids"):
        _get("/team-ranking", {"season_ids": "169"})  # a wider answer
    with pytest.raises(AssetFetchError, match="asked for season_ids"):
        _get("/team-standings", {"season_ids": "170"})  # another phase
    transport({"/team-standings": _response(body=[])})
    with pytest.raises(NoDataError):
        _get("/team-standings", {"season_ids": "999"})


@pytest.mark.parametrize("bad", ["", "a", "169,x", [], [1.5]])
def test_season_ids_must_be_integers(bad, transport):
    t = transport({"/team-standings": _ok("captures/team-standings.json")})
    with pytest.raises(ValueError, match="season_ids"):
        _get("/team-standings", {"season_ids": bad})
    assert t.calls == []


def test_roster_and_player_echoes_are_checked(transport):
    transport({"/team/roster": _ok("captures/team__roster.json"), "/player": _ok("captures/player.json")})
    assert _get("/team/roster", {"team_id": 13, "season_id": 169})["_request"] == {"team_id": 13, "season_id": 169}
    assert _get("/team/roster", {"team_id": 13})["team_id"] == 13
    with pytest.raises(AssetFetchError, match="asked for team 12"):
        _get("/team/roster", {"team_id": 12, "season_id": 169})
    with pytest.raises(AssetFetchError, match="season 170"):
        _get("/team/roster", {"team_id": 13, "season_id": 170})
    assert _get("/player", {"player_id": 9799})["player_id"] == 9799
    with pytest.raises(AssetFetchError, match="player 1"):
        _get("/player", {"player_id": 1})
    transport({"/team/roster": _response(body=[]), "/player": _response(body=[])})
    with pytest.raises(NoDataError):
        _get("/team/roster", {"team_id": 13, "season_id": 1})
    with pytest.raises(NoDataError):
        _get("/player", {"player_id": 1})


def test_the_list_routes_and_a_bare_list_is_wrapped(transport):
    transport(
        {
            "/leagues": _ok("full/leagues.json"),
            "/teams/base": _ok("full/teams__base.json"),
            "/seasons/standings": _ok("full/seasons__standings.json"),
        }
    )
    for route in ("/leagues", "/teams/base", "/seasons/standings"):
        out = _get(route)
        assert set(out) == {"items", "_request"} and out["items"]
    for route in ("/leagues", "/teams/base", "/seasons/standings"):
        transport({route: _response(body=[])})
        with pytest.raises(NoDataError):
            _get(route)


def test_a_missing_or_non_integer_parameter_is_a_value_error_before_any_request(transport):
    t = transport(_game_routes())
    for route, params in (
        ("/boxscore", {}),
        ("/boxscore", {"play_schedule_id": "abc"}),
        ("/boxscore", {"play_schedule_id": True}),
        ("/team/roster", {}),
        ("/player", {"player_id": 1.5}),
        ("/team-ranking", {}),
        ("/play-schedule-months", {}),
    ):
        with pytest.raises(ValueError):
            _get(route, params)
    with pytest.raises(ValueError, match="not a W League route"):
        _get("/coach", {"coach_id": 1})
    assert t.calls == []


def test_every_attempt_waits_its_turn_retries_included(monkeypatch):
    waits: List[str] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    monkeypatch.setattr("time.sleep", lambda s: None)
    t = Router(
        {"/teams/base": [_response(503, text="busy"), _response(503, text="busy"), _ok("full/teams__base.json")]}
    )
    monkeypatch.setattr(requests.Session, "request", t)
    _get("/teams/base")
    assert len(t.calls) == 3 and len(waits) == 3  # one wait per attempt, not one per call
    assert rt._bb.SESSION is bb.SESSION and bb.MIN_INTERVAL >= 1.0


def test_a_dependency_request_waits_too(monkeypatch, transport):
    waits: List[str] = []
    transport(_game_routes())
    monkeypatch.setattr(bb, "_polite_wait", lambda url, interval=bb.MIN_INTERVAL: waits.append(url))
    _get("/pbp/team-stats", {"play_schedule_id": GID})
    assert len(waits) == 2  # the route and the header it is checked against
    waits.clear()
    transport(
        {"/leagues": _ok("full/leagues.json"), "/play-schedule-months": _ok("captures/play-schedule-months.json")}
    )
    rt.clear_cache()
    _get("/play-schedule-months", {"season": 2026})
    assert len(waits) == 2  # the route and the league list the season is mapped with


def test_retry_after_is_honoured_on_429_and_503(monkeypatch):
    slept: List[float] = []
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    monkeypatch.setattr("time.sleep", lambda s: slept.append(s))
    for status in (429, 503):
        slept.clear()
        t = Router(
            {
                "/teams/base": [
                    _response(status, text="slow", headers={"Retry-After": "7"}),
                    _ok("full/teams__base.json"),
                ]
            }
        )
        monkeypatch.setattr(requests.Session, "request", t)
        _get("/teams/base")
        assert 7.0 in slept, (status, slept)


def test_the_session_spaces_two_requests_by_the_interval(monkeypatch):
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(bb.time, "monotonic", lambda: state["now"])
    monkeypatch.setattr(
        bb.time, "sleep", lambda s: (state["slept"].append(s), state.__setitem__("now", state["now"] + s))
    )
    monkeypatch.setattr(bb, "_last", {})
    monkeypatch.setattr(requests.Session, "request", Router({"/a": _response(200, {}), "/b": _response(200, {})}))
    bb.SESSION.request("GET", f"{BASE}/a")
    bb.SESSION.request("GET", f"{BASE}/b")
    assert state["slept"] == [pytest.approx(bb.MIN_INTERVAL)]


def test_http_errors_and_a_dead_connection_use_the_package_vocabulary(transport, monkeypatch):
    transport({"/teams/base": _response(404, text="no")})
    with pytest.raises(NoDataError):
        _get("/teams/base")
    transport({"/teams/base": _response(400, text="bad")})
    with pytest.raises(ValueError):
        _get("/teams/base")
    for answer in (_response(502, text="bad gateway"), _response(200, text="<html>"), _response(200, text="")):
        transport({"/teams/base": answer})
        with pytest.raises(AssetFetchError):
            _get("/teams/base")
    monkeypatch.setattr(requests.Session, "request", Router({"/teams/base": requests.ConnectionError("reset")}))
    with pytest.raises(AssetFetchError):
        _get("/teams/base")


# --- the generated wrappers and their YAML ------------------------------------------------------------------------


def test_the_yaml_lists_every_route_of_the_spec():
    paths = json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8"))
    assert len(paths) == 15 and set(paths) == {e["path"] for e in DOC["endpoints"]}
    assert len(DOC["endpoints"]) == 19 and len({e["short"] for e in DOC["endpoints"]}) == 19
    assert set(paths) == set(rt._ROUTES)


def test_the_family_header_and_every_endpoint_is_documented():
    assert DOC["api"] == "wjbl" and DOC["host"] == BASE and DOC["name_pattern"] == "wjbl_{short}"
    assert DOC["getter_module"] == "sportsdataverse.basketball.wjbl.wjbl_runtime"
    notes = " ".join(DOC["docstring"]["notes"])
    for needle in (
        "robots.txt",
        "Personal data",
        "wehoop",
        "END years",
        "kiyaku",
        "paged by month",
        "Unofficial",
        "HTTP 200",
    ):
        assert needle in notes, needle
    for e in DOC["endpoints"]:
        assert e["parser"].startswith("parse_wjbl_") and e["returns_schema"] == f"native/wjbl/{e['short']}"
        assert (SCHEMA_DIR / f"{e['short']}.yaml").exists()
        assert e["example_args"] is not None


def test_the_schemas_match_the_parsers_and_the_snippet_covers_every_column():
    snippet = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "notes" / "wjbl_descriptions.yaml").read_text(encoding="utf-8")
    )
    for e in DOC["endpoints"]:
        schema = yaml.safe_load((SCHEMA_DIR / f"{e['short']}.yaml").read_text(encoding="utf-8"))
        assert all(c["description"] == "" for c in schema["columns"])  # descriptions live in the snippet only
        assert [c["name"] for c in schema["columns"]] == list(P.SCHEMAS[e["parser"]])
        rows = snippet[f"wjbl_{e['short']}"]
        assert {c["name"] for c in schema["columns"]} == set(rows)
        for name, text in rows.items():
            assert len(text) >= 15 and text.strip().lower() != name, (e["short"], name, text)
        types = {c["name"]: c["type"] for c in schema["columns"]}
        for col, dtype in P.SCHEMAS[e["parser"]].items():
            assert types[col] == {pl.Int64: "integer", pl.Float64: "numeric", pl.String: "character"}[dtype]


def test_the_japanese_vocabulary_is_translated_in_the_descriptions():
    snippet = yaml.safe_load(
        (ROOT / "tools" / "codegen" / "notes" / "wjbl_descriptions.yaml").read_text(encoding="utf-8")
    )
    action = snippet["wjbl_pbp"]["action1"]
    for needle in ("プレイヤーインアウト", "substitution", "リバウンド", "rebound", "シュート", "shot"):
        assert needle in action, needle
    assert (
        "regular season" in snippet["wjbl_schedule"]["season_title"]
        and "Premier" in snippet["wjbl_schedule"]["season_full_name"]
    )
    assert "勝" in snippet["wjbl_team_standings"]["win_lost"] and "wins" in snippet["wjbl_team_standings"]["win_lost"]
    assert all(
        not re.search(r"[ぁ-んァ-ン一-龥]", t) or re.search(r"[A-Za-z]{4}", t)  # Japanese only ever sits beside English
        for rows in snippet.values()
        for t in rows.values()
    )


WRAPPERS = [
    # name, kwargs, fixture, route, wire params
    ("wjbl_leagues", {}, "full/leagues.json", "/leagues", {"target": "play_schedule"}),
    (
        "wjbl_game_months",
        {"season": 2026},
        "captures/play-schedule-months.json",
        "/play-schedule-months",
        {"season": 2026},
    ),
    (
        "wjbl_schedule",
        {"season": 2026, "game_month": "2026-04", "team_id": 13},
        "full/play-schedules@45_2026-04.json",
        "/play-schedules",
        {"season": 2026, "game_month": "2026-04", "team_id": 13},
    ),
    (
        "wjbl_game",
        {"play_schedule_id": GID},
        "full/play-schedule__detail@7987.json",
        "/play-schedule/detail",
        {"play_schedule_id": GID},
    ),
    (
        "wjbl_period_scores",
        {"play_schedule_id": GID},
        "full/play-schedule__detail@7987.json",
        "/play-schedule/detail",
        {"play_schedule_id": GID},
    ),
    (
        "wjbl_game_info",
        {"play_schedule_id": GID},
        "full/play-schedule@7987.json",
        "/play-schedule",
        {"play_schedule_id": GID},
    ),
    ("wjbl_box_score", {"play_schedule_id": GID}, "full/boxscore@7987.json", "/boxscore", {"play_schedule_id": GID}),
    (
        "wjbl_team_box_score",
        {"play_schedule_id": GID},
        "full/boxscore@7987.json",
        "/boxscore",
        {"play_schedule_id": GID},
    ),
    (
        "wjbl_pbp",
        {"play_schedule_id": GID},
        "full/pbp__play-by-play@7987.json",
        "/pbp/play-by-play",
        {"play_schedule_id": GID},
    ),
    (
        "wjbl_team_period_stats",
        {"play_schedule_id": GID},
        "full/pbp__team-stats@7987.json",
        "/pbp/team-stats",
        {"play_schedule_id": GID},
    ),
    (
        "wjbl_player_period_leaders",
        {"play_schedule_id": GID},
        "full/pbp__player-stats@7987.json",
        "/pbp/player-stats",
        {"play_schedule_id": GID},
    ),
    ("wjbl_standing_seasons", {}, "full/seasons__standings.json", "/seasons/standings", {}),
    (
        "wjbl_team_standings",
        {"season_ids": "169"},
        "captures/team-standings.json",
        "/team-standings",
        {"season_ids": "169"},
    ),
    (
        "wjbl_head_to_head",
        {"season_ids": "169"},
        "captures/team-standings.json",
        "/team-standings",
        {"season_ids": "169"},
    ),
    (
        "wjbl_team_ranking",
        {"season_ids": "169,170"},
        "captures/team-ranking.json",
        "/team-ranking",
        {"season_ids": "169,170"},
    ),
    ("wjbl_teams", {}, "full/teams__base.json", "/teams/base", {}),
    (
        "wjbl_roster",
        {"team_id": 13, "season_id": 169},
        "captures/team__roster.json",
        "/team/roster",
        {"team_id": 13, "season_id": 169},
    ),
    (
        "wjbl_staff",
        {"team_id": 13, "season_id": 169},
        "captures/team__roster.json",
        "/team/roster",
        {"team_id": 13, "season_id": 169},
    ),
    ("wjbl_player", {"player_id": 9799}, "captures/player.json", "/player", {"player_id": 9799}),
]


@pytest.fixture
def seen(monkeypatch):
    calls = []

    def fake(fixture):
        data = _body(fixture)
        data = dict(data) if isinstance(data, dict) else {"items": data}

        def _get(url, params=None, **kw):
            calls.append((url, dict(params or {})))
            return {**data, "_request": {}}

        monkeypatch.setattr(api, "_get", _get)
        return calls

    return fake


def test_nineteen_wrappers_are_exported_with_the_parsers():
    names = [n for n in wjbl.__all__ if n.startswith("wjbl_")]
    assert len(names) == 19 and all(callable(getattr(wjbl, n)) for n in names)
    assert sum(n.startswith("parse_wjbl_") for n in wjbl.__all__) == 19
    assert {w[0] for w in WRAPPERS} == set(names)


@pytest.mark.parametrize("name,kwargs,fixture,route,wire", WRAPPERS, ids=[w[0] for w in WRAPPERS])
def test_each_wrapper_sends_its_route_and_wire_params_and_returns_a_frame(name, kwargs, fixture, route, wire, seen):
    calls = seen(fixture)
    df = getattr(wjbl, name)(**kwargs)
    assert isinstance(df, pl.DataFrame) and df.height > 0
    url, params = calls[0]
    assert url == BASE + route and params == wire, (url, params)
    assert not isinstance(getattr(wjbl, name)(**kwargs, return_as_pandas=True), pl.DataFrame)
    raw = getattr(wjbl, name)(**kwargs, return_parsed=False)
    assert isinstance(raw, dict) and "_request" in raw


def test_a_caller_can_override_a_wire_param_through_params(seen):
    calls = seen("full/play-schedules@45_2026-04.json")
    wjbl.wjbl_schedule(season=2026, params={"league_id": 20, "season": None})
    assert calls[0][1] == {"season": None, "game_month": None, "team_id": None, "league_id": 20}


def test_required_arguments_come_first_and_the_season_is_documented_as_an_end_year():
    sig = inspect.signature(wjbl.wjbl_schedule)
    assert (
        list(sig.parameters)[:3] == ["season", "game_month", "team_id"]
        and sig.parameters["season"].default is inspect._empty
    )
    doc = inspect.getdoc(wjbl.wjbl_schedule)
    assert "END year" in doc and "NOT the whole season" in doc and "wehoop" in doc
    assert inspect.signature(wjbl.wjbl_pbp).parameters["play_schedule_id"].default is inspect._empty


def test_the_wrappers_use_the_wjbl_runtime_getter():
    assert api._get is rt._get


def test_the_fixture_readme_names_its_source():
    text = (FIX / "README.md").read_text(encoding="utf-8")
    for needle in ("18ee8c3", "api.wjbl.01core.app", "2026-10-12", "2026-10-13", "full/"):
        assert needle in text, needle
    names = {p.name for p in (FIX / "full").glob("*.json")}
    assert len(names) == 10 and all((FIX / "full" / n).stat().st_size < 300_000 for n in names)


def test_no_committed_fixture_carries_a_phone_number_an_email_or_a_blood_type():
    for path in FIX.rglob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert not _EMAIL.search(text) and not _PHONE.search(text), path.name
        assert '"player_blood": ""' not in text and not re.search(r'"player_blood": "[^\[]', text), path.name
