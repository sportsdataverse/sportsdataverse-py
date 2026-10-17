"""Offline tests for the TBF (``tbf``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies of one finished BSL 2026-27 game, the standings, the Tofas
team statistics and the points leaders in ``tests/fixtures/tbf/full/`` (dtypes come from those), and the refs capture
samples in ``tests/fixtures/tbf/captures/``. The runtime is driven through a scripted ``requests`` adapter mounted on
a ``PoliteSession`` subclass, so every refusal is exercised end to end and every attempt is counted. No network, and
nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.tbf as tbf_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.tbf import tbf_parsers as P
from sportsdataverse.basketball.tbf import tbf_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "tbf"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "tbf.yaml"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_tbf  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
GAME = 346279
JSON = {"content-type": "application/json"}
FINAL_SCORE = (90, 89)


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def _stamped(name: str, **req: Any) -> Dict[str, Any]:
    body = _full(name)
    body[R.REQUEST_KEY] = {"match_id": GAME, "home_team_process_id": 265995, "away_team_process_id": 265993, **req}
    return body


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_tbf.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    entry = gen_tbf.ENDPOINTS[short]
    df = gen_tbf._frame(entry)
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[entry[1]]), short
    ids = [c for c in df.columns if c.endswith("_id") and c != "genius_id" or c in ("rank", "person_id")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}
    assert df.schema.get("genius_id", pl.Int64) == pl.Int64


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_tbf_player_box(_stamped("stats"))
    assert box.schema["fg_pct"] == pl.Int64 and box.schema["plus_minus"] == pl.Int64  # '7' / '' strings
    assert box.schema["seconds_played"] == pl.Int64 and box.schema["jersey_number"] == pl.Utf8
    assert box.schema["efficiency"] == pl.Int64 and box.schema["is_starter"] == pl.Boolean
    team = P.parse_tbf_team_box(_stamped("stats"))
    assert team.schema["efficiency"] == pl.Float64 and team.schema["team_rebounds"] == pl.Int64
    stats = P.parse_tbf_team_player_stats(_stamped("team_stats", team_process_id=265995))
    assert stats.schema["points"] == pl.Int64 and stats.schema["fg_pct"] == pl.Float64  # strings on the site
    assert stats.schema["plus_minus"] == pl.Float64 and stats.schema["seconds_per_game"] == pl.Int64
    table = P.parse_tbf_standings(_stamped("standings"))
    assert table.schema["head_to_head_points"] == pl.Int64 and table.schema["team_process_id"] == pl.Int64
    leaders = P.parse_tbf_leaders(_stamped("leaders_points", category="points"))
    assert leaders.schema["value"] == pl.Float64 and leaders.schema["person_id"] == pl.Int64  # strings on the site
    shots = P.parse_tbf_shots(_stamped("shots"))
    assert shots.schema["x"] == pl.Int64 and shots.schema["made"] == pl.Boolean
    hi = P.parse_tbf_highlights(_stamped("summary"))
    assert hi.schema["home_value"] == pl.Utf8 and hi.schema["home_number"] == pl.Float64
    assert hi.filter(pl.col("category") == "TimeLeading")["home_number"].null_count() == 1  # '28:38' is a time


def test_float_ids_are_cast_to_integers_never_stringified():
    header = P.parse_tbf_game_header(_stamped("header"))
    r = header.row(0, named=True)
    assert (r["activity_id"], r["season_id"], r["home_team_process_id"], r["away_team_process_id"]) == (
        22214,
        174,
        265995,
        265993,
    )  # 22214.0 etc. in the body
    assert P._i(22214.0) == 22214 and P._i("22214") == 22214 and P._i(22214.5) is None and P._i(True) is None
    assert P._i(float("nan")) is None and P._s(22214.0) == "22214"


def test_game_header_and_roster():
    r = P.parse_tbf_game_header(_stamped("header")).row(0, named=True)
    assert (r["home_score"], r["away_score"], r["status_id"], r["genius_id"]) == (*FINAL_SCORE, 1, 2913588)
    assert r["season"] == 2027 and str(r["tipoff"]) == "2026-09-25 19:00:00" and r["match_id"] == GAME
    assert r["home_team"] == "TOFAŞ" and r["referee"]
    roster = P.parse_tbf_game_players(_stamped("header"))
    assert set(roster["side"].unique()) == {"home", "away"} and roster.height == len(_full("header")["data"]["players"])
    assert "player_id" not in roster.columns  # the header's playerId is 0 on the sample: not a usable key


def test_box_score_adds_up_to_the_team_totals_the_periods_and_the_final_score():
    box, team = P.parse_tbf_player_box(_stamped("stats")), P.parse_tbf_team_box(_stamped("stats"))
    per = P.parse_tbf_period_scores(_stamped("summary"))
    assert team["side"].to_list() == ["home", "away"] and team["team_process_id"].to_list() == [265995, 265993]
    for side, pts in zip(("home", "away"), FINAL_SCORE):
        mine = box.filter(pl.col("side") == side)
        t = team.filter(pl.col("side") == side)
        assert mine["points"].sum() == pts == t["points"][0]
        assert per.filter(pl.col("side") == side)["points"].sum() == pts
        assert (
            mine["fg_made"].sum() == t["fg_made"][0]
            and mine["rebounds"].sum() + t["team_rebounds"][0] == t["rebounds"][0]
        )
    row = box.filter(pl.col("player_id") == 196413).row(0, named=True)
    assert (row["points"], row["fg_made"], row["fg_attempted"], row["seconds_played"], row["plus_minus"]) == (
        18,
        7,
        14,
        1519,
        7,
    )
    assert box.filter(pl.col("is_starter")).group_by("side").len()["len"].to_list() == [5, 5]


def test_pbp_runs_in_game_order_and_ends_on_the_final_score():
    pbp = P.parse_tbf_pbp(_stamped("flow"))
    nums = pbp["action_number"].to_list()
    # game order is the API's row order; a period-end line can carry a LOWER number than the last play (884 after 885)
    assert nums[:600] == sorted(nums[:600]) and sum(a > b for a, b in zip(nums, nums[1:])) == 1
    last = pbp.row(pbp.height - 1, named=True)
    assert (last["action"], last["action_en"], last["home_score"], last["away_score"]) == (
        "Maç Sonu",
        "game end",
        *FINAL_SCORE,
    )
    assert pbp["action_en"].null_count() == 0  # every action in the sample has a gloss
    assert set(pbp["side"].drop_nulls().unique()) == {"home", "away"}
    assert pbp["home_score"].drop_nulls().is_sorted() and pbp["period"].is_sorted()
    assert pbp.filter(pl.col("player_name").is_null())["side"].null_count() > 0  # game events carry no side


def test_shots_and_game_leaders_and_highlights():
    shots = P.parse_tbf_shots(_stamped("shots"))
    assert shots["x"].min() >= 0 and shots["x"].max() <= 100 and shots["y"].min() >= 0 and shots["y"].max() <= 100
    assert shots.filter(pl.col("made")).height > 0 and shots["shot_player_id"].null_count() == 0
    lead = P.parse_tbf_game_leaders(_stamped("leaders_game"))
    assert lead.group_by("category").len()["len"].max() <= 10 and lead["rank"].min() == 1
    hi = P.parse_tbf_highlights(_stamped("summary"))
    assert hi.filter(pl.col("category") == "Sayi").row(0, named=True)["home_number"] == 90.0


def test_season_tables():
    table = (
        P.parse_tbf_standings(
            _stamped("standings"),
        )
        if False
        else P.parse_tbf_standings(_stamped("standings"))
    )
    assert table["rank"].min() == 1 and table["team_process_id"].is_unique().all()
    assert (table["won"] + table["lost"] == table["played"]).all()
    leaders = P.parse_tbf_leaders(_stamped("leaders_points", category="points"))
    assert leaders.height == 40 and set(leaders["kind"]) == {"total", "average"}
    assert leaders.filter(pl.col("kind") == "total")["rank"].to_list() == list(range(1, 21))
    stats = P.parse_tbf_team_player_stats(_stamped("team_stats", team_process_id=265995))
    assert stats["points_per_game"].null_count() == 0  # the average rows joined by person id
    row = stats.filter(pl.col("person_id") == 196413).row(0, named=True)
    assert (row["games"], row["points"], row["seconds_played"]) == (3, 56, 5120)


def test_personal_data_is_not_in_any_frame():
    banned = re.compile(r"birth|dogum|age$|email|phone|address|photo|image|logo|height_|^age", re.I)
    for short, entry in gen_tbf.ENDPOINTS.items():
        cols = [c for c in gen_tbf._frame(entry).columns if c != "height_cm"]
        assert not [c for c in cols if banned.search(c)], (short, cols)
    roster = P.parse_tbf_team_roster(
        _stamped("header") | {"data": _cap("Team__get-team-active-player-list-by-season-and-league")["data"]}
    )
    assert "birth_day" not in roster.columns and "age" not in roster.columns


# --- parser contract ----------------------------------------------------------------------------

ALL = sorted(set(P.TABLES))


@pytest.mark.parametrize("short", ALL)
@pytest.mark.parametrize("raw", [None, {}, "x", [], {"data": None}, {"data": "x"}, {"data": []}, {"data": {}}, 5])
def test_empty_or_malformed_input_is_a_typed_zero_row_frame(short, raw):
    df = P.parse(short, raw)
    assert df.height == 0 and dict(df.schema) == dict(P.SPECS[short])
    assert pl.concat([df, P.parse(short, raw)]).height == 0  # two empty frames stack


@pytest.mark.parametrize("short", ALL)
def test_public_parsers_follow_the_contract(short):
    fn = getattr(P, f"parse_tbf_{short}")
    assert fn(None).height == 0
    pdf = fn(None, return_as_pandas=True)
    assert list(pdf.columns) == list(P.SPECS[short])
    extra = fn(None, columns=["not_a_column"])
    assert "not_a_column" in extra.columns


def test_return_as_pandas_and_non_empty_concat():
    import pandas as pd

    pdf = P.parse_tbf_pbp(_stamped("flow"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) == P.parse_tbf_pbp(_stamped("flow")).height
    a, b = P.parse_tbf_player_box(_stamped("stats")), P.parse_tbf_player_box({})
    assert pl.concat([a, b]).height == a.height


def test_public_parsers_are_documented():
    assert len(P.__all__) == len(set(P.__all__)) == 24
    for name in P.__all__:
        doc = getattr(P, name).__doc__
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert set(P.__all__) <= set(tbf_pkg.__all__)
    assert all(n.startswith("tbf_") for n in tbf_pkg.__all__ if not n.startswith("parse_"))
    assert len([n for n in tbf_pkg.__all__ if not n.startswith("parse_")]) == len(gen_tbf.ENDPOINTS) == 32


def test_action_glossary_covers_the_vocabulary():
    assert len(P.ACTIONS) == 77 and all(isinstance(v, str) and v for v in P.ACTIONS.values())
    actions = {e["action"] for e in _full("flow")["data"]["events"]}
    assert actions <= set(P.ACTIONS)


# --- YAML + generator ---------------------------------------------------------------------------


def test_yaml_lists_every_spec_route_or_says_why_not():
    spec = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec) == 33
    doc = _yaml()
    wrapped = {e["path"].replace("{player_id}", "{playerId}") for e in doc["endpoints"]}
    assert wrapped <= spec and wrapped | set(gen_tbf.NOT_WRAPPED) == spec
    assert len(doc["endpoints"]) == 32 and len({e["short"] for e in doc["endpoints"]}) == 32
    assert doc["host"] == "https://miniappapi.tbf.org.tr/webapi-service/api"
    assert "nikayazilim" not in YAML_PATH.read_text(encoding="utf-8").replace("nikayazilim.com) serve", "")


def test_descriptions_are_complete_and_schemas_stay_blank():
    notes = yaml.safe_load(gen_tbf.NOTES.read_text(encoding="utf-8"))
    for block, cols in notes.items():
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col.lower(), (block, col)
    for f in (ROOT / "tools" / "codegen" / "schemas" / "native" / "tbf").glob("*.yaml"):
        schema = yaml.safe_load(f.read_text(encoding="utf-8"))
        assert all(not c["description"] for c in schema["columns"]), f.name
        assert sorted(c["name"] for c in schema["columns"]) == sorted(notes[schema["schema"]]), f.name


def test_the_rendered_module_matches_the_yaml():
    from sportsdataverse.basketball.tbf import tbf as api

    assert sorted(api.__all__) == sorted(f"tbf_{e['short']}" for e in _yaml()["endpoints"])
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "nikayazilim.com" in notes and "Turnstile" in notes and "automated" in notes


# --- runtime: a scripted transport -------------------------------------------------------------------


class Site(HTTPAdapter):
    """A scripted TBF API: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []
        self.kwargs: List[Dict[str, Any]] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        self.kwargs.append(kwargs)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = request.url
        resp.request = request
        return resp

    def routes(self) -> List[str]:
        return [urlsplit(c.url).path.replace(R.BASE, "") for c in self.calls]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh no-redirect polite session; the process caches start empty."""

    def install(handler):
        adapter = Site(handler)
        session = R._TbfSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_GAMES", {})
        monkeypatch.setattr(R, "_PREFIX", {})
        return adapter

    return install


def _env(data: Any, **extra: Any) -> Dict[str, Any]:
    return {"data": data, "isSuccess": True, "message": "Success", "problemDetails": None, **extra}


def _paged(body: Dict[str, Any]) -> Dict[str, Any]:
    body = copy.deepcopy(body)
    body.update(count=len(body["data"]), pageNumber=1, pageCount=1, hasPreviousPage=False, hasNextPage=False)
    return body


def _seasons_body(n: int = 20) -> Dict[str, Any]:
    body = _paged(_cap("Season__get-seasons"))
    body["data"] = [dict(body["data"][k % 3], seasonId=200 - k) for k in range(n)]
    body["count"] = n
    return body


def _twenty(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(rows[n % len(rows)], rank=n) for n in range(20)]


#: route -> body, from the committed fixtures (a callable gets the request and returns the full answer).
def _tbf(over: Dict[str, Any] | None = None, *, status: int = 200, headers=None):
    leaders = {
        "sum": _twenty(_cap("Player__get-asist-leaders")["data"]["sum"]),
        "avg": _twenty(_cap("Player__get-asist-leaders")["data"]["avg"]),
    }
    seasons = _seasons_body()
    daily = _cap("Match__get-daily-matches")
    for g in daily["data"]:
        g["totalCount"] = len(g["matches"])
    base: Dict[str, Any] = {
        "/Season/get-seasons": seasons,
        "/League/get-leagues-for-filter": _cap("League__get-leagues-for-filter"),
        "/League/get-leagues-and-seasons-by-prefix": _cap("League__get-leagues-and-seasons-by-prefix"),
        "/League/get-league-weeks": _cap("League__get-league-weeks"),
        "/League/get-standings-table": _full("standings"),
        "/Match/get-all-matches-for-filter": _paged(_cap("Match__get-all-matches-for-filter")),
        "/Match/get-daily-matches": daily,
        "/Match/mac-header": _full("header"),
        "/Match/mac-istatistik": _full("stats"),
        "/Match/game-flow": _full("flow"),
        "/Match/shot-chart": _full("shots"),
        "/Match/mac-ozet": _full("summary"),
        "/Match/mac-leader": _full("leaders_game"),
        "/Player/get-player-list": _paged(_cap("Player__get-player-list")),
        "/Player/get-player-detail-profile-and-career-info/196413": _cap(
            "Player__get-player-detail-profile-and-career-info"
        ),
        "/Player/get-asist-leaders": _env(leaders),
        "/Player/get-point-leaders": _full("leaders_points"),
        "/Team/get-teams-by-leauge": _cap("Team__get-teams-by-leauge"),
        "/Team/get-team-detail-statistic": _full("team_stats"),
        "/Team/get-team-detail-matches-by-season-and-league": _cap(
            "Team__get-team-detail-matches-by-season-and-league"
        ),
        "/Team/get-team-active-player-list-by-season-and-league": _cap(
            "Team__get-team-active-player-list-by-season-and-league"
        ),
        "/Team/get-team-detail-team-staff-by-season-and-league": _cap(
            "Team__get-team-detail-team-staff-by-season-and-league"
        ),
    }
    base.update(over or {})

    def handler(req):
        route = urlsplit(req.url).path.replace(R.BASE, "")
        val = base[route]
        if callable(val):
            return val(req)
        return status, headers or JSON, val if isinstance(val, (str, bytes)) else json.dumps(val)

    return handler


def _daily_for(activity_id: int) -> Dict[str, Any]:
    """The daily capture with every game moved to one league, so the scope check passes."""
    body = _cap("Match__get-daily-matches")
    for g in body["data"]:
        g["totalCount"] = len(g["matches"])
        for m in g["matches"]:
            m["activityId"] = float(activity_id)
    return body


def _call(short: str, **args: Any) -> Any:
    """What a generated wrapper does: build the URL from the YAML, call the runtime, parse."""
    ep = next(e for e in _yaml()["endpoints"] if e["short"] == short)
    url = _yaml()["host"] + ep["path"].format(**args)
    params = {p["query_key"]: args[p["name"]] for p in ep.get("extra_params", []) if p["name"] in args}
    raw = R._get(url, params=params)
    return getattr(P, ep["parser"])(raw)


def _with(route: str, mutate: Callable[[Dict[str, Any]], None], base: Dict[str, Any] | None = None):
    body = copy.deepcopy(base if base is not None else _full(route.split(":")[0]))
    mutate(body)
    return body


def test_game_wrappers_end_to_end_and_the_header_is_fetched_once(site, waits):
    adapter = site(_tbf())
    assert _call("player_box", match_id=GAME).height == 24
    assert _call("team_box", match_id=GAME).height == 2
    assert _call("pbp", match_id=GAME).height == 639
    assert _call("shots", match_id=GAME).height == 123
    assert _call("period_scores", match_id=GAME).height == 8
    assert _call("highlights", match_id=GAME).height == 30
    assert _call("game_leaders", match_id=GAME).height == 55
    assert _call("game_header", match_id=GAME).height == 1 and _call("game_players", match_id=GAME).height == 24
    routes = adapter.routes()
    assert routes.count("/Match/mac-header") == 1 and len(routes) == 8  # one header, then one request per route
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request
    assert {urlsplit(c.url).netloc for c in adapter.calls} == {R.HOST}  # never a vendor origin host


def test_league_wrappers_map_the_end_year_to_the_internal_ids(site, waits):
    adapter = site(_tbf())
    assert _call("weeks", season=2027, competition="bsl").height == 3
    q = parse_qs(urlsplit(adapter.calls[-1].url).query)
    assert q == {"seasonId": ["174"], "leagueId": ["22214"]}  # 2026-2027 -> season 174, activity 22214
    assert adapter.routes() == ["/League/get-leagues-and-seasons-by-prefix", "/League/get-league-weeks"]
    _call("weeks", season=2026, competition="bsl")  # 2025-2026: the cached prefix list is reused
    assert adapter.routes()[2:] == ["/League/get-league-weeks"]
    assert parse_qs(urlsplit(adapter.calls[-1].url).query)["leagueId"] == ["20728"]
    _call("standings", season=2027, competition="bsl", week=3)
    assert parse_qs(urlsplit(adapter.calls[-1].url).query)["hafta"] == ["3"]
    assert len(waits.urls) == len(adapter.calls) == 4


def test_a_year_the_league_list_lacks_is_no_data(site):
    site(_tbf())
    with pytest.raises(NoDataError, match="ending 1999"):
        _call("weeks", season=1999, competition="bsl")


def test_schedule_without_a_week_walks_every_week(site, waits):
    adapter = site(_tbf())
    assert _call("schedule", season=2027, competition="bsl").height == 3 * 3  # 3 weeks x the 3-row capture
    assert adapter.routes().count("/Match/get-all-matches-for-filter") == 3 and len(waits.urls) == len(adapter.calls)
    assert _call("schedule", season=2027, competition="bsl", week=1).height == 3
    assert {c.url.split("WeekFilter=")[1].split("&")[0] for c in adapter.calls[-1:]} == {"1"}


def test_the_other_season_wrappers(site):
    site(_tbf())
    assert _call("seasons").height == 20
    assert _call("leagues", season=2027).height == 3
    assert _call("league_seasons", competition="bsl").height == 3
    assert _call("players", season=2027, competition="bsl").height == 3
    assert _call("teams", season=2027, competition="bsl").height >= 1
    assert _call("leaders_assists", season=2027).height == 40
    assert _call("leaders_points", season=2027).height == 40
    assert _call("team_player_stats", season=2027, team_process_id=265995).height == 11
    assert _call("team_games", season=2027, team_process_id=265995).height == 3
    assert _call("team_roster", season=2027, team_process_id=265995).height == 3
    assert _call("team_staff", season=2027, team_process_id=265995).height == 3
    career = _call("player_career", player_id=196413)
    assert career.height > 0 and set(career["person_id"]) == {196413} and career["season"].null_count() == 0


def test_daily_matches_with_and_without_a_league(site):
    adapter = site(_tbf())
    assert _call("daily_matches", match_date="2026-10-11").height > 0
    assert "ActivityId" not in adapter.calls[-1].url
    adapter = site(_tbf({"/Match/get-daily-matches": _daily_for(22214)}))
    _call("daily_matches", match_date="2026-10-11", season=2027, competition="bsl")
    assert parse_qs(urlsplit(adapter.calls[-1].url).query)["ActivityId"] == ["22214"]
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        _call("daily_matches", match_date="11/10/2026")


# --- refusals ------------------------------------------------------------------------------------


def test_an_unfinished_game_is_a_fetch_failure(site):
    def mutate(b):
        b["data"]["matchStatusId"] = 4

    site(_tbf({"/Match/mac-header": _with("header", mutate)}))
    for short in ("player_box", "game_header", "pbp"):
        with pytest.raises(AssetFetchError, match="not finished"):
            _call(short, match_id=GAME)


def test_a_header_without_a_roster_or_score_is_refused(site):
    def mutate(b):
        b["data"]["players"] = []

    site(_tbf({"/Match/mac-header": _with("header", mutate)}))
    with pytest.raises(AssetFetchError, match="header without"):
        _call("pbp", match_id=GAME)


def test_a_body_for_another_game_is_refused(site):
    def other(b):
        b["data"]["matchId"] = "345014"

    site(_tbf({"/Match/game-flow": _with("flow", other), "/Match/shot-chart": _with("shots", other)}))
    for short in ("pbp", "shots"):
        with pytest.raises(AssetFetchError, match="matchId '345014'"):
            _call(short, match_id=GAME)


@pytest.mark.parametrize("bad", [346279, None])
def test_the_echoed_match_id_must_be_the_string_asked_for(site, bad):
    def mutate(b):
        b["data"]["matchId"] = bad

    site(_tbf({"/Match/mac-ozet": _with("summary", mutate)}))
    with pytest.raises(AssetFetchError, match="out of scope: matchId"):
        _call("period_scores", match_id=GAME)  # an int (or null) is not the string the API echoes


def test_a_box_score_for_another_game_is_refused_by_its_totals_and_roster(site):
    def totals(b):
        b["data"]["homeTeamTotal"]["points"] = 91

    def stranger(b):
        b["data"]["homeTopFive"][0]["playerName"] = "Somebody Else"

    site(_tbf({"/Match/mac-istatistik": _with("stats", totals)}))
    with pytest.raises(AssetFetchError, match="team totals"):
        _call("player_box", match_id=GAME)
    site(_tbf({"/Match/mac-istatistik": _with("stats", stranger)}))
    with pytest.raises(AssetFetchError, match="not on this game's header roster"):
        _call("player_box", match_id=GAME)


def test_play_by_play_must_end_the_game_at_the_final_score(site):
    def cut(b):
        del b["data"]["events"][-1]

    def wrong_score(b):
        b["data"]["events"][-1]["score"] = "91-89"

    site(_tbf({"/Match/game-flow": _with("flow", cut)}))
    with pytest.raises(AssetFetchError, match="game not final"):
        _call("pbp", match_id=GAME)
    site(_tbf({"/Match/game-flow": _with("flow", wrong_score)}))
    with pytest.raises(AssetFetchError, match="final event score"):
        _call("pbp", match_id=GAME)


def test_period_scores_must_sum_to_the_final_score(site):
    def mutate(b):
        b["data"]["awayTeamScore"][0]["score"] += 1

    site(_tbf({"/Match/mac-ozet": _with("summary", mutate)}))
    with pytest.raises(AssetFetchError, match="period scores sum"):
        _call("period_scores", match_id=GAME)


def test_a_finished_game_without_play_by_play_or_shots_is_no_data(site):
    def empty_flow(b):
        b["data"]["events"] = []

    def empty_shots(b):
        b["data"]["shotInfos"] = []

    site(_tbf({"/Match/game-flow": _with("flow", empty_flow), "/Match/shot-chart": _with("shots", empty_shots)}))
    with pytest.raises(NoDataError, match="2025-26"):
        _call("pbp", match_id=GAME)
    with pytest.raises(NoDataError, match="shots"):
        _call("shots", match_id=GAME)
    assert _call("player_box", match_id=GAME).height == 24  # the box score of the same game is fine


def test_leader_rows_by_players_of_another_game_are_refused(site):
    def mutate(b):
        b["data"]["categories"][0]["leaders"][0]["playerName"] = "Somebody Else"

    site(_tbf({"/Match/mac-leader": _with("leaders_game", mutate)}))
    with pytest.raises(AssetFetchError, match="not on this game's header roster"):
        _call("game_leaders", match_id=GAME)


def test_error_envelopes_and_non_envelope_bodies_are_refused(site):
    err = {"data": None, "isSuccess": False, "message": "", "problemDetails": {"type": "SomeError", "title": "x"}}
    site(_tbf({"/Match/mac-header": err}))
    with pytest.raises(AssetFetchError, match="error envelope"):
        _call("game_header", match_id=GAME)
    site(_tbf({"/Match/mac-header": {"hello": "world"}}))
    with pytest.raises(AssetFetchError, match="not the TBF envelope"):
        _call("game_header", match_id=GAME)
    site(_tbf({"/Match/mac-header": b"<html>blocked</html>"}))
    with pytest.raises(AssetFetchError):
        _call("game_header", match_id=GAME)
    gone = {"data": None, "isSuccess": True, "message": "", "problemDetails": {"type": "NotFoundException"}}
    site(_tbf({"/Match/mac-header": gone}))
    with pytest.raises(NoDataError):
        _call("game_header", match_id=GAME)


def test_a_404_is_no_data_and_a_400_a_value_error(site):
    site(_tbf(status=404))
    with pytest.raises(NoDataError):
        _call("game_header", match_id=1)
    site(_tbf(status=400))
    with pytest.raises(ValueError, match="HTTP 400"):
        _call("seasons")


def test_a_redirect_is_never_followed_and_is_a_fetch_failure(site, waits):
    adapter = site(
        _tbf({"/Match/mac-header": lambda req: (302, {"Location": "https://tbfweb.nikayazilim.com/x"}, b"")})
    )
    with pytest.raises(AssetFetchError):
        _call("game_header", match_id=GAME)
    assert len(adapter.calls) == 1  # the Location header was not followed
    assert {urlsplit(c.url).netloc for c in adapter.calls} == {R.HOST}


def test_a_500_is_not_retried_but_429_and_5xx_gateways_are_and_honour_retry_after(site, waits):
    adapter = site(_tbf(status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        _call("seasons")
    assert len(adapter.calls) == 1 and len(waits.urls) == 1
    waits.urls.clear()
    state = {"n": 0}

    def flaky(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_full("header"))

    adapter = site(_tbf({"/Match/mac-header": flaky}))
    assert _call("game_header", match_id=GAME).height == 1
    assert len(adapter.calls) == 3 and len(waits.urls) == 3  # every attempt, retries included, waits its turn
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    adapter = site(_tbf(status=503))
    with pytest.raises(AssetFetchError, match="HTTP 503"):
        _call("seasons")
    assert len(adapter.calls) == R.ATTEMPTS and len(waits.urls) == R.ATTEMPTS


def test_a_connection_failure_names_the_exception_class_only(site, monkeypatch):
    def boom(*a, **k):
        raise requests.ConnectionError("secret-host-detail")

    site(_tbf())
    monkeypatch.setattr(R, "download", boom)
    with pytest.raises(AssetFetchError, match="ConnectionError") as e:
        _call("seasons")
    assert "secret-host-detail" not in str(e.value)


def test_capped_and_paged_lists_that_are_not_whole_are_refused(site):
    def short_seasons(b):
        b["data"] = b["data"][:19]
        b["count"] = 19

    def partial(b):
        b["hasNextPage"] = True

    def short_leaders(b):
        b["data"]["avg"] = b["data"]["avg"][:19]

    site(_tbf({"/Season/get-seasons": _with("x", short_seasons, _seasons_body())}))
    with pytest.raises(AssetFetchError, match="capped list drifted"):
        _call("seasons")
    site(
        _tbf(
            {
                "/Match/get-all-matches-for-filter": _with(
                    "x", partial, _paged(_cap("Match__get-all-matches-for-filter"))
                )
            }
        )
    )
    with pytest.raises(AssetFetchError, match="partial page"):
        _call("schedule", season=2027, competition="bsl", week=1)
    site(_tbf({"/Player/get-point-leaders": _with("x", short_leaders, _full("leaders_points"))}))
    with pytest.raises(AssetFetchError, match="capped list drifted"):
        _call("leaders_points", season=2027)


def test_rows_for_another_league_season_or_team_are_refused(site):
    def other_activity(b):
        b["data"][0]["standings"][0]["faaliyetAltAlanId"] = 20728.0

    def other_season(b):
        b["data"][0]["seasonId"] = 172

    def other_prefix(b):
        b["data"][0]["prefix"] = "tbl"

    def other_team(b):
        b["data"]["maclar"][0]["takimAId"] = 1
        b["data"]["maclar"][0]["takimBId"] = 2

    def other_day(b):
        b["data"][0]["matches"][0]["matchDateOnly"] = "2026-10-10"

    site(_tbf({"/League/get-standings-table": _with("standings", other_activity)}))
    with pytest.raises(AssetFetchError, match="faaliyetAltAlanId"):
        _call("standings", season=2027, competition="bsl")
    site(_tbf({"/Team/get-teams-by-leauge": _with("x", other_season, _cap("Team__get-teams-by-leauge"))}))
    with pytest.raises(AssetFetchError, match="the league's season is 174"):
        _call("teams", season=2027, competition="bsl")
    site(_tbf({"/Player/get-player-list": _with("x", other_prefix, _paged(_cap("Player__get-player-list")))}))
    with pytest.raises(AssetFetchError, match="seasonId/prefix"):
        _call("players", season=2027, competition="bsl")
    site(
        _tbf(
            {
                "/Team/get-team-detail-matches-by-season-and-league": _with(
                    "x", other_team, _cap("Team__get-team-detail-matches-by-season-and-league")
                )
            }
        )
    )
    with pytest.raises(AssetFetchError, match="is not team 265995's"):
        _call("team_games", season=2027, team_process_id=265995)
    daily = _cap("Match__get-daily-matches")
    for g in daily["data"]:
        g["totalCount"] = len(g["matches"])
    site(_tbf({"/Match/get-daily-matches": _with("x", other_day, daily)}))
    with pytest.raises(AssetFetchError, match="matchDateOnly"):
        _call("daily_matches", match_date="2026-10-11")


def test_a_prefix_list_for_another_league_is_refused(site):
    def mutate(b):
        b["data"][0]["url"] = "tbl-2026-2027"

    site(
        _tbf(
            {
                "/League/get-leagues-and-seasons-by-prefix": _with(
                    "x", mutate, _cap("League__get-leagues-and-seasons-by-prefix")
                )
            }
        )
    )
    with pytest.raises(AssetFetchError, match="is not a bsl- league"):
        _call("league_seasons", competition="bsl")


def test_empty_lists_for_an_unknown_id_are_no_data(site):
    empty = _env([])
    site(_tbf({"/Team/get-team-detail-team-staff-by-season-and-league": empty, "/League/get-league-weeks": empty}))
    with pytest.raises(NoDataError):
        _call("team_staff", season=2027, team_process_id=1)
    with pytest.raises(NoDataError):
        _call("weeks", season=2027, competition="bsl")


@pytest.mark.parametrize("bad", ["abc", None, True, 1.5, -3])
def test_a_bad_match_id_is_a_value_error_before_any_request(site, bad):
    adapter = site(_tbf())
    with pytest.raises(ValueError, match="match_id"):
        R._get(f"{R.API}/Match/mac-header", params={"match_id": bad})
    assert adapter.calls == []


def test_a_bad_competition_or_season_is_a_value_error(site):
    adapter = site(_tbf())
    with pytest.raises(ValueError, match="competition"):
        R._get(f"{R.API}/League/get-league-weeks", params={"season": 2027, "competition": "euroleague"})
    with pytest.raises(ValueError, match="season"):
        R._get(f"{R.API}/League/get-league-weeks", params={"season": "next", "competition": "bsl"})
    with pytest.raises(ValueError, match="not a TBF app API URL"):
        R._get("https://tbfapi.nikayazilim.com/webapi-service/api/Match/mac-header", params={"match_id": GAME})
    with pytest.raises(ValueError, match="not a wrapped TBF route"):
        R._get(f"{R.API}/Team/get-team-detail-main", params={})
    assert adapter.calls == []


def test_personal_fields_are_redacted_in_every_body(site):
    def mutate(b):
        b["data"][0]["birthDay"] = "1990-01-01"
        b["data"][0]["age"] = 36

    site(
        _tbf(
            {
                "/Team/get-team-active-player-list-by-season-and-league": _with(
                    "x", mutate, _cap("Team__get-team-active-player-list-by-season-and-league")
                )
            }
        )
    )
    raw = R._get(
        f"{R.API}/Team/get-team-active-player-list-by-season-and-league",
        params={"season": 2027, "team_process_id": 265995},
    )
    assert raw["data"][0]["birthDay"] == "<redacted>" and raw["data"][0]["age"] == "<redacted>"
    assert "1990" not in json.dumps(raw)


def test_the_request_is_stamped_for_the_parsers(site):
    site(_tbf())
    raw = R._get(f"{R.API}/Match/game-flow", params={"match_id": GAME})
    assert raw[R.REQUEST_KEY] == {
        "match_id": GAME,
        "home_team_process_id": 265995,
        "away_team_process_id": 265993,
        "activity_id": 22214,
        "season_id": 174,
    }


def test_the_polite_session_spaces_every_attempt(monkeypatch):
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    session = R._TbfSession()
    session.mount("https://", Site(lambda req: (200, JSON, json.dumps(_env([])))))
    for _ in range(3):
        session.get(f"{R.API}/Season/get-seasons")
    assert len(seen) == 3 and isinstance(R.SESSION, basketball_runtime.PoliteSession)
