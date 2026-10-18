"""Offline tests for the BNXT League (``sportpress``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies of one game (11295, 2026-27, double overtime) and
of the season-level routes in ``tests/fixtures/sportpress/full/`` (dtypes come from those: the refs captures
mistype a rate as an integer), and the refs capture samples in ``tests/fixtures/sportpress/captures/``.
The runtime is driven through a scripted ``requests`` adapter mounted on a ``PoliteSession``, so every
refusal is exercised end to end and every attempt is counted. No network, and nothing reads the private
sdv-internal-refs checkout (the spec's routes are snapshotted in ``spec_paths.json``).
"""

from __future__ import annotations

import copy
import gzip
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.bnxt as bnxt_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.bnxt import bnxt_parsers as P
from sportsdataverse.basketball.bnxt import bnxt_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "sportpress"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "sportpress.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "sportpress"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_sportpress  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
GAME, COMP = 11295, 25
BUNDLE_KEY = "A" * 12 + "b" * 12 + "7" * 12  # 36 letters / digits, NOT the documented fallback
ALL_PARSERS = {n: getattr(P, f"parse_sportpress_{n}") for n in P.SPECS}


def _full(name: str) -> Dict[str, Any]:
    gz = FIXTURES / "full" / f"{name}.json.gz"
    text = (
        gzip.open(gz, "rt", encoding="utf-8").read()
        if gz.exists()
        else (FIXTURES / "full" / f"{name}.json").read_text("utf-8")
    )
    return json.loads(text)


def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _stamp(body: Dict[str, Any], **ctx: Any) -> Dict[str, Any]:
    body = copy.deepcopy(body)
    body[R.REQUEST_KEY] = ctx
    return body


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_sportpress.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_sportpress._frame(gen_sportpress.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == {n: d for n, (d, _p) in P.SPECS[short].items()}, short
    ids = [c for c in df.columns if c.endswith("_id") or c in ("season", "genius_id")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


def _capture_names() -> List[str]:
    return sorted(p.stem for p in (FIXTURES / "captures").glob("*.json"))


def _shorts_for(base: str) -> List[str]:
    return [s for s, e in gen_sportpress.ENDPOINTS.items() if e[3][1] == base]


@pytest.mark.parametrize("name", _capture_names())
def test_every_committed_capture_parses_to_its_documented_schema(name):
    base = name.split("@")[0]
    shorts = _shorts_for(base)
    assert shorts, f"no wrapper reads {base}"
    body = _cap(name)
    body = {"data": body} if isinstance(body, list) else body
    for short in shorts:
        df = ALL_PARSERS[short](body)
        assert dict(df.schema) == {n: d for n, (d, _p) in P.SPECS[short].items()}, (name, short)
        assert all(SNAKE.match(c) for c in df.columns)


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_sportpress_game_box(_stamp(_full("boxscore__game__competitionId__gameId"), game_id=GAME))
    # the full box mixes 41.7 and 75 in one percentage column, and the 3-row capture holds whole numbers only
    raw = [
        p["free_throw_percent"] for t in _full("boxscore__game__competitionId__gameId")["data"] for p in t["players"]
    ]
    assert {type(v) for v in raw} == {int, float}
    assert box.schema["free_throw_percent"] == pl.Float64 and box.schema["three_point_percent"] == pl.Float64
    assert box.schema["points"] == pl.Int64 and box.schema["minute"] == pl.Int64 and box.schema["starter"] == pl.Boolean
    assert box.schema["jersey"] == pl.Utf8 and box.schema["birth_date"] == pl.Date
    avg = P.parse_sportpress_team_player_averages(_full("competition-team__player-averages__competitionTeamId"))
    assert avg.schema["points"] == pl.Float64 and avg.schema["minute"] == pl.Float64
    assert avg.schema["played_games"] == pl.Int64 and avg.schema["games_started"] == pl.Int64
    sched = P.parse_sportpress_schedule(_full("schedule__club__season"))
    assert sched.schema["game_time"] == pl.Datetime("us") and sched.schema["home_score"] == pl.Int64
    film = P.parse_sportpress_game_film(_stamp(_full("film__game__competitionId__gameId"), game_id=GAME))
    assert film.schema["x"] == pl.Int64 and film.schema["order"] == pl.Int64


def test_the_box_score_adds_up_and_carries_the_request_ids():
    box = P.parse_sportpress_game_box(
        _stamp(_full("boxscore__game__competitionId__gameId"), game_id=GAME, competition_id=COMP)
    )
    assert box.height == 21 and box["game_id"].unique().to_list() == [GAME]
    assert box["competition_id"].unique().to_list() == [COMP]
    pts = dict(box.group_by("team_side").agg(pl.col("points").sum()).iter_rows())
    assert pts == {"home": 113, "away": 106}
    totals = P.parse_sportpress_game_team_box(_stamp(_full("boxscore__game__competitionId__gameId"), game_id=GAME))
    assert dict(zip(totals["team_side"], totals["points"])) == pts
    assert totals["minute"].to_list() == [250, 250]  # 5 players x (40 + 2 x 5)
    assert totals["plusminus"].to_list() == [7, -7]
    assert box["competition_team_id"].n_unique() == 2 and box["roster_id"].dtype == pl.Int64
    assert set(box["position"].drop_nulls()) <= {
        "point_guard",
        "shooting_guard",
        "small_forward",
        "power_forward",
        "center",
    }


def test_game_find_periods_and_overtime():
    find = _stamp(_full("schedule__find__gameId"), game_id=GAME)
    game = P.parse_sportpress_game_find(find).row(0, named=True)
    assert game["game_id"] == GAME and game["period_name"] == "Extra2" and game["status"] == "finished"
    assert (game["home_score"], game["away_score"]) == (113, 106) and game["season"] == 2027
    assert str(game["game_time"]) == "2026-10-10 20:30:00"
    periods = P.parse_sportpress_game_periods(find)
    assert sorted(periods["period"].unique().to_list()) == [1, 2, 3, 4, 5, 6]  # 5 and 6 are the two overtimes
    assert dict(periods.group_by("team_side").agg(pl.col("score").sum()).iter_rows()) == {"home": 113, "away": 106}


def test_film_is_ordered_and_ends_on_the_final_score():
    film = P.parse_sportpress_game_film(
        _stamp(_full("film__game__competitionId__gameId"), game_id=GAME, competition_id=COMP)
    )
    assert film.height == 645 and film["order"].is_sorted() and film["order"].n_unique() == film.height
    assert (film["home_score"].max(), film["away_score"].max()) == (113, 106)
    assert set(film["team_side"].drop_nulls()) == {"home", "away"}
    assert {1000, 1001, 1011, 1101} <= set(film["event_code"].to_list())
    subs = film.filter(pl.col("event_code") == 1011)
    assert subs["player2_id"].null_count() == 0  # the substitution is the only event with a second player
    assert film.filter(pl.col("event_code") != 1011)["player2_id"].null_count() == film.height - subs.height


def test_overview_frames():
    ov = _stamp(_full("schedule__overview__gameId"), game_id=GAME)
    totals = P.parse_sportpress_game_overview(ov)
    assert totals["team_side"].to_list() == ["home", "away"] and totals["score"].to_list() == [113, 106]
    assert totals["points"].to_list() == [113, 106] and totals["game_id"].unique().to_list() == [GAME]
    top = P.parse_sportpress_game_top_performers(ov)
    assert top.height == 2 and top["player_id"].dtype == pl.Int64 and top["points"].dtype == pl.Int64
    leaders = P.parse_sportpress_game_leaders(ov)
    assert (
        set(leaders["stat"]) >= {"points", "total_rebound", "assist", "value"} and leaders["value"].dtype == pl.Float64
    )
    assert leaders.filter(pl.col("stat") == "points").height == 2


def test_slider_carries_the_genius_id_and_the_schedule_does_not():
    slider = P.parse_sportpress_game_slider(_full("schedule__game-slider__competitionId"))
    assert slider.height >= 14 and slider["genius_id"].dtype == pl.Int64 and slider["genius_id"].null_count() == 0
    assert "genius_id" not in P.parse_sportpress_schedule(_full("schedule__club__season")).columns
    assert set(slider["status"]) <= {"finished", "ready"}


def test_schedule_and_roster_frames():
    sched = P.parse_sportpress_schedule(_full("schedule__club__season"))
    assert sched.height == 24 and sched["game_id"].is_unique().all() and sched["season"].unique().to_list() == [2027]
    roster = P.parse_sportpress_season_roster(_stamp(_full("roster__season-players__season"), season=2027))
    assert roster.height == 30 and roster["season"].unique().to_list() == [2027]
    assert (
        roster["roster_id"].is_unique().all()
        and roster.schema["height"] == pl.Int64
        and roster.schema["jersey"] == pl.Utf8
    )
    assert roster["player_id"].dtype == roster["roster_id"].dtype == roster["competition_team_id"].dtype == pl.Int64


def test_rankings_are_long_with_a_rank_and_the_request_competition():
    body = _stamp(_cap("player__average-rankings__competitionId"), competition_id=24)
    ranks = P.parse_sportpress_player_rankings(body)
    assert ranks["category"].n_unique() == 9 and ranks["competition_id"].unique().to_list() == [24]
    assert ranks.filter(pl.col("category") == "points")["rank"].to_list() == [1, 2, 3]
    pct = ranks.filter(pl.col("category") == "two_point_percent")
    assert (
        pct["throw_made"].null_count() == 0
        and ranks.filter(pl.col("category") == "points")["throw_made"].null_count() == 3
    )
    teams = P.parse_sportpress_team_rankings(
        _stamp(_cap("competition-team__average-rankings__competitionId"), competition_id=24)
    )
    assert teams["category"].n_unique() == 9 and teams["value"].dtype == pl.Float64


def test_standings_columns():
    table = P.parse_sportpress_standings(
        _stamp(_cap("standings__competition__competitionId__phase__phaseId"), competition_id=24, phase_id=169)
    )
    assert table["played"].to_list() == (table["win"] + table["loss"] + table["even"]).to_list()
    assert table.schema["percent"] == pl.Float64 and table.schema["streak_is_win"] == pl.Boolean
    assert table["phase_id"].unique().to_list() == [169]


def test_player_dob_is_kept_and_no_contact_column_exists():
    cols = {c for spec in P.SPECS.values() for c in spec}
    assert "birth_date" in cols
    assert not any(
        re.search(r"(email|phone|facebook|twitter|instagram|address|website|passport|salary)", c) for c in cols
    )
    staff = P.parse_sportpress_competition_staff(_cap("competition-team__all"))
    assert "birth_date" not in staff.columns


def test_ids_are_one_dtype_and_never_float_origin_strings():
    for short in P.SPECS:
        df = gen_sportpress._frame(gen_sportpress.ENDPOINTS[short])
        for col in df.columns:
            if col.endswith("_id") or col in ("season", "rank", "genius_id"):
                assert df.schema[col] == pl.Int64, (short, col)
            if df.schema[col] == pl.Utf8:
                assert not df[col].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, col)
    by_name: Dict[str, set] = {}
    for spec in P.SPECS.values():
        for col, (dtype, _p) in spec.items():
            by_name.setdefault(col, set()).add(dtype)
    assert {c: d for c, d in by_name.items() if len(d) > 1 and c.endswith("_id")} == {}


# --- the parser contract ------------------------------------------------------------------------

BAD_INPUTS = [
    None,
    [],
    {},
    "x",
    5,
    {"data": 5},
    {"data": [1, 2]},
    {"data": {}},
    {"data": [], "_request": {"season": 1}},
]


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", BAD_INPUTS, ids=repr)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == {n: d for n, (d, _p) in P.SPECS[short].items()}


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    assert pl.concat([empty, empty]).height == 0
    full = gen_sportpress._frame(gen_sportpress.ENDPOINTS[short])
    assert pl.concat([empty, full]).height == full.height


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_return_as_pandas(short):
    df = ALL_PARSERS[short](gen_sportpress._fixture(gen_sportpress.ENDPOINTS[short][3]), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) > 0


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_sportpress_club_seasons({}, columns=["season", "extra"])
    assert df.columns == ["season", "extra"] and df.height == 0


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    assert len(P.__all__) == 28
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc
    assert bnxt_pkg.__all__ and set(P.__all__) <= set(bnxt_pkg.__all__)
    assert not [n for n in dir(P) if n.startswith("parse_") and n not in P.__all__]


# --- the YAML, the schemas and the descriptions ---------------------------------------------------


def test_yaml_lists_every_spec_route():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text("utf-8")))
    assert len(spec_paths) == 23
    assert {e[0] for e in gen_sportpress.ENDPOINTS.values()} == spec_paths
    shorts = [e["short"] for e in doc["endpoints"]]
    assert len(shorts) == len(set(shorts)) == 28 and set(shorts) == set(P.SPECS)
    assert doc["host"] == "https://bnxt.sportpress.info/api/v1" and doc["parser_columns"] is True
    seen = {gen_sportpress.ENDPOINTS[e["short"]][0] for e in doc["endpoints"]}
    assert seen == spec_paths  # a wrapper reads every route


def test_no_path_token_named_league_or_sport():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_the_yaml_carries_the_owner_flags_and_the_season_rule():
    notes = " ".join(yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))["docstring"]["notes"])
    for needle in ("Unofficial", "key rotated?", "END year", "genius_id", "18 of 1,246", "dirty", "not finished"):
        assert needle in notes, needle


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert all(c.get("description", "") == "" for c in schema["columns"])


def test_the_description_notes_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_sportpress.NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"sportpress_{s}" for s in gen_sportpress.ENDPOINTS}
    for key, cols in notes.items():
        assert set(cols) == set(P.SPECS[key[len("sportpress_") :]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col and not gen_sportpress._BANNED.match(text), (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    film = yaml.safe_load(gen_sportpress.NOTES.read_text(encoding="utf-8"))["sportpress_game_film"]
    assert "unverified" in film["x"] and "unverified" in film["y"] and "unverified" in film["event_code"]
    assert "unverified" in yaml.safe_load(gen_sportpress.NOTES.read_text("utf-8"))["sportpress_schedule"]["game_time"]


def test_the_wrapper_module_is_the_rendered_yaml(tmp_path):
    """The committed ``sportpress.py`` is byte-identical to a fresh render of ``sportpress.yaml`` (family only)."""
    import generate
    import spec

    api = spec.load_flat_api(YAML_PATH, {})
    out = tmp_path / f"{api.module}.py"
    out.write_text(generate.render_flat_module(api, league_prefix="bnxt"), encoding="utf-8", newline="\n")
    generate._ruff_format_dir(tmp_path)
    live = (ROOT / "sportsdataverse" / "basketball" / "bnxt" / "sportpress.py").read_text(encoding="utf-8")
    assert live == out.read_text(encoding="utf-8")
    assert live.startswith("# GENERATED by tools/codegen/generate.py -- DO NOT EDIT.")
    assert {n for n in re.findall(r"^def (sportpress_\w+)\(", live, re.M)} == {f"sportpress_{s}" for s in P.SPECS}
    assert len(bnxt_pkg.__all__) == 56


# --- runtime: a scripted transport -------------------------------------------------------------------

JSON = {"content-type": "application/json; charset=utf-8"}
HTML = {"content-type": "text/html; charset=utf-8"}
HOME = '<html><head><script src="/js/app.0a1b2c3d.js"></script></head><body></body></html>'
BUNDLE_JS = 'var x=1;headers["X-Authorization"]="%s";var y=2;'


class Site(HTTPAdapter):
    """A scripted bnxtleague.com + Sportpress: ``handler(request)`` returns ``(status, headers, body)``."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = request.url
        resp.request = request
        return resp

    def api_calls(self) -> List[requests.PreparedRequest]:
        return [c for c in self.calls if "sportpress.info" in (c.url or "")]

    def api_paths(self) -> List[str]:
        return [urlsplit(c.url).path.removeprefix("/api/v1") for c in self.api_calls()]


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
    """Mount a Site on a fresh polite session; the key cache and the finished-game cache start empty."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_key", {"value": None, "source": None})
        monkeypatch.setattr(R, "_finished", {})
        return adapter

    return install


def _api(
    routes: Dict[str, Any],
    *,
    key: str = BUNDLE_KEY,
    bundle_key: Optional[str] = None,
    bundle: bool = True,
    status: int = 200,
    headers=None,
):
    """Handler: the site pages, then ``routes[path]`` (a body, or a callable ``(query) -> body``) for the API."""

    def handler(req):
        url = urlsplit(req.url)
        if url.netloc == "bnxtleague.com":
            if not bundle:
                return 500, HTML, b""
            if url.path == "/":
                return 200, HTML, HOME
            return 200, {"content-type": "application/javascript"}, BUNDLE_JS % (bundle_key or key)
        path = url.path.removeprefix("/api/v1")
        if req.headers.get("X-Authorization") != key:
            return 401, JSON, json.dumps({"errors": [{"code": "401", "message": "Unauthorized"}]})
        body = routes[path]
        body = body(parse_qs(url.query)) if callable(body) else body
        return status, headers or JSON, body if isinstance(body, (str, bytes)) else json.dumps(body)

    return handler


def _finished_game(gid: int = GAME, comp: int = COMP, status: str = "finished") -> Dict[str, Any]:
    body = _full("schedule__find__gameId")
    body["data"].update({"id": gid, "status": status})
    body["data"]["competition"]["id"] = comp
    return body


def _game_routes(**over: Any) -> Dict[str, Any]:
    routes = {
        f"/schedule/find/{GAME}": _finished_game(),
        f"/boxscore/game/{COMP}/{GAME}": _full("boxscore__game__competitionId__gameId"),
        f"/film/game/{COMP}/{GAME}": _full("film__game__competitionId__gameId"),
        f"/schedule/overview/{GAME}": _full("schedule__overview__gameId"),
    }
    routes.update(over)
    return routes


def test_wrappers_end_to_end_the_key_is_read_from_the_bundle_once_and_every_attempt_waits(site, waits):
    adapter = site(_api({**_game_routes(), "/schedule/club/2027": _full("schedule__club__season")}))
    sched = bnxt_pkg.sportpress_schedule(season=2027)
    assert sched.height == 24 and sched["season"].unique().to_list() == [2027]
    box = bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME)
    film = bnxt_pkg.sportpress_game_film(competition_id=COMP, game_id=GAME)
    assert box.height == 21 and film.height == 645
    bundle_calls = [c for c in adapter.calls if "bnxtleague.com" in c.url]
    assert [urlsplit(c.url).path for c in bundle_calls] == ["/", "/js/app.0a1b2c3d.js"]  # read once
    assert all(
        c.headers["X-Authorization"] == BUNDLE_KEY and c.headers["X-Localization"] == "en" for c in adapter.api_calls()
    )
    q = parse_qs(urlsplit(adapter.api_calls()[0].url).query)
    assert q == {"clubs[0]": ["1"], "clubs[1]": ["2"], "month": ["-1"]}
    assert adapter.api_paths().count(f"/schedule/find/{GAME}") == 1  # the finished-game gate is cached
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per attempt, bundle reads included
    assert R._key["source"] == "bundle"


def test_return_parsed_false_hands_back_the_stamped_body(site):
    site(_api(_game_routes()))
    raw = bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"competition_id": COMP, "game_id": GAME} and len(raw["data"]) == 2
    assert bnxt_pkg.parse_sportpress_game_team_box(raw).height == 2


def test_the_documented_fallback_key_is_used_when_the_bundle_cannot_be_read(site):
    adapter = site(
        _api({"/competition-team/club/seasons": {"data": [{"season": 2026}]}}, key=R.FALLBACK_KEY, bundle=False)
    )
    assert bnxt_pkg.sportpress_club_seasons()["season"].to_list() == [2026]
    assert R._key["source"] == "fallback" and adapter.api_calls()[0].headers["X-Authorization"] == R.FALLBACK_KEY


@pytest.mark.parametrize(
    "js", ["no key here", 'headers["X-Authorization"]="short"', 'headers["X-Authorization"]="' + "!" * 36 + '"']
)
def test_a_bundle_without_a_valid_key_falls_back(site, js):
    def handler(req):
        if "bnxtleague.com" in req.url:
            return (
                (200, HTML, HOME) if urlsplit(req.url).path == "/" else (200, {"content-type": "text/javascript"}, js)
            )
        ok = req.headers["X-Authorization"] == R.FALLBACK_KEY
        return (200, JSON, json.dumps({"data": [{"season": 2026}]})) if ok else (401, JSON, "{}")

    site(handler)
    assert bnxt_pkg.sportpress_club_seasons().height == 1 and R._key["source"] == "fallback"


def test_a_401_rereads_the_bundle_once_and_succeeds_with_the_rotated_key(site):
    rotated = "Z" * 36
    state = {"reads": 0}

    def handler(req):
        url = urlsplit(req.url)
        if url.netloc == "bnxtleague.com":
            if url.path == "/":
                state["reads"] += 1
                return 200, HTML, HOME
            return (
                200,
                {"content-type": "text/javascript"},
                BUNDLE_JS % (BUNDLE_KEY if state["reads"] == 1 else rotated),
            )
        if req.headers["X-Authorization"] != rotated:
            return 401, JSON, "{}"
        return 200, JSON, json.dumps({"data": [{"season": 2026}]})

    adapter = site(handler)
    assert bnxt_pkg.sportpress_club_seasons().height == 1
    assert state["reads"] == 2 and len(adapter.api_calls()) == 2


def test_a_key_the_api_keeps_rejecting_raises_with_the_rotated_hint_and_never_prints_the_key(site):
    adapter = site(_api({}, key="Q" * 36, bundle_key=BUNDLE_KEY))  # the API wants a key the bundle does not hold
    with pytest.raises(AssetFetchError) as exc:
        bnxt_pkg.sportpress_club_seasons()
    text = str(exc.value)
    assert "key rotated?" in text and "HTTP 401" in text
    assert BUNDLE_KEY not in text and R.FALLBACK_KEY not in text and "Q" * 36 not in text
    assert len(adapter.api_calls()) == 2  # one renewal, then the refusal


def test_the_key_is_scrubbed_from_any_error_text():
    R._key["value"] = BUNDLE_KEY
    try:
        assert R._scrub(f"x {BUNDLE_KEY} y {R.FALLBACK_KEY}") == "x <key> y <key>"
    finally:
        R._key["value"] = None


def test_404_is_no_data_400_is_a_value_error_and_other_statuses_are_fetch_failures(site, waits):
    site(_api({"/competition-team/club/seasons": b""}, status=404))
    with pytest.raises(NoDataError):
        bnxt_pkg.sportpress_club_seasons()
    site(_api({"/competition-team/club/seasons": b""}, status=422))
    with pytest.raises(ValueError, match="HTTP 422"):
        bnxt_pkg.sportpress_club_seasons()
    site(_api({"/competition-team/club/seasons": b""}, status=403))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        bnxt_pkg.sportpress_club_seasons()


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def handler(req):
        if "bnxtleague.com" in req.url:
            return _api({})(req)
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps({"data": [{"season": 2026}]})

    adapter = site(handler)
    assert bnxt_pkg.sportpress_club_seasons().height == 1
    assert len(adapter.api_calls()) == 3 and len(waits.urls) == len(adapter.calls) == 5  # bundle(2) + three attempts
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    adapter = site(_api({"/competition-team/club/seasons": b""}, status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        bnxt_pkg.sportpress_club_seasons()
    assert len(adapter.api_calls()) == R.ATTEMPTS and len(waits.urls) == R.ATTEMPTS + 2


@pytest.mark.parametrize(
    "body,headers",
    [
        (b"", JSON),
        (b"not json", JSON),
        (json.dumps({"errors": [{"code": "500", "message": "Server Error"}]}), JSON),
        (json.dumps({"message": "Server Error", "exception": "X"}), JSON),
        (json.dumps([1, 2]), JSON),
        (json.dumps({"rows": []}), JSON),
        (json.dumps({"data": [], "extra": 1}), JSON),
        (json.dumps({"data": "x"}), JSON),
        (json.dumps({"data": [1]}), JSON),
        ("{}", {"content-type": "text/plain"}),
        ("{}", HTML),
    ],
)
def test_error_envelopes_stubs_and_non_json_bodies_are_refused(site, body, headers):
    site(_api({"/competition-team/club/seasons": body}, headers=headers))
    with pytest.raises(AssetFetchError):
        bnxt_pkg.sportpress_club_seasons()


@pytest.mark.parametrize(
    "short,args,path",
    [
        ("club_seasons", {}, "/competition-team/club/seasons"),
        ("phases", {"season": 2020}, "/phase/season/2020"),
        ("schedule", {"season": 2021}, "/schedule/club/2021"),
        ("standings", {"competition_id": 3, "phase_id": 9}, "/standings/competition/3/phase/9"),
        ("team_roster", {"competition_team_id": 99999}, "/roster/team-players/99999"),
        ("roster_find", {"roster_id": 99999}, "/roster/find/99999"),
        ("player_career", {"roster_id": 99999}, "/player/career-averages/99999"),
        ("nationality_codes", {"season": 2021}, "/roster/season-nationality-codes/2021"),
        ("team_rankings", {"competition_id": 2}, "/competition-team/average-rankings/2"),
    ],
)
def test_an_empty_answer_is_no_data_not_an_empty_frame(site, short, args, path):
    site(_api({path: {"data": []} if short not in ("team_rankings", "nationality_codes") else []}))
    with pytest.raises(NoDataError):
        getattr(bnxt_pkg, f"sportpress_{short}")(**args)


def test_the_season_end_year_is_sent_as_is_and_checked_against_the_body(site):
    body = {
        "data": [{"competition": {"id": 24, "season": 2026, "name": "BNXT League 2025-2026"}, "phases": [{"id": 169}]}]
    }
    adapter = site(_api({"/phase/season/2026": body}))
    out = bnxt_pkg.sportpress_phases(season=2026)
    assert adapter.api_paths() == ["/phase/season/2026"]  # END year 2026 = 2025-26, no START-year shift
    assert out["season"].to_list() == [2026] and out["competition_id"].to_list() == [24]
    site(_api({"/phase/season/2027": body}))
    with pytest.raises(AssetFetchError, match="out of scope.*season 2027"):
        bnxt_pkg.sportpress_phases(season=2027)


def _sched(**edit: Any) -> Dict[str, Any]:
    body = _full("schedule__club__season")
    for k, v in edit.items():
        body["data"][0][k] = v
    return body


@pytest.mark.parametrize(
    "short,args,path,body,needle",
    [
        (
            "competition_teams",
            {"competition_id": 24},
            "/competition-team/all",
            lambda: _cap("competition-team__all@2022"),
            "out of scope",
        ),
        (
            "schedule",
            {"season": 2026},
            "/schedule/club/2026",
            lambda: _full("schedule__club__season"),
            "not of season 2026",
        ),
        (
            "game_slider",
            {"competition_id": 24},
            "/schedule/game-slider/24",
            lambda: _full("schedule__game-slider__competitionId"),
            "not of competition 24",
        ),
        (
            "team_find",
            {"competition_team_id": 5},
            "/competition-team/find/5",
            lambda: _cap("competition-team__find__competitionTeamId@541"),
            "team 541, asked for 5",
        ),
        (
            "roster_find",
            {"roster_id": 5},
            "/roster/find/5",
            lambda: _cap("roster__find__rosterId@220"),
            "roster 220, asked for 5",
        ),
        (
            "team_roster",
            {"competition_team_id": 5},
            "/roster/team-players/5",
            lambda: _cap("roster__team-players__competitionTeamId@541"),
            "not of competition team 5",
        ),
        (
            "team_games",
            {"competition_team_id": 5},
            "/competition-team/statistics-by-game/total/5",
            lambda: _cap("competition-team__statistics-by-game__total__competitionTeamId@541"),
            "not a finished game of team 5",
        ),
        (
            "game_find",
            {"game_id": 5},
            "/schedule/find/5",
            lambda: _full("schedule__find__gameId"),
            "game 11295, asked for 5",
        ),
        (
            "referees",
            {},
            "/staff/list",
            lambda: _cap("competition-team__find__competitionTeamId@541"),
            "not a Sportpress list envelope",
        ),
    ],
)
def test_a_body_for_another_scope_is_a_fetch_failure_not_an_empty_frame(site, short, args, path, body, needle):
    site(_api({path: body()}))
    with pytest.raises(AssetFetchError, match=needle):
        getattr(bnxt_pkg, f"sportpress_{short}")(**args)


def test_the_phase_filter_is_checked_against_the_games(site):
    site(_api({"/schedule/club/2027": _full("schedule__club__season")}))
    with pytest.raises(AssetFetchError, match="not of phase 1"):
        bnxt_pkg.sportpress_schedule(season=2027, phase_id=1)


def test_an_unfinished_game_is_refused_before_its_box_score_is_requested(site):
    adapter = site(_api(_game_routes(**{f"/schedule/find/{GAME}": _finished_game(status="ready")})))
    for fn in (
        lambda: bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME),
        lambda: bnxt_pkg.sportpress_game_film(competition_id=COMP, game_id=GAME),
        lambda: bnxt_pkg.sportpress_game_overview(game_id=GAME),
    ):
        with pytest.raises(AssetFetchError, match="not finished"):
            fn()
    assert all(p.startswith("/schedule/find") for p in adapter.api_paths())  # no box / film / overview request


def test_a_competition_id_that_is_not_the_games_is_out_of_scope(site):
    site(_api(_game_routes(**{f"/boxscore/game/24/{GAME}": _full("boxscore__game__competitionId__gameId")})))
    with pytest.raises(AssetFetchError, match="game 11295 is of competition 25"):
        bnxt_pkg.sportpress_game_box(competition_id=24, game_id=GAME)


def test_a_box_score_for_other_teams_is_out_of_scope(site):
    box = _full("boxscore__game__competitionId__gameId")
    box["data"][0]["team"]["id"] = 1
    site(_api(_game_routes(**{f"/boxscore/game/{COMP}/{GAME}": box})))
    with pytest.raises(AssetFetchError, match="team 1 is not a side of game 11295"):
        bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME)
    box = _full("boxscore__game__competitionId__gameId")
    box["data"][1]["is_home"] = True
    site(_api(_game_routes(**{f"/boxscore/game/{COMP}/{GAME}": box})))
    with pytest.raises(AssetFetchError, match="one home and one away"):
        bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME)


def test_an_empty_box_score_is_no_data(site):
    site(_api(_game_routes(**{f"/boxscore/game/{COMP}/{GAME}": {"data": []}})))
    with pytest.raises(NoDataError):
        bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME)


def test_a_film_that_does_not_end_on_the_final_score_is_refused(site):
    film = _full("film__game__competitionId__gameId")
    film["data"] = film["data"][:200]  # a truncated play-by-play
    site(_api(_game_routes(**{f"/film/game/{COMP}/{GAME}": film})))
    with pytest.raises(AssetFetchError, match="truncated or foreign"):
        bnxt_pkg.sportpress_game_film(competition_id=COMP, game_id=GAME)
    film = _full("film__game__competitionId__gameId")
    film["data"][5]["order"] = 1
    site(_api(_game_routes(**{f"/film/game/{COMP}/{GAME}": film})))
    with pytest.raises(AssetFetchError, match="strictly increasing"):
        bnxt_pkg.sportpress_game_film(competition_id=COMP, game_id=GAME)
    film = _full("film__game__competitionId__gameId")
    film["data"][3]["team"]["id"] = 4
    site(_api(_game_routes(**{f"/film/game/{COMP}/{GAME}": film})))
    with pytest.raises(AssetFetchError, match="outside game 11295"):
        bnxt_pkg.sportpress_game_film(competition_id=COMP, game_id=GAME)


def test_an_overview_for_other_sides_or_scores_is_refused_and_an_empty_one_is_no_data(site):
    ov = _full("schedule__overview__gameId")
    ov["data"]["team_totals"][0]["competitor"]["finalScore"] = 1
    site(_api(_game_routes(**{f"/schedule/overview/{GAME}": ov})))
    with pytest.raises(AssetFetchError, match="final score differs"):
        bnxt_pkg.sportpress_game_overview(game_id=GAME)
    ov = _full("schedule__overview__gameId")
    ov["data"]["top_performer"][1]["competitor"]["competition_team"]["id"] = 3
    site(_api(_game_routes(**{f"/schedule/overview/{GAME}": ov})))
    with pytest.raises(AssetFetchError, match="side / team"):
        bnxt_pkg.sportpress_game_top_performers(game_id=GAME)
    site(
        _api(
            _game_routes(
                **{f"/schedule/overview/{GAME}": {"data": {"team_totals": [], "top_performer": [], "game_leaders": []}}}
            )
        )
    )
    with pytest.raises(NoDataError):
        bnxt_pkg.sportpress_game_overview(game_id=GAME)


@pytest.mark.parametrize(
    "body,needle",
    [
        ({"efficiency": [{"team": {}, "value": 1}]}, "nine ranking categories"),
        ({k: [{"team": {}}] for k in R.RANK_KEYS}, "without"),
        ({k: "x" for k in R.RANK_KEYS}, "not a list"),
    ],
)
def test_rankings_must_be_the_nine_category_object(site, body, needle):
    site(_api({"/competition-team/average-rankings/24": body}))
    with pytest.raises(AssetFetchError, match=needle):
        bnxt_pkg.sportpress_team_rankings(competition_id=24)


def test_a_single_ranking_category_is_accepted_when_one_was_asked_for(site):
    one = {"points": _cap("competition-team__average-rankings__competitionId")["points"]}
    adapter = site(
        _api(
            {
                "/player/average-rankings/24": lambda q: (
                    _cap("player__average-rankings__competitionId")
                    if "type" not in q
                    else {"points": _cap("player__average-rankings__competitionId")["points"]}
                )
            }
        )
    )
    assert bnxt_pkg.sportpress_player_rankings(competition_id=24, type="points")["category"].unique().to_list() == [
        "points"
    ]
    assert parse_qs(urlsplit(adapter.api_calls()[0].url).query)["type"] == ["points"]
    assert one  # the team capture holds the same category


def test_nationality_codes_list_is_wrapped_and_validated(site):
    site(_api({"/roster/season-nationality-codes/2026": _cap("roster__season-nationality-codes__season")}))
    codes = bnxt_pkg.sportpress_nationality_codes(season=2026)
    assert codes["season"].unique().to_list() == [2026] and codes.height == 3
    site(_api({"/roster/season-nationality-codes/2026": [1, 2]}))
    with pytest.raises(AssetFetchError, match="not a list of codes"):
        bnxt_pkg.sportpress_nationality_codes(season=2026)


def test_personal_fields_are_dropped_but_a_players_birth_date_stays(site):
    raw = _full("boxscore__game__competitionId__gameId")
    raw["data"][0]["players"][0]["player"].update({"email": "a@b.c", "phone": "1", "facebook": "f", "twitter": "t"})
    raw["data"][0]["team"]["club"].update({"website": "http://x", "address": "street"})
    raw["data"][0]["staff"] = {"birthdate": "1970-01-01", "name": "Coach", "mobile": "2"}
    site(_api(_game_routes(**{f"/boxscore/game/{COMP}/{GAME}": raw})))
    body = bnxt_pkg.sportpress_game_box(competition_id=COMP, game_id=GAME, return_parsed=False)
    text = json.dumps(body)
    for gone in ("a@b.c", "facebook", "twitter", "phone", "street", "http://x", "1970-01-01", "mobile"):
        assert gone not in text, gone
    assert body["data"][0]["staff"] == {"name": "Coach"}
    assert bnxt_pkg.parse_sportpress_game_box(body)["birth_date"].null_count() < 21  # player DOB kept


def test_the_scrub_keeps_birthdate_under_player2_and_drops_it_elsewhere():
    out = R._scrub_personal(
        {"player2": {"birthdate": "2000-01-01", "e_mail": "x"}, "staff": {"birthdate": "1990-01-01"}}
    )
    assert out == {"player2": {"birthdate": "2000-01-01"}, "staff": {}}


@pytest.mark.parametrize("bad", ["abc", "1.5", None, 1.5, True, ""])
def test_a_bad_id_is_a_value_error_before_any_request(site, bad):
    adapter = site(_api({}))
    with pytest.raises(ValueError):
        R._get(f"{R.API}/schedule/find/{bad}")
    assert adapter.calls == []


def test_unsupported_kwargs_and_foreign_urls_are_refused(site):
    adapter = site(_api({}))
    with pytest.raises(TypeError, match="unsupported"):
        R._get(f"{R.API}/competition-team/club/seasons", None, headers={})
    for url in (
        f"{R.API}/article/list",
        "https://bnxt.sportpress.info/other/schedule/find/1",
        "https://example.com/schedule/find/1",
    ):
        with pytest.raises(ValueError, match="not a Sportpress route"):
            R._get(url)
    assert adapter.calls == []


def test_a_connection_failure_names_the_exception_class_only(monkeypatch, waits):
    class Boom(HTTPAdapter):
        def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
            raise requests.ConnectionError("HTTPSConnectionPool: X-Authorization: " + R.FALLBACK_KEY)

    session = basketball_runtime.PoliteSession()
    session.mount("https://", Boom())
    monkeypatch.setattr(R, "SESSION", session)
    monkeypatch.setattr(R, "_key", {"value": None, "source": None})
    with pytest.raises(AssetFetchError) as exc:
        bnxt_pkg.sportpress_club_seasons()
    assert "ConnectionError" in str(exc.value) and R.FALLBACK_KEY not in str(exc.value)


def test_the_wrappers_send_ids_as_given_and_the_request_is_stamped(site):
    adapter = site(_api({"/roster/team-players/541": _cap("roster__team-players__competitionTeamId@541")}))
    raw = bnxt_pkg.sportpress_team_roster(competition_team_id=541, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"competition_team_id": 541} and adapter.api_paths() == ["/roster/team-players/541"]


def test_no_fixture_holds_the_header_key():
    for path in FIXTURES.rglob("*"):
        if path.is_file():
            text = gzip.open(path, "rt", encoding="utf-8").read() if path.suffix == ".gz" else path.read_text("utf-8")
            assert R.FALLBACK_KEY not in text, path
            assert "X-Authorization" not in text, path


def test_the_runtime_is_on_the_shared_no_redirect_polite_session():
    assert R.SESSION is basketball_runtime.NOREDIRECT_SESSION
    assert isinstance(R.SESSION, basketball_runtime.PoliteSession)
    assert R.RETRY_STATUSES >= {429, 503} and R.ATTEMPTS == 3
    assert Optional  # typing import kept for the helpers above
