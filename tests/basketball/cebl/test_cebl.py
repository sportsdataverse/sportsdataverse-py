"""Offline tests for the CEBL (``cebl``) flat-API family: parsers, runtime, YAML and rendered module.

Parsers are asserted against real data: the FULL 2025 bodies in ``tests/fixtures/cebl/full/`` (dtypes come from
those: the refs captures keep 3 rows and cannot show a cancelled game or a league row) and the refs capture
samples in ``tests/fixtures/cebl/captures/``. The runtime is driven through a scripted ``requests`` adapter
mounted on a ``PoliteSession``, so every refusal is exercised end to end and every attempt is counted. No
network, and nothing reads the private sdv-internal-refs checkout (the spec's routes are snapshotted in
``spec_paths.json``).
"""

from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.cebl as cebl_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.cebl import cebl_parsers as P
from sportsdataverse.basketball.cebl import cebl_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "cebl"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "cebl.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "cebl"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_cebl  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
BUNDLE_KEY = "Bk" * 15  # 30 letters / digits, NOT the documented fallback
ALL_PARSERS = {n: getattr(P, f"parse_cebl_{n}") for n in P.SPECS}


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _stamped(body: Any, **ctx: Any) -> Dict[str, Any]:
    return {"rows": copy.deepcopy(body), R.REQUEST_KEY: ctx}


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_cebl.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_cebl._frame(gen_cebl.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0 or short == "news"  # the news feed has never held an article
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == P.SCHEMAS[short]
    ids = [c for c in df.columns if c.endswith("_id") and c not in ("news_id",)] + ["season"] * ("season" in df.columns)
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


def test_full_games_2025_dtypes_and_the_cancelled_game():
    body = _full("games__season__2025")
    df = P.parse_cebl_games(_stamped(body, kind="games", season=2025))
    assert df.height == len(body) == 128
    assert df["season"].unique().to_list() == [2025]
    assert df["game_id"].n_unique() == 128 and df["genius_match_id"].null_count() == 0
    assert df.schema["start_time_utc"] == pl.Datetime("us", "UTC")
    assert sorted(df["status"].unique().to_list()) == ["CANELLED", "COMPLETE"]  # the API's own spelling
    cancelled = df.filter(pl.col("status") == "CANELLED")
    assert cancelled.height == 1 and cancelled["period"].to_list() == [3]
    assert df.filter(pl.col("competition") == "FINALS").height == 7
    # the banked match 2600563 is Edmonton (home, Genius tm.1) v Calgary
    row = df.filter(pl.col("genius_match_id") == 2600563)
    assert row["game_id"].to_list() == [14708701] and row["home_team_id"].to_list() == [18]


def test_genius_match_id_comes_from_the_stats_url_for_every_season():
    for name, want in (("games__season__2019", 1140283), ("games__season__2026", 2798717)):
        assert P.parse_cebl_games(_cap(name))["genius_match_id"][0] == want
    log = P.parse_cebl_player_gamelog(_stamped(_cap("players__player_id__gamelog__570965_2025"), player_id=570965))
    assert log["genius_match_id"][0] == 2600680 and log["player_id"].unique().to_list() == [570965]


def test_full_player_table_is_float_figures_with_int_ids():
    body = _full("players__statistics__aggregated__PER_GAME_2025")
    df = P.parse_cebl_player_stats(_stamped(body, kind="player_stats", season=2025, mode="PER_GAME"))
    assert df.height == 175 and df["player_id"].n_unique() >= 170
    assert df.schema["points"] == pl.Float64 and df.schema["played"] == pl.Int64
    assert df.schema["field_goal_percentage"] == pl.Float64
    assert df["season"].unique().to_list() == [2025] and df["mode"].unique().to_list() == ["PER_GAME"]
    assert df["points"].null_count() == 0 and df["points"].max() < 60  # per-game, not totals


def test_full_team_table_has_exactly_one_league_row():
    body = _full("teams__statistics__aggregated__TOTALS_2025_REGULAR")
    df = P.parse_cebl_team_stats(_stamped(body, kind="team_stats", season=2025, mode="TOTALS", competition="REGULAR"))
    assert df.height == 11 and df["is_league_row"].sum() == 1
    league = df.filter(pl.col("is_league_row") == True)  # noqa: E712
    assert league["team_id"].null_count() == 1 and league["name_en"].null_count() == 1
    assert league["points"][0] == league["points_against"][0]  # TOTALS: every point is scored and allowed once
    assert df.schema["points"] == pl.Float64 and df.schema["win_percentage"] == pl.Float64


def test_full_standings_2025():
    df = P.parse_cebl_standings(_stamped(_full("teams__season__2025"), kind="standings", season=2025))
    assert df.height == 10 and df["season"].unique().to_list() == [2025]
    assert df.schema["win_percentage"] == pl.Float64 and df.schema["games_behind"] == pl.Float64
    assert df.schema["points_difference"] == pl.Int64 and df["team_id"].n_unique() == 10
    assert df["win_percentage"].max() <= 1.0  # a 0-1 fraction, not a percentage


def test_leaders_are_a_long_frame_keyed_by_statistic():
    body = _cap("players__statistics__leaders__PER_GAME_2025")
    df = P.parse_cebl_player_leaders(_stamped(body, kind="leaders", season=2025, mode="PER_GAME"))
    assert df.height == 12 and set(df["statistic"]) == set(body)
    assert df.schema["value"] == pl.Float64 and df.schema["ranking"] == pl.Int64
    assert df["season"].unique().to_list() == [2025]
    assert df.filter(pl.col("statistic") == "POINTS")["ranking"].to_list() == [1, 2, 3]


def test_career_row_is_flagged_and_the_profile_is_one_row():
    seasons = P.parse_cebl_player_seasons(
        _stamped(_cap("players__player_id__statistics__570965_TOTALS_career"), mode="TOTALS")
    )
    assert seasons["is_career"].to_list() == [True] and seasons["season"].null_count() == 1
    assert seasons["points"][0] > 100  # TOTALS
    per = P.parse_cebl_player_seasons(_cap("players__player_id__statistics__570965_PER_GAME"))
    assert per["is_career"].to_list() == [False, False, True]  # the capture keeps the career row last
    player = P.parse_cebl_player(_cap("players__player_id__570965"))
    assert player.height == 1 and player["player_id"][0] == 570965 and player.schema["birth_date"] == pl.Date


def test_an_all_null_profile_is_a_typed_zero_row_frame():
    null = {k: None for k in _cap("players__player_id__570965")}
    df = P.parse_cebl_player(null)
    assert df.height == 0 and dict(df.schema) == P.SCHEMAS["player"]


def test_the_roster_carries_the_request_and_keeps_a_null_jersey():
    body = _cap("teams__team_id__roster__season__14_2025")
    df = P.parse_cebl_roster(_stamped(body, kind="roster", season=2025, team_id=14))
    assert df["team_id"].unique().to_list() == [14] and df["season"].unique().to_list() == [2025]
    assert df.schema["jersey_number"] == pl.Utf8 and df.schema["birth_date"] == pl.Date


def test_gamelog_counts_are_ints_even_when_the_api_sends_strings():
    df = P.parse_cebl_player_gamelog(_cap("players__player_id__gamelog__570965_2025"))
    assert df.schema["rebounds"] == pl.Int64 and df.schema["minutes"] == pl.Float64
    assert df["season"].unique().to_list() == [2025] and df.schema["game_date"] == pl.Datetime("us", "UTC")


def test_news_row_shape_follows_the_upstream_schema():
    row = {
        "id": 7,
        "title": "T",
        "image_url": "u",
        "html_content": "<p>x</p>",
        "published_at_utc": "2026-05-01T12:00:00Z",
    }
    df = P.parse_cebl_news(_stamped([row], kind="news", language="en"))
    assert df.row(0, named=True)["news_id"] == 7 and df["language"][0] == "en"
    assert df.schema["published_at_utc"] == pl.Datetime("us", "UTC")


# --- the parser contract ------------------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", 7, [1, 2], {"rows": []}, _stamped([], kind="x")])
def test_empty_or_malformed_input_is_the_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert df.height == 0 and dict(df.schema) == P.SCHEMAS[short]
    assert pl.concat([df, ALL_PARSERS[short]({})]).height == 0  # two empty frames stack


@pytest.mark.parametrize("short", sorted(gen_cebl.ENDPOINTS))
def test_return_as_pandas(short):
    import pandas as pd

    entry = gen_cebl.ENDPOINTS[short]
    out = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and list(out.columns) == list(P.SCHEMAS[short])


def test_every_public_parser_has_the_docstring_sections():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "::"):
            assert part in doc, (name, part)
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc
    assert set(P.__all__) <= set(cebl_pkg.__all__)
    assert not [n for n in dir(P) if n.startswith("parse_") and n not in P.__all__]


# --- the YAML, the schemas and the descriptions ---------------------------------------------------


def test_yaml_lists_every_spec_route():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text("utf-8")))
    assert len(spec_paths) == 12
    assert {e[0] for e in gen_cebl.ENDPOINTS.values()} == spec_paths
    shorts = [e["short"] for e in doc["endpoints"]]
    assert len(shorts) == len(set(shorts)) == 12 and set(shorts) == set(P.SPECS)
    assert doc["host"] == "https://api.data.cebl.ca" and doc["module"] == "cebl"
    assert {e["path"] for e in doc["endpoints"]} == spec_paths


def test_no_path_token_named_league_or_sport():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_the_yaml_carries_the_owner_flags_the_season_rule_and_the_genius_join():
    notes = " ".join(yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))["docstring"]["notes"])
    for needle in (
        "CEBL own API, owner-approved gray area",
        "key rotated?",
        "END year",
        "genius_match_id",
        "null scores",
        "CANELLED",
    ):
        assert needle in notes, needle


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SCHEMAS[short])
    assert all(c.get("description", "") == "" for c in schema["columns"])


def test_the_description_notes_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_cebl.NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"cebl_{s}" for s in gen_cebl.ENDPOINTS}
    for key, cols in notes.items():
        assert set(cols) == set(P.SCHEMAS[key[len("cebl_") :]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col and not gen_cebl._BANNED.match(text), (key, col)


def test_the_wrapper_module_is_the_rendered_yaml(tmp_path):
    """The committed ``cebl.py`` is byte-identical to a fresh render of ``cebl.yaml`` (family only)."""
    import generate
    import spec

    api = spec.load_flat_api(YAML_PATH, {})
    out = tmp_path / f"{api.module}.py"
    out.write_text(generate.render_flat_module(api, league_prefix="cebl"), encoding="utf-8", newline="\n")
    generate._ruff_format_dir(tmp_path)
    live = (ROOT / "sportsdataverse" / "basketball" / "cebl" / "cebl.py").read_text(encoding="utf-8")
    assert live == out.read_text(encoding="utf-8")
    assert live.startswith("# GENERATED by tools/codegen/generate.py -- DO NOT EDIT.")
    assert set(re.findall(r"^def (cebl_\w+)\(", live, re.M)) == {f"cebl_{s}" for s in P.SPECS}
    assert len(cebl_pkg.__all__) == 24


# --- runtime: a scripted transport -------------------------------------------------------------------

JSON = {"content-type": "application/json; charset=utf-8"}
HTML = {"content-type": "text/html; charset=utf-8"}
CHUNK = "static/chunks/app/league-leaders/page-3d6b0f7141194e04.js"
HOME = f'<html><script src="/_next/static/chunks/main-app-1.js"></script>self.__next_f.push(["{CHUNK}"])</html>'
BUNDLE_JS = 'let r=n.Z.create({baseURL:"https://api.data.cebl.ca/",headers:{"X-Api-Key":"%s"}});'


class Site(HTTPAdapter):
    """A scripted Stats Hub + CEBL API: ``handler(request)`` returns ``(status, headers, body)``."""

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
        return [c for c in self.calls if "api.data.cebl.ca" in (c.url or "")]

    def api_paths(self) -> List[str]:
        return [urlsplit(c.url).path for c in self.api_calls()]


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
    """Mount a Site on a fresh polite session; the key cache starts empty."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_key", {"value": None, "source": None})
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
    """Handler: the hub pages, then ``routes[path]`` (a body, or a callable ``(query) -> body``) for the API."""

    def handler(req):
        url = urlsplit(req.url)
        if url.netloc == "cebl-stats-hub.web.app":
            if not bundle:
                return 500, HTML, b""
            if url.path == "/league-leaders":
                return 200, HTML, HOME
            return 200, {"content-type": "application/javascript"}, BUNDLE_JS % (bundle_key or key)
        if req.headers.get("X-Api-Key") != key:
            return 401, JSON, json.dumps({"detail": "Unauthorized"})
        body = routes[url.path]
        body = body(parse_qs(url.query)) if callable(body) else body
        return status, headers or JSON, body if isinstance(body, (str, bytes)) else json.dumps(body)

    return handler


def test_wrappers_end_to_end_the_key_is_read_from_the_bundle_once_and_every_attempt_waits(site, waits):
    adapter = site(_api({"/games/2025/": _full("games__season__2025"), "/seasons/": _cap("seasons")}))
    games = cebl_pkg.cebl_games(season=2025)
    assert games.height == 128 and R._key["source"] == "bundle" and R._key["value"] == BUNDLE_KEY
    assert cebl_pkg.cebl_seasons().height == 3
    paths = [urlsplit(c.url).path for c in adapter.calls]
    assert paths == ["/league-leaders", f"/_next/{CHUNK}", "/games/2025/", "/seasons/"]  # key read once
    assert len(waits.urls) == len(adapter.calls) == 4  # one polite wait per attempt
    sent = adapter.api_calls()[0].headers
    assert sent["X-Api-Key"] == BUNDLE_KEY and sent["Origin"] == R.HUB


def test_the_documented_fallback_key_is_used_when_the_bundle_cannot_be_read(site):
    adapter = site(_api({"/seasons/": _cap("seasons")}, key=R.FALLBACK_KEY, bundle=False))
    assert cebl_pkg.cebl_seasons().height == 3
    assert R._key["source"] == "fallback" and adapter.api_calls()[0].headers["X-Api-Key"] == R.FALLBACK_KEY


@pytest.mark.parametrize(
    "js", ["no key here", 'headers:{"X-Api-Key":"short"}', 'headers:{"X-Api-Key":"' + "!" * 30 + '"}']
)
def test_a_bundle_without_a_valid_key_falls_back(site, js):
    def handler(req):
        if "cebl-stats-hub" in req.url:
            return (200, HTML, HOME) if req.url.endswith("/league-leaders") else (200, HTML, js)
        return _api({"/seasons/": _cap("seasons")}, key=R.FALLBACK_KEY)(req)

    site(handler)
    assert cebl_pkg.cebl_seasons().height == 3 and R._key["source"] == "fallback"


def test_a_401_rereads_the_bundle_once_and_succeeds_with_the_rotated_key(site):
    rotated = "Z" * 30
    state = {"reads": 0}

    def handler(req):
        url = urlsplit(req.url)
        if url.netloc == "cebl-stats-hub.web.app":
            if url.path == "/league-leaders":
                state["reads"] += 1
                return 200, HTML, HOME
            return (
                200,
                {"content-type": "text/javascript"},
                BUNDLE_JS % (BUNDLE_KEY if state["reads"] == 1 else rotated),
            )
        if req.headers["X-Api-Key"] != rotated:
            return 401, JSON, "{}"
        return 200, JSON, json.dumps(_cap("seasons"))

    adapter = site(handler)
    assert cebl_pkg.cebl_seasons().height == 3
    assert state["reads"] == 2 and len(adapter.api_calls()) == 2


def test_a_key_the_api_keeps_rejecting_raises_with_the_rotated_hint_and_never_prints_the_key(site):
    adapter = site(_api({}, key="Q" * 30, bundle_key=BUNDLE_KEY))  # the API wants a key the bundle does not hold
    with pytest.raises(AssetFetchError) as exc:
        cebl_pkg.cebl_seasons()
    text = str(exc.value)
    assert "key rotated?" in text and "HTTP 401" in text
    assert BUNDLE_KEY not in text and R.FALLBACK_KEY not in text and "Q" * 30 not in text
    # the bundle still holds the key the API just refused: no pointless second try with the same key
    assert len(adapter.api_calls()) == 1


def test_the_key_is_scrubbed_from_any_error_text():
    R._key["value"] = BUNDLE_KEY
    try:
        assert R._scrub(f"x {BUNDLE_KEY} y {R.FALLBACK_KEY}") == "x <key> y <key>"
    finally:
        R._key["value"] = None


def test_404_is_no_data_422_is_a_value_error_and_other_statuses_are_fetch_failures(site):
    site(_api({"/seasons/": b""}, status=404))
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_seasons()
    site(_api({"/seasons/": b""}, status=422))
    with pytest.raises(ValueError, match="HTTP 422"):
        cebl_pkg.cebl_seasons()
    site(_api({"/seasons/": b""}, status=403))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        cebl_pkg.cebl_seasons()


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def handler(req):
        if "cebl-stats-hub" in req.url:
            return _api({})(req)
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_cap("seasons"))

    adapter = site(handler)
    assert cebl_pkg.cebl_seasons().height == 3
    assert len(adapter.api_calls()) == 3 and len(waits.urls) == len(adapter.calls) == 5  # bundle(2) + three attempts
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    adapter = site(_api({"/seasons/": b""}, status=503))
    with pytest.raises(AssetFetchError, match="HTTP 503"):
        cebl_pkg.cebl_seasons()
    assert len(adapter.api_calls()) == R.ATTEMPTS and len(waits.urls) == R.ATTEMPTS + 2


def test_an_http_500_is_not_retried(site):
    adapter = site(_api({"/seasons/": b""}, status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        cebl_pkg.cebl_seasons()
    assert len(adapter.api_calls()) == 1


@pytest.mark.parametrize(
    "body,headers",
    [
        ("<html>nope</html>", {"content-type": "text/html"}),
        ("not json", JSON),
        ("", JSON),
        ('{"detail": "boom"}', JSON),  # an error envelope where a list is expected
        ('{"seasons": []}', JSON),
        ("[1, 2]", JSON),
    ],
)
def test_a_wrong_body_is_a_fetch_failure(site, body, headers):
    site(_api({"/seasons/": body}, headers=headers))
    with pytest.raises(AssetFetchError):
        cebl_pkg.cebl_seasons()


def test_an_empty_list_is_no_data_not_an_empty_frame(site):
    site(_api({"/games/1999/": "[]", "/teams/1999/": "[]"}))
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_games(season=1999)
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_standings(season=1999)


def test_an_all_null_profile_is_no_data_and_another_players_profile_is_refused(site):
    null = {k: None for k in _cap("players__player_id__570965")}
    site(_api({"/players/999999999/": null, "/players/570965/": _cap("players__player_id__530")}))
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_player(player_id=999999999)
    with pytest.raises(AssetFetchError, match="another player"):
        cebl_pkg.cebl_player(player_id=570965)


def test_games_for_another_season_or_team_are_refused(site):
    site(_api({"/games/2026/": _full("games__season__2025")}))
    with pytest.raises(AssetFetchError, match="another season"):
        cebl_pkg.cebl_games(season=2026)
    site(_api({"/games/2025/": _full("games__season__2025")}))
    with pytest.raises(AssetFetchError, match="another team"):
        cebl_pkg.cebl_games(season=2025, team_id=20)  # most 2025 games are not Ottawa's


def test_a_team_filter_is_sent_and_checked(site):
    adapter = site(_api({"/games/2025/": _cap("games__season__2025_team20")}))
    df = cebl_pkg.cebl_games(season=2025, team_id=20)
    assert df.height == 3 and parse_qs(urlsplit(adapter.api_calls()[0].url).query) == {"team_id": ["20"]}


def test_leaders_must_be_keyed_by_the_statistics_asked_for(site):
    body = _cap("players__statistics__leaders__PER_GAME_2025")
    site(_api({"/players/statistics/leaders/": body}))
    df = cebl_pkg.cebl_player_leaders(statistics="POINTS,REBOUNDS,ASSISTS,TARGET_SCORES", mode="PER_GAME", season=2025)
    assert df.height == 12 and df["mode"].unique().to_list() == ["PER_GAME"]
    with pytest.raises(AssetFetchError, match="keyed by the statistics"):
        cebl_pkg.cebl_player_leaders(statistics="POINTS", mode="PER_GAME", season=2025)
    site(_api({"/players/statistics/leaders/": {"POINTS": []}}))
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_player_leaders(statistics="POINTS", mode="PER_GAME", season=1999)


def test_a_player_table_for_another_team_is_refused(site):
    site(_api({"/players/statistics/aggregated/": _cap("players__statistics__aggregated__PER_GAME_2025")}))
    with pytest.raises(AssetFetchError, match="another team"):
        cebl_pkg.cebl_player_stats(mode="PER_GAME", season=2025, team_id=20)


def test_a_team_table_needs_exactly_one_league_row(site):
    body = _full("teams__statistics__aggregated__TOTALS_2025_REGULAR")
    site(_api({"/teams/statistics/aggregated/": body}))
    assert cebl_pkg.cebl_team_stats(mode="TOTALS", season=2025, competition="REGULAR").height == 11
    teams_only = [r for r in body if r["id"] is not None]
    site(_api({"/teams/statistics/aggregated/": teams_only}))
    with pytest.raises(AssetFetchError, match="league row"):
        cebl_pkg.cebl_team_stats(mode="TOTALS", season=2025)


def test_career_only_must_answer_the_career_row_alone(site):
    career = _cap("players__player_id__statistics__570965_TOTALS_career")
    adapter = site(_api({"/players/570965/statistics/": career}))
    df = cebl_pkg.cebl_player_seasons(player_id=570965, mode="TOTALS", career_only=True)
    assert df["is_career"].to_list() == [True]
    assert parse_qs(urlsplit(adapter.api_calls()[0].url).query)["career_only"] == ["true"]
    site(_api({"/players/570965/statistics/": _cap("players__player_id__statistics__570965_PER_GAME")}))
    with pytest.raises(AssetFetchError, match="career row"):
        cebl_pkg.cebl_player_seasons(player_id=570965, mode="TOTALS", career_only=True)
    site(_api({"/players/530/statistics/": _cap("players__player_id__statistics__570965_PER_GAME")}))
    with pytest.raises(AssetFetchError, match="another player"):
        cebl_pkg.cebl_player_seasons(player_id=530, mode="PER_GAME")


def test_a_game_log_for_another_season_is_refused(site):
    site(_api({"/players/570965/gamelog/": _cap("players__player_id__gamelog__570965_2025")}))
    assert cebl_pkg.cebl_player_gamelog(player_id=570965, season=2025).height == 3
    with pytest.raises(AssetFetchError, match="another season"):
        cebl_pkg.cebl_player_gamelog(player_id=570965, season=2026)


def test_the_news_feed_is_no_data_while_it_answers_an_empty_list(site):
    site(_api({"/news/articles/en/": "[]"}))
    with pytest.raises(NoDataError):
        cebl_pkg.cebl_news(language="en")


def test_contact_fields_are_dropped_from_every_body(site):
    body = copy.deepcopy(_cap("teams__team_id__roster__season__14_2025"))
    for r in body:
        r.update({"email": "a@b.c", "phone_number": "1", "home_address": "x"})
    site(_api({"/teams/14/roster/2025/": body}))
    raw = cebl_pkg.cebl_roster(team_id=14, season=2025, return_parsed=False)
    assert all(set(r) == set(body[0]) - {"email", "phone_number", "home_address"} for r in raw["rows"])


def test_arguments_are_checked_before_any_request(site):
    adapter = site(_api({}))
    with pytest.raises(ValueError, match="mode"):
        cebl_pkg.cebl_player_stats(mode="TOTAL")
    with pytest.raises(ValueError, match="competition"):
        cebl_pkg.cebl_player_stats(mode="TOTALS", competition="PLAYOFFS")
    with pytest.raises(ValueError, match="segment"):
        cebl_pkg.cebl_team_stats(mode="TOTALS", segment="JANUARY")
    with pytest.raises(ValueError, match="4-digit"):
        cebl_pkg.cebl_games(season=25)
    with pytest.raises(ValueError, match="not a CEBL route"):
        cebl_pkg.cebl_player(player_id="12a")
    with pytest.raises(TypeError, match="unsupported"):
        cebl_pkg.cebl_seasons(headers={"x": "1"})
    for url in (f"{R.API}/other/", "https://example.com/seasons/", f"{R.API}/seasons"):
        with pytest.raises(ValueError, match="not a CEBL route"):
            R._get(url)
    assert adapter.calls == []


def test_the_request_is_stamped_and_the_season_is_the_end_year_unchanged(site):
    adapter = site(_api({"/teams/2025/": _full("teams__season__2025")}))
    raw = cebl_pkg.cebl_standings(season=2025, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"kind": "standings", "season": 2025}
    assert adapter.api_paths() == ["/teams/2025/"]  # calendar year = END year: no mapping


def test_a_connection_failure_names_the_exception_class_only(monkeypatch, waits):
    class Boom(HTTPAdapter):
        def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
            raise requests.ConnectionError("HTTPSConnectionPool: X-Api-Key: " + R.FALLBACK_KEY)

    session = basketball_runtime.PoliteSession()
    session.mount("https://", Boom())
    monkeypatch.setattr(R, "SESSION", session)
    monkeypatch.setattr(R, "_key", {"value": None, "source": None})
    with pytest.raises(AssetFetchError) as exc:
        cebl_pkg.cebl_seasons()
    assert "ConnectionError" in str(exc.value) and R.FALLBACK_KEY not in str(exc.value)


def test_no_fixture_holds_the_header_key():
    for path in FIXTURES.rglob("*"):
        if path.is_file():
            text = path.read_text("utf-8")
            assert R.FALLBACK_KEY not in text, path
            assert "X-Api-Key" not in text, path


def test_the_runtime_is_on_the_shared_no_redirect_polite_session():
    assert R.SESSION is basketball_runtime.NOREDIRECT_SESSION
    assert isinstance(R.SESSION, basketball_runtime.PoliteSession)
    assert R.RETRY_STATUSES >= {429, 503} and R.ATTEMPTS == 3
    assert 500 not in R.RETRY_STATUSES
