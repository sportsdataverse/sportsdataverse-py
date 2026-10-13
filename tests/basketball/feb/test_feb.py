"""Offline tests for the FEB (``feb``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL game bodies of one game (2535725) in
``tests/fixtures/feb/full/`` (dtypes come from those), the refs capture samples of five games and the
schedule page records in ``tests/fixtures/feb/captures/``, and the refs' cut-down HTML pages in
``tests/fixtures/feb/*.html`` (the HTML parser's input). The runtime is driven through a scripted
``requests`` adapter mounted on a ``PoliteSession``, so every refusal is exercised end to end and every
attempt is counted. No network, and nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import base64
import gzip
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.feb as feb_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.feb import feb_parsers as P
from sportsdataverse.basketball.feb import feb_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "feb"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "feb.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "feb"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_feb  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
GAME = 2535725
ALL_PARSERS = {n: getattr(P, f"parse_feb_{n}") for n in P.SPECS}
ID_SUFFIXES = ("_id", "_code")


def _full(name: str) -> Dict[str, Any]:
    gz = FIXTURES / "full" / f"{name}.json.gz"
    text = (
        gzip.open(gz, "rt", encoding="utf-8").read()
        if gz.exists()
        else (FIXTURES / "full" / f"{name}.json").read_text("utf-8")
    )
    body = json.loads(text)
    body[R.REQUEST_KEY] = {"game_id": GAME}
    return body


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_feb.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_feb._frame(gen_feb.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short
    ids = [c for c in df.columns if c.endswith("_id") or c in ("group", "season")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_feb_player_box(_full("BoxScore"))
    assert box.schema["fgp"] == pl.Float64 and box.schema["p3p"] == pl.Float64  # "34,8": decimal comma
    assert box.schema["seconds_played"] == pl.Int64 and box.schema["shirt_number"] == pl.Utf8
    assert box.schema["starter"] == pl.Boolean
    shots = P.parse_feb_shots(_full("ShotChart"))
    assert shots.schema["x"] == pl.Float64 and shots.schema["y"] == pl.Float64
    pbp = P.parse_feb_pbp(_full("KeyFacts"))
    assert pbp.schema["x"] == pl.Float64 and pbp.schema["log_time"] == pl.Datetime("us")
    team = P.parse_feb_team_stats(_full("TeamStats"))
    assert team.schema["fgp"] == pl.Float64 and team.schema["team_code"] == pl.Utf8


def test_decimal_comma_percentages_and_string_ids_are_cast_at_the_boundary():
    box = P.parse_feb_player_box(_full("BoxScore"))
    raw = _full("BoxScore")["BOXSCORE"]["TEAM"][0]["PLAYER"][0]
    row = box.filter(pl.col("team_side") == "home").row(0, named=True)
    assert row["fgp"] == pytest.approx(float(raw["fgp"].replace(",", ".")))
    assert row["player_id"] == int(raw["id"]) and isinstance(row["player_id"], int)
    assert row["seconds_played"] == int(raw["min"]) and row["min_formatted"] == raw["minFormatted"]


def test_box_score_adds_up_to_the_header_and_the_team_totals():
    info = P.parse_feb_game_info(_full("TeamStats")).row(0, named=True)
    box = P.parse_feb_player_box(_full("BoxScore"))
    pts = dict(box.group_by("team_side").agg(pl.col("pts").sum()).iter_rows())
    assert pts == {"home": info["home_score"], "away": info["away_score"]} == {"home": 69, "away": 95}
    totals = P.parse_feb_team_stats(_full("TeamStats"))
    assert totals["team_side"].to_list() == ["home", "away"] and totals["pts"].to_list() == [69, 95]
    assert totals["team_id"].to_list() == [info["home_team_id"], info["away_team_id"]]
    from_box = P.parse_feb_team_stats(_full("BoxScore"))  # box-score totals fill what /TeamStats leaves null
    assert from_box["seconds_played"].null_count() == 0 and totals["seconds_played"].null_count() == 2
    assert from_box["fgm"].to_list() == totals["fgm"].to_list()


def test_game_info_and_period_scores():
    info = P.parse_feb_game_info(_full("TeamStats"))
    assert info.height == 1
    row = info.row(0, named=True)
    assert row["comp_id"] == 115 and row["game_id"] == GAME and row["clock"] == "FINAL" and row["status"] == 3
    assert row["group_label"] == "Único"  # right-padded to 20 characters on the wire
    assert str(row["start_time"]) == "2026-09-26 19:00:00"
    assert row["home_team_code"] == "02014401" and row["home_club_code"] == "020144"  # zero-padded strings
    periods = P.parse_feb_period_scores(_full("TeamStats"))
    assert periods["period"].to_list() == [1, 2, 3, 4]
    assert periods["home_points"].sum() == row["home_score"] and periods["away_points"].sum() == row["away_score"]


def test_pbp_is_oldest_first_and_ends_on_the_final_score():
    pbp = P.parse_feb_pbp(_full("KeyFacts"))
    assert pbp.height == 570 and pbp["play_num"].is_sorted() and pbp["play_num"][0] == 1
    scored = pbp.filter(pl.col("score_home").is_not_null()).tail(1).row(0, named=True)
    assert (scored["score_home"], scored["score_away"]) == (69, 95)
    assert set(pbp["team_side"].drop_nulls()) == {"home", "away"}
    assert set(pbp["action"].drop_nulls()) == {
        "shoot", "fthrow", "rebound", "assist", "recovery", "lose", "blockshot", "foul", "subst", "timeout", "period",
    }  # fmt: skip
    ticks = pbp.filter(pl.col("clock") == "08:34")
    assert ticks["clock_seconds"].unique().to_list() == [514]
    assert pbp["game_id"].unique().to_list() == [GAME]


def test_shots_join_the_pbp_shoot_lines_and_resolve_the_shooter():
    pbp = P.parse_feb_pbp(_full("KeyFacts")).filter(pl.col("action") == "shoot")
    shots = P.parse_feb_shots(_full("ShotChart"))
    assert shots.height == pbp.height == 122
    assert sorted(round(v, 4) for v in shots["x"]) == sorted(round(v, 4) for v in pbp["x"])
    assert shots["made"].sum() == pbp.filter(pl.col("log_param4") == "1").height == 49
    assert set(shots["team_side"]) == {"home", "away"}  # the feed's 0/1 side code, normalised
    assert shots["player_id"].null_count() == 0
    # a shot's resolved player belongs to the shooting side's roster, under that shirt number
    box = P.parse_feb_player_box(_full("BoxScore"))
    assert (
        box.schema["player_id"] == shots.schema["player_id"]
        and box.schema["shirt_number"] == shots.schema["shirt_number"]
    )
    joined = shots.join(box, on=["team_side", "shirt_number"], how="left", suffix="_box")
    assert (joined["player_id"] == joined["player_id_box"]).all()


def test_shot_shooter_is_null_when_the_shirt_number_is_not_unique():
    raw = _full("ShotChart")
    for team in raw["SHOTCHART"]["TEAM"]:
        team["PLAYER"][1]["no"] = team["PLAYER"][0]["no"]
    shots = P.parse_feb_shots(raw)
    dup = {t["PLAYER"][0]["no"] for t in raw["SHOTCHART"]["TEAM"]}
    assert shots.filter(pl.col("shirt_number").is_in(dup))["player_id"].null_count() > 0


def test_leaders_overview_and_roster_ids_are_one_dtype():
    leaders = P.parse_feb_leaders(_full("Ranking"))
    assert set(leaders["category"]) == {"points", "rebounds", "assists", "fouls"}
    assert leaders.filter(pl.col("category") == "points")["rank"].max() == 3
    assert leaders["value"].null_count() == 0
    box = P.parse_feb_player_box(_full("BoxScore"))
    assert leaders["player_id"].is_in(box["player_id"]).all()
    ov = P.parse_feb_overview(_full("overview"))
    assert (
        ov.schema["game_id"] == pl.Int64
        and ov.schema["comp_id"] == pl.Int64
        and ov["period_scores"][0] == "16-16;19-16;10-21;14-14"
    )
    assert str(ov["start_time_utc"][0]) == "2026-10-12 09:00:00"


def test_2020_21_games_are_text_only_and_the_parsers_keep_the_schema():
    old = _cap("KeyFacts__gameId@2171130")
    pbp = P.parse_feb_pbp(old)
    assert pbp.height > 0 and pbp.schema == pl.Schema(P.SPECS["pbp"])
    assert pbp["action"].null_count() == pbp.height and pbp["player_id"].null_count() == pbp.height
    assert pbp["text"].null_count() == 0
    info = P.parse_feb_game_info(old).row(0, named=True)
    assert info["status"] is None and info["home_team_code"] is None
    box = P.parse_feb_player_box(_cap("BoxScore__gameId@2171130"))
    assert box["captain"].null_count() == box.height


def test_schedule_pages():
    sched = P.parse_feb_schedule(_cap("calendario"))
    assert set(sched["season"]) == {2027} and set(sched["group"]) == {1}
    played = sched.filter(pl.col("status") == "played").row(0, named=True)
    assert played["game_id"] == 2535725 and played["home_score"] == 69 and played["date"] is None
    assert str(played["round_date"]) == "2026-09-25"
    todo = sched.filter(pl.col("status") == "scheduled").row(0, named=True)
    assert todo["weekday"] == "Saturday" and str(todo["date"]) == "2026-10-17" and todo["time"] == "19:30"
    results = P.parse_feb_results(_cap("resultados"))
    assert set(results["section"]) == {"results", "next_round"} and set(results["comp_id"]) == {115}
    nxt = results.filter(pl.col("section") == "next_round")
    assert nxt.height > 0 and nxt["home_score"].null_count() == nxt.height
    live = P.parse_feb_standings(_cap("resultados"))
    assert set(live["table"]) == {"live"} and live["win_pct"].max() == 100.0 and live["streak"][0] == 3
    final = P.parse_feb_standings(_cap("resultados@g4_t2025"))
    assert set(final["table"]) == {"final"} and final["played"].null_count() == final.height


def test_sides_ids_and_season_are_normalised_in_every_frame():
    for name, body in (("player_box", "BoxScore"), ("team_stats", "TeamStats"), ("leaders", "Ranking")):
        df = ALL_PARSERS[name](_full(body))
        assert set(df["team_side"]) == {"home", "away"}, name
        assert df.schema["team_id"] == pl.Int64, name


# --- the parser contract: empty, malformed, pandas, columns ---------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", 7, {"HEADER": None}, {"BOXSCORE": {}}, {"games": []}])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(gen_feb.ENDPOINTS))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = gen_feb._frame(gen_feb.ENDPOINTS[short])
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([full, empty]).schema == full.schema


@pytest.mark.parametrize("short", sorted(gen_feb.ENDPOINTS))
def test_return_as_pandas(short):
    entry = gen_feb.ENDPOINTS[short]
    pdf = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert list(pdf.columns) == list(P.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_feb_leaders({}, columns=["game_id", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_no_float_origin_id_strings():
    for short in P.SPECS:
        df = gen_feb._frame(gen_feb.ENDPOINTS[short])
        for c, dt in df.schema.items():
            if dt == pl.Utf8 and (c.endswith("_id") or c in ("shirt_number", "team_code", "club_code")):
                assert not df[c].drop_nulls().str.contains(r"\.0$").any(), (short, c)


# --- the YAML, schemas and generator --------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert spec_paths == {e[0] for e in gen_feb.ENDPOINTS.values()}
    doc = _yaml()
    got = {(e["short"], e["path"]) for e in doc["endpoints"]}
    assert got == {(s, e[1]) for s, e in gen_feb.ENDPOINTS.items()}
    assert len(doc["endpoints"]) == 11 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    assert {e["path"] for e in doc["endpoints"] if e.get("host")} == {"/calendario.aspx", "/resultados.aspx"}


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_yaml_carries_the_legal_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "aviso legal" in notes and "database" in notes and "legal sign-off" in notes
    assert "bearer JWT" in notes and "never prints" in notes and "2020-21" in notes


@pytest.mark.parametrize("short", sorted(gen_feb.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in manual_column_descriptions.yaml


def test_the_description_snippet_covers_every_column_without_filler():
    if not gen_feb.SNIPPET.exists():  # gitignored; the generator writes it
        pytest.skip("run tools/codegen/gen_feb.py first")
    snippet = yaml.safe_load(gen_feb.SNIPPET.read_text(encoding="utf-8"))
    assert set(snippet) == {f"feb_{s}" for s in gen_feb.ENDPOINTS}
    for key, cols in snippet.items():
        assert set(cols) == set(P.SPECS[key[4:]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col, (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    assert "unverified" in gen_feb.LOCAL["x"] and "unverified" in gen_feb.LOCAL["y"]
    assert "assumed" in gen_feb.LOCAL_BY["game_info"]["start_time"]


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert feb_pkg.__all__ and set(P.__all__) <= set(feb_pkg.__all__)


# --- runtime: a scripted transport -------------------------------------------------------------------


def _jwt(
    exp: Optional[float] = None, client: str = "baloncestoenvivoapp", scope: Tuple[str, ...] = ("livestats.api",)
) -> str:
    def seg(d: Dict[str, Any]) -> str:
        return base64.urlsafe_b64encode(json.dumps(d).encode()).rstrip(b"=").decode()

    payload = {"client_id": client, "scope": list(scope), "exp": exp if exp is not None else time.time() + 86400}
    return f"{seg({'alg': 'RS256', 'typ': 'JWT'})}.{seg(payload)}.{seg({'sig': 'x' * 10})}"


def _home(token: str) -> str:
    return f'<html><body><div id="contentToken"><input name="_ctl0:token" value="{token}"/></div></body></html>'


class Site(HTTPAdapter):
    """A scripted FEB: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

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
        resp.url = headers.get("x-final-url") or request.url
        resp.request = request
        return resp

    def api_calls(self) -> List[requests.PreparedRequest]:
        return [c for c in self.calls if "intrafeb" in (c.url or "")]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(R.time, "sleep", lambda s: slept.append(s), raising=False)
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh polite session; the bearer cache starts empty."""
    holder: Dict[str, Any] = {}

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_tok", {"value": None, "exp": 0.0})
        holder["adapter"] = adapter
        return adapter

    return install


JSON = {"content-type": "application/json; charset=utf-8"}
HTML = {"content-type": "text/html; charset=utf-8"}
PROBLEM = {"content-type": "application/problem+json; charset=utf-8"}


def _api_site(
    body_for: Callable[[str], Any],
    token: Optional[str] = None,
    status: int = 200,
    headers: Optional[Dict[str, str]] = None,
):
    tok = token or _jwt()

    def handler(req):
        if req.url.rstrip("/") == "https://baloncestoenvivo.feb.es":
            return 200, HTML, _home(tok)
        body = body_for(req.url)
        return status, headers or JSON, body if isinstance(body, (str, bytes)) else json.dumps(body)

    return handler, tok


def _raw(name: str) -> Dict[str, Any]:
    body = _full(name)
    body.pop(R.REQUEST_KEY)
    return body


def test_game_wrappers_end_to_end_and_the_token_is_fetched_once(site, waits):
    handler, tok = _api_site(
        lambda u: _raw(u.split("/api/v1/")[1].split("/")[0] if "overview" not in u else "overview")
    )
    adapter = site(handler)
    box = feb_pkg.feb_player_box(game_id=GAME)
    assert box.height == 24 and box["game_id"].unique().to_list() == [GAME]
    pbp = feb_pkg.feb_pbp(game_id=GAME)
    assert pbp.height == 570
    assert feb_pkg.feb_overview().height == 1
    assert [c.url for c in adapter.calls].count("https://baloncestoenvivo.feb.es/") == 1  # one token fetch
    assert all(c.headers["Authorization"] == f"Bearer {tok}" for c in adapter.api_calls())
    assert adapter.api_calls()[0].url.endswith(f"/BoxScore/{GAME}")
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request


def test_return_parsed_false_hands_back_the_stamped_envelope(site):
    handler, _ = _api_site(lambda u: _raw("TeamStats"))
    site(handler)
    raw = feb_pkg.feb_team_stats(game_id=GAME, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"game_id": GAME} and set(raw) >= {"HEADER", "TEAMSTATS", "TICKER"}
    assert feb_pkg.parse_feb_period_scores(raw).height == 4


def test_the_token_never_appears_in_any_error(site):
    tok = _jwt()
    handler, _ = _api_site(lambda u: "<html>" + tok + "</html>", token=tok, headers=HTML)
    site(handler)
    with pytest.raises(AssetFetchError) as exc:
        feb_pkg.feb_pbp(game_id=GAME)
    assert tok not in str(exc.value) and "eyJ" not in str(exc.value)
    assert R._redact(f"x {tok} y") == "x <redacted-jwt> y"


def test_401_renews_the_token_once_then_succeeds(site):
    stale, fresh = _jwt(), _jwt(exp=time.time() + 90000)
    state = {"homes": 0}

    def handler(req):
        if req.url.rstrip("/") == "https://baloncestoenvivo.feb.es":
            state["homes"] += 1
            return 200, HTML, _home(stale if state["homes"] == 1 else fresh)
        if req.headers["Authorization"] == f"Bearer {stale}":
            return 401, {}, b""
        return 200, JSON, json.dumps(_raw("TeamStats"))

    adapter = site(handler)
    assert feb_pkg.feb_team_stats(game_id=GAME).height == 2
    assert state["homes"] == 2 and len(adapter.api_calls()) == 2


def test_a_token_the_api_keeps_rejecting_is_a_fetch_failure(site):
    def handler(req):
        if req.url.rstrip("/") == "https://baloncestoenvivo.feb.es":
            return 200, HTML, _home(_jwt())
        return 401, {}, b""

    site(handler)
    with pytest.raises(AssetFetchError, match="rejected the bearer token"):
        feb_pkg.feb_team_stats(game_id=GAME)


@pytest.mark.parametrize(
    "page",
    [
        "<html></html>",
        _home("not-a-jwt"),
        _home(_jwt(client="someotherapp")),
        _home(_jwt(scope=("other.api",))),
        _home(_jwt(exp=time.time() - 10)),
    ],
)
def test_a_bad_token_page_is_refused(site, page):
    site(lambda req: (200, HTML, page))
    with pytest.raises(AssetFetchError):
        feb_pkg.feb_pbp(game_id=GAME)


def test_404_problem_json_is_no_data(site):
    handler, _ = _api_site(
        lambda u: json.dumps({"type": "x", "title": "Not Found", "status": 404, "traceId": "t"}),
        status=404,
        headers=PROBLEM,
    )
    site(handler)
    with pytest.raises(NoDataError):
        feb_pkg.feb_pbp(game_id=1)


def test_400_is_a_value_error_and_other_statuses_are_fetch_failures(site, waits):
    handler, _ = _api_site(lambda u: b"", status=400)
    site(handler)
    with pytest.raises(ValueError, match="HTTP 400"):
        feb_pkg.feb_pbp(game_id=GAME)
    handler, _ = _api_site(lambda u: b"", status=403)
    site(handler)
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        feb_pkg.feb_pbp(game_id=GAME)


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}
    tok = _jwt()

    def handler(req):
        if req.url.rstrip("/") == "https://baloncestoenvivo.feb.es":
            return 200, HTML, _home(tok)
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_raw("Ranking"))

    adapter = site(handler)
    assert feb_pkg.feb_leaders(game_id=GAME).height == 24
    assert len(adapter.api_calls()) == 3 and len(waits.urls) == len(adapter.calls) == 4  # home + three attempts
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    handler, _ = _api_site(lambda u: b"", status=500)
    adapter = site(handler)
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        feb_pkg.feb_shots(game_id=GAME)
    assert len(adapter.api_calls()) == R.ATTEMPTS
    assert len(waits.urls) == R.ATTEMPTS + 1


@pytest.mark.parametrize(
    "body,headers",
    [
        (b"", JSON),
        (b"not json", JSON),
        (json.dumps({"title": "Bad", "status": 500, "traceId": "t"}), JSON),
        (json.dumps([1, 2]), JSON),
        (json.dumps({"HEADER": {}}), JSON),
        (json.dumps({}), JSON),
        ("{}", {"content-type": "text/plain"}),
        ("{}", PROBLEM),
    ],
)
def test_error_envelopes_stubs_and_non_json_bodies_are_refused(site, body, headers):
    handler, _ = _api_site(lambda u: body, headers=headers)
    site(handler)
    with pytest.raises(AssetFetchError):
        feb_pkg.feb_pbp(game_id=GAME)


def test_a_changed_filled_block_set_is_refused(site):
    changed = _raw("BoxScore")
    changed["RANKING"] = {"TEAMS": []}
    handler, _ = _api_site(lambda u: changed)
    site(handler)
    with pytest.raises(AssetFetchError, match="filled blocks"):
        feb_pkg.feb_player_box(game_id=GAME)
    empty = _raw("BoxScore")
    empty["BOXSCORE"] = {}
    handler, _ = _api_site(lambda u: empty)
    site(handler)
    with pytest.raises(AssetFetchError, match="BOXSCORE block is empty"):
        feb_pkg.feb_player_box(game_id=GAME)


def test_a_play_by_play_of_another_game_is_a_wrong_scope_failure_not_an_empty_frame(site):
    handler, _ = _api_site(lambda u: _raw("KeyFacts"))
    site(handler)
    with pytest.raises(AssetFetchError, match="out of scope: a play of game 2535725, asked for 99"):
        feb_pkg.feb_pbp(game_id=99)


def test_the_overview_of_an_off_day_is_no_data(site):
    off = _raw("overview")
    off["OVERVIEW"]["COMPETITIONS"] = []
    handler, _ = _api_site(lambda u: off)
    site(handler)
    with pytest.raises(NoDataError):
        feb_pkg.feb_overview()
    crossed = _raw("overview")
    crossed["OVERVIEW"]["COMPETITIONS"][0]["GAMES"][0]["CompID"] = "999"
    handler, _ = _api_site(lambda u: crossed)
    site(handler)
    with pytest.raises(AssetFetchError, match="outside its competition"):
        feb_pkg.feb_overview()


@pytest.mark.parametrize("bad", ["abc", None, True, 1.5, -3])
def test_a_bad_game_id_is_a_value_error_before_any_request(site, bad):
    adapter = site(lambda req: (500, {}, b""))
    with pytest.raises((ValueError, TypeError)):
        R._get(f"{R.API}/BoxScore/{bad}")
    assert adapter.calls == []


def test_unsupported_kwargs_and_foreign_urls_are_refused(site):
    site(lambda req: (500, {}, b""))
    with pytest.raises(TypeError):
        R._get(f"{R.API}/overview", timeout=3)
    with pytest.raises(ValueError, match="not a FEB route"):
        R._get("https://example.org/BoxScore/1")


# --- runtime: the schedule pages ---------------------------------------------------------------------


def _page_site(page: str, final: Optional[str] = None):
    def handler(req):
        return 200, {**HTML, **({"x-final-url": final} if final else {})}, page

    return handler


def test_the_season_end_year_is_mapped_to_the_sites_start_year(site, waits):
    adapter = site(_page_site(_html("calendario_g1_t2026.html"), "https://baloncestoenvivo.feb.es/calendario/1/2026"))
    sched = feb_pkg.feb_schedule(group=1, season=2027)
    assert "t=2026" in adapter.calls[0].url and "g=1" in adapter.calls[0].url and "season" not in adapter.calls[0].url
    assert set(sched["season"]) == {2027} and sched.height == 4
    adapter = site(_page_site(_html("resultados_g1_t2025.html"), "https://baloncestoenvivo.feb.es/resultados/1/2025"))
    final = feb_pkg.feb_standings(group=1, season=2026)
    assert "t=2025" in adapter.calls[0].url and set(final["season"]) == {2026}
    assert set(final["table"]) == {"final"}
    assert len(waits.urls) == 2


def test_results_and_raw_record(site):
    site(_page_site(_html("resultados_g1_t2026.html"), "https://baloncestoenvivo.feb.es/resultados/1/2026"))
    rec = feb_pkg.feb_results(group=1, season=2027, return_parsed=False)
    assert rec[R.REQUEST_KEY] == {"group": 1, "season": 2027, "season_start": 2026} and rec["comp_id"] == "115"
    assert feb_pkg.parse_feb_results(rec).height > 0


def test_a_page_for_another_season_or_group_is_refused_not_returned_empty(site):
    # the t=2026 page served for a t=2025 request (final URL says 2025: the site ignored the season)
    site(_page_site(_html("calendario_g1_t2026.html"), "https://baloncestoenvivo.feb.es/calendario/1/2025"))
    with pytest.raises(AssetFetchError, match="out of scope"):
        feb_pkg.feb_schedule(group=1, season=2026)
    site(_page_site(_html("calendario_g1_t2026.html"), "https://baloncestoenvivo.feb.es/calendario/4/2026"))
    with pytest.raises(AssetFetchError, match="out of scope"):
        feb_pkg.feb_schedule(group=4, season=2027)


def test_an_unknown_group_redirected_to_error_is_no_data(site):
    site(_page_site("<html><body>error</body></html>", "https://baloncestoenvivo.feb.es/error"))
    with pytest.raises(NoDataError, match="no group 999"):
        feb_pkg.feb_schedule(group=999, season=2027)


def test_any_other_final_url_is_a_fetch_failure(site):
    site(_page_site(_html("calendario_g1_t2026.html"), "https://baloncestoenvivo.feb.es/login"))
    with pytest.raises(AssetFetchError, match="final URL"):
        feb_pkg.feb_schedule(group=1, season=2027)


def test_empty_non_html_and_drifted_pages(site):
    final = "https://baloncestoenvivo.feb.es/calendario/1/2026"
    site(lambda req: (200, HTML, b"  "))
    with pytest.raises(AssetFetchError, match="empty body"):
        feb_pkg.feb_schedule(group=1, season=2027)
    site(lambda req: (200, JSON, b"{}"))
    with pytest.raises(AssetFetchError, match="non-HTML"):
        feb_pkg.feb_schedule(group=1, season=2027)
    html = _html("calendario_g1_t2026.html").replace(">Local<", ">Home<")
    site(_page_site(html, final))
    with pytest.raises(AssetFetchError, match="header drift"):
        feb_pkg.feb_schedule(group=1, season=2027)
    site(_page_site("<html><body><p>no header</p></body></html>", final))
    with pytest.raises(AssetFetchError, match="page header"):
        feb_pkg.feb_schedule(group=1, season=2027)


def test_a_right_page_with_no_games_is_no_data(site):
    html = re.sub(r"<table\b[^>]*DataGrid3.*?</table>", "", _html("calendario_g1_t2026.html"), flags=re.S)
    site(_page_site(html, "https://baloncestoenvivo.feb.es/calendario/1/2026"))
    with pytest.raises(NoDataError):
        feb_pkg.feb_schedule(group=1, season=2027)


@pytest.mark.parametrize(
    "params", [{"group": None, "season": 2027}, {"group": "x", "season": 2027}, {"group": 1, "season": 2027.5}]
)
def test_schedule_arguments_are_validated_before_any_request(site, params):
    adapter = site(lambda req: (500, {}, b""))
    with pytest.raises(ValueError):
        R._get(R.SITE + "/calendario.aspx", params)
    assert adapter.calls == []


def test_a_connection_failure_names_the_exception_class_only(site, monkeypatch):
    class Boom(HTTPAdapter):
        def send(self, request, **kwargs):  # type: ignore[no-untyped-def]
            raise requests.ConnectionError(
                "HTTPSConnectionPool(host='baloncestoenvivo.feb.es'): Authorization: Bearer " + _jwt()
            )

    session = basketball_runtime.PoliteSession()
    session.mount("https://", Boom())
    monkeypatch.setattr(R, "SESSION", session)
    monkeypatch.setattr(R, "_tok", {"value": None, "exp": 0.0})
    with pytest.raises(AssetFetchError) as exc:
        feb_pkg.feb_schedule(group=1, season=2027)
    assert "ConnectionError" in str(exc.value) and "eyJ" not in str(exc.value) and "Bearer" not in str(exc.value)


def test_token_from_page_returns_the_expiry():
    exp = time.time() + 5000
    tok, got = R.token_from_page(_home(_jwt(exp=exp)))
    assert JWT_LIKE.fullmatch(tok) and got == pytest.approx(exp)


JWT_LIKE = re.compile(r"eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}")


def test_no_fixture_holds_a_bearer_token():
    for path in FIXTURES.rglob("*"):
        if path.is_file():
            text = gzip.open(path, "rt", encoding="utf-8").read() if path.suffix == ".gz" else path.read_text("utf-8")
            assert not R.JWT_RE.search(text), path
