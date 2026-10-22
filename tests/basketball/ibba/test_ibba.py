"""Offline tests for the IBBA ASMX (``ibba``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies fetched on 2026-10-12 in ``tests/fixtures/ibba/full/``
(a finished game's box score, a season's teams, a roster, a round of games; dtypes come from those) and the
refs capture samples of every ASMX op in ``tests/fixtures/ibba/captures/asmx/``. The runtime is driven through
a scripted ``requests`` adapter mounted on a ``PoliteSession``, so every refusal is exercised end to end and
every attempt is counted. No network, and nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.ibba as ibba_pkg
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.ibba import ibba_parsers as P
from sportsdataverse.basketball.ibba import ibba_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "ibba"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "ibba.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "ibba"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_ibba  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(P, f"parse_ibba_{n}") for n in P.SPECS}
OP_SHORT = {e[0].lstrip("/"): s for s, e in gen_ibba.ENDPOINTS.items()}


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text("utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / "asmx" / f"{name}.json").read_text("utf-8"))


CAPTURES = sorted(p.stem for p in (FIXTURES / "captures" / "asmx").glob("*.json"))


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("capture", CAPTURES)
def test_every_asmx_capture_parses_to_its_pinned_schema(capture):
    short = OP_SHORT[capture.split("@")[0]]
    df = ALL_PARSERS[short](_cap(capture))
    assert df.height > 0, capture
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), capture
    ids = [c for c in df.columns if (c.endswith("_id") and c != "external_id") or c == "season"]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


@pytest.mark.parametrize("short", sorted(gen_ibba.ENDPOINTS))
def test_every_wrapper_parses_its_generator_fixture(short):
    df = ALL_PARSERS[short](gen_ibba.ENDPOINTS[short][1]())
    assert df.height > 0 and dict(df.schema) == dict(P.SPECS[short]), short


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_ibba_box_score(_full("Boxscore"))
    assert box.schema["fg2_pct"] == pl.Float64 and box.schema["ft_pct"] == pl.Float64
    assert box.schema["pts"] == pl.Int64 and box.schema["minutes"] == pl.Int64 and box.schema["starter"] == pl.Boolean
    assert box.schema["jersey"] == pl.Utf8 and box.schema["player_id"] == pl.Int64
    avg = P.parse_ibba_player_stats(_cap("PlayerStatsById@21912_2026_true_false"))
    tot = P.parse_ibba_player_stats(_cap("PlayerStatsById@21912_2026_false_false"))
    assert avg.schema == tot.schema  # is_averages flips values, never dtypes
    assert avg.schema["pts"] == pl.Float64 and avg.row(0, named=True)["pts"] == pytest.approx(9.75)
    assert tot.row(0, named=True)["fg2_pct"] is None  # the totals row sends empty percentages
    games = P.parse_ibba_games(_full("Games"))
    assert games.schema["game_date"] == pl.Date and games.schema["home_score"] == pl.Int64
    assert games.schema["overtime"] == pl.Boolean and games.schema["tip_off"] == pl.Utf8
    players = P.parse_ibba_players(_full("Players"))
    assert players.schema["height_m"] == pl.Float64 and players.schema["birth_date"] == pl.Date


def test_renames_and_the_foul_direction_are_pinned():
    raw = _full("Boxscore")["boxscore"]
    first = next(r for r in raw if r["player_year_id"] != "0")
    row = (
        P.parse_ibba_box_score(_full("Boxscore"))
        .filter(pl.col("player_id") == int(first["player_year_id"]))
        .row(0, named=True)
    )
    assert row["fouls"] == int(first["fouls_for"]) and row["fouls_drawn"] == int(first["fouls_against"])
    assert (
        row["fg2m"] == int(first["p21"])
        and row["fg2a"] == int(first["p22"])
        and row["turnovers"] == int(first["tournovers"])
    )
    assert row["jersey"] == first["jersy"]


def test_box_score_lines_add_up_to_the_total_line():
    box = P.parse_ibba_box_score(_full("Boxscore"))
    assert set(box["row_type"].unique()) == {"player", "team", "total"}
    assert box.filter(pl.col("row_type") == "total").height == 2
    totals = {r["team_id"]: r["pts"] for r in box.filter(pl.col("row_type") == "total").iter_rows(named=True)}
    summed = dict(box.filter(pl.col("row_type") != "total").group_by("team_id").agg(pl.col("pts").sum()).iter_rows())
    assert summed == totals and sorted(totals.values()) == [92, 104]
    assert (
        box.filter(pl.col("row_type") != "player")["player_id"].null_count()
        == box.filter(pl.col("row_type") != "player").height
    )
    assert box.filter(pl.col("row_type") == "player").height > 0


def test_html_entities_are_unescaped_and_empty_text_is_null():
    games = P.parse_ibba_games(_cap("Games@5_0_0_0_26392_2026"))
    raw = _cap("Games@5_0_0_0_26392_2026")["games"][0]
    assert "&#39;" in raw["PlaceOfGame"] and "'" in games["venue"][0] and "&" not in games["venue"][0]
    assert P.parse_ibba_games(_cap("Games@5_0_0_0_0_2005"))["tip_off"].null_count() == 3  # 2004-05 rows carry no time


def test_contact_details_and_coach_birth_dates_are_dropped():
    for short in ("teams", "team", "coaches"):
        assert not [c for c in P.SPECS[short] if re.search(r"mail|phone|fax|birth", c)], short
    assert "birth_date" in P.SPECS["players"]  # a player's public bio stays


def test_games_derive_the_segev_join_key_and_the_external_flag():
    league = P.parse_ibba_games(_cap("Games@5_0_0_0_26392_2026")).row(0, named=True)
    assert league["segev_game_id"] == 50 and league["external_id"] == "50" and league["is_external"] is False
    ext = P.parse_ibba_games_by_team(_cap("GamesByTeam@10_2026"))
    assert ext["segev_game_id"].null_count() == ext.height  # a news URL is not a Segev id
    assert ext["external_id"][0].startswith("https://") and ext["is_external"].all()
    old = P.parse_ibba_games(_cap("Games@5_0_0_0_0_2005"))
    assert old["segev_game_id"].null_count() == old.height and old["season"].unique().to_list() == [2005]


def test_join_keys_have_one_dtype_and_join_across_routes():
    teams = P.parse_ibba_teams(_full("Teams"))
    box = P.parse_ibba_box_score(_full("Boxscore"))
    games = P.parse_ibba_games(_full("Games"))
    players = P.parse_ibba_players(_full("Players"))
    pairs = [
        (box.schema["team_id"], teams.schema["team_id"]),
        (games.schema["home_team_id"], teams.schema["team_id"]),
        (players.schema["team_id"], teams.schema["team_id"]),
        (box.schema["player_id"], players.schema["player_id"]),
        (box.schema["game_id"], games.schema["game_id"]),
    ]
    assert all(a == b == pl.Int64 for a, b in pairs)
    mac = set(box.filter((pl.col("row_type") == "player") & (pl.col("team_id") == 1109))["player_id"].to_list())
    assert mac and mac <= set(players["player_id"].to_list())  # the roster joins its team's box players
    assert box["team_id"].unique().to_list()[0] in teams["team_id"].to_list()
    assert 26392 in games["game_id"].to_list()


def test_no_float_origin_id_strings():
    for short, parser in ALL_PARSERS.items():
        df = parser(gen_ibba.ENDPOINTS[short][1]())
        for c, dt in df.schema.items():
            if (c.endswith("_id") and c != "external_id") or c == "season":
                assert dt == pl.Int64, (short, c)
            if dt == pl.Utf8:
                assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("junk", [{}, [], None, "x", {"x": 1}, {"boxscore": "x"}, {"games": [1, "a"]}])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, junk):
    df = ALL_PARSERS[short](junk)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_two_empty_frames_concatenate_and_stack_with_a_real_one(short):
    empty = ALL_PARSERS[short]({})
    real = ALL_PARSERS[short](gen_ibba.ENDPOINTS[short][1]())
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, real]).height == real.height


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_return_as_pandas(short):
    pdf = ALL_PARSERS[short](gen_ibba.ENDPOINTS[short][1](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and list(pdf.columns) == list(P.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_ibba_boards({}, columns=["board_id", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc
    assert len(P.__all__) == 16 and set(P.__all__) <= set(ibba_pkg.__all__)


# --- the YAML, schemas and generator --------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert spec_paths == {e[0] for e in gen_ibba.ENDPOINTS.values()}
    doc = _yaml()
    assert {(e["short"], e["path"]) for e in doc["endpoints"]} == {(s, e[0]) for s, e in gen_ibba.ENDPOINTS.items()}
    assert len(doc["endpoints"]) == 16 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_yaml_params_match_the_wsdl_parameter_lists():
    by_short = {e["short"]: e for e in _yaml()["endpoints"]}
    for op, (names, _key) in R.OPS.items():
        short = OP_SHORT[op]
        wire = {p["query_key"] for p in by_short[short].get("extra_params", [])}
        assert wire == set(names), (op, wire, names)
    cyear = [p for e in by_short.values() for p in e.get("extra_params", []) if p["query_key"] == "cYear"]
    assert cyear and all(p["name"] == "season" and p["required"] for p in cyear)


def test_yaml_carries_the_access_season_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "keyless" in notes and "one second apart" in notes and "not redistributed" in notes
    assert "END year" in notes and "cYear" in notes and "per-SEASON" in notes
    assert "2004-05" in notes and "women's league" in notes


@pytest.mark.parametrize("short", sorted(gen_ibba.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in manual_column_descriptions.yaml


def test_the_description_snippet_covers_every_column_without_filler():
    if not gen_ibba.SNIPPET.exists():  # gitignored; the generator writes it
        pytest.skip("run tools/codegen/gen_ibba.py first")
    snippet = yaml.safe_load(gen_ibba.SNIPPET.read_text(encoding="utf-8"))
    assert set(snippet) == {f"ibba_{s}" for s in gen_ibba.ENDPOINTS}
    for key, cols in snippet.items():
        assert set(cols) == set(P.SPECS[key[5:]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col and not gen_ibba.BANNED.match(text), (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    assert "inferred" in gen_ibba.LOCAL["red_line"] and "inferred" in gen_ibba.LOCAL["plus_minus"]
    assert "unverified" in gen_ibba.LOCAL["coach_id"]
    assert "DRAWN" in gen_ibba.LOCAL["fouls_drawn"] and "COMMITTED" in gen_ibba.LOCAL["fouls"]


# --- runtime: a scripted transport -------------------------------------------------------------------

JSON = {"content-type": "application/json; charset=utf-8"}
HTML = {"content-type": "text/html; charset=utf-8"}


class Site(HTTPAdapter):
    """A scripted host: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

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


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh polite session."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        return adapter

    return install


def _serve(body: Any, status: int = 200, headers: Dict[str, str] | None = None):
    payload = body if isinstance(body, (str, bytes)) else json.dumps(body)
    return lambda req: (status, headers or JSON, payload)


def _query(req: requests.PreparedRequest) -> Dict[str, str]:
    from urllib.parse import parse_qsl, urlsplit

    return dict(parse_qsl(urlsplit(req.url).query))


def test_season_is_sent_as_cyear_unchanged_and_the_request_is_stamped(site):
    adapter = site(_serve(_cap("Boards@2026")))
    raw = ibba_pkg.ibba_boards(season=2026, return_parsed=False)
    assert _query(adapter.calls[0]) == {"cYear": "2026"} and adapter.calls[0].url.startswith(R.HOST + "/Boards")
    assert raw[R.REQUEST_KEY] == {"op": "Boards", "params": {"cYear": "2026"}}
    df = ibba_pkg.ibba_boards(season=2026)
    assert df["season"].unique().to_list() == [2026] and df.height == 3


def test_every_wsdl_parameter_is_always_sent_with_0_meaning_all(site):
    adapter = site(_serve(_full("Games")))
    assert ibba_pkg.ibba_games(season=2026, board_id=5, board_round=1).height == 7
    assert _query(adapter.calls[0]) == {
        "board_id": "5", "board_round": "1", "team_id": "0", "team_uid": "0", "game_id": "0", "cYear": "2026",
    }  # fmt: skip
    adapter = site(_serve(_cap("PlayerStatsById@21912_2026_true_false")))
    ibba_pkg.ibba_player_stats(player_id=21912, season=2026, is_averages=True)
    assert _query(adapter.calls[0]) == {
        "player_id": "21912",
        "cYear": "2026",
        "isAverages": "true",
        "isPlayOff": "false",
    }
    adapter = site(_serve(_cap("StatsTypes")))
    assert ibba_pkg.ibba_stats_types().height == 3 and _query(adapter.calls[0]) == {}


def test_a_full_season_box_score_runs_end_to_end(site):
    site(_serve(_full("Boxscore")))
    box = ibba_pkg.ibba_box_score(game_id=26392, season=2026)
    assert box.height == 27 and box["game_id"].unique().to_list() == [26392]


@pytest.mark.parametrize(
    ("handler", "exc", "match"),
    [
        (_serve(b"", 404), NoDataError, "No data found"),
        (_serve(b"", 200, JSON), AssetFetchError, "empty 200 body"),  # EX-SYNC-d: a blank 2xx is a failed fetch
        (_serve({"boxscore": []}), NoDataError, "no rows"),
        (_serve("no connection", 200, HTML), AssetFetchError, "not JSON"),
        (_serve("no connection", 200, JSON), AssetFetchError, "not JSON"),
        (_serve({"error": "x"}), AssetFetchError, "not the Boxscore envelope"),
        (_serve({"boxscore": [], "extra": 1}), AssetFetchError, "not the Boxscore envelope"),
        (_serve({"boxscore": "x"}), AssetFetchError, "list of objects"),
        (_serve(b"", 500), AssetFetchError, "HTTP 500"),
        (_serve(b"", 400), ValueError, "HTTP 400"),
    ],
)
def test_refusals(site, handler, exc, match):
    site(handler)
    with pytest.raises(exc, match=match):
        ibba_pkg.ibba_box_score(game_id=26392, season=2026, return_parsed=False)


def _box_with(**over):
    body = _full("Boxscore")
    for row in body["boxscore"]:
        if row["jersy"] == "TOTAL":
            row.update(over.get(row["team_id"], {}))
    return body


def test_a_wrong_scope_row_is_a_fetch_error_never_an_empty_frame(site):
    other = _full("Boxscore")
    for r in other["boxscore"]:
        r["game_id"] = "26393"
    site(_serve(other))
    with pytest.raises(AssetFetchError, match="out of scope: game_id='26393'"):
        ibba_pkg.ibba_box_score(game_id=26392, season=2026)
    site(_serve(_full("Boxscore")))
    with pytest.raises(AssetFetchError, match="out of scope: year_id='2026'"):
        ibba_pkg.ibba_box_score(game_id=26392, season=2027)
    site(_serve(_cap("Boards@2026")))
    with pytest.raises(AssetFetchError, match="out of scope: year_id"):
        ibba_pkg.ibba_boards(season=2005)
    site(_serve(_full("Games")))
    with pytest.raises(AssetFetchError, match="a game without team 1111"):
        ibba_pkg.ibba_games(season=2026, team_id=1111)
    with pytest.raises(AssetFetchError, match="out of scope: BOARD_ID='5'"):
        ibba_pkg.ibba_games(season=2026, board_id=10)


def test_an_int_echo_is_not_a_match(site):
    body = _cap("Boards@2026")
    for r in body["boards"]:
        r["year_id"] = 2026
    site(_serve(body))
    with pytest.raises(AssetFetchError, match="out of scope"):
        ibba_pkg.ibba_boards(season=2026)


def test_an_unfinished_box_score_is_refused(site):
    live = _box_with(**{"1109": {"minutes": "150"}, "1120": {"minutes": "150"}})
    site(_serve(live))
    with pytest.raises(AssetFetchError, match="not final: TOTAL minutes 150"):
        ibba_pkg.ibba_box_score(game_id=26392, season=2026)
    tied = _box_with(**{"1109": {"pts": "92"}})
    site(_serve(tied))
    with pytest.raises(AssetFetchError, match="tied at"):
        ibba_pkg.ibba_box_score(game_id=26392, season=2026)
    one_team = _full("Boxscore")
    one_team["boxscore"] = [r for r in one_team["boxscore"] if r["team_id"] == "1109"]
    site(_serve(one_team))
    with pytest.raises(AssetFetchError, match="two-team box score"):
        ibba_pkg.ibba_box_score(game_id=26392, season=2026)
    overtime = _box_with(**{"1109": {"minutes": "225"}, "1120": {"minutes": "225"}})
    site(_serve(overtime))
    assert ibba_pkg.ibba_box_score(game_id=26392, season=2026).height == 27  # 200 + 25 x 1 overtime is final


def test_bad_parameters_never_reach_the_host(site):
    adapter = site(_serve(_cap("Boards@2026")))
    with pytest.raises(ValueError, match="non-negative integer"):
        ibba_pkg.ibba_box_score(game_id="abc", season=2026)
    with pytest.raises(ValueError, match="non-negative integer"):
        ibba_pkg.ibba_boards(season=-1)
    with pytest.raises(ValueError, match="must be a bool"):
        ibba_pkg.ibba_player_stats(player_id=1, season=2026, is_averages="yes")
    with pytest.raises(ValueError, match="non-empty string"):
        ibba_pkg.ibba_leaders(stat_type=" ", season=2026)
    with pytest.raises(ValueError, match="needs board_id"):
        ibba_pkg.ibba_teams(season=2026, board_id=None)
    with pytest.raises(TypeError, match="unsupported"):
        ibba_pkg.ibba_boards(season=2026, bogus=1)
    with pytest.raises(ValueError, match="not an IBBA ASMX route"):
        R._get("https://basket.co.il/ws/ws.asmx/News", {})
    with pytest.raises(ValueError, match="not an IBBA ASMX route"):
        R._get("https://example.com/ws/ws.asmx/Boards", {"cYear": 2026})
    assert adapter.calls == []


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def handler(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_cap("Boards@2026"))

    adapter = site(handler)
    assert ibba_pkg.ibba_boards(season=2026).height == 3
    assert len(adapter.calls) == 3 and len(waits.urls) == 3  # one polite wait per attempt, retries included
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_500_is_not_retried_and_a_dead_connection_is_a_fetch_error(site, waits):
    adapter = site(_serve(b"Missing parameter: cYear.", 500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        ibba_pkg.ibba_boards(season=2026)
    assert len(adapter.calls) == 1 and len(waits.urls) == 1

    class Boom(HTTPAdapter):
        n = 0

        def send(self, request, **kwargs):
            Boom.n += 1
            raise requests.ConnectionError("proxy secret=abc")

    session = basketball_runtime.PoliteSession()
    session.mount("https://", Boom())
    import sportsdataverse.basketball.ibba.ibba_runtime as rt

    rt.SESSION = session
    try:
        with pytest.raises(AssetFetchError, match="ConnectionError") as err:
            ibba_pkg.ibba_boards(season=2026)
        assert "secret" not in str(err.value) and Boom.n == R.ATTEMPTS
    finally:
        rt.SESSION = basketball_runtime.SESSION


def test_the_shared_session_is_the_polite_one():
    assert R.SESSION is basketball_runtime.SESSION and isinstance(R.SESSION, basketball_runtime.PoliteSession)
