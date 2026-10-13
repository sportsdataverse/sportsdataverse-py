"""Offline tests for the Segev Stats (``ibba_segev``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies of Segev game 50 (and the 2026-27 box score of game 419)
in ``tests/fixtures/ibba/full/`` (dtypes come from those) and the refs capture samples of games 50, 300 and 419
in ``tests/fixtures/ibba/captures/segev/``. The runtime is driven through a scripted ``requests`` adapter
mounted on a ``PoliteSession``. No network, and nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import polars as pl
import pytest
import yaml

import sportsdataverse.basketball.ibba as ibba_pkg
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.ibba import ibba_segev_parsers as P
from sportsdataverse.basketball.ibba import ibba_segev_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

from .test_ibba import HTML, Site, _query, _serve

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "ibba"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "ibba_segev.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "ibba_segev"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_ibba_segev  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(P, f"parse_ibba_segev_{n}") for n in P.SPECS}
CAP = FIXTURES / "captures" / "segev"


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text("utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((CAP / f"{name}.json").read_text("utf-8"))


INFO, BOX, ACTIONS = _full("getGameInfo"), _full("getBoxScore"), _full("getActions")
BOX419 = _full("getBoxScore_419")
SHORTS_BY_METHOD = {
    "getGameInfo": ("game_info", "rosters"),
    "getBoxScore": ("player_box", "team_box", "period_scores"),
    "getActions": ("pbp",),
}
CAPTURES = sorted(p.stem for p in CAP.glob("*.json"))


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("capture", CAPTURES)
def test_every_segev_capture_parses_to_its_pinned_schema(capture):
    method = capture.split("@")[0]
    for short in SHORTS_BY_METHOD[method]:
        df = ALL_PARSERS[short](_cap(capture))
        assert df.height > 0, (capture, short)
        assert all(SNAKE.match(c) for c in df.columns)
        assert dict(df.schema) == dict(P.SPECS[short]), (capture, short)


@pytest.mark.parametrize("short", sorted(gen_ibba_segev.ENDPOINTS))
def test_every_wrapper_parses_its_generator_fixture(short):
    df = ALL_PARSERS[short](gen_ibba_segev.ENDPOINTS[short][1]())
    assert df.height > 0 and dict(df.schema) == dict(P.SPECS[short]), short


def test_a_parser_fed_another_methods_body_gives_an_empty_typed_frame():
    for method, shorts in SHORTS_BY_METHOD.items():
        for other, body in (("getGameInfo", INFO), ("getBoxScore", BOX), ("getActions", ACTIONS)):
            if other == method:
                continue
            for short in shorts:
                if (
                    short in ("game_info", "rosters", "pbp")
                    and other in ("getGameInfo", "getActions")
                    and method != other
                ):
                    continue  # getActions carries the same header as getGameInfo
                df = ALL_PARSERS[short](body)
                assert df.height == 0 and dict(df.schema) == dict(P.SPECS[short]), (short, other)


def test_the_actions_body_carries_the_game_info_header():
    assert P.parse_ibba_segev_game_info(ACTIONS).equals(P.parse_ibba_segev_game_info(INFO))
    assert P.parse_ibba_segev_rosters(ACTIONS).height == P.parse_ibba_segev_rosters(INFO).height == 31


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_ibba_segev_player_box(BOX)
    assert box.schema["fg2m"] == pl.Int64 and box.schema["vps"] == pl.Float64
    assert (
        box.schema["starter"] == pl.Boolean
        and box.schema["minutes"] == pl.Utf8
        and box.schema["seconds_played"] == pl.Int64
    )
    pbp = P.parse_ibba_segev_pbp(ACTIONS)
    assert pbp.schema["x"] == pl.Float64 and pbp.schema["y"] == pl.Float64 and pbp.schema["action_id"] == pl.Int64
    assert pbp.schema["fast_break"] == pl.Boolean and pbp.schema["foul_free_throws"] == pl.Int64
    info = P.parse_ibba_segev_game_info(INFO)
    assert info.schema["start_time"] == pl.Datetime("us") and info.schema["finished"] == pl.Boolean


def test_game_info_reads_the_header_and_the_competition():
    row = P.parse_ibba_segev_game_info(INFO).row(0, named=True)
    assert (row["game_id"], row["competition"], row["home_team_id"], row["away_team_id"]) == (
        50,
        "Winner League",
        11,
        2,
    )
    assert row["start_time"].isoformat() == "2025-10-12T18:40:00" and row["periods"] == 4 and row["finished"] is True
    assert row["clock"] == "00:00" and row["home_team"] == "BEER SHEVA"
    women = P.parse_ibba_segev_game_info(_cap("getGameInfo@300")).row(0, named=True)
    assert women["competition"] == "Women" and women["game_id"] == 300


def test_segev_ids_are_integers_and_pbp_ids_belong_to_the_roster():
    rosters = P.parse_ibba_segev_rosters(INFO)
    pbp = P.parse_ibba_segev_pbp(ACTIONS)
    box = P.parse_ibba_segev_player_box(BOX)
    assert rosters.schema["player_id"] == pbp.schema["player_id"] == box.schema["player_id"] == pl.Int64
    assert pbp.schema["team_id"] == box.schema["team_id"] == rosters.schema["team_id"] == pl.Int64
    roster_ids = set(rosters["player_id"].to_list())
    assert set(pbp["player_id"].drop_nulls().to_list()) <= roster_ids
    assert set(box.filter(pl.col("row_type") == "player")["player_id"].to_list()) <= roster_ids
    assert (
        set(pbp["team_id"].drop_nulls().to_list())
        == set(P.parse_ibba_segev_team_box(BOX)["team_id"].to_list())
        == {11, 2}
    )


def test_box_lines_add_up_to_the_team_totals_and_the_score():
    box = P.parse_ibba_segev_player_box(BOX)
    teams = P.parse_ibba_segev_team_box(BOX)
    assert teams["team_side"].to_list() == ["home", "away"] and teams["score"].to_list() == [92, 104]
    assert teams["pts"].to_list() == teams["score"].to_list()
    for col in ("pts", "fg2m", "fg2_missed", "fg3m", "ftm", "dreb", "oreb", "fouls", "assists", "turnovers", "val"):
        summed = dict(box.group_by("team_side").agg(pl.col(col).sum()).iter_rows())
        assert summed == dict(zip(teams["team_side"], teams[col])), col
    assert box.filter(pl.col("row_type") == "team").height == 2
    assert (teams["fg2a"] == teams["fg2m"] + teams["fg2_missed"]).all()
    periods = P.parse_ibba_segev_period_scores(BOX)
    assert periods["period"].to_list() == [1, 2, 3, 4]  # the feed pads seven periods with zeros
    assert [periods["home_points"].sum(), periods["away_points"].sum()] == [92, 104]
    assert periods["home_fouls"].sum() == teams["fouls"][0]


def test_the_game_stat_keys_renamed_between_seasons_land_in_one_column():
    old, new = P.parse_ibba_segev_team_box(BOX), P.parse_ibba_segev_team_box(BOX419)
    assert old.schema == new.schema
    assert old["pts_in_paint"].to_list() == [26, 30] and new["pts_in_paint"].to_list() == [36, 44]
    assert old["pts_from_turnovers"].null_count() == new["pts_from_turnovers"].null_count() == 0
    assert old["fast_break_possessions"].to_list() == [6, 5] and new["fast_break_possessions"][0] == 9
    assert old["defensive_stops"].null_count() == 2 and new["defensive_stops"].null_count() == 0
    assert new["biggest_scoring_run"][0] == "23-0 (36-6)" and old["biggest_scoring_run"].null_count() == 2
    assert old["fast_break_pts"].to_list() == [12, 8]


def test_play_by_play_is_in_game_order_and_its_scores_close_on_the_final():
    pbp = P.parse_ibba_segev_pbp(ACTIONS)
    assert pbp.height == 768 and pbp["action_id"].is_sorted() and pbp["action_id"].is_unique().all()
    scoring = pbp.filter(pl.col("score_home").is_not_null())
    assert scoring.height == 96
    last = scoring.row(-1, named=True)
    assert (last["score_home"], last["score_away"]) == (92, 104)
    assert (scoring["score_home"].diff().drop_nulls() >= 0).all() and (
        scoring["score_away"].diff().drop_nulls() >= 0
    ).all()
    made = pbp.filter(pl.col("type").is_in(["shot", "freeThrow"]) & (pl.col("made") == "made"))
    assert made.height == 96  # every made shot and free throw carries a running score
    shots = pbp.filter(pl.col("type") == "shot")
    assert shots.height == 137 and shots["x"].null_count() == 0 and shots["points"].is_in([2, 3]).all()
    assert shots["x"].min() >= 0 and shots["x"].max() <= 1470 and shots["y"].min() >= 0
    dunks = shots.filter(pl.col("subtype") == "dunk")
    assert dunks.height > 0 and dunks["x"].is_between(600, 800).all() and dunks["y"].is_between(100, 150).all()
    assert pbp.filter(pl.col("player_id") == 0).height == 0  # 0 means no player and is nulled


def test_every_action_type_keeps_its_own_fields():
    seen = {}
    for f in sorted(CAP.glob("getActions@50~*.json")):
        df = P.parse_ibba_segev_pbp(json.loads(f.read_text("utf-8")))
        seen[f.stem.split("~")[1]] = df
        assert df["type"].unique().to_list() == [f.stem.split("~")[1]], f.name
    assert set(seen) == {"assist", "block", "clock", "deflection", "foul-drawn", "foul", "freeThrow", "game", "quarter",
                         "rebound", "shot", "steal", "substitution", "timeout", "turnover"}  # fmt: skip
    assert seen["shot"]["made"].null_count() == 0 and seen["shot"]["fast_break"].null_count() == 0
    assert (
        seen["freeThrow"]["free_throw_number"].null_count() == 0
        and seen["freeThrow"]["free_throws_awarded"].null_count() == 0
    )
    assert (
        seen["foul"]["foul_kind"].null_count() < seen["foul"].height
        and seen["foul"]["fouled_on_jersey"].null_count() == 0
    )
    sub = seen["substitution"]
    assert ((sub["player_in_jersey"].is_null()) ^ (sub["player_out_jersey"].is_null())).all()  # exactly one of the two
    assert seen["quarter"]["subtype"].is_in(["start-of-quarter", "end-of-quarter"]).all()
    assert seen["game"]["team_id"].null_count() == seen["game"].height  # game-level actions have no team
    assert seen["rebound"]["subtype"].is_in(["offensive", "defensive"]).all()
    assert seen["assist"]["parent_action_id"].null_count() == 0


def test_team_side_and_the_away_home_score_order():
    pbp = P.parse_ibba_segev_pbp(ACTIONS)
    first = pbp.filter(pl.col("score_home").is_not_null()).row(0, named=True)
    assert (
        (first["score_away"], first["score_home"]) == (0, 2) and first["team_side"] == "home" and first["team_id"] == 11
    )
    home = pbp.filter(pl.col("team_side") == "home")["team_id"].unique().to_list()
    assert home == [11]


def test_no_float_origin_id_strings():
    for short, parser in ALL_PARSERS.items():
        df = parser(gen_ibba_segev.ENDPOINTS[short][1]())
        for c, dt in df.schema.items():
            if c.endswith("_id") or c == "game_id":
                assert dt == pl.Int64, (short, c)
            if dt == pl.Utf8:
                assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize(
    "junk", [{}, [], None, "x", {"result": 1}, {"result": {"boxscore": []}}, {"result": {"actions": [1]}}]
)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, junk):
    df = ALL_PARSERS[short](junk)
    assert isinstance(df, pl.DataFrame) and df.height == 0 and dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_two_empty_frames_concatenate_and_stack_with_a_real_one(short):
    empty = ALL_PARSERS[short]({})
    real = ALL_PARSERS[short](gen_ibba_segev.ENDPOINTS[short][1]())
    assert pl.concat([empty, empty]).height == 0 and pl.concat([empty, real]).height == real.height


@pytest.mark.parametrize("short", sorted(P.SPECS))
def test_return_as_pandas(short):
    pdf = ALL_PARSERS[short](gen_ibba_segev.ENDPOINTS[short][1](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and list(pdf.columns) == list(P.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_ibba_segev_pbp({}, columns=["game_id", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert len(P.__all__) == 6 and set(P.__all__) <= set(ibba_pkg.__all__)


# --- the YAML, schemas and generator --------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route_and_method():
    snap = json.loads((FIXTURES / "spec_paths_segev.json").read_text(encoding="utf-8"))
    assert snap == {"/api/": ["getActions", "getBoxScore", "getGameInfo"]}
    doc = _yaml()
    assert len(doc["endpoints"]) == 6 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    assert {e["fixed_params"]["method"] for e in doc["endpoints"]} == set(snap["/api/"]) == set(R.METHODS)
    assert {e["short"]: e["fixed_params"]["method"] for e in doc["endpoints"]} == gen_ibba_segev.METHOD
    for e in doc["endpoints"]:
        assert e["path"] == "/realtimestat_heb/api/" and [p["name"] for p in e["extra_params"]] == ["game_id"]
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"]))


def test_yaml_carries_the_access_id_and_caveat_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "keyless" in notes and "one second apart" in notes and "text/html" in notes
    assert "flat Segev id space" in notes and "competition" in notes and "no game list" in notes
    assert "JERSEY" in notes and "unverified" in notes and "away-home" in notes


@pytest.mark.parametrize("short", sorted(gen_ibba_segev.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}


def test_the_description_snippet_covers_every_column_without_filler():
    if not gen_ibba_segev.SNIPPET.exists():  # gitignored; the generator writes it
        pytest.skip("run tools/codegen/gen_ibba_segev.py first")
    snippet = yaml.safe_load(gen_ibba_segev.SNIPPET.read_text(encoding="utf-8"))
    assert set(snippet) == {f"ibba_segev_{s}" for s in gen_ibba_segev.ENDPOINTS}
    for key, cols in snippet.items():
        assert set(cols) == set(P.SPECS[key[len("ibba_segev_") :]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col, (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    for col in ("x", "y"):
        assert "unverified" in gen_ibba_segev.LOCAL[col]
    assert "MISSED" in gen_ibba_segev.LOCAL["fg2_missed"] and "unverified" in gen_ibba_segev.LOCAL["vps"]
    assert "away-home" in gen_ibba_segev.LOCAL["score_away"]


# --- runtime: a scripted transport -------------------------------------------------------------------


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh polite session for the Segev runtime."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        return adapter

    return install


#: Segev serves its JSON as text/html.
SEGEV_HEADERS = HTML


def _envelope(block: Dict[str, Any]) -> Dict[str, Any]:
    return {"jsonrpc": "2.0", "id": "42", "error": None, "result": block}


def test_method_and_game_id_are_sent_and_json_served_as_html_is_accepted(site):
    adapter = site(_serve(ACTIONS, headers=SEGEV_HEADERS))
    raw = ibba_pkg.ibba_segev_pbp(game_id=50, return_parsed=False)
    assert _query(adapter.calls[0]) == {"method": "getActions", "game_id": "50"}
    assert adapter.calls[0].url.startswith(R.URL) and raw[R.REQUEST_KEY] == {"method": "getActions", "game_id": 50}
    assert ibba_pkg.ibba_segev_pbp(game_id=50).height == 768
    adapter = site(_serve(BOX, headers=SEGEV_HEADERS))
    assert ibba_pkg.ibba_segev_team_box(game_id=50).height == 2
    assert _query(adapter.calls[0])["method"] == "getBoxScore"
    adapter = site(_serve(INFO, headers=SEGEV_HEADERS))
    assert ibba_pkg.ibba_segev_rosters(game_id=50).height == 31
    assert _query(adapter.calls[0])["method"] == "getGameInfo"


def _err(code, message="x"):
    return {"jsonrpc": "2.0", "id": "1", "result": {"success": False}, "error": {"code": code, "message": message}}


def _with(body, path, value):
    out = json.loads(json.dumps(body))
    node = out
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return out


@pytest.mark.parametrize(
    ("handler", "exc", "match"),
    [
        (_serve(b"", 404), NoDataError, "No data found"),
        (_serve(_err("-32000", "game not found")), NoDataError, "not found"),
        (_serve(_err("-33000", "method not found")), AssetFetchError, "rpc error code '-33000'"),
        (_serve(_err(-32000)), AssetFetchError, "rpc error"),  # an int code is not the string code
        (_serve(b"", 200, HTML), AssetFetchError, "empty body"),
        (_serve("<html>maintenance</html>", 200, HTML), AssetFetchError, "not JSON"),
        (_serve({"hello": 1}), AssetFetchError, "not a JSON-RPC envelope"),
        (_serve(_with(ACTIONS, ["result", "success"], False)), AssetFetchError, "success is not true"),
        (_serve(_with(ACTIONS, ["result", "gameInfo", "gameId"], "51")), AssetFetchError, "out of scope: gameId='51'"),
        (_serve(_with(ACTIONS, ["result", "gameInfo", "gameId"], 50)), AssetFetchError, "out of scope"),
        (_serve(_with(ACTIONS, ["result", "gameInfo", "gameFinished"], False)), AssetFetchError, "not final"),
        (_serve(_with(ACTIONS, ["result", "actions"], [])), NoDataError, "no actions"),
        (_serve(_with(ACTIONS, ["result", "gameInfo"], None)), AssetFetchError, "no gameInfo"),
        (_serve(b"", 500), AssetFetchError, "HTTP 500"),
        (_serve(b"", 400), ValueError, "HTTP 400"),
    ],
)
def test_refusals(site, handler, exc, match):
    site(handler)
    with pytest.raises(exc, match=match):
        ibba_pkg.ibba_segev_pbp(game_id=50, return_parsed=False)


def test_box_score_refusals(site):
    site(_serve(_with(BOX, ["result", "boxscore", "gameInfo", "gameId"], "419")))
    with pytest.raises(AssetFetchError, match="out of scope: gameId='419'"):
        ibba_pkg.ibba_segev_player_box(game_id=50)
    site(_serve(_with(BOX, ["result", "boxscore", "gameInfo", "gameFinished"], False)))
    with pytest.raises(AssetFetchError, match="not final"):
        ibba_pkg.ibba_segev_team_box(game_id=50)
    site(_serve(_with(BOX, ["result", "boxscore", "awayTeam"], None)))
    with pytest.raises(AssetFetchError, match="no boxscore with two teams"):
        ibba_pkg.ibba_segev_period_scores(game_id=50)


def test_a_game_header_of_another_game_is_refused(site):
    site(_serve(INFO))
    with pytest.raises(AssetFetchError, match="out of scope: gameId='50'"):
        ibba_pkg.ibba_segev_game_info(game_id=419)


def test_bad_parameters_never_reach_the_host(site):
    adapter = site(_serve(INFO))
    for bad in ("abc", 0, -3, None, True, 2.5):
        with pytest.raises(ValueError, match="game_id"):
            ibba_pkg.ibba_segev_game_info(game_id=bad)
    with pytest.raises(ValueError, match="method must be"):
        R._get(R.URL, {"method": "getGames", "game_id": 50})
    with pytest.raises(TypeError, match="unsupported"):
        ibba_pkg.ibba_segev_game_info(game_id=50, bogus=1)
    with pytest.raises(ValueError, match="not a Segev route"):
        R._get("https://example.com/realtimestat_heb/api/", {"method": "getGameInfo", "game_id": 50})
    assert adapter.calls == []


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def handler(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "9"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, SEGEV_HEADERS, json.dumps(INFO)

    adapter = site(handler)
    assert ibba_pkg.ibba_segev_game_info(game_id=50).height == 1
    assert len(adapter.calls) == 3 and len(waits.urls) == 3  # one polite wait per attempt, retries included
    assert max(waits.slept) >= 9.0  # Retry-After honoured


def test_a_dead_connection_is_a_fetch_error_naming_only_the_exception_class(monkeypatch, waits):
    import requests
    from requests.adapters import HTTPAdapter

    class Boom(HTTPAdapter):
        n = 0

        def send(self, request, **kwargs):
            Boom.n += 1
            raise requests.ConnectionError("proxy secret=abc")

    session = basketball_runtime.PoliteSession()
    session.mount("https://", Boom())
    monkeypatch.setattr(R, "SESSION", session)
    with pytest.raises(AssetFetchError, match="ConnectionError") as err:
        ibba_pkg.ibba_segev_game_info(game_id=50)
    assert "secret" not in str(err.value) and Boom.n == R.ATTEMPTS


def test_the_shared_session_is_the_polite_one():
    assert R.SESSION is basketball_runtime.SESSION and isinstance(R.SESSION, basketball_runtime.PoliteSession)
