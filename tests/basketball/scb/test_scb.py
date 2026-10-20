"""Offline tests for the BBL / DBBL (``scb``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against the real captures in ``tests/fixtures/scb/`` (refs captures, truncated) and the
full bodies in ``tests/fixtures/scb/full/`` (dtypes come from those). The runtime is driven through a fake
``requests.Session.request`` transport, so the throttle, the auth headers and the error vocabulary are exercised
end to end. No network, no refs checkout.
"""

from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import basketball_runtime as BR
from sportsdataverse.basketball.scb import scb_parsers as P
from sportsdataverse.basketball.scb import scb_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "scb"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "scb.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "scb"
NOTES = ROOT / "tools" / "codegen" / "notes" / "scb_descriptions.yaml"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_scb  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = [getattr(P, n) for n in P.__all__ if n.startswith("parse_")]
KEY = "k" * 20 + "a:b" + "c" * 20  # a page key with a colon: the secret turns it into "-"
SECRET = KEY[::-1].replace(":", "-")
BBL, DBBL = R.HOSTS["bbl"], R.HOSTS["dbbl"]


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _body(short: str) -> Any:
    entry = gen_scb.ENDPOINTS[short]
    return R._stamp(_load(entry[3]), entry[4])


def _parsed(short: str) -> pl.DataFrame:
    return gen_scb.ENDPOINTS[short][2](_body(short))


def _schema(short: str) -> Dict[str, Any]:
    return yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))


_SCHEMA_DTYPE = {"integer": pl.Int64, "numeric": pl.Float64, "logical": pl.Boolean, "character": pl.String}


# --- parsers over every committed fixture -----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = _parsed(short)
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    for col in df.columns:  # one dtype per id: strings, except the ids the API always sends as integers
        if re.search(r"(^|_)id$", col):
            assert df.schema[col] == (pl.Int64 if col in P._INT_IDS else pl.String), (short, col)
    assert set(df["league"]) == {gen_scb.ENDPOINTS[short][4]["league"]}, short
    if "season" in df.columns:
        assert df.schema["season"] == pl.Int64 and df["season"].null_count() == 0, short


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
def test_committed_schema_matches_the_parser(short):
    committed = _schema(short)
    df = _parsed(short)
    assert [c["name"] for c in committed["columns"]] == df.columns
    assert {c["name"]: _SCHEMA_DTYPE[c["type"]] for c in committed["columns"]} == dict(df.schema)
    assert committed["schema"] == f"scb_{short}" and all(c["description"] == "" for c in committed["columns"])


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
@pytest.mark.parametrize("empty", [{}, [], None])
def test_empty_frame_stacks_onto_the_populated_one(short, empty):
    parser = gen_scb.ENDPOINTS[short][2]
    cols = [c["name"] for c in _schema(short)["columns"]]
    full, none = parser(_body(short), columns=cols), parser(empty, columns=cols)
    assert none.height == 0 and dict(none.schema) == dict(full.schema)
    assert pl.concat([none, full], how="vertical").height == full.height
    assert pl.concat([parser(empty), parser(_body(short))], how="diagonal").height == full.height


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
def test_dtype_module_is_the_returns_schema(short):
    from sportsdataverse.basketball.scb.scb_dtypes import DTYPES

    parser = gen_scb.ENDPOINTS[short][2].__name__
    for col in _schema(short)["columns"]:
        assert P._CODES[DTYPES[parser][col["name"]]] == _SCHEMA_DTYPE[col["type"]], (parser, col)


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3, {"_request": {"league": "bbl"}}, [[1], "x", None]])
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
def test_empty_payload_keeps_the_documented_columns(parser):
    df = parser([], columns=["league", "season", "game_id", "real_player_id", "points_for", "is_active", "value"])
    assert df.height == 0
    assert dict(df.schema) == {
        "league": pl.String,
        "season": pl.Int64,
        "game_id": pl.String,
        "real_player_id": pl.Int64,
        "points_for": pl.String,
        "is_active": pl.Boolean,
        "value": pl.Float64,
    }


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
def test_return_as_pandas(short):
    parser = gen_scb.ENDPOINTS[short][2]
    df = parser(_body(short), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) == _parsed(short).height
    assert isinstance(parser([], return_as_pandas=True), pd.DataFrame)


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
def test_every_public_parser_documents_args_returns_raises_and_an_example(parser):
    doc = parser.__doc__ or ""
    for section in ("Args:", "Returns:", "Raises:", "Example:"):
        assert section in doc, (parser.__name__, section)
    assert ">>>" not in doc


# --- id discipline ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "want"), [(123, "123"), (123.0, "123"), (12.5, "12.5"), ("77 ", "77"), ("", None), (None, None)]
)
def test_ids_never_stringify_a_float_origin_value(value, want):
    assert P._s(value) == want


def test_games_and_box_score_share_the_game_id_dtype():
    assert _parsed("games").schema["game_id"] == _parsed("game_players").schema["game_id"] == pl.String
    assert _parsed("games_recent").schema["game_id"] == _parsed("game_actions").schema["game_id"] == pl.String


def test_playoff_tree_ids_become_strings():
    games, series = _parsed("playoff_games"), _parsed("playoff_series")
    for col in ("game_id", "game_winner_id", "home_team_id", "guest_team_id", "team_a_id", "team_b_id"):
        assert games.schema[col] == pl.String, col
    assert series.schema["team_a_id"] == series.schema["match_winner_id"] == pl.String
    assert set(games["round"]) <= {"quarter_finals", "semi_finals", "finals"}


def test_the_ambiguous_player_id_is_not_a_column_anywhere():
    for short in gen_scb.ENDPOINTS:
        assert "player_id" not in _parsed(short).columns, short


def test_season_player_crosswalk_joins_without_a_dtype_surprise():
    players = _parsed("season_players")
    roster = _parsed("player_statistics")
    assert players.schema["season_player_id"] == roster.schema["season_player_id"] == pl.String
    assert players.schema["real_player_id"] == roster.schema["real_player_id"] == pl.Int64


def test_top_scorer_id_is_the_season_player_id_on_both_hosts():
    bbl = P.parse_scb_games(_load("bbl/games__2026.json"))
    dbbl = _parsed("games")
    assert bbl.schema["top_scorer_season_player_id"] == dbbl.schema["top_scorer_season_player_id"] == pl.String
    raw = _load("full/dbbl_games_2024_first40.json")["items"][0]["topScorer"]
    assert dbbl["top_scorer_season_player_id"][0] == str(raw["playerId"]) and isinstance(raw["playerId"], int)


# --- season mapping ---------------------------------------------------------------------------------


@pytest.mark.parametrize(("season", "want"), [(2026, 2025), ("2026", 2025), (1967, 1966), (None, None)])
def test_season_end_year_maps_to_the_start_year(season, want):
    assert R.season_id(season) == want


@pytest.mark.parametrize("bad", [0, -1, "x", 2026.5, True, [2026]])
def test_season_id_rejects_a_non_year(bad):
    with pytest.raises(ValueError, match="END year"):
        R.season_id(bad)


def test_frames_carry_both_season_keys():
    games = _parsed("games")
    assert (games["season"] == games["season_id"] + 1).all() and set(games["season_id"]) == {2024}
    stats = _parsed("player_statistics")
    assert set(stats["season"]) == {2026} and set(stats["display_type"]) == {"MAIN_ROUND"}
    assert set(_parsed("seasons")["season"]) == set(_parsed("seasons")["season_id"] + 1)


def test_the_yaml_maps_the_public_season_through_the_transform():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    eps = {e["short"]: e for e in doc["endpoints"]}
    assert eps["season_players"]["path_params"][0]["transform"] == "season_id"
    q = {p["name"]: p for p in eps["games"]["extra_params"]}
    assert q["season"]["query_key"] == "seasonId" and q["season"]["transform"] == "season_id"
    assert eps["games"]["fixed_params"] == {"currentPage": 1}


# --- shapers ----------------------------------------------------------------------------------------


def test_box_score_lines_and_team_rows():
    players, teams = _parsed("game_players"), _parsed("game_teams")
    assert players.height == 23 and set(players["side"]) == {"home", "guest"}
    assert set(players["game_id"]) == set(teams["game_id"]) == {"2006920"} and teams.height == 2
    assert teams.schema["seconds_played"] == pl.Int64 and teams["seconds_played"].to_list() == [12000, 12000]
    assert teams.schema["team_line_offensive_rebounds"] == pl.Int64  # "2" and "0" strings in the body
    assert players.filter(pl.col("seconds_played").is_null()).height == 2  # did not play
    assert players["plus_minus"].null_count() == 2 and players.schema["plus_minus"] == pl.Int64  # '' strings
    assert players.schema["is_starting_five"] == pl.Boolean
    totals = players.group_by("side").agg(pl.col("points").sum())
    assert dict(zip(totals["side"], totals["points"])) == dict(zip(teams["side"], teams["points"]))


def test_percentages_are_float_even_when_whole():
    players = _parsed("game_players")
    pct = [c for c in players.columns if c.endswith("_percent")]
    assert pct and all(players.schema[c] == pl.Float64 for c in pct)
    stats = _parsed("player_statistics")
    per_game = [c for c in stats.columns if c.endswith("_per_game")]
    assert per_game and all(stats.schema[c] == pl.Float64 for c in per_game)
    assert stats.schema["points"] == pl.Int64 and stats.schema["games_played"] == pl.Int64
    assert stats["two_point_shot_success_percent"].null_count() > 0  # null with no attempts


def test_field_goals_are_recomputed_from_the_parts_when_the_totals_disagree():
    raw = _load("full/bbl_game_stats_2006920.json")
    ok = P.parse_scb_game_teams(raw)
    assert ok["field_goals_recomputed"].to_list() == [False, False]
    bad = copy.deepcopy(raw)
    stat = bad["homeTeam"]["gameStat"]
    stat["fieldGoalsMade"], stat["fieldGoalsAttempted"] = 8, 30  # 2005-06: 2PM 24 + 3PM 8 = 32
    row = P.parse_scb_game_teams(bad).filter(pl.col("side") == "home").row(0, named=True)
    assert row["field_goals_recomputed"] is True
    assert (row["field_goals_made"], row["field_goals_attempted"]) == (32, 75)
    assert row["field_goals_success_percent"] == 42.7
    assert P.parse_scb_game_players(bad)["field_goals_recomputed"].sum() == 0


def test_actions_hold_the_shot_locations_and_the_clock():
    a = _parsed("game_actions")
    assert a.height == 516 and a.schema["action_id"] == a.schema["order_id"] == pl.Int64
    shots = a.filter(pl.col("type").is_in(["TWO_POINT_THROW", "THREE_POINT_THROW"]))
    assert shots["shot_x"].null_count() == 0 and shots.height == 142  # every attempt is located in this game
    assert a.filter(pl.col("type") == "START")["shot_x"].null_count() == 4  # (0, 0) is "no location"
    assert a.schema["shot_x"] == a.schema["shot_y"] == pl.Int64
    first = a.row(0, named=True)
    assert (first["game_time"], first["seconds_left"], first["side"]) == ("00:10:00", 600, None)
    assert set(a["side"].drop_nulls()) == {"home", "guest"}
    assert a.filter(pl.col("qualifiers").str.contains("LAYUP")).height > 0
    assert a.schema["qualifiers"] == pl.String and a.schema["is_successful"] == pl.Boolean


def test_lineup_markers_have_a_null_type_and_old_games_have_no_actions():
    marker = {"gameId": "5992", "actions": [{"id": 1, "type": None, "period": 1, "gameTime": "00:10:00"}]}
    assert P.parse_scb_game_actions(marker)["type"].null_count() == 1
    old = P.parse_scb_game_actions(_load("bbl/games__gameId__stats__2005.json"))
    assert old.height == 0
    assert P.parse_scb_game_players(_load("bbl/games__gameId__stats__2005.json")).height > 0


def test_latest_period_is_the_period_number():
    assert [P._period(v) for v in ("Q4", "OT1", "OT2", 4, "", None, "x")] == [4, 5, 6, 4, None, None, None]
    games = _parsed("games")
    assert set(games["latest_period"].drop_nulls()) == {4, 5} and games.schema["latest_period"] == pl.Int64
    recent = _parsed("games_recent")
    assert set(recent["bucket"]) == {"past", "present", "future"}
    assert recent.filter(pl.col("bucket") == "past")["latest_period"].drop_nulls().to_list() == [4, 4, 4]


def test_unplayed_games_have_null_scores_not_zero():
    games = _parsed("games")
    pre = games.filter(pl.col("status") == "PRE")
    assert pre.height == 4 and pre["home_score"].null_count() == 4
    assert games.filter(pl.col("status") == "OFFICIAL")["home_score"].null_count() == 0


def test_game_header_has_the_status_fields_a_games_page_lacks():
    header = P.parse_scb_games(R._stamp(_load("bbl/games__gameId__2026.json"), {"league": "bbl"}))
    assert header.height == 1 and {"stat_status", "game_progress"} <= set(header.columns)
    assert "stat_status" not in _parsed("games").columns
    assert header["referee1_id"][0] is not None and header.schema["venue_id"] == pl.String  # an int on /recent


def test_standings_rename_the_football_goals_and_keep_table_points():
    df = _parsed("standings")
    assert not [c for c in df.columns if "goal" in c]
    assert {"total_points_for", "total_points_against", "total_point_difference"} <= set(df.columns)
    assert {"table_points", "table_points_against"} <= set(df.columns)
    assert df.schema["total_victory_percentage"] == pl.Float64 and df.schema["has_reached_playoffs"] == pl.Boolean
    assert json.loads(df["ranking_comparison_info"][0])[0]["position"] in range(1, 20)


def test_records_are_long_and_numeric():
    per_game = _parsed("records_per_game")
    assert set(per_game["scope"]) == {"season", "all_time"} and per_game.schema["value"] == pl.Float64
    assert "points" in set(per_game["stat"]) and "seasonTeamId" not in per_game.columns
    assert "season_team_id" not in per_game.columns
    averages = _parsed("records_season_averages")
    assert "assist_average" in set(averages["stat"]) and averages.schema["value"] == pl.Float64


def test_transfers_split_the_person_id_from_the_season_player_id():
    df = _parsed("transfers")
    assert df.schema["real_player_id"] == pl.Int64 and df.schema["season_player_id"] == pl.String
    assert df["real_player_id"][0] == 140183148 and df["season_player_id"][0] == "2005312"
    both = _parsed("transfers_teams")
    assert set(both["direction"]) <= {"joining", "leaving"} and both.height > 0
    assert _parsed("team_rosters").schema["height"] == pl.Float64


def test_season_crosswalk_frames():
    players, teams = _parsed("season_players"), _parsed("season_teams")
    assert set(players["season"]) == set(teams["season"]) == {2027}
    assert {"season_player_id", "real_player_id", "season_team_id"} <= set(players.columns)
    assert teams.schema["tlc"] == pl.String


def test_personal_and_contact_data_is_left_out():
    for short in ("referees", "team", "season_player", "team_rosters"):
        cols = " ".join(_parsed(short).columns)
        for banned in ("profession", "email", "phone", "address", "image", "social", "salutation"):
            assert banned not in cols, (short, banned)
    assert "birth_date" not in _parsed("referees").columns


def test_playoff_series_and_games_number_the_rounds():
    series = _parsed("playoff_series")
    assert series.filter(pl.col("round") == "finals").height == 1
    assert series.filter(pl.col("round") == "quarter_finals")["series"].to_list() == [1, 2, 3]
    games = _parsed("playoff_games")
    assert games.height == series["games"].sum()
    assert games.select("round", "series", "game_id").is_duplicated().sum() == 0


def test_team_statistics_have_no_player_columns():
    df = _parsed("team_statistics")
    assert "season_player_id" not in df.columns and "team_id" in df.columns


# --- runtime ----------------------------------------------------------------------------------------


class _Resp:
    def __init__(
        self, status: int, body: Any = None, *, text: str | None = None, headers: Dict[str, str] | None = None
    ):
        self.status_code = status
        self.text = text if text is not None else json.dumps(body)
        self.content = self.text.encode()
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.url = "https://example.test/"
        self.reason = "x"

    def json(self) -> Any:
        return json.loads(self.text)


class _Net:
    """A fake transport: ``requests.Session.request`` answers from a script, and every call is recorded."""

    def __init__(self, clock: Dict[str, Any]) -> None:
        self.clock = clock
        self.calls: List[Dict[str, Any]] = []
        self.answers: List[Callable[[str], Any]] = []
        self.key_page = _Resp(
            200, text=f'<html><script>{{"key":"{KEY}"}}</script></html>', headers={"Content-Type": "text/html"}
        )

    def reply(self, *responses: _Resp) -> None:
        it = iter(responses)
        self.answers = [lambda url, it=it: next(it)]

    def __call__(self, method, url, *args, **kwargs):
        self.calls.append(
            {"url": url, "params": kwargs.get("params"), "headers": kwargs.get("headers"), "at": self.clock["now"]}
        )
        if "easycredit-bbl.de" in url:
            return self.key_page
        return self.answers[0](url)

    @property
    def api(self) -> List[Dict[str, Any]]:
        return [c for c in self.calls if "easycredit-bbl.de" not in c["url"]]


@pytest.fixture
def net(monkeypatch):
    """A fake clock (sleep advances it, so nothing waits) and a scripted transport; the secret cache starts empty."""
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(BR.time, "monotonic", lambda: state["now"])

    def sleep(s):
        state["slept"].append(s)
        state["now"] += s

    monkeypatch.setattr(BR.time, "sleep", sleep)
    monkeypatch.setattr(BR, "_last", {})
    monkeypatch.setattr(R, "_secrets", {})
    fake = _Net(state)
    monkeypatch.setattr(BR.requests.Session, "request", lambda self, method, url, *a, **k: fake(method, url, *a, **k))
    return fake


def _game(**over):
    g = {"id": "10130", "seasonId": 2024, "competition": "DBBL", "stage": "MAIN_ROUND", "status": "OFFICIAL"}
    return {**g, **over}


def _page(items, pages=1):
    return {"items": items, "totalPages": pages, "currentPage": 1, "pageSize": 500}


def test_the_secret_is_the_reversed_page_key_with_colons_turned_into_dashes(net):
    net.reply(_Resp(200, [{"id": 2025, "isActive": True}]))
    R._get(BBL + "/seasons", {})
    key_call, api_call = net.calls
    assert key_call["url"] == R.KEY_PAGE
    assert api_call["headers"]["x-api-key"] == "publicWebUser" and api_call["headers"]["x-api-secret"] == SECRET
    assert ":" not in SECRET and R._derive_secret("ab:cd" + "e" * 30) == ("e" * 30 + "dc-ba")


def test_the_page_key_is_scraped_once_per_process(net):
    net.reply(*[_Resp(200, [{"id": 2025}]) for _ in range(3)])
    for _ in range(3):
        R._get(BBL + "/seasons", {})
    assert len([c for c in net.calls if c["url"] == R.KEY_PAGE]) == 1 and len(net.api) == 3


def test_a_react_key_is_not_the_api_key(net):
    net.key_page = _Resp(200, text=f'{{"key":"1"}} {{"key":"{KEY}"}}', headers={"Content-Type": "text/html"})
    net.reply(_Resp(200, [{"id": 2025}]))
    R._get(BBL + "/seasons", {})
    assert net.api[0]["headers"]["x-api-secret"] == SECRET
    net2 = R._page_key
    with pytest.raises(AssetFetchError, match="page layout changed"):
        net2('{"key":"short"}')


def test_league_picks_the_host_and_is_never_sent(net):
    net.reply(_Resp(200, _page([_game()])))
    R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 500, "competition": None})
    call = net.api[0]
    assert call["url"] == DBBL + "/games" and "league" not in call["params"] and call["params"]["seasonId"] == 2024
    assert "competition" not in call["params"]
    assert R._build_query({"league": "bbl", "seasonId": 2024}) == {"seasonId": 2024}


def test_an_unknown_league_is_a_value_error(net):
    with pytest.raises(ValueError, match="league must be one of"):
        R._get(BBL + "/seasons", {"league": "pro-a"})
    assert net.calls == []  # nothing was requested


def test_the_body_is_stamped_with_league_and_the_end_year(net):
    net.reply(_Resp(200, _page([_game()])))
    body = R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 500})
    assert body["_request"] == {"league": "dbbl", "season": 2025}
    net.reply(_Resp(200, [{"seasonTeam": {"seasonId": 2025}, "competition": "BBL"}]))
    rows = R._get(BBL + "/seasons/2025/team-statistics", {"displayType": "MAIN_ROUND"})
    assert rows[0]["_request"] == {"league": "bbl", "season": 2026, "display_type": "MAIN_ROUND"}


def test_every_attempt_waits_its_turn_and_a_retry_after_is_honoured(net):
    net.reply(_Resp(503, {"e": 1}, headers={"Retry-After": "3"}), _Resp(503, {"e": 1}), _Resp(200, [{"id": 2025}]))
    R._get(BBL + "/seasons", {})
    sent = [c["at"] for c in net.api]
    assert len(sent) == 3
    assert sent[1] - sent[0] >= BR.MIN_INTERVAL and sent[2] - sent[1] >= BR.MIN_INTERVAL
    assert any(s >= 3 for s in net.clock["slept"])  # Retry-After: 3 on the first 503


def test_the_key_page_request_is_throttled_like_any_other(net):
    net.reply(_Resp(200, [{"id": 2025}]))
    R._get(BBL + "/seasons", {})
    assert net.calls[0]["url"] == R.KEY_PAGE
    assert BR._last.get("www.easycredit-bbl.de") is not None  # recorded on the page's own host


def test_a_refused_secret_is_re_derived_once_then_refused(net):
    net.reply(
        _Resp(401, text="<html>unauthorised</html>", headers={"Content-Type": "text/html"}), _Resp(200, [{"id": 2025}])
    )
    assert R._get(BBL + "/seasons", {})[0]["id"] == 2025
    assert len([c for c in net.calls if c["url"] == R.KEY_PAGE]) == 2  # the first key page, then a re-derive
    net.reply(*[_Resp(403, {"status": "error", "error": "No valid api key provided."}) for _ in range(3)])
    with pytest.raises(AssetFetchError, match="refused the derived x-api-secret") as err:
        R._get(DBBL + "/seasons", {})
    assert SECRET not in str(err.value) and KEY not in str(err.value) and "HTTP 403" in str(err.value)


def test_an_echo_of_the_key_or_secret_in_a_body_is_redacted(net):
    net.reply(_Resp(200, text=json.dumps({"status": "error", "error": f"bad key {KEY} secret {SECRET}"})))
    with pytest.raises(AssetFetchError) as err:
        R._get(BBL + "/seasons", {})
    assert KEY not in str(err.value) and SECRET not in str(err.value)
    net.reply(_Resp(500, text=f"boom {SECRET}", headers={"Content-Type": "text/plain"}))
    with pytest.raises(AssetFetchError) as err2:
        R._get(BBL + "/seasons", {}, num_retries=0)
    assert SECRET not in str(err2.value) and R.REDACTED in str(err2.value)


@pytest.mark.parametrize(
    ("answer", "exc"),
    [
        (_Resp(404, text="nope"), NoDataError),
        (_Resp(400, text='"not a valid gameId"'), NoDataError),
        (_Resp(400, text='"currentPage is required and needs to be a number"'), ValueError),
        (_Resp(422, text='"bad"'), ValueError),
        (_Resp(200, [], headers={}), NoDataError),
        (_Resp(200, _page([])), NoDataError),
        (_Resp(200, {"seasonId": 2025, "quarterFinals": [], "semiFinals": [], "finals": {}}), NoDataError),
        (_Resp(200, {"present": [], "future": [], "past": []}), NoDataError),
        (_Resp(500, text="x", headers={"Content-Type": "text/plain"}), AssetFetchError),
        (_Resp(200, text="<html>hi</html>", headers={"Content-Type": "text/html"}), AssetFetchError),
        (_Resp(200, text=""), AssetFetchError),
        (_Resp(200, "just a string"), AssetFetchError),
        (_Resp(200, {"status": "error", "error": "No valid api key provided."}), AssetFetchError),
    ],
)
def test_the_error_vocabulary(net, answer, exc):
    net.reply(answer)
    path = "/seasons/2025/playoff-tree" if exc is NoDataError and "quarterFinals" in answer.text else "/games/10130"
    if "present" in answer.text:
        path = "/games/recent"
    with pytest.raises(exc):
        R._get(BBL + path, {"league": "dbbl"}, num_retries=0)


@pytest.mark.parametrize("status", [204, 205])
def test_no_content_is_an_empty_answer(net, status):
    # EX-SYNC-a: a 204 / 205 on the data request is {} (zero rows), as from every runtime; it was NoDataError.
    net.reply(_Resp(status, text=""))
    assert R._get(BBL + "/games/10130", {"league": "dbbl"}, num_retries=0) == {}


def test_a_key_page_that_answers_no_content_is_a_failed_fetch(net):
    # The bootstrap is not the data request: a 204 there is a page with no key to read, an unknown answer.
    net.key_page = _Resp(204, text="")
    net.reply(_Resp(200, [{"id": 2025}]))
    with pytest.raises(AssetFetchError, match="the API key could not be read"):
        R._get(BBL + "/seasons", {}, num_retries=0)


def test_not_found_and_failed_fetch_are_never_the_same_error():
    assert not issubclass(NoDataError, AssetFetchError) and not issubclass(AssetFetchError, NoDataError)


def test_a_truncated_page_is_refused_not_returned_as_the_season(net):
    net.reply(_Resp(200, _page([_game()], pages=3)))
    with pytest.raises(AssetFetchError, match="truncated page.*raise page_size"):
        R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 1})
    net.reply(_Resp(200, _page([_game()], pages=1)))
    assert (
        len(R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 500})["items"])
        == 1
    )


@pytest.mark.parametrize(
    ("path", "params", "league", "body"),
    [
        ("/games", {"seasonId": 2024, "currentPage": 1}, "dbbl", _page([_game(seasonId=2023)])),  # another season
        ("/games", {"seasonId": 2024, "currentPage": 1}, "dbbl", _page([_game(seasonId="2024")])),  # id-type strict
        ("/games", {"seasonId": 2024, "currentPage": 1}, "bbl", _page([_game()])),  # DBBL rows from the BBL host
        ("/games", {"seasonId": 2024, "currentPage": 1}, "dbbl", _page([_game(competition="BBL")])),
        (
            "/games",
            {"seasonId": 2024, "currentPage": 1},
            "dbbl",
            _page([{k: v for k, v in _game().items() if k != "competition"}]),
        ),
        ("/games/10130", {}, "dbbl", _game(id="10131")),  # another game
        ("/games/10130/stats", {}, "dbbl", {"gameId": "999", "status": "OFFICIAL", "g": {"competition": "DBBL"}}),
        ("/standings", {"seasonId": 2024}, "dbbl", [{"seasonId": 2023, "competition": "DBBL"}]),
        ("/seasons/2025", {}, "bbl", {"id": 2024, "teams": []}),
        ("/seasons/2025/playoff-tree", {}, "bbl", {"seasonId": 2024, "finals": {"games": [1]}}),
        (
            "/seasons/2025/team-statistics",
            {"displayType": "MAIN_ROUND"},
            "bbl",
            [{"seasonTeam": {"seasonId": 2024}, "competition": "BBL"}],
        ),
        ("/teams/413", {}, "bbl", {"id": "414"}),
        ("/season-players/56414", {}, "bbl", {"id": "1"}),
        ("/records/per-game/2025", {}, "bbl", {"seasonId": "2024", "seasonRecords": {"points": [1]}}),
    ],
)
def test_a_body_for_another_game_season_club_or_league_is_refused(net, path, params, league, body):
    net.reply(_Resp(200, body))
    with pytest.raises(AssetFetchError, match="out of scope"):
        R._get(BBL + path, {"league": league, **params})


def test_a_box_score_of_a_game_that_is_not_over(net):
    stats = {"gameId": "10130", "home": {"competition": "DBBL"}}
    for status, exc in (("PRE", NoDataError), ("LIVE", AssetFetchError)):
        net.reply(_Resp(200, {**stats, "status": status}))
        with pytest.raises(exc, match="not started|in play"):
            R._get(BBL + "/games/10130/stats", {"league": "dbbl"})
    for status in ("POST", "OFFICIAL"):
        net.reply(_Resp(200, {**stats, "status": status}))
        assert R._get(BBL + "/games/10130/stats", {"league": "dbbl"})["status"] == status


def test_real_captures_pass_the_runtime_checks(net):
    for rel, path, league, params in (
        ("bbl/games__gameId__stats__2026.json", "/games/2006920/stats", "bbl", {}),
        ("dbbl/games__gameId__2024.json", "/games/10130", "dbbl", {}),
        ("dbbl/standings__2024.json", "/standings", "dbbl", {"seasonId": 2024}),
        ("bbl/seasons__seasonId__2026.json", "/seasons/2026", "bbl", {}),
        ("bbl/teams__teamId.json", "/teams/413", "bbl", {}),
        ("bbl/season-players__seasonPlayerId.json", "/season-players/56414", "bbl", {}),
        ("bbl/records__per-game__seasonId__2025.json", "/records/per-game/2025", "bbl", {}),
        ("bbl/seasons__seasonId__playoff-tree__2025.json", "/seasons/2025/playoff-tree", "bbl", {}),
        (
            "dbbl/seasons__seasonId__team-statistics__2025.json",
            "/seasons/2025/team-statistics",
            "dbbl",
            {"displayType": "MAIN_ROUND"},
        ),
        ("bbl/games__recent.json", "/games/recent", "bbl", {}),
    ):
        net.reply(_Resp(200, _load(rel)))
        body = R._get(BBL + path, {"league": league, **params})
        assert (body[0] if isinstance(body, list) else body)["_request"]["league"] == league, rel


def test_the_full_games_page_and_a_foreign_competition_filter(net):
    net.reply(_Resp(200, _load("full/dbbl_games_2024_first40.json")))
    body = R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 500})
    assert len(body["items"]) == 40
    foreign = _load("full/dbbl_games_2024_first40.json")
    foreign["items"][0]["competition"] = "BBL"
    net.reply(_Resp(200, foreign))
    with pytest.raises(AssetFetchError, match="on the dbbl host"):
        R._get(BBL + "/games", {"league": "dbbl", "seasonId": 2024, "currentPage": 1, "pageSize": 500})


def test_a_connection_failure_is_an_asset_fetch_error_not_empty(net, monkeypatch):
    def boom(session, method, url, *a, **k):
        if "easycredit" in url:
            return net.key_page
        raise ConnectionError("reset")

    monkeypatch.setattr(BR.requests.Session, "request", boom)
    with pytest.raises(AssetFetchError, match="fetch failed after retries"):
        R._get(BBL + "/seasons", {}, num_retries=0)


# --- YAML and the spec ------------------------------------------------------------------------------


def test_yaml_lists_every_route_and_wrapper():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 17
    assert {e["short"] for e in doc["endpoints"]} == set(gen_scb.ENDPOINTS) and len(doc["endpoints"]) == 23
    assert {e[0] for e in gen_scb.ENDPOINTS.values()} == spec_paths
    wrapper_paths = {e["path"] for e in doc["endpoints"]}
    assert {re.sub(r"\{[^}]+\}", "{}", p) for p in wrapper_paths} == {re.sub(r"\{[^}]+\}", "{}", p) for p in spec_paths}
    assert doc["name_pattern"] == "scb_{short}" and doc["parser_columns"] is True and doc["schema_prefix"] == "scb_"
    assert doc["getter_module"] == "sportsdataverse.basketball.scb.scb_runtime"


def test_no_path_token_named_league_or_sport():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for e in doc["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]
        assert e["returns_schema"] == f"native/scb/{e['short']}" and (SCHEMA_DIR / f"{e['short']}.yaml").exists()


def test_every_dual_host_wrapper_takes_league_and_the_bbl_only_ones_do_not():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    bbl_only = {"referees", "transfers", "transfers_teams", "team_rosters"}
    for e in doc["endpoints"]:
        names = [p["name"] for p in e.get("extra_params", [])]
        assert ("league" in names) == (e["short"] not in bbl_only), e["short"]
        if "league" in names:
            league = e["extra_params"][names.index("league")]
            assert league["choices"] == ["bbl", "dbbl"] and league["default"] == "bbl"


def test_the_yaml_docstring_carries_the_legal_notices():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    notes = " ".join(doc["docstring"]["notes"])
    assert "Impressum" in notes and "written permission" in notes and "DBBL" in notes
    assert KEY not in notes and "publicWebUser" not in notes  # the constant header value is not a note either


def test_descriptions_cover_every_column_and_are_not_filler():
    notes = yaml.safe_load(NOTES.read_text(encoding="utf-8"))
    for short in gen_scb.ENDPOINTS:
        cols = [c["name"] for c in _schema(short)["columns"]]
        assert sorted(notes[f"scb_{short}"]) == sorted(cols), short
        for col, desc in notes[f"scb_{short}"].items():
            assert len(desc) >= 15 and desc.lower() != col.lower(), (short, col, desc)


def test_the_unverified_caveats_stay_in_the_descriptions():
    notes = yaml.safe_load(NOTES.read_text(encoding="utf-8"))
    assert "unverified" in notes["scb_game_actions"]["shot_x"] and "unverified" in notes["scb_game_actions"]["shot_y"]
    assert "table points" in notes["scb_standings"]["table_points"]


def test_committed_fixtures_hold_no_credentials():
    for path in FIXTURES.rglob("*.json"):
        text = path.read_text(encoding="utf-8")
        assert "x-api-secret" not in text and "publicWebUser" not in text and R.REDACTED not in text, path
        assert re.search(r"\b[0-9a-f]{64}\b", text) is None, path  # the page key is 64 hex characters today


def test_summaries_are_prose_with_a_terminal_period():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for ep in doc["endpoints"]:
        s = ep["summary"]
        assert s.endswith(".") and "{" not in s and len(s.split()) > 1 and not s.startswith(("/", "GET ")), s


@pytest.mark.parametrize("short", sorted(gen_scb.ENDPOINTS))
def test_an_unusable_body_without_columns_still_has_the_documented_schema(short):
    """``parse_x(None)`` (no ``columns=``) is the documented zero-row frame, so it stacks onto a populated one."""
    parser = gen_scb.ENDPOINTS[short][2]
    cols = [c["name"] for c in _schema(short)["columns"]]
    full = parser(_body(short), columns=cols)
    none = parser(None)
    # DTYPES is per parser (several endpoints can share one), so the fallback may carry extra columns, in its own order
    assert none.height == 0 and none.width > 0
    shared = [c for c in cols if c in none.columns]  # a stamped column (``bucket``) is added outside DTYPES
    assert shared and all(none.schema[c] == full.schema[c] for c in shared)
    assert pl.concat([none, full], how="diagonal").height == full.height


def test_list_cells_are_compact_json_strings_never_list_columns():
    """Settles it: a list-valued field (standings ``last10_outcomes``, a player's ``previous_teams``) is JSON text."""
    import json

    seen = {}
    for short in gen_scb.ENDPOINTS:
        df = _parsed(short)
        assert not [c for c, t in df.schema.items() if isinstance(t, pl.List)], short
        for c in df.columns:
            if df.schema[c] == pl.String:
                vals = df[c].drop_nulls().to_list()
                lists = [v for v in vals if v.startswith("[")]
                if lists:
                    seen[(short, c)] = lists[0]
    assert ("standings", "last10_outcomes") in seen and ("season_player", "previous_teams") in seen
    for text in seen.values():
        assert isinstance(json.loads(text), list)
        assert text == json.dumps(json.loads(text), ensure_ascii=False, separators=(",", ":"))
