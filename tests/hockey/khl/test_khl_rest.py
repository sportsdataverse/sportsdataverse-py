"""Offline tests for the KHL site REST (``khl_rest``) parsers, on the committed real captures.

``tests/fixtures/khl_rest/*.json`` are the truncated refs captures (3 rows per array);
``full/game__text.json`` is one complete game page (2025-26 game 898236), the source of the
game-page dtypes. No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.hockey.khl import khl_rest_parsers as P

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "khl_rest"
YAML = ROOT / "tools" / "codegen" / "endpoints" / "khl_rest.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "khl_rest"
SPEC_ROUTES = FIXTURES / "spec_routes.json"  # the refs spec's route list, committed by gen_khl.py
FULL = "full/game__text.json"

PARSERS: Dict[str, Callable[..., Any]] = {
    "calendar__list.json": P.parse_khl_schedule,
    "game__header.json": P.parse_khl_game_header,
    "game__text.json": P.parse_khl_game_info,
    "game__protocol.json": P.parse_khl_game_protocol,
    "standings__regular.json": P.parse_khl_standings,
    "standings__playoff.json": P.parse_khl_playoff_bracket,
    "stat__players.json": P.parse_khl_player_stats,
    "players__profile.json": P.parse_khl_player_profile,
    "players__stats.json": P.parse_khl_player_career,
    "clubs__team.json": P.parse_khl_roster,
    "filters__statplayers.json": P.parse_khl_stat_filters,
}
GAME_TEXT = {
    "game_info": P.parse_khl_game_info,
    "player_box": P.parse_khl_player_box,
    "pbp": P.parse_khl_pbp,
    "shots": P.parse_khl_shots,
}
ALL_PARSERS = [*PARSERS.values(), *GAME_TEXT.values()]
_SNAKE = re.compile(r"^[a-z0-9_]+$")
_ID_COL = re.compile(r"(^|_)(id|club_id|player_id|tournament_id|team_season_id|game_id)$")


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def full() -> Any:
    return _load(FULL)


@pytest.mark.parametrize("fname", sorted(PARSERS))
def test_every_capture_parses_to_snake_case_rows_with_int_ids(fname):
    df = PARSERS[fname](_load(fname))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(_SNAKE.match(c) for c in df.columns), df.columns
    for col in df.columns:
        if _ID_COL.search(col):
            assert df.schema[col] == pl.Int64, (fname, col, df.schema[col])


@pytest.mark.parametrize("short", sorted(GAME_TEXT))
def test_game_text_shapers_on_the_full_body(full, short):
    df = GAME_TEXT[short](full)
    assert df.height > 0
    assert set(df["game_id"]) == {898236} and set(df["tournament_id"]) == {1369}


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize(
    "payload",
    [
        {},
        [],
        None,
        "x",
        {"status": "error", "data": None, "errors": [{"message": "x"}]},
        {"status": "success", "data": []},
    ],
    ids=["dict", "list", "none", "str", "error", "empty-data"],
)
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_empty_payload_keeps_the_documented_columns():
    df = P.parse_khl_roster({}, columns=["group", "player_id", "lastname"])
    assert df.schema == {"group": pl.Utf8, "player_id": pl.Int64, "lastname": pl.Utf8}


@pytest.mark.parametrize("parser", [P.parse_khl_schedule, P.parse_khl_pbp, P.parse_khl_player_box, P.parse_khl_shots])
def test_fixed_schema_shapers_keep_their_schema_when_empty(full, parser):
    source = _load("calendar__list.json") if parser is P.parse_khl_schedule else full
    assert parser({}).schema == parser(source).schema


def _body(ep: Dict[str, Any]) -> Any:
    fixture = {v.__name__: k for k, v in PARSERS.items()}
    return _load(FULL) if ep["path"] == "/game/text/" else _load(fixture[ep["parser"]])


ENDPOINTS = yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"]


@pytest.mark.parametrize("ep", ENDPOINTS, ids=lambda e: e["short"])
def test_an_empty_frame_concatenates_with_a_populated_one(ep):
    parser = getattr(P, ep["parser"])
    empty, full_df = parser({}), parser(_body(ep))
    assert empty.height == 0 and empty.schema == full_df.schema
    assert pl.concat([empty, full_df], how="vertical").height == full_df.height
    assert parser({}, columns=list(P.SCHEMAS[f"khl_{ep['short']}"])).schema == full_df.schema  # as the wrapper calls it


@pytest.mark.parametrize("ep", ENDPOINTS, ids=lambda e: e["short"])
def test_parser_schema_is_the_committed_returns_schema(ep):
    schema = yaml.safe_load((SCHEMAS / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
    code = {
        "integer": pl.Int64,
        "numeric": pl.Float64,
        "character": pl.Utf8,
        "logical": pl.Boolean,
        "list": pl.List(pl.Int64),
    }
    assert P.SCHEMAS[schema["schema"]] == {c["name"]: code[c["type"]] for c in schema["columns"]}


def test_a_sparse_body_is_backfilled_with_the_documented_dtypes():
    body = _load("clubs__team.json")
    for players in body["data"]["players"].values():
        for p in players:
            p.pop("age", None)
            p["favorite"] = None
    full_df = P.parse_khl_roster(_load("clubs__team.json"))
    sparse = P.parse_khl_roster(body)
    assert sparse.schema == full_df.schema and sparse["age"].null_count() == sparse.height


def test_return_as_pandas(full):
    assert isinstance(P.parse_khl_pbp(full, return_as_pandas=True), pd.DataFrame)
    assert isinstance(P.parse_khl_roster(_load("clubs__team.json"), return_as_pandas=True), pd.DataFrame)


# --- schedule -------------------------------------------------------------------------------


def test_schedule_drops_the_all_star_tournament():
    body = _load("calendar__list.json")
    week = body["data"]["GAMES"][0]
    day = next(iter(week))
    star = dict(week[day][0], id=901626, tnId=1423)  # All-Star games ride along under their own tnId
    week[day] = [*week[day], star]
    df = P.parse_khl_schedule(body)
    assert set(df["tournament_id"]) == {1369}
    assert 901626 not in df["game_id"].to_list()


def test_schedule_without_an_echo_keeps_the_majority_tournament():
    body = _load("calendar__list.json")
    del body["data"]["values"]
    assert set(P.parse_khl_schedule(body)["tournament_id"]) == {1369}


def test_schedule_ids_names_and_decision():
    df = P.parse_khl_schedule(_load("calendar__list.json"))
    first = df.row(0, named=True)
    assert first["home_club_id"] and first["home_name"]  # names joined from data.TEAMS by club id
    assert set(df["decision"].drop_nulls()) <= {"OT", "SO"}
    assert df["date"].is_sorted()


# --- game page --------------------------------------------------------------------------------


def test_game_info_separates_club_and_team_season_ids(full):
    row = P.parse_khl_game_info(full).row(0, named=True)
    assert (row["home_club_id"], row["home_team_season_id"]) == (7, 14934)  # Spartak: club 7, team-season 14934
    assert (row["away_club_id"], row["away_team_season_id"]) == (71, 14929)
    assert (row["home_score"], row["away_score"], row["ots"]) == (2, 3, "SO")


def test_game_info_goalie_jerseys_resolve_to_player_ids(full):
    row = P.parse_khl_game_info(full).row(0, named=True)
    assert (row["gk_a_jn"], row["gk_b_jn"]) == (40, 53)  # gameItem.gkA / gkB are jerseys
    players = full["data"]["game"]["players"]
    assert row["gk_a_player_id"] == int(players["A"]["40"]["id"])
    assert row["gk_b_player_id"] == int(players["B"]["53"]["id"])


def test_game_header_matches_game_info_ids():
    header = P.parse_khl_game_header(_load("game__header.json")).row(0, named=True)
    assert (header["game_id"], header["tournament_id"], header["home_club_id"]) == (898236, 1369, 7)


def test_player_box_resolves_jerseys_through_players(full):
    df = P.parse_khl_player_box(full)
    row = df.filter((pl.col("period") == 0) & (pl.col("team_side") == "A") & (pl.col("jn") == 4)).row(0, named=True)
    assert row["player_id"] == 41928  # players.A["4"].id
    assert df["player_id"].null_count() == 0
    assert set(df["period"]) == {0, 1, 2, 3, 4}


def test_player_box_dtypes_come_from_the_full_body(full):
    # fow_pct arrives as 0 (int) on some rows and "50.00" on others: always Float64
    raw = [r["stats"]["fow_pct"] for r in full["data"]["game"]["tab_stats"]["playerstats"]["A"][0]["data"]]
    assert {type(v) for v in raw} == {int, str}
    df = P.parse_khl_player_box(full)
    for col in ("fow_pct", "ts", "dt", "as", "pcd"):
        assert df.schema[col] == pl.Float64, col
    for col in ("g", "a", "shots", "pim", "fo", "fow", "bls", "hits", "fa", "p_intc"):
        assert df.schema[col] == pl.Int64, col
    assert df.schema["sft"] == pl.Utf8 and df["sft"].str.contains(r"^\d+:\d{2}\(\d+\)$").all()
    assert df["fow_pct"].max() > 1  # a real percentage, not a truncated integer


def test_pbp_is_chronological_and_resolves_every_jersey(full):
    df = P.parse_khl_pbp(full)
    plays = df.filter(pl.col("event_type") != "period")
    assert plays["time_seconds"].is_sorted()  # gameLegend is newest first; rows are oldest first
    assert df["event_index"].to_list() == list(range(df.height))
    assert set(df["event_type"]) >= {"period", "faceoff", "shot", "block", "action", "penalty", "goal"}
    player_events = df.filter(pl.col("jn").is_not_null() & (pl.col("jn") != 0))
    assert player_events["player_id"].null_count() == 0
    faceoffs = df.filter(pl.col("event_type") == "faceoff")
    assert faceoffs["won_player_id"].null_count() == 0 and faceoffs["lost_player_id"].null_count() == 0
    goals = df.filter(pl.col("event_type") == "goal")
    assert goals.select("score_a", "score_b").row(-1) == (2, 2)  # the shootout decided 2:3


def test_pbp_time_drift_is_normalised(full):
    body = json.loads(json.dumps(full))
    legend = body["data"]["game"]["tab_playbyplay"]["gameLegend"]
    shot = next(e for e in legend["1"] if e["type"] == "shot")
    shot["time"] = str(shot["time"])  # 2013-era bodies send the seconds as a numeric string
    del shot["timems"]
    df = P.parse_khl_pbp(body)
    assert df.schema["time_seconds"] == pl.Int64 and df.schema["time"] == pl.Utf8
    assert df.equals(P.parse_khl_pbp(full))
    markers = df.filter(pl.col("event_type") == "period")
    assert markers["time_seconds"].null_count() == markers.height  # their time is a wall-clock HH:MM


def test_shots_grid_and_players(full):
    df = P.parse_khl_shots(full)
    assert df["x"].is_between(0, 1000).all() and df["y"].is_between(0, 500).all()
    assert set(df["shot_type"]) <= {1, 2}
    assert df["player_id"].null_count() == 0
    assert df.schema["indent_left"] == pl.Float64


# --- other routes -----------------------------------------------------------------------------


def test_protocol_ids_come_from_the_pdf_links():
    df = P.parse_khl_game_protocol(_load("game__protocol.json"))
    assert set(df["game_id"]) == {898236} and set(df["tournament_id"]) == {1369}
    assert df["scorer_player_id"].null_count() == 0


def test_standings_split_goals_and_list_every_table():
    df = P.parse_khl_standings(_load("standings__regular.json"))
    assert {"league", "west", "east"} <= set(df["table"])
    top = df.filter(pl.col("table") == "league").row(0, named=True)
    assert (top["club_id"], top["goals_for"], top["goals_against"]) == (37, 252, 184)


def test_stat_players_parse_the_player_id_from_the_link():
    df = P.parse_khl_player_stats(_load("stat__players.json"))
    assert df["player_id"].to_list()[0] == 43033  # /players/43033/
    assert df.schema["sog_pct"] == pl.Float64 and df.schema["toi_avg"] == pl.Utf8


def test_player_career_keeps_both_team_id_spaces():
    df = P.parse_khl_player_career(_load("players__stats.json"))
    row = df.row(0, named=True)
    assert (row["player_id"], row["tournament_id"], row["club_id"], row["team_season_id"]) == (41928, 1436, 7, 15558)
    assert df.schema["fo_pct"] == pl.Float64  # "-" upstream -> null, still a rate


# --- YAML and schemas --------------------------------------------------------------------------


def test_yaml_lists_every_spec_route():
    doc = yaml.safe_load(YAML.read_text(encoding="utf-8"))
    paths = {e["path"] for e in doc["endpoints"]}
    assert paths == set(json.loads(SPEC_ROUTES.read_text(encoding="utf-8")))
    assert len(paths) == 11 and len(doc["endpoints"]) == 14  # /game/text/ feeds four wrappers
    assert doc["getter_module"] == "sportsdataverse.hockey.khl.khl_runtime" and doc["auth"] is True
    for ep in doc["endpoints"]:
        assert not {"league", "sport"} & {p["name"] for p in ep.get("extra_params", [])}
        assert ep["summary"].endswith(".") and not ep["summary"].startswith(("/", "GET ", "POST "))


@pytest.mark.parametrize("ep", yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"], ids=lambda e: e["short"])
def test_committed_schema_matches_the_parser(ep):
    schema = yaml.safe_load((SCHEMAS / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
    assert schema["schema"] == f"khl_{ep['short']}"
    fixture = {v: k for k, v in PARSERS.items() if v.__name__ == ep["parser"]}
    parser = getattr(P, ep["parser"])
    body = _load(FULL) if ep["path"] == "/game/text/" else _load(fixture[parser])
    assert [c["name"] for c in schema["columns"]] == parser(body).columns


def test_playoff_bracket_game_ids_are_a_list_of_int64() -> None:
    df = P.parse_khl_playoff_bracket(_load("standings__playoff.json"))
    assert df.schema["game_ids"] == pl.List(pl.Int64)
    assert [901691, 901699] in df["game_ids"].to_list()
    assert P.parse_khl_playoff_bracket({}).schema["game_ids"] == pl.List(pl.Int64)


def test_null_data_body_is_a_zero_row_documented_frame() -> None:
    df = P.parse_khl_playoff_bracket({"data": None})
    assert df.height == 0 and df.schema == P.parse_khl_playoff_bracket({}).schema


def test_goalie_stats_columns_are_declared_and_typed():
    DECL = list(P.SCHEMAS["khl_player_stats"])
    df = P.parse_khl_player_stats(_load("stat__players_goalies.json"), columns=DECL)
    goalie = ["w", "l", "sop", "ga", "sv", "sv_pct", "gaa", "so", "toi"]
    assert set(goalie) <= set(DECL)
    assert df["group"].unique().to_list() == ["goalies"]
    # gaa is a rate and toi text even on all-zero / "-" pages
    assert [df.schema[c] for c in goalie] == [pl.Int64] * 5 + [pl.Float64] * 2 + [pl.Int64, pl.Utf8]
    assert df.columns[: len(DECL)] == DECL  # no stray extras
    assert df["gp"].to_list() == [0, 0, 0]  # the site lists no-game goalies first (gaa ascending)
    assert df["toi"].to_list() == [None, None, None]  # the site's "-" placeholder is null, not text
    skaters = P.parse_khl_player_stats(_load("stat__players.json"), columns=DECL)
    assert skaters.select(goalie).null_count().row(0) == (skaters.height,) * len(goalie)
    assert [skaters.schema[c] for c in goalie] == [df.schema[c] for c in goalie]


def test_goalie_stats_descriptions_exist():
    manual = yaml.safe_load((ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml").read_text("utf-8"))
    assert {"w", "l", "sop", "ga", "sv", "sv_pct", "gaa", "so", "toi"} <= set(manual["khl_player_stats"])
