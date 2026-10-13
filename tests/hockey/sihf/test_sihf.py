"""Offline tests for the SIHF (``sihf_api_web``) flat-API family.

Asserts the parsers against the real captures in ``tests/fixtures/sihf_api_web/`` (never
synthetic payloads), the committed returns-schemas against what the parsers emit, the
wrappers' request construction through a fake getter, and the empty / malformed contract.
No network, except the gated live smokes at the bottom (``SDV_PY_LIVE_TESTS=1``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.hockey.sihf import sihf_api_web
from sportsdataverse.hockey.sihf import sihf_api_web_parsers as P
from sportsdataverse.hockey.sihf.sihf_api_web_parsers import (
    _col,
    parse_sihf_game_goalie_stats,
    parse_sihf_game_info,
    parse_sihf_game_lineups,
    parse_sihf_game_player_stats,
    parse_sihf_game_team_stats,
    parse_sihf_pbp,
    parse_sihf_phases,
    parse_sihf_table,
)
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sihf_api_web"
SCHEMA_DIR = Path(__file__).parents[3] / "tools" / "codegen" / "schemas" / "native" / "sihf_api_web"
GAME = "gameoverview__20261105000325.json"  # goaltender change + revoked goal
SHOOTOUT = "gameoverview__20261105000009.json"

GAME_PARSERS = {
    "pbp": parse_sihf_pbp,
    "game_info": parse_sihf_game_info,
    "game_player_stats": parse_sihf_game_player_stats,
    "game_goalie_stats": parse_sihf_game_goalie_stats,
    "game_team_stats": parse_sihf_game_team_stats,
    "game_lineups": parse_sihf_game_lineups,
}
# The 22 detail tables: wrapper short -> SIHF table alias (11 team, 8 skater, 3 goaltender).
DETAIL_ALIASES = {
    "team_goals": "teamGoal",
    "team_goals_by_position": "teamGoalPosNation",
    "team_shots": "teamShotDetail",
    "team_power_play": "teamPp",
    "team_penalty_kill": "teamPk",
    "team_penalties": "teamFoul",
    "team_shootouts": "teamShootout",
    "team_faceoffs": "teamFaceoff",
    "team_faceoffs_by_zone": "teamFaceoffZone",
    "team_faceoffs_by_zone_per_game": "teamFaceoffZoneGame",
    "team_attendance": "teamSpectator",
    "player_goals_assists": "playerGoalAssist",
    "player_shots": "playerShotDetail",
    "player_penalties": "playerFoul",
    "player_shootouts": "playerShootout",
    "player_faceoffs": "playerFaceoff",
    "player_faceoffs_by_zone": "playerFaceoffZone",
    "player_faceoffs_by_zone_per_game": "playerFaceoffZoneGame",
    "player_time_on_ice": "playerTimeOnIce",
    "goalie_shots": "goalkeeperShotDetail",
    "goalie_wins": "goalkeeperWin",
    "goalie_shootouts": "goalkeeperShootout",
}
TABLES = {
    "results": "table__results__2026__1.json",
    "standings": "table__standing__2026__1.json",
    "player_stats": "table__player__2026__1.json",
    "goalie_stats": "table__goalkeeper__2026__1.json",
    **{short: f"table__{alias}__2026__1.json" for short, alias in DETAIL_ALIASES.items()},
}
# Whole (untruncated) 2025-26 National League bodies: the numeric dtypes are checked against these.
FULL = sorted((FIXTURES / "full").glob("*.json"))
PHASES = "table__results__2026__1__phases.json"


def _load(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --- tables -------------------------------------------------------------------


def test_results_expand_typed_cells():
    df = parse_sihf_table(_load(TABLES["results"]))
    assert df.height == 3
    first = df.row(0, named=True)
    assert (first["season"], first["phase_id"], first["row_type"]) == (2026, None, "data")
    assert "league_id" not in df.columns  # the league travels in searchQuery, which is not echoed
    # season order: the opener first
    assert (first["home_team_name"], first["away_team_name"]) == ("HC Ambri-Piotta", "EHC Kloten")
    assert first["date"].isoformat() == "2025-09-09"
    assert (first["score_home"], first["score_away"], first["decision"]) == (2, 1, None)  # "" = regulation
    assert (first["score_per_period_home"], first["score_per_period_away"]) == ([0, 1, 1], [0, 1, 0])
    assert first["game_id"] == 20261105000001
    assert df.schema["date"] == pl.Date and df.schema["game_id"] == pl.Int64
    assert df.schema["home_team_id"] == pl.Int64
    assert df.schema["phase_id"] == pl.Int64  # Int64 even when every row is null


def test_current_season_results_cover_the_whole_schedule():
    # captured with the season-wide Date range: without it the current season answers one game day
    df = parse_sihf_table(_load("table__results__2027__1.json"))
    assert set(df["season"]) == {2027}
    assert df["date"].min().isoformat() >= "2026-07-01"


def test_standings_types_and_order():
    df = parse_sihf_table(_load(TABLES["standings"]))
    assert df.height == 14
    assert df["rank"].to_list() == list(range(1, 15))
    assert df.schema["points_quotient"] == pl.Float64
    assert df.schema["goals_difference"] == pl.Int64
    assert set(df["row_type"]) == {"data"}  # the "line" highlights are separators, not summary rows


def test_standings_are_the_full_regular_season_table():
    # Captured with ContentType=All. Without it SIHF serves the away-games table: 26 GP each, and
    # HC Davos on 50 points instead of 117 (measured 2026-10-12 against the season's results).
    df = parse_sihf_table(_load(TABLES["standings"]))
    assert set(df["games_played"]) == {52}  # 14 teams x 4 meetings
    assert df["games_played"].sum() == 2 * 364  # every one of the 364 regular-season games, twice
    top = df.row(0, named=True)
    assert (top["team_acronym"], top["points"]) == ("HCD", 117)
    wins = ["games_won_in_time", "games_won_extra_time", "games_won_penalty"]
    losses = ["games_lost_in_time", "games_lost_extra_time", "games_lost_penalty"]
    assert (df.select(pl.sum_horizontal(wins + losses)).to_series() == df["games_played"]).all()
    assert set(parse_sihf_table(_load("table__standing__2025__1.json"))["games_played"]) == {52}


def test_a_second_league_publishes_the_documented_standings_columns():
    # The columns come from each league's own header; the MyHockey League's matches the National League's.
    df = parse_sihf_table(_load("table__standing__2026__100.json"))
    assert df.height == 11 and set(df["season"]) == {2026}
    assert df.schema == parse_sihf_table(_load(TABLES["standings"])).schema


def test_goalie_table_tags_the_league_average_row():
    df = parse_sihf_table(_load(TABLES["goalie_stats"]))
    avg = df.filter(pl.col("row_type") == "average")
    assert avg.height == 1
    assert avg["team_name"].item() == "Durchschnitt"
    assert avg["rank"].item() is None  # "" upstream
    assert df.schema["seconds_played"] == pl.String  # "mm:ss", despite the name


def test_second_season_fixture_echoes_its_season():
    df = parse_sihf_table(_load("table__standing__2025__1.json"))
    assert set(df["season"]) == {2025}
    assert "Fribourg-Gottéron" in parse_sihf_table(_load(TABLES["standings"]))["team_name"].to_list()  # UTF-8 intact


def test_all_blank_table_columns_keep_their_documented_dtypes():
    # A column blank in every row infers as Null, a periods column of empty lists as List(Null),
    # and season is null when the body does not echo it: each still gets its documented dtype.
    body = _load(TABLES["results"])
    header = [h["alias"] for h in body["header"]]
    for row in body["data"]:
        row[header.index("date")] = ""
        row[header.index("decision")] = ""
        row[header.index("scorePerPeriod")] = {"type": "periods", "homeTeam": [], "awayTeam": []}
    body["filters"] = [f for f in body["filters"] if f.get("alias") != "Season"]
    df = parse_sihf_table(body)
    assert df.schema == parse_sihf_table(_load(TABLES["results"])).schema
    assert df["date"].null_count() == df["season"].null_count() == df.height


def test_table_pandas():
    assert isinstance(parse_sihf_table(_load(TABLES["standings"]), return_as_pandas=True), pd.DataFrame)


# --- the 22 detail tables -------------------------------------------------------

_SCHEMA_TYPES = {pl.Int64: "integer", pl.Float64: "double", pl.String: "character"}


@pytest.mark.parametrize("short", sorted(DETAIL_ALIASES))
def test_detail_table_parses_with_snake_case_and_int_ids(short):
    df = parse_sihf_table(_load(TABLES[short]))
    assert df.height == 3 and set(df["season"]) == {2026}
    assert all(c == c.lower() and " " not in c for c in df.columns)
    assert df.schema["team_id"] == pl.Int64 and df["team_id"].null_count() == 0
    assert set(df["row_type"]) == {"data"}


@pytest.mark.parametrize("path", FULL, ids=lambda p: p.stem)
def test_full_body_numeric_columns_parse_every_cell(path):
    # The dtypes are re-derived from WHOLE bodies: an Int64 / Float64 column must parse every cell
    # that is not SIHF's "-" / "" placeholder (a decimal in an Int64 column would become null).
    body = json.loads(path.read_text(encoding="utf-8"))
    df = parse_sihf_table(body)
    assert df.height == len(body["data"]) == (body["pages"] or len(body["data"]))
    for i, h in enumerate(body["header"]):
        col = _col(h["alias"])
        if col in df.columns and df.schema[col] in (pl.Int64, pl.Float64):
            filled = sum(1 for r in body["data"] if str(r[i]).strip() not in ("", "-"))
            assert df[col].drop_nulls().len() == filled, col
    short = next(s for s, a in DETAIL_ALIASES.items() if path.name == f"table__{a}__2026__1.json")
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [(c["name"], c["type"]) for c in schema["columns"]] == [(c, _SCHEMA_TYPES[t]) for c, t in df.schema.items()]


def test_mm_ss_times_stay_strings_and_the_placeholder_is_null():
    df = parse_sihf_table(json.loads((FIXTURES / "full" / "table__playerTimeOnIce__2026__1.json").read_text()))
    assert df.schema["time_on_ice"] == df.schema["time_on_ice_per_shift"] == pl.String
    assert df.schema["shifts_per_game"] == pl.Float64
    assert df["time_on_ice_per_shift"].null_count() == 4  # "-" for the four skaters without a shift
    assert df["position"].null_count() == 2  # "-" upstream
    assert df["time_on_ice"].drop_nulls().str.contains(r"^\d+:\d{2}$").all()


@pytest.mark.parametrize(
    "alias",
    ["teamGoal", "teamGoalPosNation", "teamShotDetail", "teamPp", "teamFaceoffZone", "teamShootout"],
)
def test_team_table_league_average_row(alias):
    df = parse_sihf_table(json.loads((FIXTURES / "full" / f"table__{alias}__2026__1.json").read_text()))
    assert df.height == 15
    avg = df.filter(pl.col("row_type") == "average")
    assert avg.select("rank", "team_id", "team_name", "team_acronym").rows() == [(None, None, "Durchschnitt", None)]
    teams = df.filter(pl.col("row_type") == "data")
    assert teams.height == 14 and teams["team_id"].null_count() == 0 and teams["rank"].null_count() == 0


def test_goal_tables_balance_league_wide():
    teams = parse_sihf_table(json.loads((FIXTURES / "full" / "table__teamGoal__2026__1.json").read_text())).filter(
        pl.col("row_type") == "data"
    )
    assert teams["goals_for"].sum() == teams["goals_against"].sum()


def test_attendance_drops_the_thousands_separator_and_tags_the_total_row():
    df = parse_sihf_table(json.loads((FIXTURES / "full" / "table__teamSpectator__2026__1.json").read_text()))
    total = df.filter(pl.col("row_type") == "total")
    assert total.select("team_id", "team_name", "spectators", "home_games").rows() == [(None, "Total", 3458381, 460)]
    teams = df.filter(pl.col("row_type") == "data")
    assert teams["spectators"].sum() == 3458381  # "3’458’381" upstream; the 14 teams add up to the total
    assert teams["home_games"].sum() == 460
    # quotients are per-game rates: Float64 like every other *_quotient, even when SIHF rounds them
    assert df.schema["spectators_quotient"] == df.schema["stadium_capacity_quotient"] == pl.Float64
    assert teams.filter(pl.col("team_name") == "HC Ajoie")["spectators_quotient"].to_list() == [4231.0]


def test_every_quotient_alias_is_a_float_rate():
    """``*Quotient`` columns are ratios; time quotients (``mm:ss``) stay String, the rest are Float64."""
    quotients = {a for a in P._INT_ALIASES | P._FLOAT_ALIASES if a.endswith("Quotient")}
    assert quotients and quotients <= P._FLOAT_ALIASES
    assert not {a for a in P._INT_ALIASES if a.endswith("Quotient") or a.endswith("Percent")}


@pytest.mark.parametrize(
    "bad",
    [
        {"filters": [{"alias": ["Season"], "selected": 2026}, {"alias": {"x": 1}, "selected": 1}]},
        {"highlightedPositions": [{"style": ["total"], "rows": [1]}, {"style": {"a": 1}, "rows": [2]}]},
    ],
)
def test_an_unhashable_filter_alias_or_highlight_style_is_ignored_not_a_crash(bad):
    raw = json.loads((FIXTURES / "full" / "table__teamSpectator__2026__1.json").read_text())
    raw.update(bad)
    df = parse_sihf_table(raw)
    assert df.height == len(raw["data"])


def test_goalies_below_the_line_have_no_rank_but_are_not_summary_rows():
    df = parse_sihf_table(json.loads((FIXTURES / "full" / "table__goalkeeperWin__2026__1.json").read_text()))
    avg = df.filter(pl.col("row_type") == "average")
    assert avg.select("player", "team_id", "team_name").rows() == [("Durchschnitt", None, "Durchschnitt")]
    unranked = df.filter(pl.col("rank").is_null() & (pl.col("row_type") == "data"))
    assert unranked.height == 18 and unranked["team_id"].null_count() == 0


@pytest.mark.parametrize(
    "cell",
    [
        {"id": 100000, "name": "Durchschnitt", "acronym": "Durchschnitt", "signed": False, "type": "team"},
        {"name": "Durchschnitt"},  # the shape the 2026-10-12 refs samples document: no id, no type
        {"id": 100000, "name": "?", "type": "team"},
    ],
)
def test_the_average_row_is_found_without_highlighted_positions(cell):
    body = json.loads((FIXTURES / "full" / "table__teamGoal__2026__1.json").read_text())
    body["highlightedPositions"] = []
    avg_index = next(i for i, r in enumerate(body["data"]) if r[1].get("name") == "Durchschnitt")
    body["data"][avg_index][1] = cell
    df = parse_sihf_table(body)
    assert df["row_type"].to_list() == ["average" if i == avg_index else "data" for i in range(df.height)]
    assert df.row(avg_index, named=True)["team_id"] is None
    assert df.filter(pl.col("row_type") == "data")["team_id"].null_count() == 0


def test_the_legacy_goalie_table_average_row_nulls_its_pseudo_team_id():
    avg = parse_sihf_table(_load(TABLES["goalie_stats"])).filter(pl.col("row_type") == "average")
    assert avg.select("team_id", "team_acronym").rows() == [(None, None)]


def test_game_info_spectators_drop_the_thousands_separator():
    body = _load(GAME)
    body["details"]["venue"]["spectators"] = "6’130"
    assert parse_sihf_game_info(body)["spectators"].item() == 6130


# --- phases -------------------------------------------------------------------


def test_phases_split_the_game_count_and_drop_all():
    df = parse_sihf_phases(_load(PHASES))
    assert df.schema == {"season": pl.Int64, "phase_id": pl.Int64, "phase_name": pl.String, "n_games": pl.Int64}
    assert set(df["season"]) == {2026}
    assert df.row(0) == (2026, 5318, "Playoff Final", 7)
    assert df.filter(pl.col("phase_id") == 4940).row(0) == (2026, 4940, "Regular Season", 364)
    assert df.height == 7  # 8 entries less the "all" pseudo-entry


def test_standings_phase_count_is_not_a_game_count():
    # The standings table writes "Regular Season (3)" in every league; 3 is not its game count.
    df = parse_sihf_phases(_load("table__standing__2026__1__phases.json"))
    assert df.rows() == [(2026, 4940, "Regular Season", None)]


def test_a_phase_without_a_count_keeps_its_name():
    raw = _load(PHASES)
    raw["filters"][1]["entries"] = [{"alias": "657", "name": "Regular Season"}]
    assert parse_sihf_phases(raw).rows() == [(2026, 657, "Regular Season", None)]


@pytest.mark.parametrize(
    "entry",
    [
        {"alias": "657"},
        {"alias": "657", "name": "  "},
        {"alias": "657", "name": ["x"]},
        {"alias": "657", "name": {"a": 1}},
    ],
)
def test_a_phase_name_of_the_wrong_type_is_null(entry):
    raw = _load(PHASES)
    raw["filters"][1]["entries"] = [entry]
    assert parse_sihf_phases(raw).rows() == [(2026, 657, None, None)]


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        [],
        "x",
        {"filters": [{"alias": "Season", "selected": "2026"}]},
        # a block of the wrong type reads as absent (``x or []`` let a number through and raised)
        {"filters": 5},
        {"header": 5, "filters": [{"alias": "Phase", "entries": 5}]},
        {"filters": [{"alias": "Phase", "entries": "4940"}, "x"]},
    ],
)
def test_phases_parser_empty_keeps_its_columns(payload):
    df = parse_sihf_phases(payload)
    assert df.height == 0
    assert df.schema == {"season": pl.Int64, "phase_id": pl.Int64, "phase_name": pl.String, "n_games": pl.Int64}


# --- the game page ------------------------------------------------------------


def test_pbp_events_in_game_order():
    df = parse_sihf_pbp(_load(GAME))
    assert df["event_index"].to_list() == list(range(1, df.height + 1))
    seconds = df["game_seconds"].to_list()
    assert seconds == sorted(seconds)
    assert set(df["event_type"]) == {"goal", "penalty", "goalie", "revoked_goal"}
    goals = df.filter(pl.col("event_type") == "goal")
    last = goals.row(-1, named=True)
    assert (last["score_home"], last["score_away"]) == (4, 3)
    assert goals["player_name"].null_count() == 0
    # plus / minus are listed for even-strength goals only
    pp = goals.filter(pl.col("strength") == "PP1")
    assert pp.height and all(v == [] for v in pp["plus_player_ids"].to_list())
    eq = goals.filter(pl.col("strength") == "EQ")
    assert all(len(v) == 5 for v in eq["plus_player_ids"].to_list())


def test_pbp_goalie_change_and_revoked_goal():
    df = parse_sihf_pbp(_load(GAME))
    changes = df.filter(pl.col("event_type") == "goalie").select("time", "player_name", "goalie_change").rows()
    assert ("36:24", "Boltshauser Luca", "out") in changes
    assert ("36:24", "Meyer Robin", "in") in changes
    revoked = df.filter(pl.col("event_type") == "revoked_goal")
    assert revoked["revoke_reason"].to_list() == ["CC-Offiside"]


def test_pbp_shootout_rows_follow_the_periods():
    df = parse_sihf_pbp(_load(SHOOTOUT))
    so = df.filter(pl.col("event_type") == "shootout")
    assert so["shootout_number"].to_list() == list(range(1, 11))
    assert so["shootout_scored"].sum() == 3
    assert so["goalie_name"].null_count() == 0
    assert set(so["team_side"]) == {"home", "away"}
    assert df["event_type"].to_list()[-so.height :] == ["shootout"] * so.height


def test_game_info_one_row():
    row = parse_sihf_game_info(_load(GAME)).row(0, named=True)
    assert row["game_id"] == 20261105000325
    assert (row["home_team_name"], row["away_team_name"]) == ("HC Davos", "SCL Tigers")
    assert (row["home_score"], row["away_score"]) == (4, 3)
    assert sum(row["home_score_by_period"]) == row["home_score"]
    assert len(row["period_indicators"]) == len(row["home_shots_by_period"])
    assert row["spectators"] and row["referees"]


def test_game_info_missing_names_stay_null():
    # A missing indicator / official name / broadcaster must be a null, never the string "None".
    body = _load(GAME)
    for s in body["result"]["scores"]:
        del s["indicator"]
    body["details"]["referees"] = [{"fullName": None}, {}]
    body["details"]["linesmen"] = [{"fullName": "A B"}, {"fullName": None}]
    body["broadcasts"] = {"entries": [None]}
    row = parse_sihf_game_info(body).row(0, named=True)
    assert row["period_indicators"] == [None] * len(row["home_score_by_period"])
    assert (row["referees"], row["linesmen"], row["broadcasts"]) == (None, "A B", None)


def test_game_player_stats_resolve_player_ids():
    df = parse_sihf_game_player_stats(_load(GAME))
    players = df.filter(pl.col("row_type") == "data")
    assert players["player_id"].null_count() == 0
    assert set(df["team_side"]) == {"home", "away"}
    totals = df.filter(pl.col("row_type") == "total")
    assert totals.height == 2
    for side in ("home", "away"):
        side_players = players.filter(pl.col("team_side") == side)
        side_total = totals.filter(pl.col("team_side") == side)
        assert side_players["goals"].sum() == side_total["goals"].item()


def test_a_shared_jersey_leaves_the_player_id_null():
    # Two roster players on one team with one jersey: the box-score row cannot be resolved, so no guess.
    body = _load(GAME)
    box = parse_sihf_game_player_stats(body).filter(pl.col("row_type") == "data").row(0, named=True)
    twin = next(p for p in body["players"] if str(p["id"]) == str(box["player_id"]))
    body["players"].append({**twin, "id": 99999999, "fullName": "Twin Player"})
    again = parse_sihf_game_player_stats(body).filter(
        (pl.col("team_side") == box["team_side"]) & (pl.col("player_number") == box["player_number"])
    )
    assert again["player_id"].to_list() == [None]


def test_game_goalie_and_team_stats():
    goalies = parse_sihf_game_goalie_stats(_load(GAME))
    assert goalies.height == 4 and goalies["player_id"].null_count() == 0
    stats = parse_sihf_game_team_stats(_load(GAME))
    sog = stats.filter(pl.col("statistic") == "SOG Total").row(0, named=True)
    info = parse_sihf_game_info(_load(GAME)).row(0, named=True)
    assert int(sog["home_value"]) == sum(info["home_shots_by_period"])


def test_game_lineups_units_and_absentees():
    df = parse_sihf_game_lineups(_load(GAME))
    dressed = df.filter(pl.col("unit").is_not_null())
    assert set(dressed["unit"]) >= {"goalkeeper", "defense_left", "forward_center"}
    assert dressed.filter(pl.col("is_captain")).height == 2  # one per side
    absent = df.filter(pl.col("unit").is_null())
    assert absent.height and absent["absence_reason"].null_count() == 0
    assert df["player_name"].null_count() == 0


# --- contract -----------------------------------------------------------------


# Badly typed nested blocks: each must read as absent, never raise (the parser contract).
_MALFORMED_GAMES = [
    {"gameId": 1, "details": ["x"]},
    {"gameId": 1, "details": {"homeTeam": "x", "awayTeam": 5, "venue": [], "referees": "x"}},
    {"gameId": 1, "summary": "x", "result": 5, "status": [], "stats": "x", "lineUps": [1], "players": 7},
    {"gameId": 1, "summary": {"periods": [{"goals": "x", "fouls": [5]}, "x"], "shootout": "x"}},
    {"gameId": 1, "result": {"scores": "x", "sogs": 3}, "broadcasts": "x", "league": "x"},
    {"gameId": 1, "stats": [{"alias": "gameTeamStats", "data": 5}], "lineUps": {"homeTeam": {"taggedPlayers": 1}}},
]


def _mutated(name: str, change: Any) -> Dict[str, Any]:
    body = _load(name)
    change(body)
    return body


def _team_stat_object(b: Dict[str, Any]) -> None:
    next(t for t in b["stats"] if t.get("alias") == "gameTeamStats")["data"][0][0] = {"x": 1}


def _mixed_table_cell(b: Dict[str, Any]) -> None:
    b["data"][0][0] = ["Mo"]  # a list in one row, strings in the others


# A leaf scalar of the wrong type (re-review R779-3): each used to raise polars' ComputeError.
_WRONG_LEAF_TYPES = [
    ("pbp", GAME, lambda b: b["summary"]["periods"][0].update(name={"x": 1})),
    ("game_info", GAME, lambda b: b["details"]["homeTeam"].update(name={"x": 1})),
    ("game_lineups", GAME, lambda b: b["details"]["homeAbsentees"][0].update(acronym=["SU"])),
    ("game_info", GAME, lambda b: b["details"]["venue"].update(name=["x"])),
    ("game_team_stats", GAME, _team_stat_object),
    ("results", TABLES["results"], _mixed_table_cell),
    # an unhashable team name must not break the summary-row check
    ("team_goals", TABLES["team_goals"], lambda b: b["data"][0][1].update(name=["Durchschnitt"])),
]


@pytest.mark.parametrize(("short", "fname", "change"), _WRONG_LEAF_TYPES, ids=[s for s, _, _ in _WRONG_LEAF_TYPES])
def test_a_leaf_of_the_wrong_type_becomes_null_without_raising(short, fname, change):
    parser = GAME_PARSERS.get(short, parse_sihf_table)
    clean = parser(_load(fname))
    df = parser(_mutated(fname, change))
    assert df.schema == clean.schema
    assert df.height == clean.height


def test_pbp_event_with_a_non_string_text_is_kept():
    df = parse_sihf_pbp({"gameId": 1, "summary": {"periods": [{"name": "1", "goals": [{"text": 5, "time": "01:00"}]}]}})
    assert df.height == 1
    assert df.row(0, named=True)["goal_tags"] is None


@pytest.mark.parametrize("payload", [None, {}, {"gameId": None}, [], "x", *_MALFORMED_GAMES])
@pytest.mark.parametrize("short", sorted(GAME_PARSERS))
def test_game_parsers_empty_keep_their_columns(short, payload):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    df = GAME_PARSERS[short](payload)
    # game_info keeps its one row (the game id is there); everything else has nothing to show
    assert df.height == (1 if short == "game_info" and isinstance(payload, dict) and payload.get("gameId") else 0)
    assert df.columns == [c["name"] for c in schema["columns"]]


_EMPTY_TABLES = [None, {}, [], "x", {"header": [], "data": []}, {"header": [{"alias": "a"}], "data": "x"}]


@pytest.mark.parametrize("payload", _EMPTY_TABLES)
@pytest.mark.parametrize("short", sorted(TABLES))
def test_table_parser_empty_keeps_the_documented_schema(short, payload):
    # The columns AND dtypes of an empty table are the ones the real capture parses to.
    names = [c["name"] for c in yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]]
    df = parse_sihf_table(payload, columns=names)
    assert df.height == 0
    assert df.columns == names
    assert df.schema == parse_sihf_table(_load(TABLES[short])).schema


@pytest.mark.parametrize("payload", _EMPTY_TABLES)
def test_table_parser_empty_without_columns(payload):
    assert parse_sihf_table(payload).height == 0


@pytest.mark.parametrize(
    ("fn", "short"),
    [
        (sihf_api_web.sihf_results, "results"),
        (sihf_api_web.sihf_standings, "standings"),
        (sihf_api_web.sihf_player_stats, "player_stats"),
        (sihf_api_web.sihf_goalie_stats, "goalie_stats"),
    ],
)
def test_table_wrappers_return_the_documented_schema_when_empty(monkeypatch, fn, short):
    body = _load(TABLES[short])
    body["data"] = []  # a season / phase with no rows yet
    _capture(monkeypatch, body)
    df = fn(season=2026)
    assert df.height == 0
    assert df.schema == parse_sihf_table(_load(TABLES[short])).schema


@pytest.mark.parametrize(
    ("short", "fname"), [*TABLES.items(), *((s, GAME) for s in GAME_PARSERS)], ids=lambda v: str(v)
)
def test_committed_schema_matches_the_parser(short, fname):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    parser = {**GAME_PARSERS, "phases": parse_sihf_phases}.get(short, parse_sihf_table)
    df = parser(_load(fname))
    assert df.columns == [c["name"] for c in schema["columns"]]


# --- wrappers -----------------------------------------------------------------


def _capture(monkeypatch: pytest.MonkeyPatch, body: Any) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        calls.append({"url": url, "params": dict(params or {})})
        return body

    monkeypatch.setattr(sihf_api_web, "_get", fake)
    return calls


@pytest.mark.parametrize(
    ("fn", "alias"),
    [
        (sihf_api_web.sihf_results, "results"),
        (sihf_api_web.sihf_standings, "standing"),
        (sihf_api_web.sihf_player_stats, "player"),
        (sihf_api_web.sihf_goalie_stats, "goalkeeper"),
    ],
)
def test_table_wrappers_send_alias_and_filters(monkeypatch, fn, alias):
    calls = _capture(monkeypatch, _load(TABLES["standings"]))
    fn(season=2026, league_id=2)
    assert calls[0]["url"] == "https://data.sihf.ch/statistic/api/cms/table"
    p = calls[0]["params"]
    # the runtime moves League into searchQuery; the wrapper hands it over as a parameter
    assert (p["alias"], p["searchQuery"], p["Season"], p["League"], p["language"]) == (alias, "1//", 2026, 2, "en")
    # only standings take ContentType, and always send it (without it SIHF answers the away table)
    assert p.get("ContentType") == ("All" if alias == "standing" else None)


def test_phases_wrapper_asks_for_the_phase_list(monkeypatch):
    calls = _capture(monkeypatch, _load(PHASES))
    df = sihf_api_web.sihf_phases(season=2026, league_id=2, table="player")
    p = calls[0]["params"]
    # an empty Phase is what makes SIHF list the phases (the runtime folds it into filterQuery)
    assert (p["alias"], p["searchQuery"], p["Season"], p["League"], p["Phase"]) == ("player", "1//", 2026, 2, "")
    assert df.height == 7


def test_phases_table_is_a_closed_set():
    with pytest.raises(ValueError, match="table"):
        sihf_api_web.sihf_phases(season=2026, table="player_stats")


def test_game_wrapper_sends_game_id(monkeypatch):
    calls = _capture(monkeypatch, _load(GAME))
    df = sihf_api_web.sihf_pbp(game_id=20261105000325)
    assert calls[0]["params"]["alias"] == "gameDetail"
    assert calls[0]["params"]["searchQuery"] == 20261105000325
    assert df.height > 0
    raw = sihf_api_web.sihf_pbp(game_id=20261105000325, return_parsed=False)
    assert raw["gameId"] == "20261105000325"


_ENDPOINTS = yaml.safe_load(
    (Path(__file__).parents[3] / "tools" / "codegen" / "endpoints" / "sihf_api_web.yaml").read_text(encoding="utf-8")
)["endpoints"]


@pytest.mark.parametrize("ep", _ENDPOINTS, ids=lambda ep: ep["short"])
def test_the_documented_example_url_is_the_request_the_wrapper_sends(monkeypatch, ep):
    # The docstring's Example URL (and the docs page's Valid URL) must be what goes on the wire,
    # i.e. AFTER sihf_runtime._build_query folds League / Season / Phase / Date / ContentType.
    from urllib.parse import urlencode

    from sportsdataverse.hockey.sihf import sihf_runtime

    sent: List[str] = []

    class _Stop(Exception):
        pass

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        sent.append(f"{url}?{urlencode(params or {}, doseq=True)}")
        raise _Stop

    monkeypatch.setattr(sihf_runtime._rt, "_get", fake)
    fn = _wrapper(ep["short"])
    with pytest.raises(_Stop):
        fn(**ep["example_args"])
    documented = next(
        line.split("Example URL:", 1)[1].strip() for line in (fn.__doc__ or "").splitlines() if "Example URL:" in line
    )
    assert sent == [documented]


_ALL_TABLE_ALIASES = {"results", "standing", "player", "goalkeeper", *DETAIL_ALIASES.values()}


def test_yaml_wraps_every_table_alias_once():
    # sdv-internal-refs sihf/ documents 26 table aliases: results, standing, 11 team, 9 skater, 4 goalie.
    table_eps = [ep for ep in _ENDPOINTS if ep["path"] == "/table" and ep["short"] != "phases"]
    aliases = [ep["fixed_params"]["alias"] for ep in table_eps]
    assert len(aliases) == len(set(aliases)) == 26
    assert set(aliases) == _ALL_TABLE_ALIASES
    for ep in table_eps:
        assert ep["parser"] == "parse_sihf_table"
        assert ep["returns_schema"] == f"native/sihf_api_web/{ep['short']}"
        assert (SCHEMA_DIR / f"{ep['short']}.yaml").exists()
        assert [p["name"] for p in ep["extra_params"]][:3] == ["season", "league_id", "phase_id"]


def _wrapper(short: str) -> Any:
    """The generated wrapper; the 22 detail wrappers exist once integration runs codegen."""
    fn = getattr(sihf_api_web, f"sihf_{short}", None)
    if fn is None:
        pytest.skip(f"sihf_{short} is generated by tools/codegen/generate.py (integration, not this branch)")
    return fn


@pytest.mark.parametrize(("short", "alias"), sorted(DETAIL_ALIASES.items()))
def test_detail_wrappers_send_alias_and_filters(monkeypatch, short, alias):
    fn = _wrapper(short)
    calls = _capture(monkeypatch, _load(TABLES[short]))
    df = fn(season=2026, league_id=2, phase_id=4940)
    assert calls[0]["url"] == "https://data.sihf.ch/statistic/api/cms/table"
    p = calls[0]["params"]
    assert (p["alias"], p["searchQuery"], p["Season"], p["League"], p["Phase"]) == (alias, "1//", 2026, 2, 4940)
    assert "ContentType" not in p
    assert df.height == 3


@pytest.mark.parametrize("short", sorted(DETAIL_ALIASES))
def test_detail_wrappers_return_the_documented_schema_when_empty(monkeypatch, short):
    fn = _wrapper(short)
    body = _load(TABLES[short])
    body["data"] = []
    _capture(monkeypatch, body)
    assert fn(season=2026).schema == parse_sihf_table(_load(TABLES[short])).schema


def test_language_is_a_closed_set():
    with pytest.raises(ValueError, match="language"):
        sihf_api_web.sihf_results(season=2026, language="xx")


# --- live (gated) ---------------------------------------------------------------


@skip_if_no_live
def test_live_standings_and_game():
    from sportsdataverse.errors import NoDataError

    df = sihf_api_web.sihf_standings(season=2026, league_id=1)
    assert df.height == 14 and set(df["season"]) == {2026}
    assert set(df["games_played"]) == {52}  # the full table, not the away table SIHF serves by default
    pbp = sihf_api_web.sihf_pbp(game_id=20261105000421)
    assert pbp.filter(pl.col("event_type") == "goal").height == 5
    with pytest.raises(NoDataError):
        sihf_api_web.sihf_results(season=1890)
    with pytest.raises(NoDataError):
        sihf_api_web.sihf_standings(season=2026, league_id=999)


@skip_if_no_live
def test_live_phases_feed_the_table_wrappers():
    from sportsdataverse.errors import NoDataError

    phases = sihf_api_web.sihf_phases(season=2026, league_id=1)
    final = phases.filter(pl.col("phase_name") == "Playoff Final").row(0, named=True)
    games = sihf_api_web.sihf_results(season=2026, league_id=1, phase_id=final["phase_id"])
    assert games.height == final["n_games"]
    overall = sihf_api_web.sihf_phases(season=2026, league_id=1, table="player")
    assert "Playoff Overall" in set(overall["phase_name"])
    with pytest.raises(NoDataError):
        sihf_api_web.sihf_phases(season=1890)


@skip_if_no_live
def test_live_current_season():
    # 2026-27: the season that answered 400 MissingLeagueId before the league moved into searchQuery
    results = sihf_api_web.sihf_results(season=2027)
    assert results.height > 300 and set(results["season"]) == {2027}
    assert sihf_api_web.sihf_standings(season=2027).height == 14
    assert sihf_api_web.sihf_player_stats(season=2027).height > 100
