"""Lega Basket Serie A parsers run over the committed fixtures: FULL bodies for the dtypes, refs captures per route."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, List, Tuple

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball.lba import lba_dtypes, lba_parsers as P

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "legabasket"
ROOT = Path(__file__).resolve().parents[3]
SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")


def _load(name: str) -> Any:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def _cap(route: str, season: str = "2026") -> str:
    return f"captures/{season}/{route.strip('/').replace('/', '__')}.json"


# parser -> the fixtures it runs over: the full body first, then the refs captures of every season.
CASES: List[Tuple[str, str]] = [
    ("championships", "full/championships__s2026.json"),
    ("championships", _cap("/championships/get-championships", "1995")),
    ("calendar", "full/calendar__602__d1.json"),
    ("calendar", _cap("/championships/get-championships-calendar-by-id", "2017")),
    ("game", "full/matches__25602.json"),
    ("game", _cap("/championships/get-championships-matches-by-id", "1995")),
    ("box_score", "full/matches__25602.json"),
    ("box_score", _cap("/championships/get-championships-matches-scores-by-id", "2017")),
    ("box_score", _cap("/championships/get-championships-matches-scores-by-id", "1995")),
    ("team_box_score", "full/matches__25602.json"),
    ("team_box_score", _cap("/championships/get-championships-matches-scores-by-id", "2026")),
    ("pbp", "full/pbp__25602.json"),
    ("pbp", _cap("/championships/get-championships-matches-play-by-play", "2017")),
    ("pbp_legend", "full/pbp__25602.json"),
    ("shots", _cap("/matches/get-championship-matches-shots-by-id", "2026")),
    ("shots", _cap("/matches/get-championship-matches-shots-by-id", "2017")),
    ("standings", "full/ranking__602__d3.json"),
    ("standings", _cap("/championships/get-championship-ranking", "1995")),
    ("teams", "full/teams__2026.json"),
    ("teams", _cap("/teams/get-teams", "1995")),
    ("roster", _cap("/teams/get-team-roster", "2026")),
    ("roster", _cap("/teams/get-team-roster", "1995")),
    ("roster_coach", _cap("/teams/get-team-roster", "2026")),
    ("team_players_stats", "full/team_players_stats__1764.json"),
    ("team_players_stats", _cap("/teams/get-team-players-stats", "1995")),
    ("players", _cap("/players/get-players", "2026")),
    ("players", _cap("/players/get-players", "1995")),
    ("player", _cap("/players/get-player-by-id", "2026")),
    ("player_stats", _cap("/players/get-player-stats", "2026")),
    ("game_log", "full/player_matches__969.json"),
    ("game_log", _cap("/players/get-player-matches", "1995")),
    ("players_leaderboard", "full/players_statistics__points.json"),
    ("players_leaderboard", _cap("/statistics/get-players-statistics", "1995")),
    ("teams_leaderboard", "full/teams_statistics__offensive.json"),
    ("teams_leaderboard", _cap("/statistics/get-teams-statistics-by-type", "2017")),
]
SHORTS = sorted({s for s, _ in CASES})


def _parser(short: str):
    return getattr(P, f"parse_lba_{short}")


@pytest.mark.parametrize(("short", "fixture"), CASES)
def test_fixture_parses_to_a_non_empty_snake_case_frame_with_int64_ids(short, fixture):
    df = _parser(short)(_load(fixture))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(SNAKE.match(c) for c in df.columns), df.columns
    for col in df.columns:
        if re.search(r"(^|_)id$", col):
            assert df.schema[col] == pl.Int64, (short, col, df.schema[col])
    assert "season" not in df.columns or df.schema["season"] == pl.Int64


@pytest.mark.parametrize("short", SHORTS)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short):
    schema = lba_dtypes.DTYPES[f"parse_lba_{short}"]
    for raw in ({}, [], None, "x", 3, {"match": []}, {"standings": None}):
        df = _parser(short)(raw)
        assert df.height == 0
        assert list(df.columns) == list(schema)
        assert all(df.schema[c] == P._DTYPE_CODES[code] for c, code in schema.items())


@pytest.mark.parametrize("short", SHORTS)
def test_two_empty_frames_stack_and_an_empty_one_stacks_onto_a_populated_one(short):
    empty = _parser(short)({})
    assert pl.concat([empty, _parser(short)(None)]).height == 0
    full = next(f for s, f in CASES if s == short)
    populated = _parser(short)(_load(full))
    assert pl.concat([empty, populated]).height == populated.height


@pytest.mark.parametrize("short", SHORTS)
def test_return_as_pandas(short):
    import pandas as pd

    full = next(f for s, f in CASES if s == short)
    df = _parser(short)(_load(full), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) > 0
    assert isinstance(_parser(short)({}, return_as_pandas=True), pd.DataFrame)


@pytest.mark.parametrize("short", SHORTS)
def test_returns_schema_yaml_matches_the_parser_dtypes(short):
    y = yaml.safe_load((ROOT / f"tools/codegen/schemas/native/legabasket/{short}.yaml").read_text(encoding="utf-8"))
    assert y["schema"] == f"lba_{short}"
    assert [c["name"] for c in y["columns"]] == list(lba_dtypes.DTYPES[f"parse_lba_{short}"])
    assert all(c["description"] == "" for c in y["columns"])  # descriptions live in manual_column_descriptions.yaml


def test_the_yaml_lists_every_route_of_the_spec():
    spec_paths = set(json.loads((FIX / "spec_paths.json").read_text(encoding="utf-8")))
    doc = yaml.safe_load((ROOT / "tools/codegen/endpoints/legabasket.yaml").read_text(encoding="utf-8"))
    assert {e["path"] for e in doc["endpoints"]} == spec_paths
    assert len(spec_paths) == 16 and len(doc["endpoints"]) == 19
    assert doc["name_pattern"] == "lba_{short}" and doc["module"] == "legabasket"
    for e in doc["endpoints"]:
        for p in e.get("path_params", []):
            assert p["name"] not in ("league", "sport")  # the renderer substitutes those as ESPN slugs
    assert any("Non è consentito copiare" in n for n in doc["docstring"]["notes"])  # the legal notice


# --- numeric dtypes come from the FULL bodies -------------------------------------------------------------------------


def test_rates_are_float64_even_when_a_sample_holds_whole_numbers():
    box = P.parse_lba_box_score(_load("full/matches__25602.json"))
    for col in ("t2_perc", "t3_perc", "tl_perc", "val_oer"):
        assert box.schema[col] == pl.Float64, col
    # the 3-row truncated capture of 1995-96 may hold whole numbers only: the schema still types them Float64
    old = P.parse_lba_box_score(_load(_cap("/championships/get-championships-matches-scores-by-id", "1995")))
    assert old.schema["t2_perc"] == pl.Float64
    assert P.parse_lba_standings(_load("full/ranking__602__d3.json")).schema["avg_points_made"] == pl.Float64
    assert P.parse_lba_teams_leaderboard(_load("full/teams_statistics__offensive.json")).schema["score"] == pl.Float64


def test_numeric_strings_parse_to_float64():
    lb = P.parse_lba_players_leaderboard(_load("full/players_statistics__points.json"))
    assert lb.schema["minutes"] == pl.Float64 and lb.schema["score"] == pl.Float64
    assert lb["score"].null_count() == 0
    st = P.parse_lba_standings(_load("full/ranking__602__d3.json"))
    assert st.schema["wins_percent"] == pl.Float64 and st["wins_percent"].max() == 100.0  # "100%" on the wire
    player = P.parse_lba_player(_load(_cap("/players/get-player-by-id")))
    assert player.schema["stats_avg_rs_points_avg"] == pl.Float64


# --- seasons: START on the wire, END in the frames ------------------------------------------------------------------------


def test_season_is_the_end_year_and_start_year_columns_are_kept():
    ch = P.parse_lba_championships(_load("full/championships__s2026.json"))
    assert ch["year"].to_list() == [2026] and ch["season"].to_list() == [2027]
    game = P.parse_lba_game(_load("full/matches__25602.json"))
    assert game["year"][0] == 2026 and game["season"][0] == 2027
    old = P.parse_lba_calendar(_load(_cap("/championships/get-championships-calendar-by-id", "1995")))
    assert set(old["season"]) == {1996}


def test_standings_wire_season_is_renamed_so_it_cannot_pass_for_the_sdv_key():
    st = P.parse_lba_standings(_load("full/ranking__602__d3.json"))
    assert set(st["season"]) == {2027} and set(st["season_start_year"]) == {2026}


def test_request_season_stamp_fills_a_body_without_a_year():
    raw = _load("full/player_matches__969.json")
    for g in raw["matches_stats"]["data"]:
        g["player_team_code"] = "bad"
    raw["_request"] = {"season": 2027}
    assert set(P.parse_lba_game_log(raw)["season"]) == {2027}
    raw.pop("_request")
    assert set(P.parse_lba_game_log(raw)["season"]) == {None}


# --- id key spaces ----------------------------------------------------------------------------------------------------------


def test_game_and_team_ids_join_across_frames_with_equal_dtypes():
    box = P.parse_lba_box_score(_load("full/matches__25602.json"))
    teams = P.parse_lba_teams(_load("full/teams__2026.json"))
    cal = P.parse_lba_calendar(_load("full/calendar__602__d1.json"))
    assert box.schema["team_id"] == teams.schema["team_id"] == cal.schema["h_team_id"] == pl.Int64
    assert box.schema["game_id"] == cal.schema["game_id"] == pl.Int64
    assert set(box["team_id"]) <= set(teams["team_id"])  # a team-season id: the same season's team list
    assert 25602 in cal["game_id"].to_list()
    assert cal.schema["websocket_match_id"] == pl.Int64  # a string on the wire, a different id space from game_id


def test_team_ids_are_per_season_and_club_id_is_the_stable_key():
    teams = P.parse_lba_teams(_load("full/teams__2026.json"))
    old = P.parse_lba_teams(_load(_cap("/teams/get-teams", "1995")))
    assert teams.schema["club_id"] == old.schema["club_id"] == pl.Int64
    assert not set(teams["team_id"]) & set(old["team_id"])


def test_player_ids_are_global_across_box_roster_and_game_log():
    box = P.parse_lba_box_score(_load("full/matches__25602.json"))
    log = P.parse_lba_game_log(_load("full/player_matches__969.json"))
    assert 969 in box["player_id"].to_list() and set(log["player_id"]) == {969}
    assert box.schema["player_id"] == log.schema["player_id"] == pl.Int64


# --- dedicated shapers --------------------------------------------------------------------------------------------------------


def test_box_score_has_one_row_per_player_with_the_side_and_the_team_rows_are_separate():
    body = _load("full/matches__25602.json")
    box, teams = P.parse_lba_box_score(body), P.parse_lba_team_box_score(body)
    n = len(body["scores"]["ht"]["rows"]) + len(body["scores"]["vt"]["rows"])
    assert box.height == n and set(box["side"]) == {"home", "visitor"}
    assert set(teams["row_type"]) == {"team", "totals"} and teams.height == 4
    totals = teams.filter(pl.col("row_type") == "totals")
    assert totals.filter(pl.col("side") == "home")["pun"][0] == body["match"]["home_final_score"]


def test_box_score_scores_route_and_game_route_give_the_same_frame():
    full = P.parse_lba_box_score(_load("full/matches__25602.json"))
    only = P.parse_lba_box_score(_load(_cap("/championships/get-championships-matches-scores-by-id")))
    assert list(only.columns) == list(full.columns) and only.height <= full.height


def test_pbp_adds_game_context_scores_and_the_coordinate_frame_by_era():
    pbp = P.parse_lba_pbp(_load("full/pbp__25602.json"))
    assert pbp.height == 605 and set(pbp["game_id"]) == {25602} and set(pbp["coord_frame"]) == {"court_pct"}
    assert set(pbp["team_side"].drop_nulls()) == {"home", "visitor"}
    scored = pbp.filter(pl.col("home_score").is_not_null())
    assert scored.height > 0 and scored.schema["home_score"] == pl.Int64
    last = scored.tail(1)
    assert (last["home_score"][0], last["visitor_score"][0]) == (109, 94)  # the final score of 25602
    old = P.parse_lba_pbp(_load(_cap("/championships/get-championships-matches-play-by-play", "2017")))
    assert set(old["coord_frame"]) == {"half_court_px"} and set(old["season"]) == {2018}
    assert old["home_score"].null_count() == old.height  # no score text before 2021-22


def test_pbp_side_is_the_basket_the_court_zone_is_measured_from_not_the_team():
    pbp = P.parse_lba_pbp(_load("full/pbp__25602.json"))
    located = pbp.filter(
        pl.col("side").is_not_null() & pl.col("x").is_not_null() & pl.col("side_area_zone").is_not_null()
    )
    assert located.height > 300
    # zone A = the half of basket `side` (0: x < 50, 1: x >= 50); D = the other half
    mismatch = located.filter((pl.col("x") < 50) != ((pl.col("side") == 0) == (pl.col("side_area_zone") == "A")))
    assert mismatch.height == 0
    # not home/visitor (that is team_side): both teams have both values, and a team's shots switch basket at half-time
    assert located.group_by("team_side").agg(pl.col("side").n_unique())["side"].to_list() == [2, 2]
    shots = pbp.filter(pl.col("description").str.contains("punti") & pl.col("side").is_not_null())
    halves = shots.group_by("team_side", pl.col("period") <= 2).agg(pl.col("side").unique())
    assert all(len(v) == 1 for v in halves["side"].to_list())
    by_team = halves.with_columns(pl.col("side").list.first()).group_by("team_side").agg(pl.col("side").n_unique())
    assert by_team["side"].to_list() == [2, 2]


def test_pbp_legend_and_shots():
    legend = P.parse_lba_pbp_legend(_load("full/pbp__25602.json"))
    assert legend.height == 59 and legend.schema["code"] == pl.Int64
    assert legend.filter(pl.col("code") == 9)["label"][0] == "Schiacciata"
    shots = P.parse_lba_shots(_load(_cap("/matches/get-championship-matches-shots-by-id")))
    assert shots.height == 20 and set(shots["side"]) == {"home", "visitor"}
    z3 = shots.filter((pl.col("side") == "home") & (pl.col("zone") == 3))
    assert (z3["made"][0], z3["attempts"][0]) == (17, 21) and z3["num"][0] == "17/21"
    only7 = P.parse_lba_shots(_load(_cap("/matches/get-championship-matches-shots-by-id", "2017")))
    assert set(only7["zone"]) == {7}  # 2017-18 sends one zone holding every shot and free throw


def test_roster_and_team_stats_carry_the_team_context():
    roster = P.parse_lba_roster(_load(_cap("/teams/get-team-roster")))
    assert set(roster["team_id"]) == {1764} and set(roster["season"]) == {2027}
    assert {"club_id", "club_code", "player_id"} <= set(roster.columns)
    coach = P.parse_lba_roster_coach(_load(_cap("/teams/get-team-roster")))
    assert coach.height == 1 and coach["coach_id"][0] == 339
    tps = P.parse_lba_team_players_stats(_load("full/team_players_stats__1764.json"))
    assert set(tps["team_id"]) == {1764} and tps.schema["points_sum"] == pl.Int64
    assert tps.schema["shots_2p_perc_sum"] == pl.Float64


def test_player_career_has_one_row_per_championship_and_statistic():
    career = P.parse_lba_player_stats(_load(_cap("/players/get-player-stats")))
    assert set(career["stat_type"]) == {"avg", "max", "sum"} and set(career["player_id"]) == {969}
    assert career.schema["points"] == pl.Float64  # averages hold fractions, totals whole numbers
    assert career.schema["season"] == pl.Int64


def test_game_header_keeps_1995_period_scores_as_sent():
    old = P.parse_lba_game(_load(_cap("/championships/get-championships-matches-by-id", "1995")))
    assert old.height == 1 and old["q1_hs"].null_count() == 1  # not recorded: null/0, never a real score
    assert old["referees"][0] is None or isinstance(old["referees"][0], str)


def test_non_dict_rows_and_unhashable_junk_do_not_raise():
    assert P.parse_lba_pbp({"pbp": {"actions": [1, None, {"x": [1]}]}}).height == 1
    assert P.parse_lba_box_score({"scores": {"ht": {"rows": "oops"}}}).height == 0
    assert P.parse_lba_shots({"shots": {"ht": {"3": {"num": "bad"}}}}).height == 1
