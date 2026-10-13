"""Offline tests for the Czech Extraliga (``elh``) flat-API family: parsers, endpoint YAML, schemas.

Every assertion runs on the committed real bodies in ``tests/fixtures/elh/``: the truncated refs
captures (``2026/``, ``2016/``) and the FULL untruncated bodies of game 2928321 (``full/``), which
are the dtype source. No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.hockey import elh
from sportsdataverse.hockey.elh.elh_parsers import (
    ELH_SCHEMAS,
    parse_elh_events,
    parse_elh_game_ids,
    parse_elh_pbp,
    parse_elh_pbp_goals,
    parse_elh_pbp_mobile,
    parse_elh_scoreboard,
    parse_elh_shots,
)

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "elh"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "elh.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "elh"
_SNAKE = re.compile(r"^[a-z0-9_]+$")
_ID = re.compile(r"(^|_)id$")

# fixture stem -> parsers that read it
_PARSERS: Dict[str, List[Callable[..., Any]]] = {
    "match__season__full__match_id": [parse_elh_pbp, parse_elh_pbp_goals],
    "match__season__short__match_id": [parse_elh_pbp],
    "match__season__mobile__match_id": [parse_elh_pbp_mobile],
    "visualization__shots__match_id": [parse_elh_shots],
    "visualization__events__match_id": [parse_elh_events],
    "scoreboard__date": [parse_elh_scoreboard],
}
# short -> (parser, fixture stem) for the schema test; the full body when there is one
_SHORTS = {
    "events": (parse_elh_events, "visualization__events__match_id"),
    "list_objects": (parse_elh_game_ids, "list"),
    "pbp": (parse_elh_pbp, "match__season__full__match_id"),
    "pbp_goals": (parse_elh_pbp_goals, "match__season__full__match_id"),
    "pbp_latest": (parse_elh_pbp, "match__season__short__match_id"),
    "pbp_mobile": (parse_elh_pbp_mobile, "match__season__mobile__match_id"),
    "scoreboard": (parse_elh_scoreboard, "scoreboard__date"),
    "shots": (parse_elh_shots, "visualization__shots__match_id"),
}
_ALL_PARSERS = [
    parse_elh_pbp,
    parse_elh_pbp_goals,
    parse_elh_pbp_mobile,
    parse_elh_shots,
    parse_elh_events,
    parse_elh_scoreboard,
    parse_elh_game_ids,
]


def _load(era: str, stem: str) -> Any:
    if stem == "list":
        return (FIXTURES / era / "list.xml").read_text(encoding="utf-8")
    return json.loads((FIXTURES / era / f"{stem}.json").read_text(encoding="utf-8"))


def _best(stem: str) -> Any:
    """The full body when committed, else the truncated 2026 capture."""
    return _load("full" if (FIXTURES / "full" / f"{stem}.json").exists() else "2026", stem)


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


_CASES = [
    (era, p.stem, parser)
    for era in ("2026", "2016", "full")
    for p in sorted((FIXTURES / era).glob("*.json"))
    for parser in _PARSERS[p.stem]
]


@pytest.mark.parametrize(("era", "stem", "parser"), _CASES, ids=[f"{e}/{s}/{p.__name__}" for e, s, p in _CASES])
def test_every_fixture_parses_to_a_tidy_frame(era: str, stem: str, parser: Callable[..., Any]) -> None:
    df = parser(_load(era, stem))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert all(_SNAKE.match(c) for c in df.columns), df.columns
    assert df.schema == pl.Schema(ELH_SCHEMAS[parser.__name__])
    for col, dtype in df.schema.items():
        if _ID.search(col):
            assert dtype == pl.Int64, col


def test_listing_page_parses_to_keys_with_game_ids() -> None:
    df = parse_elh_game_ids(_load("2026", "list"))
    assert df.height == 3
    assert df["hokejcz_game_id"].to_list() == [2928255, 2928256, 2928257]
    assert df.schema["hokejcz_game_id"] == pl.Int64 and df.schema["size"] == pl.Int64


def test_a_non_match_key_has_no_game_id() -> None:
    page = _load("2026", "list").replace("match/2026/full/2928255.json", "visualization/shots/2928255.json")
    assert parse_elh_game_ids(page)["hokejcz_game_id"].to_list() == [None, 2928256, 2928257]


@pytest.mark.parametrize("raw", [None, {}, [], "x", 7, {"comments": None}, {"match": []}, "<a/>"])
@pytest.mark.parametrize("parser", _ALL_PARSERS, ids=lambda p: p.__name__)
def test_empty_payload_is_zero_row_frame(parser: Callable[..., Any], raw: Any) -> None:
    df = parser(raw)
    assert df.height == 0
    assert df.schema == pl.Schema(ELH_SCHEMAS[parser.__name__])


@pytest.mark.parametrize("parser", _ALL_PARSERS, ids=lambda p: p.__name__)
def test_return_as_pandas(parser: Callable[..., Any]) -> None:
    stem = next((s for s, ps in _PARSERS.items() if parser in ps), "list")
    out = parser(_best(stem) if stem != "list" else _load("2026", "list"), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert len(out) > 0
    assert list(out.columns) == list(ELH_SCHEMAS[parser.__name__])


# ---------------------------------------------------------------- play-by-play


def test_pbp_is_oldest_first_with_typed_clock_and_scores() -> None:
    df = parse_elh_pbp(_best("match__season__full__match_id"))
    assert df.height == 119
    assert df["sort_order"].is_sorted()
    goals = df.filter(pl.col("event_type") == "goal")
    assert goals["clock"].to_list() == ["05:06", "10:31", "54:18"]
    assert goals["game_seconds"].to_list() == [306, 631, 3258]
    assert goals["period"].to_list() == [1, 1, 3]
    assert goals["home_score"].to_list() == [1, 2, 3] and goals["away_score"].to_list() == [0, 0, 0]
    assert goals["strength"].to_list() == ["EQ"] * 3


def test_pbp_clockless_entries_name_only_their_phase() -> None:
    df = parse_elh_pbp(_best("match__season__full__match_id"))
    clockless = df.filter(pl.col("clock").is_null())
    assert set(clockless["phase"].to_list()) == {"0", "1INT", "2INT", "3PER", "ERT"}
    assert clockless["period"].null_count() == clockless.height
    assert df.filter(pl.col("clock").is_not_null())["phase"].null_count() == df.height - clockless.height


def test_pbp_player_ids_are_onlajny_and_global_ids_differ() -> None:
    goal = parse_elh_pbp(_best("match__season__full__match_id")).filter(pl.col("clock") == "54:18").row(0, named=True)
    assert (goal["onlajny_player1_id"], goal["onlajny_player1_global_id"]) == (48287, 76559)  # Hradec
    assert goal["player1_last_name"] == "Hradec"
    assert goal["onlajny_team_id"] == 156 and goal["team_code"] == "SPA"  # onlajny Sparta, not hokej.cz 8
    assert goal["no_assist"] == 0


def test_pbp_penalty_carries_minutes_and_offense_and_empty_slots_are_null() -> None:
    pen = parse_elh_pbp(_best("match__season__full__match_id")).filter(pl.col("clock") == "41:02").row(0, named=True)
    assert pen["event_type"] == "penalty"
    assert (pen["penalty_minutes"], pen["penalty_offense"]) == (2, "VYSH")
    assert pen["onlajny_player2_id"] is None and pen["player2_name"] is None  # id "0", name ""


def test_pbp_2016_has_names_only() -> None:
    df = parse_elh_pbp(_load("2016", "match__season__full__match_id"))
    goal = df.filter(pl.col("event_type") == "goal").row(0, named=True)
    assert goal["player1_name"] == "Bližňák" and goal["player1_first_name"] is None
    assert goal["strength"] is None and goal["penalty_minutes"] is None


def test_pbp_lone_comment_object_is_one_row() -> None:
    raw = _best("match__season__full__match_id")
    lone = {"comments": {"comment": raw["comments"]["comment"][1]}}  # XML-to-JSON: one element = an object
    assert parse_elh_pbp(lone).height == 1


def test_pbp_two_penalties_on_one_player_are_summed() -> None:
    raw = _best("match__season__full__match_id")
    entry = next(c for c in raw["comments"]["comment"] if c["@attributes"]["label"] == "penalty")
    pen = entry["details"]["detail"]["player1"]["penalties"]["penalty"]
    entry["details"]["detail"]["player1"]["penalties"]["penalty"] = [
        pen,
        {"@attributes": {"length": "10", "offense": "OSTATNI"}},
    ]
    row = parse_elh_pbp({"comments": {"comment": [entry]}}).row(0, named=True)
    assert row["penalty_minutes"] == int(pen["@attributes"]["length"]) + 10
    assert row["penalty_offense"] == f"{pen['@attributes']['offense']},OSTATNI"


def test_pbp_goals_lists_six_skaters_per_team_per_goal() -> None:
    df = parse_elh_pbp_goals(_best("match__season__full__match_id"))
    assert df.height == 36  # 3 goals x 2 teams x 6
    per = df.group_by("comment_id", "is_scoring_team").len()
    assert per["len"].to_list() == [6] * 6
    scoring = df.filter(pl.col("is_scoring_team") == True)
    assert set(scoring["on_ice_team_code"].to_list()) == {"SPA"}
    assert (scoring["on_ice_onlajny_team_id"] == scoring["scoring_onlajny_team_id"]).all()
    hradec = df.filter((pl.col("clock") == "54:18") & (pl.col("player_name") == "Hradec")).row(0, named=True)
    assert (hradec["onlajny_player_id"], hradec["onlajny_player_global_id"]) == (48287, 76559)


def test_pbp_goal_without_on_ice_block_keeps_one_row() -> None:
    df = parse_elh_pbp_goals(_load("2016", "match__season__full__match_id"))
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["scoring_team_code"] == "LIB" and row["onlajny_player_id"] is None and row["slot"] is None


def test_mobile_ids_equal_the_full_file_comment_ids() -> None:
    mobile = parse_elh_pbp_mobile(_load("2026", "match__season__mobile__match_id"))
    full = parse_elh_pbp(_best("match__season__full__match_id"))
    assert set(mobile["comment_id"].to_list()) <= set(full["comment_id"].to_list())
    assert mobile.schema["game_seconds"] == pl.Int64


# ---------------------------------------------------------------- shots / events


def test_shots_full_body_dtypes_and_result_alias() -> None:
    df = parse_elh_shots(_best("visualization__shots__match_id"))
    assert df.height == 121
    assert df.schema["coordinate_x"] == pl.Float64 and df.schema["coordinate_y"] == pl.Float64
    assert (df["shot_result_id"] == df["match_shot_resutl_id"]).all()
    assert df.filter(pl.col("is_goal") == True)["time"].sort().to_list() == [304, 629, 3257]
    assert set(df["shot_result_id"].unique().to_list()) == {1, 2, 3, 4, 5}
    home = df.filter(pl.col("team_side") == "home")
    assert (home["hokejcz_team_id"] == 8).all() and (home["coordinate_x"] > 0).mean() > 0.9  # home shoots at +x


def test_shots_player_ids_are_hokejcz_ids() -> None:
    df = parse_elh_shots(_best("visualization__shots__match_id"))
    hradec = df.filter(pl.col("last_name") == "Hradec")
    assert set(hradec["hokejcz_player_id"].to_list()) == {10093378}
    assert hradec["jersey"].to_list()[0] == 29


def test_shots_2016_integer_coordinates_cast_and_time_is_the_period_offset() -> None:
    df = parse_elh_shots(_load("2016", "visualization__shots__match_id"))
    assert df.schema["coordinate_x"] == pl.Float64
    assert set(df["time"].to_list()) <= {0, 1200, 2400}
    assert df["period"].to_list() == [t // 1200 + 1 for t in df["time"].to_list()]
    assert df.schema["hokejcz_game_id"] == pl.Int64 and df["hokejcz_game_id"][0] == 2508598  # "2508598" in the body


def test_events_are_per_team_and_minute() -> None:
    df = parse_elh_events(_best("visualization__events__match_id"))
    assert df.height == 120
    assert df.group_by("team_side").len().sort("team_side")["len"].to_list() == [60, 60]
    last = df.filter((pl.col("team_side") == "home") & (pl.col("minute") == 60)).row(0, named=True)
    assert (last["shots"], last["team_score"]) == (61, 3)
    assert df.filter(pl.col("team_side") == "home")["shots"].is_sorted()


# ---------------------------------------------------------------- scoreboard / id crosswalk


def test_scoreboard_keeps_elh_and_both_id_spaces() -> None:
    df = parse_elh_scoreboard(_best("scoreboard__date"))
    assert df.height == 7
    assert set(df["league_id"].to_list()) == {101}
    sparta = df.filter(pl.col("hokejcz_game_id") == 2928321).row(0, named=True)
    assert sparta["onlajny_game_id"] == 532979
    assert (sparta["hokejcz_home_team_id"], sparta["onlajny_home_team_id"]) == (8, 156)
    assert str(sparta["game_date"]) == "2026-10-11" and df.schema["game_date"] == pl.Date
    assert sparta["score_periods"] == "2:0,0:0,1:0"


def test_scoreboard_league_filter() -> None:
    raw = _best("scoreboard__date")
    raw["82"] = {"league_name": "Maxa liga", "matches": raw["101"]["matches"][:1]}
    assert set(parse_elh_scoreboard(raw)["league_id"].to_list()) == {101}
    assert parse_elh_scoreboard(raw, league_id=82).height == 1
    assert parse_elh_scoreboard(raw, league_id=None).height == 8


def test_scoreboard_maps_pbp_teams_to_shot_teams() -> None:
    sb = parse_elh_scoreboard(_best("scoreboard__date"))
    shots = parse_elh_shots(_best("visualization__shots__match_id"))
    pbp = parse_elh_pbp(_best("match__season__full__match_id"))
    assert sb.schema["hokejcz_game_id"] == shots.schema["hokejcz_game_id"]
    game = shots.select("hokejcz_game_id", "hokejcz_home_team_id").unique().join(sb, on="hokejcz_game_id")
    assert game.height == 1 and game["hokejcz_home_team_id"][0] == game["hokejcz_home_team_id_right"][0]
    assert sb.schema["onlajny_home_team_id"] == pbp.schema["onlajny_team_id"]
    teams = pbp.filter(pl.col("onlajny_team_id").is_not_null()).select("onlajny_team_id").unique()
    mapped = teams.join(
        game.select(pl.col("onlajny_home_team_id").alias("onlajny_team_id"), "hokejcz_home_team_id"),
        on="onlajny_team_id",
    )
    assert mapped.rows() == [(156, 8)]


# ---------------------------------------------------------------- YAML / schemas


def test_yaml_lists_every_route() -> None:
    doc = _yaml()
    assert sorted(e["short"] for e in doc["endpoints"]) == sorted(_SHORTS)
    paths = {e["path"] for e in doc["endpoints"]}
    captured = {p.stem for p in (FIXTURES / "2026").glob("*.json")} | {"list"}
    slug = {
        e["path"].strip("/").replace("/", "__").replace("{", "").replace("}", "").removesuffix(".json") or "list"
        for e in doc["endpoints"]
    }
    assert slug == captured, "every captured route has a wrapper, and every wrapper a capture"
    assert len(paths) == 7
    assert doc["getter_module"] == "sportsdataverse.hockey.elh.elh_runtime"
    assert {e["short"]: e.get("host") for e in doc["endpoints"]}["scoreboard"] == "https://json.esports.cz/hokejcz"
    assert all(e.get("host") is None for e in doc["endpoints"] if e["short"] != "scoreboard")


def test_no_path_token_named_league_or_sport() -> None:
    for ep in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{(\w+)\}", ep["path"])), ep["path"]


def test_season_is_an_end_year_mapped_by_the_runtime_transform() -> None:
    doc = _yaml()
    assert "elh_start_year" in doc["runtime_imports"]
    for ep in doc["endpoints"]:
        for p in ep.get("path_params", []):
            if p["name"] == "season":
                assert p["transform"] == "elh_start_year" and p["type"] == "int"
                assert ep["example_args"]["season"] == 2027  # END year of the 2026-27 example game


@pytest.mark.parametrize("short", sorted(_SHORTS))
def test_schemas_match_the_parsers_on_the_full_bodies(short: str) -> None:
    parser, stem = _SHORTS[short]
    df = parser(_load("2026", "list") if stem == "list" else _best(stem))
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert schema["schema"] == f"elh_{short}"
    kinds = {pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical"}
    assert [(c["name"], c["type"]) for c in schema["columns"]] == [
        (name, kinds.get(dtype, "character")) for name, dtype in df.schema.items()
    ]
    assert all(c["description"] == "" for c in schema["columns"])  # descriptions: manual_column_descriptions.yaml


def test_home_exports_parsers_and_game_id_lister() -> None:
    assert {"parse_elh_pbp", "parse_elh_shots", "elh_game_ids"} <= set(elh.__all__)
    assert not any(name in elh.__all__ for name in ("elh_parsers", "elh_runtime"))
