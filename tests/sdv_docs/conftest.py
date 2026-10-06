import json
import sqlite3
from pathlib import Path

import pytest

from sdv_docs.schema import SCHEMA_SQL, SCHEMA_VERSION, SEARCH_SQL

DOCS = "https://py.sportsdataverse.org/docs/"
REL = "https://github.com/sportsdataverse/sportsdataverse-data/releases"

FUNCTIONS = [
    (
        "load_nhl_pbp",
        "python",
        "sportsdataverse",
        "sportsdataverse.nhl.nhl_loaders",
        "nhl",
        "loader",
        None,
        "Load NHL play-by-play.",
        "load_nhl_pbp(seasons, return_as_pandas: bool = False)",
        DOCS + "nhl/reference/loaders/pbp#load_nhl_pbp",
    ),
    (
        "load_nhl_shifts",
        "python",
        "sportsdataverse",
        "sportsdataverse.nhl.nhl_loaders",
        "nhl",
        "loader",
        None,
        "Load NHL shift charts.",
        "load_nhl_shifts(seasons, return_as_pandas: bool = False)",
        DOCS + "nhl/reference/loaders/other#load_nhl_shifts",
    ),
    (
        "load_nba_pbp",
        "python",
        "sportsdataverse",
        "sportsdataverse.nba.nba_loaders",
        "nba",
        "loader",
        None,
        "Load NBA play-by-play.",
        "load_nba_pbp(seasons, return_as_pandas: bool = False)",
        DOCS + "nba/reference/loaders#load_nba_pbp",
    ),
    (
        "espn_nba_team_roster",
        "python",
        "sportsdataverse",
        "sportsdataverse.nba.nba_espn_ext",
        "nba",
        "espn",
        None,
        "Team roster.",
        "espn_nba_team_roster(team_id, return_parsed=True, return_as_pandas=False)",
        DOCS + "nba/reference/site#espn_nba_team_roster",
    ),
    (
        "wnba_stats_shotchartdetail",
        "python",
        "sportsdataverse",
        "sportsdataverse.wnba.wnba_stats",
        "wnba",
        "flat",
        None,
        "Shot chart detail.",
        "wnba_stats_shotchartdetail(player_id, season)",
        DOCS + "wnba/reference/wnba_stats#wnba_stats_shotchartdetail",
    ),
    (
        "load_nba_pbp",
        "r",
        "hoopR",
        None,
        None,
        "function",
        "NBA Data Functions",
        "Load hoopR NBA play-by-play",
        None,
        "https://hoopR.sportsdataverse.org/reference/load_nba_pbp.md",
    ),
]
PARAMS = [
    ("load_nhl_pbp", "seasons", "int | list[int]", 1, None, "an int or iterable of seasons (>= 2010)."),
    ("load_nhl_pbp", "return_as_pandas", "bool", 0, "False", "return a pandas DataFrame instead of polars."),
    ("espn_nba_team_roster", "team_id", "int", 1, None, "ESPN team id."),
]
COLUMNS = [
    ("load_nhl_pbp", None, "event_type", "String", "Standardized event type code."),
    ("load_nhl_pbp", None, "period", "Int64", "Period number."),
    ("load_nhl_pbp", None, "game_id", "Int64", "Game id."),
    ("load_nba_pbp", None, "game_id", "Int64", "Game id."),
    ("load_nba_pbp", None, "shooting_play", "Boolean", "Whether the play was a shot."),
    ("load_nhl_shifts", None, "players_on", "String", "Players on the ice."),
]
ENDPOINTS = [
    (
        "espn_core_v2",
        "GET",
        "https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/athletes/{athlete_id}/injuries",
        "Athlete injuries",
        json.dumps([{"name": "athlete_id", "required": True}]),
        "espn_nba_athlete_injuries espn_wnba_athlete_injuries",
        "",
        "codegen",
        "https://github.com/sportsdataverse/sportsdataverse-py/blob/abc1234/tools/codegen/endpoints/espn_core_v2.yaml",
    ),
    (
        "espn_cdn",
        "GET",
        "https://cdn.espn.com/core/{league}/playbyplay",
        "Game play-by-play page data",
        "[]",
        "espn_nba_cdn_playbyplay",
        "playbyplay served for nba wnba mbb; not served for nhl",
        "codegen",
        "https://github.com/sportsdataverse/sportsdataverse-py/blob/abc1234/tools/codegen/endpoints/espn_cdn.yaml",
    ),
    (
        "ESPN Core API",
        "GET",
        "https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}/events/{event_id}/competitions/{cid}/odds",
        "Event odds",
        json.dumps([{"name": "event_id", "required": True}]),
        "",
        "",
        "openapi",
        "https://github.com/saiemgilani/sdv-swagger/blob/deadbee/espn-core-v2.openapi.yaml",
    ),
]
DATASETS = [
    (
        "load_nhl_pbp",
        "nhl",
        "nhl_pbp_full",
        REL + "/download/nhl_pbp_full/play_by_play_{season}.parquet",
        2010,
        REL + "/tag/nhl_pbp_full",
    ),
    (
        "load_nhl_shifts",
        "nhl",
        "nhl_shifts",
        REL + "/download/nhl_shifts/nhl_shifts_{season}.parquet",
        2025,
        REL + "/tag/nhl_shifts",
    ),
    (
        "load_nba_pbp",
        "nba",
        "espn_nba_pbp",
        REL + "/download/espn_nba_pbp/play_by_play_{season}.parquet",
        2002,
        REL + "/tag/espn_nba_pbp",
    ),
]
EQUIVALENTS = [("load_nba_pbp", "hoopR", "load_nba_pbp", "identity")]


def make_tiny_db(path: Path, **meta: str) -> Path:
    con = sqlite3.connect(path)
    try:
        con.executescript(SCHEMA_SQL)
        base = {
            "schema_version": str(SCHEMA_VERSION),
            "built_at": "2026-10-05T00:00:00Z",
            "sdv_py_commit": "abc1234",
            "sdv_swagger_sha": "deadbee",
        }
        con.executemany("INSERT INTO meta VALUES (?, ?)", sorted({**base, **meta}.items()))
        con.executemany("INSERT INTO functions VALUES (?,?,?,?,?,?,?,?,?,?)", FUNCTIONS)
        con.executemany("INSERT INTO params VALUES (?,?,?,?,?,?)", PARAMS)
        con.executemany("INSERT INTO columns VALUES (?,?,?,?,?)", COLUMNS)
        con.executemany("INSERT INTO endpoints VALUES (?,?,?,?,?,?,?,?,?)", ENDPOINTS)
        con.executemany("INSERT INTO datasets VALUES (?,?,?,?,?,?)", DATASETS)
        con.executemany("INSERT INTO equivalents VALUES (?,?,?,?)", EQUIVALENTS)
        con.executescript(SEARCH_SQL)
        con.commit()
    finally:
        con.close()
    return path


@pytest.fixture
def tiny_db(tmp_path: Path) -> Path:
    return make_tiny_db(tmp_path / "tiny.sqlite")
