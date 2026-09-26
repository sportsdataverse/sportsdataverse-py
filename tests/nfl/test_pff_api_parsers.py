"""Offline parser tests for the PFF Developer API, against PFF's own published examples.

Fixtures are the ``200`` examples in PFF's public spec (see ``tests/fixtures/pff_api/README.md``).
"""

import json
from pathlib import Path

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.nfl.pff_parsers import parse_pff_player_detail, parse_pff_report, parse_pff_v2_table

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "pff_api"


def load(name: str) -> dict:
    return json.loads((FIX / f"{name}.json").read_text(encoding="utf-8"))


def test_v2_team_stats_schema_comes_from_declared_columns():
    raw = load("team_stats")
    df = parse_pff_v2_table(raw)
    assert df.height == len(raw["rows"]) > 0
    assert df.columns[:3] == ["team_id", "abbreviation", "name"]
    assert df.schema["team_id"] == pl.Int64
    assert df.schema["epa_per_play"] == pl.Float64
    assert df.schema["epa_per_play_rank"] == pl.Int64


def test_v2_roster_keeps_ids_integer_and_jersey_string():
    df = parse_pff_v2_table(load("team_roster"))
    assert df.schema["player_id"] == pl.Int64
    assert df.schema["jersey"] == pl.Utf8
    assert {"depth_order", "alignment", "unit", "grade_rank", "snap_pct"} <= set(df.columns)


def test_v2_second_table_team_totals():
    raw = load("team_rushing_direction")
    rows, totals = parse_pff_v2_table(raw), parse_pff_v2_table(raw, "teamTotals")
    assert rows.height == len(raw["rows"]) and totals.height == len(raw["teamTotals"]) > 0
    # the second table is typed by ITS OWN declared columns (totalsColumns), in PFF's order
    assert totals.columns[: len(raw["totalsColumns"])] == [underscore(c["key"]) for c in raw["totalsColumns"]]


def test_v2_empty_body_keeps_declared_schema():
    raw = dict(load("team_stats"), rows=[])
    df = parse_pff_v2_table(raw)
    assert df.height == 0
    assert df.schema["team_id"] == pl.Int64 and len(df.columns) == len(raw["columns"])


def test_v2_malformed_and_pandas():
    assert parse_pff_v2_table(None).height == 0
    assert parse_pff_v2_table({"rows": "x"}).height == 0
    assert isinstance(parse_pff_v2_table(load("position_report"), return_as_pandas=True), pd.DataFrame)


def test_v1_body_with_restricted_block_still_parses_as_the_report():
    raw = load("facet_passing_summary")
    with_meta = dict(raw, restricted=["grades_pass"])  # RestrictedColumns: a list of column names
    a, b = parse_pff_report(raw), parse_pff_report(with_meta)
    assert isinstance(b, pl.DataFrame) and b.height == a.height > 0
    assert b.schema["player_id"] == pl.Int64


def test_v1_new_routes_parse():
    ts = parse_pff_report(load("team_summary"))
    assert ts.height > 0 and {"game_id", "grades_overall"} <= set(ts.columns)
    detail = parse_pff_player_detail(dict(load("player_offense_pass_blocking"), restricted=["grades_pass_block"]))
    assert detail.height > 0 and "player_id" in detail.columns


def test_v2_empty_answer_declared_all_string_keeps_ids_integer():
    # PFF declares EVERY column "string" on an empty answer: ids must still be Int64 and the
    # rest Null, so a union with a populated week keeps the real dtypes
    raw = dict(load("team_stats"), rows=[])
    raw["columns"] = [dict(c, type="string") for c in raw["columns"]]
    empty = parse_pff_v2_table(raw)
    full = parse_pff_v2_table(load("team_stats"))
    both = pl.concat([empty, full], how="diagonal_relaxed")
    assert empty.schema["team_id"] == pl.Int64 and empty.schema["epa_per_play"] == pl.Null
    assert both.schema["team_id"] == pl.Int64 and both.schema["epa_per_play"] == pl.Float64


def test_v2_all_null_string_column_is_null_and_bad_cast_never_raises():
    cols = [
        {"key": "playerId", "type": "integer"},
        {"key": "note", "type": "string"},
        {"key": "isBye", "type": "boolean"},
    ]
    df = parse_pff_v2_table({"columns": cols, "rows": [{"playerId": 1, "note": None, "isBye": "n/a"}]})
    assert df.schema["player_id"] == pl.Int64 and df.schema["note"] == pl.Null
    assert df["is_bye"].to_list() == ["n/a"]  # uncastable text kept, not an exception
