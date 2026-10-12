"""Offline tests for the KHL webcaster (``khl_webcaster``) parsers, on the committed real captures.

``tests/fixtures/khl_webcaster/*.json`` are the truncated refs captures; ``full/data.json`` is the
complete app bootstrap (every stage since 2008-09: the season table). No network.
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
from sportsdataverse.hockey.khl import khl_webcaster_parsers as W

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "khl_webcaster"
YAML = ROOT / "tools" / "codegen" / "endpoints" / "khl_webcaster.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "khl_webcaster"
SPEC_ROUTES = FIXTURES / "spec_routes.json"  # the refs spec's route list, committed by gen_khl.py

PARSERS: Dict[str, Callable[..., Any]] = {
    "data.json": W.parse_khl_webcaster_stages,
    "events_v2.json": W.parse_khl_webcaster,
    "event_v2.json": W.parse_khl_webcaster,
    "teams_v2.json": W.parse_khl_webcaster,
    "team_v2.json": W.parse_khl_webcaster,
    "players_v2.json": W.parse_khl_webcaster,
    "players_v2_light.json": W.parse_khl_webcaster,
    "leaders_v2.json": W.parse_khl_webcaster_leaders,
    "tables_v2.json": W.parse_khl_webcaster_tables,
}
SHORT_FIXTURE = {
    "stages": "full/data.json",
    "events": "events_v2.json",
    "event": "event_v2.json",
    "teams": "teams_v2.json",
    "team": "team_v2.json",
    "players": "players_v2.json",
    "players_light": "players_v2_light.json",
    "leaders": "leaders_v2.json",
    "tables": "tables_v2.json",
}
_SNAKE = re.compile(r"^[a-z0-9_]+$")


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("fname", sorted(PARSERS))
def test_every_capture_parses_to_snake_case_rows_with_int_ids(fname):
    df = PARSERS[fname](_load(fname))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(_SNAKE.match(c) for c in df.columns), df.columns
    for col in df.columns:
        if col in ("id", "khl_id", "stage_id", "tournament_id") or col.endswith(("_khl_id", "team_id", "match_id")):
            assert df.schema[col] == pl.Int64, (fname, col, df.schema[col])


@pytest.mark.parametrize("parser", sorted(set(PARSERS.values()), key=lambda f: f.__name__), ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3], ids=["dict", "list", "none", "str", "int"])
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_empty_page_keeps_the_documented_columns():
    df = W.parse_khl_webcaster([], columns=["id", "khl_id", "name"])
    assert df.schema == {"id": pl.Int64, "khl_id": pl.Int64, "name": pl.Utf8}


@pytest.mark.parametrize("ep", yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"], ids=lambda e: e["short"])
def test_an_empty_frame_concatenates_with_a_populated_one(ep):
    parser = getattr(W, ep["parser"])
    columns = list(P.SCHEMAS[f"khl_webcaster_{ep['short']}"])  # what the generated wrapper passes
    empty, full_df = parser({}, columns=columns), parser(_load(SHORT_FIXTURE[ep["short"]]), columns=columns)
    assert empty.height == 0 and empty.schema == full_df.schema
    assert pl.concat([empty, full_df], how="vertical").height == full_df.height
    if ep["parser"] != "parse_khl_webcaster":  # one schema per parser: no columns needed
        assert parser({}).schema == full_df.schema


def test_return_as_pandas():
    assert isinstance(W.parse_khl_webcaster(_load("events_v2.json"), return_as_pandas=True), pd.DataFrame)


def test_wrapped_rows_are_unwrapped_and_flattened():
    df = W.parse_khl_webcaster(_load("events_v2.json"))
    first = df.row(0, named=True)
    assert first["khl_id"] == int(first["match_id"])  # the khl.ru game id, twice
    assert {"team_a_id", "team_a_khl_id", "team_b_khl_id"} <= set(df.columns)


def test_page_object_is_one_row():
    df = W.parse_khl_webcaster(_load("event_v2.json"))
    assert df.height == 1
    row = df.row(0, named=True)
    assert (row["id"], row["khl_id"], row["team_a_khl_id"]) == (2786714, 898236, 7)  # event -> game, team -> club


def test_season_table_from_the_full_bootstrap():
    df = W.parse_khl_webcaster_stages(_load("full/data.json"))
    assert set(df["type"]) == {"regular", "playoff"}
    pick = {(r["season"], r["type"]): (r["tournament_id"], r["stage_id"]) for r in df.iter_rows(named=True)}
    assert pick[(2026, "regular")] == (1369, 370) and pick[(2026, "playoff")] == (1370, 395)
    assert pick[(2027, "regular")] == (1436, 407)
    # no arithmetic rule: the early seasons break "regular even, playoff = regular + 1"
    assert pick[(2010, "regular")][0] == 167 and pick[(2009, "playoff")][0] == 165
    assert df.schema["season"] == pl.Int64 and df.schema["percent_points_scored"] == pl.Boolean


def test_leaders_explode_categories():
    df = W.parse_khl_webcaster_leaders(_load("leaders_v2.json"))
    assert df["category"].n_unique() == 3 and df["rank"].min() == 1


def test_tables_read_the_championship_table_and_type_the_stats():
    df = W.parse_khl_webcaster_tables(_load("tables_v2.json"))
    assert df["season"].n_unique() == 3
    assert df.schema["pts_pct"] == pl.Float64 and df.schema["gp"] == pl.Int64  # numeric strings upstream
    assert df.schema["khl_id"] == pl.Int64


def test_yaml_lists_every_spec_route():
    doc = yaml.safe_load(YAML.read_text(encoding="utf-8"))
    paths = {e["path"] for e in doc["endpoints"]}
    assert paths == set(json.loads(SPEC_ROUTES.read_text(encoding="utf-8")))
    assert len(doc["endpoints"]) == len(SHORT_FIXTURE) == 9
    assert doc["getter_module"] == "sportsdataverse.hockey.khl.khl_webcaster_runtime"
    for ep in doc["endpoints"]:
        assert not {"league", "sport"} & {p["name"] for p in ep.get("extra_params", [])}
        assert any(p["query_key"] == "locale" and p["default"] == "en" for p in ep["extra_params"])


@pytest.mark.parametrize("ep", yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"], ids=lambda e: e["short"])
def test_committed_schema_matches_the_parser(ep):
    schema = yaml.safe_load((SCHEMAS / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
    assert schema["schema"] == f"khl_webcaster_{ep['short']}"
    df = getattr(W, ep["parser"])(_load(SHORT_FIXTURE[ep["short"]]))
    assert [c["name"] for c in schema["columns"]] == df.columns
