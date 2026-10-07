"""OpenLigaDB parser + generated-YAML contract."""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "openligadb"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "openligadb.yaml"


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "fixture",
    [
        "getavailableleagues",
        "getavailablegroups__league__season",
        "getavailableteams__league__season",
        "getbltable__league__season",
        "getgoalgetters__league__season",
        "getmatchdata__league__season",
        "getmatchdata__league__season__group",
    ],
)
def test_list_bodies_are_one_row_per_element(fixture):
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    body = load(fixture)
    assert isinstance(body, list)
    assert parse_openligadb(body).height == len(body)


@pytest.mark.parametrize(
    "fixture",
    ["getcurrentgroup__league", "getmatchdata__match_id", "getnextmatchbyleagueteam__league_id__team_id"],
)
def test_object_bodies_are_one_row(fixture):
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    assert parse_openligadb(load(fixture)).height == 1


def test_scalar_body_is_one_row():
    """Review Focus 3: /getlastchangedate answers a bare quoted ISO string."""
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    body = load("getlastchangedate__league__season__group")
    assert isinstance(body, str)
    df = parse_openligadb(body)
    assert df.height == 1
    assert df.columns == ["value"]
    assert df["value"][0] == body


def test_empty_payload_is_zero_rows():
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    for payload in (None, {}, [], ""):
        df = parse_openligadb(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


def test_pandas_round_trip():
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    assert isinstance(parse_openligadb(load("getavailableleagues"), return_as_pandas=True), pd.DataFrame)


def test_id_columns_are_utf8():
    from sportsdataverse.soccer.openligadb_parsers import parse_openligadb

    df = parse_openligadb(load("getavailableleagues"))
    for name, dtype in df.schema.items():
        if name == "id" or name.endswith("_id"):
            assert dtype == pl.String, name


def test_no_path_token_named_league_or_sport():
    """Review Focus 1: the module renderer substitutes {league}/{sport} as ESPN slugs."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for ep in doc["endpoints"]:
        assert "{league}" not in ep["path"], ep["short"]
        assert "{sport}" not in ep["path"], ep["short"]
        for prm in ep.get("path_params", []):
            assert prm["name"] not in {"league", "sport"}, ep["short"]


def test_league_token_is_renamed_to_slug():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    ep = next(e for e in doc["endpoints"] if e["short"] == "season_matches")
    assert ep["path"] == "/getmatchdata/{league_slug}/{season}"
    assert [p["name"] for p in ep["path_params"]] == ["league_slug", "season"]


def test_league_id_param_is_not_renamed():
    """Only the bare {league} token collides with the renderer; {league_id} is fine."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    ep = next(e for e in doc["endpoints"] if e["short"] == "next_match")
    assert ep["path"] == "/getnextmatchbyleagueteam/{league_id}/{team_id}"


def test_all_eleven_routes_are_wrapped():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert len(doc["endpoints"]) == 11
