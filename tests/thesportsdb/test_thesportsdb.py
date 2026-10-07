"""TheSportsDB v1 parser + generated-YAML contract."""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "thesportsdb"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "thesportsdb.yaml"


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "fixture,key",
    [
        ("all_sports", "sports"),
        ("all_leagues", "leagues"),
        ("lookupleague", "leagues"),
        ("search_all_teams", "teams"),
        ("lookupteam", "teams"),
        ("lookup_all_players", "player"),
        ("searchplayers", "player"),
        ("lookupplayer", "players"),
        ("eventsseason", "events"),
        ("lookupevent", "events"),
        ("eventsnextleague", "events"),
        ("lookuptable", "table"),
    ],
)
def test_rows_come_from_the_single_envelope_key(fixture, key):
    from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

    body = load(fixture)
    assert key in body, f"{fixture} envelope key changed upstream"
    df = parse_thesportsdb(body)
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(body[key])


def test_null_list_is_zero_rows():
    """A no-results lookup answers {"teams": null}, not an empty list."""
    from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

    df = parse_thesportsdb({"teams": None})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_empty_payload_is_zero_rows():
    from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

    for payload in (None, {}, [], "nope", 7):
        assert parse_thesportsdb(payload).height == 0


def test_pandas_round_trip():
    from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

    assert isinstance(parse_thesportsdb(load("all_sports"), return_as_pandas=True), pd.DataFrame)


def test_id_columns_are_utf8():
    from sportsdataverse.thesportsdb.thesportsdb_parsers import parse_thesportsdb

    df = parse_thesportsdb(load("lookupteam"))
    for name, dtype in df.schema.items():
        if name == "id" or name.endswith("_id"):
            assert dtype == pl.String, name


def test_yaml_host_pins_the_free_key_so_the_docs_links_resolve():
    """The API key is a path segment, and the host is what the reference page prints as its
    base URL and bakes into every Valid URL example. A ``{key}`` placeholder there rendered
    24 dead links, so the host carries the documented free test key and the runtime swaps
    that segment for the caller's key."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert doc["host"] == "https://www.thesportsdb.com/api/v1/json/3"
    assert "{" not in doc["host"]
    assert doc["getter_module"] == "sportsdataverse.thesportsdb.thesportsdb_runtime"


def test_every_route_has_a_short_and_a_schema():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert len(doc["endpoints"]) == 12
    for ep in doc["endpoints"]:
        assert ep["returns_schema"] == f"native/thesportsdb/{ep['short']}"
        assert ep["parser"] == "parse_thesportsdb"


def test_required_query_params_are_marked_required():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    ep = next(e for e in doc["endpoints"] if e["short"] == "team")
    prm = next(p for p in ep["extra_params"] if p["name"] == "id")
    assert prm["description"].startswith("Required.")
