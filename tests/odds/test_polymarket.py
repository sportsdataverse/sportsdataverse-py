"""Polymarket (gamma + clob) parser + generated-YAML contract."""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "polymarket"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "polymarket.yaml"


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("fixture", ["gamma-api__markets", "gamma-api__events", "gamma-api__tags"])
def test_gamma_list_bodies_are_one_row_per_element(fixture):
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load(fixture)
    assert isinstance(body, list)
    assert parse_polymarket(body).height == len(body)


def test_clob_markets_rows_come_from_data():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("clob__markets")
    assert parse_polymarket(body).height == len(body["data"])


def test_one_gamma_market_is_one_row_without_schema_column():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    df = parse_polymarket(load("gamma-api__markets__id"))
    assert df.height == 1
    assert not any(c.startswith("$") or c == "schema" for c in df.columns)


def test_book_levels_carry_a_side_and_the_asset_id():
    """A two-list envelope: bids and asks both become rows, tagged by side."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("clob__book")
    df = parse_polymarket(body)
    assert df.height == len(body["bids"]) + len(body["asks"])
    assert set(df["side"].unique()) == {"bid", "ask"}
    assert df["asset_id"].unique().to_list() == [body["asset_id"]]
    assert {"price", "size"} <= set(df.columns)


def test_token_id_round_trips_as_string():
    """Review Focus 5: asset_id is 77 digits -- a numeric cast destroys it."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("clob__book")
    df = parse_polymarket(body)
    assert df.schema["asset_id"] == pl.String
    assert df["asset_id"][0] == body["asset_id"]
    assert len(df["asset_id"][0]) > 70


@pytest.mark.parametrize("fixture,column", [("clob__midpoint", "mid"), ("clob__price", "price")])
def test_scalar_dict_bodies_are_one_row(fixture, column):
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    df = parse_polymarket(load(fixture))
    assert df.height == 1
    assert column in df.columns


def test_empty_payload_is_zero_rows():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    for payload in (None, {}, [], 7, {"data": []}, {"bids": [], "asks": []}):
        df = parse_polymarket(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


def test_pandas_round_trip():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    assert isinstance(parse_polymarket(load("gamma-api__markets"), return_as_pandas=True), pd.DataFrame)


def test_each_market_wrapper_uses_its_own_host():
    """Review Focus 4: both hosts serve /markets; a shared default would hit the wrong one."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    by_short = {e["short"]: e for e in doc["endpoints"]}
    assert by_short["gamma_markets"]["path"] == "/markets"
    assert by_short["clob_markets"]["path"] == "/markets"
    hosts = {s: e.get("host", doc["host"]) for s, e in by_short.items()}
    assert hosts["gamma_markets"] == "https://gamma-api.polymarket.com"
    assert hosts["clob_markets"] == "https://clob.polymarket.com"
    for short in ("gamma_events", "gamma_tags", "gamma_market"):
        assert hosts[short] == "https://gamma-api.polymarket.com", short
    for short in ("clob_book", "clob_midpoint", "clob_price"):
        assert hosts[short] == "https://clob.polymarket.com", short


def test_all_eight_routes_are_wrapped_once():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    shorts = [e["short"] for e in doc["endpoints"]]
    assert len(shorts) == len(set(shorts)) == 8


def test_paging_is_caller_supplied_not_followed():
    """Captures are first page only; the cursor is a parameter, never auto-followed."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    by_short = {e["short"]: e for e in doc["endpoints"]}
    assert "offset" in {p["name"] for p in by_short["gamma_markets"]["extra_params"]}
    assert "next_cursor" in {p["name"] for p in by_short["clob_markets"]["extra_params"]}
