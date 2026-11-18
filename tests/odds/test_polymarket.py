"""Polymarket (gamma + clob + US) parser + generated-YAML contract."""

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


def test_us_markets_envelope_is_one_row_per_market():
    """US /v1/markets is {"markets": [...]}: the single key is unwrapped, never one envelope row."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("gateway__v1__markets")
    assert list(body) == ["markets"]
    df = parse_polymarket(body)
    assert df.height == len(body["markets"])
    assert df["slug"].to_list() == [m["slug"] for m in body["markets"]]
    assert df.schema["id"] == pl.String and "markets" not in df.columns


def test_us_bbo_is_one_row_with_flattened_prices():
    """US /v1/markets/{slug}/bbo is {"marketData": {...}}; {value, currency} prices flatten."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("gateway__v1__markets__slug__bbo")
    inner = body["marketData"]
    df = parse_polymarket(body)
    assert df.height == 1 and "market_data" not in df.columns
    assert df["market_slug"][0] == inner["marketSlug"]
    for col, key in (("best_bid_value", "bestBid"), ("best_ask_value", "bestAsk"), ("current_px_value", "currentPx")):
        assert df[col][0] == inner[key]["value"], col  # decimal strings, kept as strings
    assert df["best_bid_currency"][0] == "USD"


def test_us_expired_bbo_keeps_the_null_bests_and_drops_the_quotes():
    """An expired market answers bestBid/bestAsk null and no longQuote/shortQuote: one row, no invented columns."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    inner = load("gateway__v1__markets__slug__bbo__expired")["marketData"]
    assert inner["bestBid"] is None and "longQuote" not in inner
    df = parse_polymarket({"marketData": inner})
    assert df.height == 1 and df["state"][0] == "MARKET_STATE_EXPIRED"
    assert df["best_bid"][0] is None and "best_bid_value" not in df.columns
    assert not any(c.startswith("long_quote") for c in df.columns)


def test_us_closed_markets_page_parses_like_the_open_one():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    body = load("gateway__v1__markets__closed")
    df = parse_polymarket(body)
    assert df.height == len(body["markets"])
    assert df["closed"].to_list() == [True] * df.height
    assert all(s.startswith("aec-") for s in df["slug"].to_list())  # closed game markets carry aec- slugs


def test_single_key_scalar_and_empty_envelopes():
    """A one-value scalar dict is still one row; an empty single-key envelope is zero rows."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    assert parse_polymarket({"mid": "0.5"}).height == 1
    assert parse_polymarket({"events": [{"id": "1"}, {"id": "2"}]}).height == 2
    for payload in (
        {"markets": []},
        {"marketData": {}},
        {"marketData": None},
        {"markets": "x"},
        {"markets": {"id": "1"}},  # a list key holding an object
        {"marketData": [{"marketSlug": "s"}]},  # an object key holding a list
    ):
        assert parse_polymarket(payload).height == 0, payload
    # an envelope keeps exactly the rows its bare list would
    rows = [{}, {"id": "1"}]
    assert parse_polymarket({"markets": rows}).height == parse_polymarket(rows).height == 1


def test_only_known_envelope_keys_are_unwrapped():
    """An unknown one-key object (a nested error) is one row with its prefix, never unwrapped."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    df = parse_polymarket({"error": {"code": "not_found", "message": "no market"}})
    assert df.height == 1 and {"error_code", "error_message"} <= set(df.columns)


@pytest.mark.parametrize(
    "fixture,schema",
    [
        ("gateway__v1__markets", "us_markets"),
        ("gateway__v1__markets__slug__bbo", "us_market_bbo"),
        ("gateway__v1__markets__slug__book", "us_market_book"),
        ("gateway__v1__markets__slug__settlement", "us_market_settlement"),
        ("gateway__v1__market__id__id", "us_market_by_id"),
        ("gateway__v1__market__slug__slug", "us_market_by_slug"),
    ],
)
def test_us_returns_schema_matches_the_parser_on_its_capture(fixture, schema):
    """The committed returns table is the parser's output on the committed capture (names and order)."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    path = YAML_PATH.parents[1] / "schemas" / "native" / "polymarket" / f"{schema}.yaml"
    columns = [c["name"] for c in yaml.safe_load(path.read_text(encoding="utf-8"))["columns"]]
    assert columns == parse_polymarket(load(fixture)).columns


def test_us_book_is_one_row_per_level_with_offers_as_asks():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    inner = load("gateway__v1__markets__slug__book")["marketData"]
    df = parse_polymarket({"marketData": inner})
    assert df.height == len(inner["bids"]) + len(inner["offers"])
    assert df.filter(pl.col("side") == "bid")["px_value"].to_list() == [b["px"]["value"] for b in inner["bids"]]
    assert df.filter(pl.col("side") == "ask")["px_value"].to_list() == [o["px"]["value"] for o in inner["offers"]]
    assert set(df["market_slug"]) == {inner["marketSlug"]} and set(df["state"]) == {"MARKET_STATE_OPEN"}
    assert not any(c.startswith("stats") for c in df.columns)  # nested stats sit beside the levels, not on them


def test_us_expired_book_has_no_levels_so_zero_rows():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    assert parse_polymarket(load("gateway__v1__markets__slug__book__expired")).height == 0


def test_us_settlement_is_one_float_row():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    df = parse_polymarket(load("gateway__v1__markets__slug__settlement"))
    assert df.rows() == [("aec-nba-sa-sac-2026-11-06", 1.0)] and df.schema["settlement"] == pl.Float64
    zero = parse_polymarket({"slug": "s", "settlement": 0})  # an int body still comes out Float64
    assert zero.schema["settlement"] == pl.Float64 and zero["settlement"].to_list() == [0.0]


@pytest.mark.parametrize("bad", ["N/A", True])
def test_us_settlement_that_is_not_a_number_is_kept_and_warned(bad):
    """A value the Float64 cast would null (or a boolean it would turn into a price) is refused, not forced."""
    from sportsdataverse.errors import SchemaDriftWarning
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    with pytest.warns(SchemaDriftWarning, match="settlement"):
        df = parse_polymarket({"slug": "s", "settlement": bad})
    assert df["settlement"].to_list() == [bad]


def test_us_book_with_a_malformed_side_is_zero_rows():
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    assert parse_polymarket({"marketData": {"bids": 5, "offers": []}}).height == 0
    assert parse_polymarket({"bids": 5, "asks": None}).height == 0


def test_us_single_market_is_the_listing_row():
    """/v1/market/id/{id} and /v1/market/slug/{slug} answer one /v1/markets row under {"market": {...}}."""
    from sportsdataverse.odds.polymarket_parsers import parse_polymarket

    by_id = parse_polymarket(load("gateway__v1__market__id__id"))
    by_slug = parse_polymarket(load("gateway__v1__market__slug__slug"))
    assert by_id.height == by_slug.height == 1 and by_id.equals(by_slug)
    assert by_id["id"].to_list() == ["7984"] and by_id.schema["id"] == pl.String
    assert set(by_id.columns) == set(parse_polymarket(load("gateway__v1__markets")).columns)
    expired = parse_polymarket(load("gateway__v1__market__slug__slug__expired"))
    assert expired.height == 1 and expired["slug"].to_list() == ["aec-nba-sa-sac-2026-11-06"]
    # 1 on the open market, 0.01 on the expired one: one dtype either way
    assert by_id.schema["minimum_trade_qty"] == expired.schema["minimum_trade_qty"] == pl.Float64
    assert expired["minimum_trade_qty"].to_list() == [0.01]
    assert parse_polymarket({"market": [{"id": "1"}]}).height == 0  # an object key holding a list


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
    us = (
        "us_markets",
        "us_market_bbo",
        "us_market_book",
        "us_market_settlement",
        "us_market_by_id",
        "us_market_by_slug",
    )
    for short in us:
        assert hosts[short] == "https://gateway.polymarket.us", short
    assert by_short["us_market_by_id"]["path"] == "/v1/market/id/{id}"
    assert [p["name"] for p in by_short["us_market_by_id"]["path_params"]] == ["id"]
    assert by_short["us_market_bbo"]["path"] == "/v1/markets/{slug}/bbo"
    assert [p["name"] for p in by_short["us_market_bbo"]["path_params"]] == ["slug"]


def test_all_fourteen_routes_are_wrapped_once():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    shorts = [e["short"] for e in doc["endpoints"]]
    assert len(shorts) == len(set(shorts)) == 14


def test_paging_is_caller_supplied_not_followed():
    """Captures are first page only; the cursor is a parameter, never auto-followed."""
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    by_short = {e["short"]: e for e in doc["endpoints"]}
    assert "offset" in {p["name"] for p in by_short["gamma_markets"]["extra_params"]}
    assert "next_cursor" in {p["name"] for p in by_short["clob_markets"]["extra_params"]}
    assert {"limit", "offset", "active", "closed"} <= {p["name"] for p in by_short["us_markets"]["extra_params"]}
