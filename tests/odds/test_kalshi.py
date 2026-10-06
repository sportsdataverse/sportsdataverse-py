"""Kalshi Trade API v2 market-data parser + generated-YAML contract."""

import json
import pathlib

import pandas as pd
import polars as pl
import pytest
import yaml

FIXTURES = pathlib.Path(__file__).parent.parent / "fixtures" / "kalshi"
YAML_PATH = pathlib.Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "kalshi.yaml"


def load(name):
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "fixture,key",
    [("events", "events"), ("markets", "markets"), ("markets__trades", "trades"), ("series", "series")],
)
def test_list_envelopes_are_one_row_per_element(fixture, key):
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    body = load(fixture)
    assert parse_kalshi(body).height == len(body[key])


def test_events_ignores_the_sibling_milestones_list():
    """Two list keys: rows come from `events`, by preference order, not whichever is first."""
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    body = load("events")
    body["milestones"] = [{"id": "m1"}, {"id": "m2"}]
    df = parse_kalshi(body)
    assert df.height == len(body["events"])
    assert "event_ticker" in df.columns


def test_event_detail_rows_come_from_the_event_not_its_markets():
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    df = parse_kalshi(load("events__event_ticker"))
    assert df.height == 1
    assert "event_ticker" in df.columns


@pytest.mark.parametrize("fixture", ["markets__ticker", "series__series_ticker"])
def test_single_object_envelopes_are_one_row(fixture):
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    assert parse_kalshi(load(fixture)).height == 1


def test_exchange_status_is_one_row():
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    df = parse_kalshi(load("exchange__status"))
    assert df.height == 1
    assert "trading_active" in df.columns


def test_orderbook_levels_are_rows_with_side_price_size():
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    book = load("markets__ticker__orderbook")["orderbook_fp"]
    df = parse_kalshi(load("markets__ticker__orderbook"))
    assert df.height == len(book["yes_dollars"]) + len(book["no_dollars"])
    assert set(df["side"].unique()) == {"yes", "no"}
    assert df.schema["price"] == pl.String
    assert df.schema["size"] == pl.String


def test_ticker_columns_are_utf8():
    """Review Focus 5: tickers like KXNFLGAME-26OCT08TBDAL-DAL are identifiers, not numbers."""
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    df = parse_kalshi(load("markets"))
    for name, dtype in df.schema.items():
        if name == "ticker" or name.endswith("_ticker") or name.endswith("_id"):
            assert dtype == pl.String, name


def test_empty_payload_is_zero_rows():
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    for payload in (None, {}, [], 7, {"cursor": "abc"}, {"events": []}):
        df = parse_kalshi(payload)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0


def test_pandas_round_trip():
    from sportsdataverse.odds.kalshi_parsers import parse_kalshi

    assert isinstance(parse_kalshi(load("markets"), return_as_pandas=True), pd.DataFrame)


def test_all_nine_routes_are_wrapped():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    assert len(doc["endpoints"]) == 9
    assert doc["host"] == "https://api.elections.kalshi.com/trade-api/v2"
    for ep in doc["endpoints"]:
        assert ep["parser"] == "parse_kalshi"


def test_cursor_is_a_caller_parameter():
    doc = yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))
    for short in ("events", "markets", "trades"):
        ep = next(e for e in doc["endpoints"] if e["short"] == short)
        assert "cursor" in {p["name"] for p in ep["extra_params"]}, short
