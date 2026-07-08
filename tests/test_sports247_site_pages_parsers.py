"""Offline tests for the sports247_site_pages stem (247sports.com front-end page
models): the transport-injectable auth-free runtime, the generic FK-surfacing +
string-numeric-cast parser on real captures, and wrapper wiring."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "sports247_site_pages"


def _load(stem: str):
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


# ===========================================================================
# Runtime (transport-injectable _get; auth-free; no trailing-slash rewrite)
# ===========================================================================


def test_get_does_not_append_trailing_slash():
    # site-pages URLs terminate in .json; a trailing slash would 404
    from sportsdataverse.cfb.sports247_site_pages_runtime import _get

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen["url"] = url
        return 200, '{"Key": 24099}'

    _get("https://247sports.com/Institution/24099.json", transport=fake)
    assert seen["url"] == "https://247sports.com/Institution/24099.json"  # unchanged


def test_get_sends_no_authorization_header():
    from sportsdataverse.cfb.sports247_site_pages_runtime import _get

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen["auth"] = headers.get("Authorization")
        seen["ua"] = headers.get("User-Agent")
        return 200, "[]"

    _get("https://247sports.com/Institution.json", {"items": 50}, transport=fake)
    assert seen["auth"] is None  # no JWT / bearer on the site-pages surface
    assert seen["ua"]  # browser UA present


def test_get_strips_none_params_and_parses_array():
    from sportsdataverse.cfb.sports247_site_pages_runtime import _get

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen["params"] = params
        return 200, '[{"Key": 1}]'

    out = _get(
        "https://247sports.com/Institution.json",
        {"items": 50, "Page": None},
        transport=fake,
    )
    assert out == [{"Key": 1}]
    assert seen["params"] == {"items": 50}


def test_get_returns_empty_dict_on_non_200_or_bad_json():
    from sportsdataverse.cfb.sports247_site_pages_runtime import _get

    assert _get("https://247sports.com/Institution.json", transport=lambda *a: (403, "")) == {}
    assert _get("https://247sports.com/Institution.json", transport=lambda *a: (200, "")) == {}
    assert _get("https://247sports.com/Institution.json", transport=lambda *a: (200, "<html>")) == {}


# ===========================================================================
# Parser (FK-surfacing + string-numeric cast) on real captures
# ===========================================================================


def test_single_object_capture_is_one_row_with_int_fk_and_cast_numerics():
    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    df = parse_sports247_site_page(_load("institution"))
    assert df.height == 1
    assert {"key", "name", "location", "state", "latitude"}.issubset(df.columns)
    assert df.schema["key"].is_integer()  # bare int PK
    assert df.schema["location"].is_integer()  # bare int FK, surfaced, NOT traversed
    assert df.schema["state"].is_integer()  # bare int FK
    assert df.schema["latitude"] == pl.Float64  # string "0.000000" cast to real dtype
    assert df.schema["name"] == pl.Utf8  # genuine string stays Utf8


def test_array_capture_with_inlined_player_flattens_fk_columns():
    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    df = parse_sports247_site_page(_load("recruits_season"))
    assert df.height > 1
    # inlined Player object -> flattened (sep="_"); bare FK ints surfaced not inlined
    assert {"key", "player_key", "player_full_name", "institution", "player_sport"}.issubset(df.columns)
    assert df.schema["player_key"].is_integer()


def test_genuine_string_numeric_hybrids_stay_utf8():
    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    # Rankable="True" and Height="6-5" style values must NOT be coerced numeric.
    df = parse_sports247_site_page(_load("institution"))
    assert df.schema["rankable"] == pl.Utf8


@pytest.mark.parametrize("payload", [None, {}, [], "nope", 3])
def test_zero_row_on_empty_or_malformed(payload):
    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    assert parse_sports247_site_page(payload).height == 0


def test_returns_pandas_when_asked():
    import pandas as pd

    from sportsdataverse.cfb.sports247_site_pages_parsers import parse_sports247_site_page

    assert isinstance(parse_sports247_site_page(_load("institution"), return_as_pandas=True), pd.DataFrame)
