"""Offline tests for the On3 rankings stem: parsers (real trimmed captures),
the buildId-discovery runtime, and the generated wrapper wiring."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "on3"


def _load(stem: str) -> dict:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


# ===========================================================================
# Parsers
# ===========================================================================


def test_parse_on3_rankings_real_capture():
    from sportsdataverse.cfb.on3_parsers import parse_on3_rankings

    df = parse_on3_rankings(_load("on3_player_rankings"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    for col in (
        "overall_rank",
        "consensus_overall_rank",
        "nil_value",
        "person_name",
        "person_rating_rating",
        "person_rating_consensus_rating",
        "person_rating_stars",
        "person_status_is_committed",
    ):
        assert col in df.columns, f"missing {col}"
    # numeric / boolean dtypes survive the pandas->polars handoff
    assert df.schema["nil_value"].is_integer() or df.schema["nil_value"].is_float()
    assert df.schema["person_status_is_committed"] == pl.Boolean
    # list-valued cells are stringified JSON
    assert df.schema["ratings"] == pl.String
    json.loads(df["ratings"][0])  # round-trips


def test_parse_on3_rankings_dedupes_colliding_snake_names():
    """person.highSchoolName and person.highSchool.name both snake-case to
    person_high_school_name — the second occurrence gains a _2 suffix."""
    from sportsdataverse.cfb.on3_parsers import parse_on3_rankings

    df = parse_on3_rankings(_load("on3_player_rankings"))
    assert "person_high_school_name" in df.columns
    assert "person_high_school_name_2" in df.columns


def test_parse_on3_team_rankings_real_capture():
    from sportsdataverse.cfb.on3_parsers import parse_on3_team_rankings

    df = parse_on3_team_rankings(_load("on3_team_rankings"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    for col in (
        "year",
        "commits",
        "overall_rank",
        "overall_consensus_rank",
        "average_nil_value",
        "organization_name",
        "five_stars",
    ):
        assert col in df.columns, f"missing {col}"


@pytest.mark.parametrize("payload", [None, {}, {"pageProps": {}}, {"pageProps": {"playerData": {"list": []}}}])
def test_parsers_return_zero_row_frame_on_empty(payload):
    from sportsdataverse.cfb.on3_parsers import parse_on3_rankings, parse_on3_team_rankings

    assert parse_on3_rankings(payload).height == 0
    assert parse_on3_team_rankings(payload).height == 0


def test_parsers_return_pandas_when_asked():
    import pandas as pd

    from sportsdataverse.cfb.on3_parsers import parse_on3_rankings

    pdf = parse_on3_rankings(_load("on3_player_rankings"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == 3


# ===========================================================================
# Runtime (buildId discovery + stale-buildId retry)
# ===========================================================================


class _Resp:
    def __init__(self, *, text: str = "", body=None):
        self.text = text
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


@pytest.fixture()
def on3_runtime():
    import sportsdataverse.cfb.on3_runtime as rt

    old = rt._build_id
    rt._build_id = None
    yield rt
    rt._build_id = old


def test_extract_build_id(on3_runtime):
    html = '<script id="__NEXT_DATA__">{"props":{},"buildId":"abc-123_XY","page":"/x"}</script>'
    assert on3_runtime._extract_build_id(html) == "abc-123_XY"
    assert on3_runtime._extract_build_id("<html>no blob</html>") is None


def test_get_discovers_build_id_and_hits_data_route(on3_runtime, monkeypatch):
    calls = []

    def fake_download(url=None, params=None, headers=None, **kwargs):
        calls.append((url, params))
        if "/_next/data/" in url:
            return _Resp(body={"pageProps": {"ok": True}})
        return _Resp(text='"buildId":"bid-one"')

    monkeypatch.setattr(on3_runtime, "download", fake_download)
    out = on3_runtime._scrape_get("https://www.on3.com/rivals/rankings/player/football/2026.json", params={"page": 2})
    assert out == {"pageProps": {"ok": True}}
    page_call, data_call = calls
    assert page_call[0] == "https://www.on3.com/db/rankings/player/football/2026/"
    assert data_call[0] == "https://www.on3.com/_next/data/bid-one/rivals/rankings/player/football/2026.json"
    # required derived query params + passthrough page
    assert data_call[1] == {"rankingType": "player", "sport": "football", "year": "2026", "page": 2}
    assert on3_runtime._build_id == "bid-one"


def test_get_refreshes_build_id_after_deploy_rotation(on3_runtime, monkeypatch):
    """A stale buildId 404s (NoESPNDataError); _get re-discovers and retries once."""
    from sportsdataverse.errors import NoESPNDataError

    on3_runtime._build_id = "stale-bid"
    state = {"page_fetches": 0}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        if "/_next/data/stale-bid/" in url:
            raise NoESPNDataError("404")
        if "/_next/data/fresh-bid/" in url:
            return _Resp(body={"pageProps": {"fresh": True}})
        state["page_fetches"] += 1
        return _Resp(text='"buildId":"fresh-bid"')

    monkeypatch.setattr(on3_runtime, "download", fake_download)
    out = on3_runtime._scrape_get("https://www.on3.com/rivals/rankings/team/football/2026.json")
    assert out == {"pageProps": {"fresh": True}}
    assert on3_runtime._build_id == "fresh-bid"
    assert state["page_fetches"] == 1


def test_get_treats_unchanged_build_id_404_as_no_data(on3_runtime, monkeypatch):
    """When re-discovery returns the SAME buildId that just 404'd, the 404 is
    authoritative (resource genuinely absent) — no second data fetch is spent."""
    from sportsdataverse.errors import NoESPNDataError

    on3_runtime._build_id = "same-bid"
    calls = {"data": 0, "page": 0}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        if "/_next/data/" in url:
            calls["data"] += 1
            raise NoESPNDataError("404")
        calls["page"] += 1
        return _Resp(text='"buildId":"same-bid"')

    monkeypatch.setattr(on3_runtime, "download", fake_download)
    assert on3_runtime._scrape_get("https://www.on3.com/rivals/rankings/player/football/2031.json") == {}
    assert calls == {"data": 1, "page": 1}


def test_get_merges_caller_headers_and_derived_params_win(on3_runtime, monkeypatch):
    seen = {}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        if "/_next/data/" in url:
            seen["headers"] = headers
            seen["params"] = params
            return _Resp(body={"pageProps": {}})
        return _Resp(text='"buildId":"b1"')

    monkeypatch.setattr(on3_runtime, "download", fake_download)
    on3_runtime._scrape_get(
        "https://www.on3.com/rivals/rankings/player/football/2026.json",
        params={"rankingType": "team", "page": 3},
        headers={"X-Test": "1"},
    )
    assert seen["headers"]["X-Test"] == "1"
    assert "User-Agent" in seen["headers"]
    # the path-derived triple is authoritative over caller params
    assert seen["params"]["rankingType"] == "player"
    assert seen["params"]["page"] == 3


def test_get_returns_empty_dict_on_unknown_path(on3_runtime):
    assert on3_runtime._scrape_get("https://www.on3.com/some/other/route.json") == {}


# ===========================================================================
# Generated wrapper wiring
# ===========================================================================


def test_wrapper_routes_fixture_through_parser(monkeypatch):
    import sportsdataverse.cfb.on3 as on3

    fixture = _load("on3_player_rankings")
    monkeypatch.setattr(on3, "_get", lambda *args, **kwargs: fixture)

    raw = on3.on3_player_rankings(year=2026, return_parsed=False)
    assert isinstance(raw, dict)
    assert "pageProps" in raw

    df = on3.on3_player_rankings(year=2026)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    assert "person_name" in df.columns


def test_all_four_wrappers_exported():
    from sportsdataverse.cfb import (
        on3_industry_player_rankings,
        on3_industry_team_rankings,
        on3_player_rankings,
        on3_team_rankings,
    )

    for fn in (on3_player_rankings, on3_industry_player_rankings, on3_team_rankings, on3_industry_team_rankings):
        assert callable(fn)
