"""Offline tests for the retargeted On3 Recruit Database (RDB) stem:
the auth-free ``on3_runtime._get``, the ``parse_on3_rdb`` envelope parser, and
the generated RDB wrapper wiring. Real trimmed captures, no network."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).parent / "fixtures" / "on3"


def _load(stem: str):
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, *, text: str = "", body=None):
        self.text = text
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


@pytest.fixture()
def rt():
    import sportsdataverse.cfb.on3_runtime as runtime

    return runtime


# ===========================================================================
# RDB runtime (auth-free plain GET)
# ===========================================================================


def test_get_hits_rdb_host_and_returns_json(rt, monkeypatch):
    seen = {}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        seen["url"] = url
        seen["params"] = params
        seen["headers"] = headers
        return _Resp(body={"list": [{"key": 1}], "pagination": {}})

    monkeypatch.setattr(rt, "download", fake_download)
    out = rt._get(
        "https://api.on3.com/public/rdb/v1/commits/latest",
        params={"sportKey": 1, "page": None},
    )
    assert seen["url"] == "https://api.on3.com/public/rdb/v1/commits/latest"
    assert seen["params"] == {"sportKey": 1}  # None-valued dropped
    assert "User-Agent" in seen["headers"]
    assert out == {"list": [{"key": 1}], "pagination": {}}


def test_get_returns_bare_list(rt, monkeypatch):
    monkeypatch.setattr(rt, "download", lambda **kw: _Resp(body=[{"a": 1}]))
    assert rt._get("https://api.on3.com/public/rdb/v1/filters/status") == [{"a": 1}]


def test_get_empty_on_non_json(rt, monkeypatch):
    monkeypatch.setattr(rt, "download", lambda **kw: _Resp(text="<html>"))
    assert rt._get("https://api.on3.com/public/rdb/v1/anything") == {}


def test_get_empty_on_no_data_error(rt, monkeypatch):
    from sportsdataverse.errors import NoESPNDataError

    def boom(**kw):
        raise NoESPNDataError("404")

    monkeypatch.setattr(rt, "download", boom)
    assert rt._get("https://api.on3.com/public/rdb/v1/anything") == {}


def test_get_merges_caller_headers(rt, monkeypatch):
    seen = {}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        seen["headers"] = headers
        return _Resp(body={})

    monkeypatch.setattr(rt, "download", fake_download)
    rt._get("https://api.on3.com/public/rdb/v1/x", headers={"X-Test": "1"})
    assert seen["headers"]["X-Test"] == "1"
    assert "User-Agent" in seen["headers"]


# ===========================================================================
# parse_on3_rdb -- three envelope shapes
# ===========================================================================


def test_parse_on3_rdb_paged():
    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    df = parse_on3_rdb(_load("team_ranking_team_rankings"))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "overall_rank" in df.columns


def test_parse_on3_rdb_single_object():
    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    df = parse_on3_rdb(_load("player_profile"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    assert "key" in df.columns


def test_parse_on3_rdb_bare_array():
    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    df = parse_on3_rdb(_load("player_all_rankings"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3


@pytest.mark.parametrize("payload", [None, {}, [], {"list": []}, "nope", 5])
def test_parse_on3_rdb_empty(payload):
    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    assert parse_on3_rdb(payload).height == 0


def test_parse_on3_rdb_pandas():
    import pandas as pd

    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    pdf = parse_on3_rdb(_load("player_profile"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == 1


# ===========================================================================
# Generated RDB wrapper wiring
# ===========================================================================


def test_wrapper_routes_through_parse_on3_rdb(monkeypatch):
    import sportsdataverse.cfb.on3 as on3

    fixture = _load("team_ranking_team_rankings")
    monkeypatch.setattr(on3, "_get", lambda *a, **k: fixture)

    df = on3.on3_team_ranking_team_rankings(sport_slug="football", year=2025)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "overall_rank" in df.columns

    raw = on3.on3_team_ranking_team_rankings(sport_slug="football", year=2025, return_parsed=False)
    assert isinstance(raw, (dict, list))


def test_player_person_rankings_wrapper_reachable():
    # The RDB /player/{personKey}/rankings native is renamed to avoid colliding
    # with the deprecated on3_player_rankings shim; it must stay importable.
    from sportsdataverse.cfb import on3_player_person_rankings

    assert callable(on3_player_person_rankings)


# ===========================================================================
# Live smoke tests (standard SDV_PY_LIVE_TESTS gate -- RDB is auth-free and
# does NOT JA3-block, so it is NOT on the separate SDV_PY_NBA_STATS_LIVE gate)
# ===========================================================================


@skip_if_no_live
def test_on3_filters_status_live():
    from sportsdataverse.cfb import on3_filters_status

    df = on3_filters_status()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0


@skip_if_no_live
def test_on3_commits_latest_live():
    from sportsdataverse.cfb import on3_commits_latest

    df = on3_commits_latest(sport_key=1)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "name" in df.columns


@skip_if_no_live
def test_on3_team_ranking_team_rankings_live():
    from sportsdataverse.cfb import on3_team_ranking_team_rankings

    df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "overall_rank" in df.columns


def test_parse_on3_rdb_scalar_array():
    # filters/status serves a bare array of strings -> single `value` column
    from sportsdataverse.cfb.on3_parsers import parse_on3_rdb

    df = parse_on3_rdb(["Committed", "Decommitted", "Signed"])
    assert df.height == 3 and df.columns == ["value"]
    # mixed scalar/dict rows: scalars wrap as {"value": ...}
    mixed = parse_on3_rdb([{"name": "x"}, "loose"])
    assert mixed.height == 2 and "value" in mixed.columns
