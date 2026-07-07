"""Offline tests for the sports247 stem (247Sports RDB public endpoints):
parsers on real captures, the transport-injectable runtime, and wrapper wiring."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "sports247"


def _load(stem: str):
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


# ===========================================================================
# Parsers
# ===========================================================================


def test_parse_sports247_teams_real_capture():
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_teams

    df = parse_sports247_teams(_load("sports247_teams_football"))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 100
    for col in ("name", "team_id", "institution_key", "conference", "conference_abbreviation", "sport"):
        assert col in df.columns, f"missing {col}"
    assert df.schema["team_id"].is_integer()
    assert df.schema["institution_key"].is_integer()


def test_parse_sports247_institution_rankings_real_capture():
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_institution_rankings

    df = parse_sports247_institution_rankings(_load("sports247_institution_rankings_fb_2026"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 5
    for col in (
        "rank",
        "composite_rank",
        "rating",
        "composite_rating",
        "commits",
        "five_stars",
        "composite_five_stars",
        "institution_key",
        "team_key",
    ):
        assert col in df.columns, f"missing {col}"
    # ranks ascend from 1 on page one
    assert df["rank"][0] == 1


@pytest.mark.parametrize("payload", [None, {}, [], {"list": []}, {"pagination": {}}, "nope"])
def test_parsers_zero_row_on_empty(payload):
    from sportsdataverse.cfb.sports247_parsers import (
        parse_sports247_institution_rankings,
        parse_sports247_teams,
    )

    assert parse_sports247_teams(payload).height == 0
    assert parse_sports247_institution_rankings(payload).height == 0


def test_parsers_return_pandas_when_asked():
    import pandas as pd

    from sportsdataverse.cfb.sports247_parsers import parse_sports247_teams

    pdf = parse_sports247_teams(_load("sports247_teams_football"), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) > 100


# ===========================================================================
# Runtime (transport-injectable _get)
# ===========================================================================


def test_get_appends_trailing_slash_and_strips_none_params():
    from sportsdataverse.cfb.sports247_runtime import _get

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen.update(url=url, params=params, headers=headers)
        return 200, '[{"teamId": 1}]'

    out = _get(
        "https://ipa.247sports.com/rdb/v1/teams",
        {"sportKey": 1, "year": None},
        transport=fake,
    )
    assert out == [{"teamId": 1}]
    assert seen["url"] == "https://ipa.247sports.com/rdb/v1/teams/"
    assert seen["params"] == {"sportKey": 1}
    assert "User-Agent" in seen["headers"]


def test_get_returns_empty_dict_on_non_200_or_bad_json():
    from sportsdataverse.cfb.sports247_runtime import _get

    assert _get("https://ipa.247sports.com/rdb/v1/teams/", transport=lambda *a: (403, "")) == {}
    assert _get("https://ipa.247sports.com/rdb/v1/teams/", transport=lambda *a: (401, "")) == {}
    assert _get("https://ipa.247sports.com/rdb/v1/teams/", transport=lambda *a: (200, "<html>")) == {}


# ===========================================================================
# Generated wrapper wiring
# ===========================================================================


def test_wrappers_route_fixtures_through_parsers(monkeypatch):
    import sportsdataverse.cfb.sports247 as s247

    monkeypatch.setattr(s247, "_get", lambda *args, **kwargs: _load("sports247_teams_football"))
    df = s247.sports247_teams()
    assert isinstance(df, pl.DataFrame)
    assert "institution_key" in df.columns

    raw = s247.sports247_teams(return_parsed=False)
    assert isinstance(raw, list)

    monkeypatch.setattr(s247, "_get", lambda *args, **kwargs: _load("sports247_institution_rankings_fb_2026"))
    rdf = s247.sports247_institution_rankings(year=2026)
    assert isinstance(rdf, pl.DataFrame)
    assert rdf.height == 5


def test_wrappers_exported_from_cfb_package():
    from sportsdataverse.cfb import sports247_institution_rankings, sports247_teams

    assert callable(sports247_teams)
    assert callable(sports247_institution_rankings)
