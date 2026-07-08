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
# Runtime (transport-injectable _get + guest-JWT bearer)
# ===========================================================================


@pytest.fixture(autouse=True)
def _stub_jwt(monkeypatch):
    """Keep _get fully offline: stub the guest-JWT mint (never hit the network)
    and reset the module-level token cache before each test."""
    import sportsdataverse.cfb.sports247_runtime as rt

    monkeypatch.setattr(rt, "_mint_guest_jwt", lambda: "stub-jwt")
    monkeypatch.setattr(rt, "_jwt", None)
    yield


def test_get_attaches_bearer_and_mints_once():
    import sportsdataverse.cfb.sports247_runtime as rt

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen["auth"] = headers.get("Authorization")
        return 200, '{"players": []}'

    rt._get("https://ipa.247sports.com/rdb/v1/recruits/", transport=fake)
    assert seen["auth"] == "Bearer stub-jwt"


def test_get_refreshes_jwt_once_on_401(monkeypatch):
    import sportsdataverse.cfb.sports247_runtime as rt

    tokens = iter(["expired", "fresh"])
    monkeypatch.setattr(rt, "_mint_guest_jwt", lambda: next(tokens))
    monkeypatch.setattr(rt, "_jwt", None)
    seen = []

    def fake(url, params, headers, proxy_url):
        seen.append(headers.get("Authorization"))
        return (401, "") if len(seen) == 1 else (200, '{"players": [{"key": 1}]}')

    out = rt._get("https://ipa.247sports.com/rdb/v1/recruits/", transport=fake)
    assert out == {"players": [{"key": 1}]}
    assert seen == ["Bearer expired", "Bearer fresh"]  # re-minted after the 401


def test_get_can_disable_auth():
    import sportsdataverse.cfb.sports247_runtime as rt

    seen = {}

    def fake(url, params, headers, proxy_url):
        seen["auth"] = headers.get("Authorization")
        return 200, "[]"

    rt._get("https://ipa.247sports.com/rdb/v1/teams/", transport=fake, auth=False)
    assert seen["auth"] is None


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


# ===========================================================================
# Generic result-set parser + JWT-unlocked endpoints
# ===========================================================================


def test_result_set_handles_array_envelope_scalar_and_single_object():
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_result_set

    # bare array
    assert parse_sports247_result_set([{"a": 1}, {"a": 2}]).height == 2
    # enveloped list (each envelope key)
    assert parse_sports247_result_set({"players": [{"k": 1}]}).height == 1
    assert parse_sports247_result_set({"results": [{"k": 1}, {"k": 2}]}).height == 2
    assert parse_sports247_result_set({"rankings": [{"k": 1}]}).height == 1
    # scalar array -> single 'value' column
    df = parse_sports247_result_set([2025, 2026])
    assert df.height == 2 and df.columns == ["value"]
    # single flat object -> one row
    assert parse_sports247_result_set({"abbreviation": "FL", "bettingUrl": None}).height == 1
    # empty / malformed
    for bad in (None, {}, {"pagination": {}}, "x", 3):
        assert parse_sports247_result_set(bad).height == 0


@pytest.mark.parametrize(
    ("stem", "min_cols"),
    [
        ("recruits_fb_2026", {"composite_rating", "composite_national_rank", "committed_institution_name"}),
        ("transfers_fb_2026", {"player_first_name", "player_transfer_source_institution"}),
        ("coaches_fb_2026", {"first_name", "overall_rank"}),
        ("transfer_portal_player_feed_fb_2026", {"full_name", "target_institution", "group_rank"}),
        ("composite_team_ranking_feed_fb_2026", {"composite_overall_rank", "five_stars"}),
        ("current_target_predictions_fb_2026", {"expert_name", "prediction", "player_name"}),
        ("tags_autocomplete", {"id", "name", "type"}),
        ("positions_fb_2026", {"group", "group_key", "label", "value"}),
        ("transfer_portal_only_team_feed_fb_2026", {"name", "number_of_transfers", "transfer_points"}),
        ("sports_year_fb", {"value"}),
    ],
)
def test_new_endpoint_fixtures_flatten_with_expected_columns(stem, min_cols):
    from sportsdataverse.cfb.sports247_parsers import parse_sports247_result_set

    df = parse_sports247_result_set(_load(f"sports247_{stem}"))
    assert df.height > 0
    assert min_cols.issubset(set(df.columns)), f"{stem} missing {min_cols - set(df.columns)}"


def test_new_wrappers_route_through_generic_parser(monkeypatch):
    import sportsdataverse.cfb.sports247 as s247

    monkeypatch.setattr(s247, "_get", lambda *a, **k: _load("sports247_recruits_fb_2026"))
    df = s247.sports247_recruits(year=2026)
    assert isinstance(df, pl.DataFrame)
    assert "composite_rating" in df.columns

    raw = s247.sports247_recruits(year=2026, return_parsed=False)
    assert isinstance(raw, dict) and "players" in raw


def test_all_unlocked_wrappers_exported():
    from sportsdataverse.cfb import (
        sports247_coaches,
        sports247_composite_team_ranking_feed,
        sports247_positions,
        sports247_recruits,
        sports247_sport_years,
        sports247_tags_autocomplete,
        sports247_target_predictions,
        sports247_transfer_portal_player_feed,
        sports247_transfer_portal_team_feed,
        sports247_transfers,
    )

    for fn in (
        sports247_recruits,
        sports247_transfers,
        sports247_coaches,
        sports247_transfer_portal_player_feed,
        sports247_composite_team_ranking_feed,
        sports247_transfer_portal_team_feed,
        sports247_target_predictions,
        sports247_sport_years,
        sports247_tags_autocomplete,
        sports247_positions,
    ):
        assert callable(fn)


def test_positions_wrapper_routes_through_generic_parser(monkeypatch):
    import sportsdataverse.cfb.sports247 as s247

    monkeypatch.setattr(s247, "_get", lambda *a, **k: _load("sports247_positions_fb_2026"))
    df = s247.sports247_positions(sport_key=1)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert {"group", "group_key", "label", "value"}.issubset(set(df.columns))

    raw = s247.sports247_positions(sport_key=1, return_parsed=False)
    assert isinstance(raw, list)
