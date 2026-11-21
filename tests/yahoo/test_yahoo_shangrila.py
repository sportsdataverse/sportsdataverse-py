"""Offline tests for the Yahoo Sports stem: the ``yahoo_shangrila_runtime._get``
getter, the two shangrila parsers, and the generated wrapper wiring. No network.

Fixture provenance:

* ``tests/fixtures/yahoo/*.json`` are **real captures**, trimmed (fewer games /
  players / plays, every collection kept) from the maintainers'
  private captures of the Yahoo API responses.
* ``SINGLE_COLLECTION`` and ``WRAPPED_COLLECTION`` below are **constructed**, not
  captured: those captures hold no body for a single-collection
  shangrila query, so those two are hand-built to match the shapes the spec
  declares (``PlayerSeasonStatsResponse`` / ``LegacyLeadersResponse``).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent.parent / "fixtures" / "yahoo"

# Constructed (see module docstring): data.players[] -- the spec's most common
# single-collection shape.
SINGLE_COLLECTION = {
    "data": {
        "players": [
            {"playerId": "nfl.p.12345", "displayName": "A Player", "team": {"teamId": "nfl.t.1"}},
            {"playerId": "nfl.p.67890", "displayName": "B Player", "team": {"teamId": "nfl.t.2"}},
        ],
    },
    "extensions": {},
}

# Constructed: the single-key wrapper shape (data.leagues[0].leaders[]).
WRAPPED_COLLECTION = {
    "data": {"leagues": [{"leaders": [{"playerId": "ncaaf.p.1"}, {"playerId": "ncaaf.p.2"}]}]},
    "extensions": {},
}

GRAPHQL_ERROR = {"errors": [{"message": "Variable 'sortStatId' has coerced Null value", "locations": []}]}


def _load(stem: str):
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


class _Resp:
    def __init__(self, body=None):
        self._body = body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


@pytest.fixture()
def rt():
    import sportsdataverse.providers.yahoo.yahoo_shangrila_runtime as runtime

    return runtime


@pytest.fixture()
def mod():
    import sportsdataverse.providers.yahoo.yahoo_shangrila as module

    return module


# ===========================================================================
# runtime (no auth -- Origin/Referer + locale defaults only)
# ===========================================================================


def test_get_sends_origin_referer_and_locale_defaults(rt, monkeypatch):
    seen = {}

    def fake_download(url=None, params=None, headers=None, **kwargs):
        seen.update(url=url, params=params, headers=headers)
        return _Resp({"data": {"leagues": []}})

    monkeypatch.setattr(rt, "download", fake_download)
    out = rt._get(
        "https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings",
        params={"league": "nfl", "season": None},
    )
    assert seen["headers"]["Origin"] == "https://sports.yahoo.com"
    assert seen["headers"]["Referer"] == "https://sports.yahoo.com/"
    # None-valued params dropped; locale defaults added
    assert seen["params"] == {"lang": "en-US", "region": "US", "tz": "America/Chicago", "league": "nfl"}
    assert out == {"data": {"leagues": []}}


def test_get_caller_locale_overrides_default(rt, monkeypatch):
    seen = {}
    monkeypatch.setattr(rt, "download", lambda url=None, params=None, **kw: (seen.update(params=params), _Resp({}))[1])
    rt._get("https://x/y", params={"lang": "fr-FR"})
    assert seen["params"]["lang"] == "fr-FR"


def test_get_raises_on_404_and_non_json(rt, monkeypatch):
    # A 404 is NoDataError and a body that is not JSON is a failed fetch (both were ``{}``);
    # a JSON array where Yahoo's object belongs is a failed fetch too (A-L18; it was ``{}``).
    from sportsdataverse.errors import AssetFetchError, NoDataError

    def raiser(**kwargs):
        raise NoDataError("404")

    monkeypatch.setattr(rt, "download", raiser)
    with pytest.raises(NoDataError):
        rt._get("https://x/y")

    bad = _Resp(None)
    bad.text = "<html>Access Denied</html>"
    monkeypatch.setattr(rt, "download", lambda **kw: bad)
    with pytest.raises(AssetFetchError, match="non-JSON body"):
        rt._get("https://x/y")

    monkeypatch.setattr(rt, "download", lambda **kw: _Resp([1, 2]))
    with pytest.raises(AssetFetchError, match="JSON array where an object was expected"):
        rt._get("https://x/y")


# ===========================================================================
# shangrila envelope: {"data": {...}, "extensions": {...}}
# ===========================================================================


def test_shangrila_single_collection():
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    df = parse_yahoo_shangrila(SINGLE_COLLECTION)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert df.schema["player_id"] == pl.Utf8  # composite Yahoo id stays Utf8
    assert df["player_id"].to_list() == ["nfl.p.12345", "nfl.p.67890"]
    assert df.schema["team_team_id"] == pl.Utf8  # nested object flattened by json_normalize


def test_shangrila_descends_single_key_wrapper():
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    df = parse_yahoo_shangrila(WRAPPED_COLLECTION)
    assert df.height == 2
    assert df["player_id"].to_list() == ["ncaaf.p.1", "ncaaf.p.2"]


def test_shangrila_tables_on_real_legacy_capture():
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila_tables

    tables = parse_yahoo_shangrila_tables(_load("shangrila_season_stats_football_passing_ncaaf"))
    # keyed by the TOP-LEVEL data keys, not the descended ones
    assert sorted(tables) == ["leagues", "stat_types"]
    leaders = tables["leagues"]
    assert leaders.height == 3
    assert leaders.schema["player_player_id"] == pl.Utf8
    assert leaders["player_player_id"][0].startswith("ncaaf.p.")
    # the list-valued `stats` cell is JSON-encoded, not dropped
    assert leaders.schema["stats"] == pl.Utf8
    assert json.loads(leaders["stats"][0])[0]["statId"]
    assert tables["stat_types"].height > 0


def test_shangrila_tables_pandas_roundtrip():
    import pandas as pd

    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila_tables

    tables = parse_yahoo_shangrila_tables(_load("shangrila_season_stats_football_passing_ncaaf"), return_as_pandas=True)
    assert all(isinstance(v, pd.DataFrame) for v in tables.values())


@pytest.mark.parametrize("payload", [None, {}, {"data": None}, {"data": {}}, GRAPHQL_ERROR, [], "nope"])
def test_shangrila_empty_and_malformed(payload):
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import (
        parse_yahoo_shangrila,
        parse_yahoo_shangrila_tables,
    )

    df = parse_yahoo_shangrila(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert parse_yahoo_shangrila_tables(payload) == {}


def test_shangrila_empty_collection_is_zero_rows():
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    assert parse_yahoo_shangrila({"data": {"players": []}, "extensions": {}}).height == 0


# ===========================================================================
# generated wrappers
# ===========================================================================


def test_generated_module_wraps_every_spec_path(mod):
    assert len(mod.__all__) == 105
    assert "yahoo_league_standings" in mod.__all__
    assert "yahoo_season_stats_football_passing_ncaaf" in mod.__all__
    # the editorial feed 404s since 2026-11-20; its two wrappers were retired
    assert not [n for n in mod.__all__ if n.startswith("yahoo_editorial_")]


def test_wrapper_builds_shangrila_url_and_maps_query_keys(mod, monkeypatch):
    seen = {}
    monkeypatch.setattr(
        mod,
        "_get",
        lambda url, params=None, **kw: (seen.update(url=url, params=params), SINGLE_COLLECTION)[1],
    )
    df = mod.yahoo_league_standings(league="nfl", season="2024", season_phase="REG")
    assert seen["url"] == "https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStandings"
    # python arg name -> Yahoo wire key
    assert seen["params"] == {"league": "nfl", "season": "2024", "seasonPhase": "REG"}
    assert isinstance(df, pl.DataFrame)


def test_wrapper_return_parsed_false_passes_raw_through(mod, monkeypatch):
    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: SINGLE_COLLECTION)
    assert mod.yahoo_league_standings(return_parsed=False) is SINGLE_COLLECTION


def test_multi_table_wrapper_returns_dict_of_frames(mod, monkeypatch):
    raw = _load("shangrila_season_stats_football_passing_ncaaf")
    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: raw)
    out = mod.yahoo_season_stats_football_passing_ncaaf(season=2024)
    assert isinstance(out, dict)
    assert sorted(out) == ["leagues", "stat_types"]


def test_numeric_ids_are_pinned_to_utf8_without_a_float_detour():
    """Yahoo ships some ids (conference, subdivision) as bare JSON numbers next to
    dotted-string ids. With a null in the column pandas types them Float64, and a
    naive cast to text would give "1.0": the id pin routes them through Int64."""
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    raw = {
        "data": {
            "teams": [
                {"teamId": "ncaaf.t.1", "conferenceId": 1, "rating": 1.5},
                {"teamId": "ncaaf.t.2", "conferenceId": None, "rating": 2.0},
            ]
        }
    }
    df = parse_yahoo_shangrila(raw)
    assert df.schema["conference_id"] == pl.Utf8
    assert df["conference_id"].to_list() == ["1", None]
    assert df.schema["rating"] == pl.Float64  # not id-shaped, left alone


def test_boolean_flags_stay_boolean_when_a_row_lacks_the_key():
    """B-M17: a flag missing from one row is null there, Boolean, never "nan"."""
    from sportsdataverse.providers.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    raw = {"data": {"plays": [{"playId": "p1", "official": True}, {"playId": "p2"}]}}
    df = parse_yahoo_shangrila(raw)
    assert df.schema["official"] == pl.Boolean
    assert df["official"].null_count() == 1 and df["official"].drop_nulls().all()
