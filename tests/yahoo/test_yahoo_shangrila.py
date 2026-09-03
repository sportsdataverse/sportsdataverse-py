"""Offline tests for the Yahoo Sports stem: the ``yahoo_shangrila_runtime._get``
getter, the three envelope parsers, and the generated wrapper wiring. No network.

Fixture provenance:

* ``tests/fixtures/yahoo/*.json`` are **real captures**, trimmed (fewer games /
  players / plays, every collection kept) from the committed bodies in
  ``sdv-internal-refs/yahoo/discovery/responses/``.
* ``SINGLE_COLLECTION`` and ``WRAPPED_COLLECTION`` below are **constructed**, not
  captured: the internal-refs repo holds no body for a single-collection
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
    import sportsdataverse.yahoo.yahoo_shangrila_runtime as runtime

    return runtime


@pytest.fixture()
def mod():
    import sportsdataverse.yahoo.yahoo_shangrila as module

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


def test_get_returns_empty_on_404_and_non_json(rt, monkeypatch):
    from sportsdataverse.errors import NoDataError

    def raiser(**kwargs):
        raise NoDataError("404")

    monkeypatch.setattr(rt, "download", raiser)
    assert rt._get("https://x/y") == {}

    monkeypatch.setattr(rt, "download", lambda **kw: _Resp(None))
    assert rt._get("https://x/y") == {}

    monkeypatch.setattr(rt, "download", lambda **kw: _Resp([1, 2]))
    assert rt._get("https://x/y") == {}


# ===========================================================================
# shangrila envelope: {"data": {...}, "extensions": {...}}
# ===========================================================================


def test_shangrila_single_collection():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    df = parse_yahoo_shangrila(SINGLE_COLLECTION)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert df.schema["player_id"] == pl.Utf8  # composite Yahoo id stays Utf8
    assert df["player_id"].to_list() == ["nfl.p.12345", "nfl.p.67890"]
    assert df.schema["team_team_id"] == pl.Utf8  # nested object flattened by json_normalize


def test_shangrila_descends_single_key_wrapper():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    df = parse_yahoo_shangrila(WRAPPED_COLLECTION)
    assert df.height == 2
    assert df["player_id"].to_list() == ["ncaaf.p.1", "ncaaf.p.2"]


def test_shangrila_tables_on_real_legacy_capture():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila_tables

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

    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila_tables

    tables = parse_yahoo_shangrila_tables(_load("shangrila_season_stats_football_passing_ncaaf"), return_as_pandas=True)
    assert all(isinstance(v, pd.DataFrame) for v in tables.values())


@pytest.mark.parametrize("payload", [None, {}, {"data": None}, {"data": {}}, GRAPHQL_ERROR, [], "nope"])
def test_shangrila_empty_and_malformed(payload):
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila, parse_yahoo_shangrila_tables

    df = parse_yahoo_shangrila(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0
    assert parse_yahoo_shangrila_tables(payload) == {}


def test_shangrila_empty_collection_is_zero_rows():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_shangrila

    assert parse_yahoo_shangrila({"data": {"players": []}, "extensions": {}}).height == 0


# ===========================================================================
# editorial envelope: {"service": {"<root>": {"<collection>": {"<id>": ...}}}}
# ===========================================================================


def test_editorial_scoreboard_real_capture():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_editorial

    tables = parse_yahoo_editorial(_load("editorial_scoreboard_ncaaf"))
    assert {"games", "teams", "teamrecord", "leagues"} <= set(tables)
    games = tables["games"]
    assert games.height == 2
    assert games.schema["entity_id"] == pl.Utf8
    assert games["entity_id"][0].startswith("ncaaf.g.")
    assert games.schema["gameid"] == pl.Utf8  # numeric-looking Yahoo ids never become floats
    # a scalar-valued collection surfaces as entity_id + value
    assert tables["teamrecord"].columns == ["entity_id", "value"]


def test_editorial_boxscore_nested_id_maps_become_rows():
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_editorial

    tables = parse_yahoo_editorial(_load("editorial_boxscore_ncaaf"))
    # player_stats[playerId][statVariation] -> one row per (player, variation)
    ps = tables["player_stats"]
    assert {"entity_id", "sub_id"} <= set(ps.columns)
    assert ps.height == 2
    assert ps["entity_id"][0].startswith("ncaaf.p.")
    assert ps["sub_id"][0].startswith("ncaaf.stat_variation.")
    # dotted stat-type keys are sanitized into column names
    assert any(c.startswith("ncaaf_stat_type_") for c in ps.columns)
    # a list-valued collection explodes to one row per element
    assert tables["gamedrives"].height == 3
    # play-by-play is a second id level, not a thousand-column single row
    assert tables["gameplay_by_play"].height == 3
    assert tables["gameplay_by_play"].width < 40


@pytest.mark.parametrize("payload", [None, {}, {"service": {}}, {"service": None}, "nope", []])
def test_editorial_empty_and_malformed(payload):
    from sportsdataverse.yahoo.yahoo_shangrila_parsers import parse_yahoo_editorial

    assert parse_yahoo_editorial(payload) == {}


# ===========================================================================
# generated wrappers
# ===========================================================================


def test_generated_module_wraps_every_spec_path(mod):
    assert len(mod.__all__) == 107
    assert "yahoo_league_standings" in mod.__all__
    assert "yahoo_season_stats_football_passing_ncaaf" in mod.__all__
    assert "yahoo_editorial_scoreboard" in mod.__all__


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


def test_wrapper_builds_editorial_url_with_path_param(mod, monkeypatch):
    seen = {}
    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: (seen.update(url=url), {})[1])
    mod.yahoo_editorial_boxscore(game_id="ncaaf.g.202509200023")
    assert seen["url"] == "https://api-secure.sports.yahoo.com/v1/editorial/s/boxscore/ncaaf.g.202509200023"


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
    """Yahoo ships conference ids as bare JSON numbers in one collection and dotted
    strings in another. Before the id pin they landed Float64 in ``divisions`` and
    Utf8 in ``teams`` -- the same id, from one payload, in two dtypes that cannot
    join -- and a naive cast to text would have produced "1.0"."""
    import json
    import pathlib as _p

    import polars as pl

    from sportsdataverse.yahoo import parse_yahoo_editorial

    fixture = _p.Path(__file__).parent.parent / "fixtures" / "yahoo" / "editorial_scoreboard_ncaaf.json"
    frames = parse_yahoo_editorial(json.loads(fixture.read_text(encoding="utf-8")))
    divisions, teams = frames["divisions"], frames["teams"]

    conf_cols = [c for c in divisions.columns if c.startswith("conferences_") and c.endswith("_id")]
    assert conf_cols, "fixture should carry numeric conference ids"
    for col in conf_cols:
        assert divisions.schema[col] == pl.Utf8, f"{col} is {divisions.schema[col]}, not Utf8"
        assert not any((v or "").endswith(".0") for v in divisions[col].drop_nulls().to_list())

    assert teams.schema["conference_id"] == pl.Utf8
    # the same id space, so the two frames must actually be joinable
    assert set(divisions[conf_cols[0]].drop_nulls().to_list()) & set(teams["conference_id"].drop_nulls().to_list())
