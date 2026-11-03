"""Offline contract tests for the ``{league}_team_aliases`` / ``{league}_player_aliases`` loaders.

sdv-reference-data publishes both as season-less files in the ``{league}_groups``
release, beside the four group tables. These pin the manifest entries, the asset
each loader asks for, the declared id dtypes (Utf8, as published) and the
description coverage; for the hand-written NFL loaders, whose ``releases.yaml``
entries are docs-only, that the documented asset is the one the code reads.
Nothing touches the network.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.nfl import get_config, update_config

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_CODEGEN = Path(__file__).resolve().parents[2] / "tools" / "codegen"
ENTRIES = {
    e["fn"]: e
    for e in yaml.safe_load((_CODEGEN / "endpoints" / "releases.yaml").read_text(encoding="utf-8"))["loaders"]
}
SCHEMAS = yaml.safe_load((_CODEGEN / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))
DESC = yaml.safe_load((_CODEGEN / "manual_column_descriptions.yaml").read_text(encoding="utf-8"))

TEAM = ["nfl", "cfb", "nba", "wnba", "mbb", "wbb", "mlb", "nhl"]
PLAYER = ["nfl", "cfb", "nba", "wnba", "mbb", "wbb"]
CASES = [(lg, "team_aliases") for lg in TEAM] + [(lg, "player_aliases") for lg in PLAYER]
IDS = {
    "team_aliases": ["team_id", "team_id_source", "source", "source_id"],
    "player_aliases": ["athlete_id", "athlete_id_source", "source", "source_player_id", "team_id", "source_team_id"],
}


@pytest.fixture(autouse=True)
def _no_nfl_cache():
    # the NFL loaders are @cached_loader; restore the process-global config after
    prior = dataclasses.asdict(get_config())
    update_config(cache_mode="off")
    yield
    update_config(**prior)


@pytest.mark.parametrize(("league", "table"), CASES)
def test_manifest_entry_is_season_less(league, table):
    e = ENTRIES[f"load_{league}_{table}"]
    assert e["league"] == league and e["tag"] == f"{league}_groups" and e["notes"]
    assert e["url"] == f"{league}_groups/{league}_{table}.parquet" and "min_season" not in e


@pytest.mark.parametrize(("league", "table"), CASES)
def test_declared_schema_keeps_utf8_ids_and_is_described(league, table):
    fn = f"load_{league}_{table}"
    cols = {c["name"]: c["type"] for c in SCHEMAS[fn]}
    assert all(cols[k] == "String" for k in IDS[table])
    assert cols["valid_from"] == cols["valid_to"] == "Int32"
    assert set(DESC[fn]) == set(cols)


@pytest.mark.parametrize(("league", "table"), CASES)
def test_loader_requests_the_documented_asset(monkeypatch, league, table):
    if league == "nfl":
        mod, reader = importlib.import_module("sportsdataverse.nfl.nfl_loaders"), "_fetch_release_parquet"
    else:
        mod, reader = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders"), "_read_release_parquet"
    seen: list[str] = []

    def fake(url):
        seen.append(url)
        return pl.DataFrame({"league": [league]})

    monkeypatch.setattr(mod, reader, fake)
    fn = getattr(mod, f"load_{league}_{table}")
    assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
    assert fn().height == 1
    assert seen == [SDV + ENTRIES[f"load_{league}_{table}"]["url"]]


def test_no_loader_for_the_diagnostic_side_files():
    assert not [fn for fn in ENTRIES if fn.endswith(("_aliases_unmatched", "_aliases_rates"))]


@pytest.mark.parametrize(("league", "table"), [c for c in CASES if c[0] != "nfl"])
def test_absent_asset_is_an_empty_frame_with_the_declared_schema(monkeypatch, league, table):
    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    monkeypatch.setattr(mod, "_read_release_parquet", lambda url: None)
    fn = f"load_{league}_{table}"
    with pytest.warns(UserWarning, match="no published asset"):
        df = getattr(mod, fn)()
    assert df.height == 0
    assert {k: str(v) for k, v in df.schema.items()} == {c["name"]: c["type"] for c in SCHEMAS[fn]}
