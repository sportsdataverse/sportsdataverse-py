"""Offline contract tests for the ``{league}_groups`` reference-table loaders.

sdv-reference-data publishes the same four tables per league under one tag:
three season-less files plus one ``team_group_seasons`` asset per season. These
pin the manifest entries, the asset each loader asks for, and -- for the
hand-written NFL loaders, whose ``releases.yaml`` entries are docs-only -- that
the documented asset is the one the code reads. Nothing touches the network.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.errors import NoDataError, SeasonNotFoundError
from sportsdataverse.nfl import get_config, update_config

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_RELEASES = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "endpoints" / "releases.yaml"

# tag/file key, sdv-py league module, fn prefix, first season
LEAGUES = [
    ("cfb", "cfb", "load_cfb_", 1869),
    ("mbb", "mbb", "load_mbb_", 2002),
    ("wbb", "wbb", "load_wbb_", 2002),
    ("nfl", "nfl", "load_nfl_", 1970),
    ("nba", "nba", "load_nba_", 1971),
    ("wnba", "wnba", "load_wnba_", 1997),
    ("mlb", "mlb", "load_mlb_", 1901),
    ("nhl", "nhl", "load_nhl_", 1918),
    ("ncaa_baseball", "mlb", "load_ncaa_baseball_", 2010),
]
SEASONLESS = ["groups", "group_seasons", "group_aliases"]
ENTRIES = {e["fn"]: e for e in yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]}


@pytest.fixture(autouse=True)
def _no_nfl_cache():
    # the NFL loaders are @cached_loader; restore the process-global config after
    prior = dataclasses.asdict(get_config())
    update_config(cache_mode="off")
    yield
    update_config(**prior)


@pytest.mark.parametrize(("key", "league", "prefix", "floor"), LEAGUES)
def test_manifest_has_the_four_loaders(key, league, prefix, floor):
    for table in [*SEASONLESS, "team_group_seasons"]:
        e = ENTRIES[f"{prefix}{table}"]
        assert e["league"] == league and e["tag"] == f"{key}_groups" and e["notes"]
        if table == "team_group_seasons":
            # the league's own season key, never a {season + N} offset
            assert e["url"] == f"{key}_groups/{key}_team_group_seasons_{{season}}.parquet"
            assert e["min_season"] == floor
        else:
            assert e["url"] == f"{key}_groups/{key}_{table}.parquet" and "min_season" not in e


def _loader(league):
    if league == "nfl":
        return importlib.import_module("sportsdataverse.nfl.nfl_loaders"), "_fetch_release_parquet"
    return importlib.import_module(f"sportsdataverse.{league}.{league}_loaders"), "_read_release_parquet"


@pytest.mark.parametrize(("key", "league", "prefix", "floor"), LEAGUES)
def test_loaders_request_the_documented_assets(monkeypatch, key, league, prefix, floor):
    mod, reader = _loader(league)
    seen: list[str] = []

    def fake(url):
        seen.append(url)
        return pl.DataFrame({"league": [key]})

    monkeypatch.setattr(mod, reader, fake)
    for table in SEASONLESS:
        fn = getattr(mod, f"{prefix}{table}")
        assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
        assert fn().height == 1
    tgs = getattr(mod, f"{prefix}team_group_seasons")
    assert tgs(seasons=[floor, 2024]).height == 2
    assert seen == [SDV + ENTRIES[f"{prefix}{t}"]["url"] for t in SEASONLESS] + [
        SDV + ENTRIES[f"{prefix}team_group_seasons"]["url"].format(season=s) for s in (floor, 2024)
    ]
    with pytest.raises(SeasonNotFoundError):
        tgs(seasons=floor - 1)


def test_nfl_missing_season_raises_and_urls_match_config(monkeypatch):
    from sportsdataverse.nfl import nfl_loaders as mod

    assert SDV + ENTRIES["load_nfl_groups"]["url"] == mod.NFL_GROUPS_URL.format(table="groups")
    assert SDV + ENTRIES["load_nfl_team_group_seasons"]["url"] == mod.NFL_TEAM_GROUP_SEASONS_URL

    def missing(url):
        raise NoDataError(f"404 {url}")

    monkeypatch.setattr(mod, "_fetch_release_parquet", missing)
    with pytest.raises(NoDataError):
        mod.load_nfl_team_group_seasons(seasons=2024)


def test_nfl_team_group_seasons_takes_a_string_season(monkeypatch):
    from sportsdataverse.nfl import nfl_loaders as mod

    seen = []

    def fetch(url):
        seen.append(url)
        return pl.DataFrame({"season": [2024]})

    monkeypatch.setattr(mod, "_fetch_release_parquet", fetch)
    assert mod.load_nfl_team_group_seasons(seasons="2024").height == 1
    assert seen == [mod.NFL_TEAM_GROUP_SEASONS_URL.format(season=2024)]
