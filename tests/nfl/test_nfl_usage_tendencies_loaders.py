"""Offline contract tests for the NFL usage-leaderboard and tendencies loaders.

Fourteen hand-written loaders over the ``espn_nfl_usage_*`` /
``espn_nfl_*_tendencies`` / ``espn_nfl_coach_careers`` releases. These pin the
URL each public season resolves to, the 2002 floor, that a missing season
RAISES (the hand-written-loader contract, unlike the 404-safe generated CFB
twins), the diagonal multi-season concat, the season-less career loader, and
that the docs-metadata entries in ``releases.yaml`` describe the same assets the
code reads. The release read is mocked; nothing here touches the network.
"""

from __future__ import annotations

import dataclasses
import inspect
from pathlib import Path

import polars as pl
import pytest
import yaml

import sportsdataverse.nfl as nfl
from sportsdataverse.errors import NoDataError, SeasonNotFoundError
from sportsdataverse.nfl import nfl_loaders as _mod
from sportsdataverse.nfl import clear_cache, get_config, update_config

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_RELEASES = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "endpoints" / "releases.yaml"
SEASONAL = list(_mod._NFL_ESPN_FOOTBALL_STEMS)


@pytest.fixture(autouse=True)
def _no_cache():
    # Snapshot the WHOLE process-global config (mode, dir, TTL, ...), not just
    # the mode: a test below points cache_dir at a tmp_path.
    prior = dataclasses.asdict(get_config())
    update_config(cache_mode="off")
    yield
    update_config(**prior)


def _capture(monkeypatch, frame_for):
    seen: list[str] = []

    def fake(url: str) -> pl.DataFrame:
        seen.append(url)
        return frame_for(url)

    monkeypatch.setattr(_mod, "_fetch_release_parquet", fake)
    return seen


def test_registry_covers_the_thirteen_seasonal_tags():
    assert len(SEASONAL) == 13
    assert SEASONAL[0] == "usage_players" and SEASONAL[-1] == "coach_tendencies"


@pytest.mark.parametrize("stem", SEASONAL)
def test_seasonal_loader_resolves_the_tag_and_stem(monkeypatch, stem):
    seen = _capture(monkeypatch, lambda url: pl.DataFrame({"season": [2024]}))
    fn = getattr(nfl, f"load_nfl_{stem}")
    out = fn(seasons=2024)  # int season accepted
    assert seen == [f"{SDV}espn_nfl_{stem}/{stem}_2024.parquet"]
    assert out.height == 1


@pytest.mark.parametrize("stem", SEASONAL)
def test_seasonal_loader_has_a_docstring_and_the_2002_floor(stem):
    fn = getattr(nfl, f"load_nfl_{stem}")
    assert fn.__doc__ and f"espn_nfl_{stem}" in fn.__doc__ and "Example:" in fn.__doc__
    assert list(inspect.signature(fn).parameters) == ["seasons", "return_as_pandas"]
    with pytest.raises(SeasonNotFoundError):
        fn(seasons=[2001])


def test_missing_season_raises_no_data_error(monkeypatch):
    def frame_for(url: str) -> pl.DataFrame:
        if "_2005." in url:
            raise NoDataError(f"404 {url}")
        return pl.DataFrame({"season": [2004]})

    _capture(monkeypatch, frame_for)
    with pytest.raises(NoDataError):
        nfl.load_nfl_usage_players(seasons=[2004, 2005])


def test_multi_season_concat_is_diagonal(monkeypatch):
    def frame_for(url: str) -> pl.DataFrame:
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        extra = {"new_col": [1.0]} if season == 2024 else {}
        return pl.DataFrame({"season": [season], "pos_team_id": [12], **extra})

    seen = _capture(monkeypatch, frame_for)
    out = nfl.load_nfl_team_tendencies(seasons=[2023, 2024])
    assert seen == [
        f"{SDV}espn_nfl_team_tendencies/team_tendencies_2023.parquet",
        f"{SDV}espn_nfl_team_tendencies/team_tendencies_2024.parquet",
    ]
    assert out.height == 2 and "new_col" in out.columns and out["new_col"].null_count() == 1


def test_coach_careers_takes_no_seasons_and_reads_the_single_asset(monkeypatch):
    fn = nfl.load_nfl_coach_careers
    assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
    assert fn.__doc__ and "espn_nfl_coach_careers" in fn.__doc__
    seen = _capture(monkeypatch, lambda url: pl.DataFrame({"coach": ["Andy Reid"], "seasons": [24]}))
    out = fn()
    assert seen == [f"{SDV}espn_nfl_coach_careers/coach_careers.parquet"]
    assert out["coach"].to_list() == ["Andy Reid"]

    def missing(url: str) -> pl.DataFrame:
        raise NoDataError("404")

    monkeypatch.setattr(_mod, "_fetch_release_parquet", missing)
    with pytest.raises(NoDataError):
        fn()


def test_pandas_round_trip(monkeypatch):
    _capture(monkeypatch, lambda url: pl.DataFrame({"season": [2024], "pos_team": ["Kansas City Chiefs"]}))
    out = nfl.load_nfl_usage_st_team(seasons=[2024], return_as_pandas=True)
    assert out.__class__.__module__.startswith("pandas") and len(out) == 1


def test_bad_stem_is_rejected():
    with pytest.raises(ValueError, match="stem must be one of"):
        _mod._load_nfl_espn_football("usage_nope", [2024])


def test_docs_metadata_matches_the_code():
    """NFL is not a generated-loader league: releases.yaml carries these entries for
    the docs only, so nothing else ties the documented asset to the one the loader
    reads. Pin that here."""
    entries = {e["fn"]: e for e in yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]}
    for stem in SEASONAL:
        e = entries[f"load_nfl_{stem}"]
        assert (
            e["league"] == "nfl" and e["tag"] == f"espn_nfl_{stem}" and e["min_season"] == _mod._NFL_ESPN_FOOTBALL_FLOOR
        )
        assert SDV + e["url"] == _mod.NFL_ESPN_FOOTBALL_URL.replace("{stem}", stem)
    careers = entries["load_nfl_coach_careers"]
    assert SDV + careers["url"] == _mod.NFL_ESPN_COACH_CAREERS_URL and "min_season" not in careers


@pytest.mark.parametrize("mode", ["memory", "filesystem"])
def test_positional_return_as_pandas_survives_a_cache_miss(monkeypatch, tmp_path, mode):
    """``load_x(2024, True)`` used to raise TypeError on a cache miss: the wrapper
    re-issued the call with ``return_as_pandas=False`` as a keyword while the
    positional True was still in ``args``. The wrapper now binds the call."""
    seen = _capture(monkeypatch, lambda url: pl.DataFrame({"season": [2024], "coach": ["Andy Reid"]}))
    prior_dir = get_config().cache_dir
    update_config(cache_mode=mode, cache_dir=tmp_path)
    clear_cache()
    try:
        out = nfl.load_nfl_usage_players([2024], True)
        assert out.__class__.__module__.startswith("pandas") and len(out) == 1
        again = nfl.load_nfl_usage_players([2024], True)  # served from the cache, still pandas
        assert again.__class__.__module__.startswith("pandas")
        # the positional and keyword forms share ONE cache entry: no third fetch
        assert isinstance(nfl.load_nfl_usage_players([2024]), pl.DataFrame)
        assert isinstance(nfl.load_nfl_usage_players([2024], return_as_pandas=False), pl.DataFrame)
        assert len(seen) == 1
        careers = nfl.load_nfl_coach_careers(True)
        assert careers.__class__.__module__.startswith("pandas") and list(careers["coach"]) == ["Andy Reid"]
        assert isinstance(nfl.load_nfl_coach_careers(), pl.DataFrame)
        assert len(seen) == 2
    finally:
        clear_cache()
        update_config(cache_dir=prior_dir)
