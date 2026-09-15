"""Offline contract tests for the CFB usage-leaderboard and tendencies loaders.

Fourteen generated loaders over the ``espn_cfb_usage_*`` / ``espn_cfb_*_tendencies``
/ ``espn_cfb_coach_careers`` releases (``tools/codegen/endpoints/releases.yaml``).
These pin the URL each public season resolves to, the 2004 floor, the 404-safe
skip, the diagonal multi-season concat, and the season-less career loader --
with the release read mocked, so nothing here touches the network.
"""

from __future__ import annotations

import inspect

import polars as pl
import pytest

import sportsdataverse.cfb as cfb
from sportsdataverse.cfb import cfb_loaders as _mod
from sportsdataverse.errors import SeasonNotFoundError

SDV = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"

SEASONAL = [
    "usage_players",
    "usage_position_groups",
    "usage_tackles",
    "usage_position_group_tackles",
    "usage_teams",
    "usage_drive_scripting",
    "usage_st_kickers",
    "usage_st_punters",
    "usage_st_returners",
    "usage_st_blocks",
    "usage_st_team",
    "team_tendencies",
    "coach_tendencies",
]


def _capture(monkeypatch, frame_for):
    seen: list[str] = []

    def fake(url: str):
        seen.append(url)
        return frame_for(url)

    monkeypatch.setattr(_mod, "_read_release_parquet", fake)
    return seen


@pytest.mark.parametrize("stem", SEASONAL)
def test_seasonal_loader_resolves_the_tag_and_stem(monkeypatch, stem):
    seen = _capture(monkeypatch, lambda url: pl.DataFrame({"season": [2024]}))
    fn = getattr(_mod, f"load_cfb_{stem}")
    out = fn(seasons=2024)
    assert seen == [f"{SDV}espn_cfb_{stem}/{stem}_2024.parquet"]
    assert out.height == 1


@pytest.mark.parametrize("stem", SEASONAL)
def test_seasonal_loader_is_exported_and_floored_at_2004(stem):
    fn = getattr(cfb, f"load_cfb_{stem}")
    assert fn is getattr(_mod, f"load_cfb_{stem}")
    assert list(inspect.signature(fn).parameters) == ["seasons", "return_as_pandas"]
    assert f"espn_cfb_{stem}" in fn.__doc__
    with pytest.raises(SeasonNotFoundError):
        fn(seasons=2003)


def test_missing_season_is_skipped_with_a_warning_and_concat_is_diagonal(monkeypatch):
    def frame_for(url: str):
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        if season == 2005:
            return None  # 404 -> None from _read_release_parquet
        extra = {"new_col": [1.0]} if season == 2006 else {}
        return pl.DataFrame({"season": [season], "pos_team_id": [2390], **extra})

    seen = _capture(monkeypatch, frame_for)
    with pytest.warns(UserWarning, match="load_cfb_team_tendencies: no data for season"):
        out = _mod.load_cfb_team_tendencies(seasons=[2004, 2005, 2006])
    assert len(seen) == 3
    assert out.height == 2 and "new_col" in out.columns
    assert out["season"].to_list() == [2004, 2006]


def test_coach_careers_takes_no_seasons_and_reads_the_single_asset(monkeypatch):
    fn = cfb.load_cfb_coach_careers
    assert list(inspect.signature(fn).parameters) == ["return_as_pandas"]
    assert "espn_cfb_coach_careers" in fn.__doc__ and "seasons" not in fn.__doc__.split("Args:")[1].split("Returns:")[0]

    seen = _capture(monkeypatch, lambda url: pl.DataFrame({"coach": ["Kirby Smart"], "games": [120]}))
    out = fn()
    assert seen == [f"{SDV}espn_cfb_coach_careers/coach_careers.parquet"]
    assert out["coach"].to_list() == ["Kirby Smart"]


def test_coach_careers_absent_asset_is_an_empty_frame_with_a_warning(monkeypatch):
    _capture(monkeypatch, lambda url: None)
    with pytest.warns(UserWarning, match="load_cfb_coach_careers: no published asset"):
        out = _mod.load_cfb_coach_careers()
    assert isinstance(out, pl.DataFrame) and out.height == 0


def test_pandas_round_trip(monkeypatch):
    _capture(monkeypatch, lambda url: pl.DataFrame({"season": [2024], "pos_team": ["Georgia Bulldogs"]}))
    out = _mod.load_cfb_usage_players(seasons=[2024], return_as_pandas=True)
    assert out.__class__.__module__.startswith("pandas") and len(out) == 1
    careers = _mod.load_cfb_coach_careers(return_as_pandas=True)
    assert careers.__class__.__module__.startswith("pandas")
