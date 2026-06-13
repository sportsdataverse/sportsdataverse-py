"""Tests for the CFB crosswalk dataset loaders.

Two shapes, reflecting the underlying data:

* ``load_cfb_teams_crosswalk`` / ``load_cfb_schedule_crosswalk`` are **per-season**
  (generated from ``tools/codegen/endpoints/releases.yaml`` into
  ``sportsdataverse.cfb.cfb_loaders``) — teams + schedules are genuinely historical.
* ``load_cfb_rosters_crosswalk`` is **season-less** (hand-written in
  ``sportsdataverse.cfb.cfb_loaders_extra``) — ESPN/Fox roster endpoints are
  current-only, so the published artifact is a single snapshot, not a per-season
  series.

The codegen machinery is covered by ``tests/codegen/test_load_module.py``; this
file locks the crosswalk-specific contract (release tag + asset filenames, the
404-safe concat, the ``min_season`` guard, and the season-less rosters shape) so
an accidental edit can't silently repoint or reshape them. All offline.
"""

from __future__ import annotations

from typing import List

import pandas as pd
import polars as pl
import pytest

import sportsdataverse.cfb.cfb_loaders as cl
import sportsdataverse.cfb.cfb_loaders_extra as cle
from sportsdataverse.cfb import (
    load_cfb_rosters_crosswalk,
    load_cfb_schedule_crosswalk,
    load_cfb_teams_crosswalk,
)
from sportsdataverse.errors import SeasonNotFoundError

# Per-season (season-loop) loaders only.
_SEASON_LOADERS = (
    (load_cfb_teams_crosswalk, "cfb_teams_crosswalk"),
    (load_cfb_schedule_crosswalk, "cfb_schedule_crosswalk"),
)


def test_crosswalk_loaders_are_exported() -> None:
    import sportsdataverse.cfb as cfb

    for name in ("load_cfb_teams_crosswalk", "load_cfb_schedule_crosswalk", "load_cfb_rosters_crosswalk"):
        assert hasattr(cfb, name), f"{name} not exported from sportsdataverse.cfb"


# ---------------------------------------------------------------------------
# Per-season loaders (teams + schedule)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("loader,stem", _SEASON_LOADERS)
def test_season_loader_url_locks_tag_and_filename(loader, stem, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[str] = []

    def fake(url: str):
        seen.append(url)
        return None  # 404-safe path -> empty frame, no network

    monkeypatch.setattr(cl, "_read_release_parquet", fake)
    loader(seasons=2024)
    assert seen == [
        f"https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/{stem}_2024.parquet"
    ]


@pytest.mark.parametrize("loader,stem", _SEASON_LOADERS)
def test_season_loader_is_404_safe_and_concats(loader, stem, monkeypatch: pytest.MonkeyPatch) -> None:
    # 2024 publishes, 2025 is missing -> one frame returned, no raise.
    def fake(url: str):
        return pl.DataFrame({"espn_team_id": [194]}) if "2024" in url else None

    monkeypatch.setattr(cl, "_read_release_parquet", fake)
    out = loader(seasons=[2024, 2025])
    assert isinstance(out, pl.DataFrame)
    assert out.height == 1


@pytest.mark.parametrize("loader,stem", _SEASON_LOADERS)
def test_season_loader_min_season_guard(loader, stem, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cl, "_read_release_parquet", lambda url: None)
    with pytest.raises(SeasonNotFoundError):
        loader(seasons=2013)  # below the 2014 multi-source floor


@pytest.mark.parametrize("loader,stem", _SEASON_LOADERS)
def test_season_loader_pandas_roundtrip(loader, stem, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cl, "_read_release_parquet", lambda url: pl.DataFrame({"espn_team_id": [194]}))
    out = loader(seasons=2024, return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


# ---------------------------------------------------------------------------
# Season-less rosters loader (current snapshot)
# ---------------------------------------------------------------------------
def test_rosters_loader_is_season_less() -> None:
    import inspect

    params = inspect.signature(load_cfb_rosters_crosswalk).parameters
    assert "seasons" not in params, "rosters crosswalk is current-only; must not take a seasons arg"
    assert "return_as_pandas" in params


def test_rosters_loader_url_locks_single_current_asset(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: List[str] = []

    def fake(url: str, **kwargs):
        seen.append(url)
        return pl.DataFrame({"espn_team_id": [194]})

    monkeypatch.setattr(cle.pl, "read_parquet", fake)
    load_cfb_rosters_crosswalk()
    assert seen == [
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_crosswalk/cfb_rosters_crosswalk.parquet"
    ]


def test_rosters_loader_pandas_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cle.pl, "read_parquet", lambda url, **k: pl.DataFrame({"espn_team_id": [194]}))
    assert isinstance(load_cfb_rosters_crosswalk(), pl.DataFrame)
    assert isinstance(load_cfb_rosters_crosswalk(return_as_pandas=True), pd.DataFrame)
