"""Smoke tests for the unified NFL nextgen-stats and PFR-advstats loaders.

These two loaders consolidate the 3 per-stat-type NGS loaders and 8
per-stat-type PFR advstats loaders into 2 unified loaders that mirror
nflreadpy's API shape:

- ``load_nfl_nextgen_stats(seasons, stat_type=...)``
- ``load_nfl_pfr_advstats(seasons, stat_type=..., summary_level=...)``

The 11 per-type functions remain callable but emit a ``DeprecationWarning``
pointing at the unified replacement. These tests verify:

1. The unified loaders return non-empty ``pl.DataFrame`` for every valid
   ``stat_type`` / ``summary_level`` combination.
2. Invalid ``stat_type`` / ``summary_level`` raise ``ValueError``.
3. Each deprecated alias emits ``DeprecationWarning`` and returns data
   shape-compatible with the unified loader.

Column-name assertions are intentionally avoided — the upstream nflverse
parquet schemas drift and we don't want spurious failures.
"""

from __future__ import annotations

import warnings

import polars as pl
import pytest

from sportsdataverse.nfl import (
    load_nfl_nextgen_stats,
    load_nfl_ngs_passing,
    load_nfl_ngs_receiving,
    load_nfl_ngs_rushing,
    load_nfl_pfr_advstats,
    load_nfl_pfr_def,
    load_nfl_pfr_pass,
    load_nfl_pfr_rec,
    load_nfl_pfr_rush,
    load_nfl_pfr_weekly_def,
    load_nfl_pfr_weekly_pass,
    load_nfl_pfr_weekly_rec,
    load_nfl_pfr_weekly_rush,
)
from tests.conftest import skip_if_no_live

# ---------------------------------------------------------------------------
# load_nfl_nextgen_stats — one smoke test per stat_type
# ---------------------------------------------------------------------------


@skip_if_no_live
@pytest.mark.parametrize("stat_type", ["passing", "rushing", "receiving"])
def test_load_nfl_nextgen_stats_2024(stat_type):
    df = load_nfl_nextgen_stats(seasons=[2024], stat_type=stat_type)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


def test_load_nfl_nextgen_stats_invalid_stat_type():
    with pytest.raises(ValueError, match="stat_type must be one of"):
        load_nfl_nextgen_stats(seasons=[2024], stat_type="bogus")


# ---------------------------------------------------------------------------
# load_nfl_pfr_advstats — one smoke test per (stat_type, summary_level)
# ---------------------------------------------------------------------------


@skip_if_no_live
@pytest.mark.parametrize("stat_type", ["pass", "rush", "rec", "def"])
@pytest.mark.parametrize("summary_level", ["week", "season"])
def test_load_nfl_pfr_advstats_2024(stat_type, summary_level):
    df = load_nfl_pfr_advstats(seasons=[2024], stat_type=stat_type, summary_level=summary_level)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0


def test_load_nfl_pfr_advstats_invalid_stat_type():
    with pytest.raises(ValueError, match="stat_type must be one of"):
        load_nfl_pfr_advstats(seasons=[2024], stat_type="bogus", summary_level="week")


def test_load_nfl_pfr_advstats_invalid_summary_level():
    with pytest.raises(ValueError, match="summary_level must be one of"):
        load_nfl_pfr_advstats(seasons=[2024], stat_type="pass", summary_level="annual")


# ---------------------------------------------------------------------------
# Deprecated NGS aliases — emit DeprecationWarning + still return data
# ---------------------------------------------------------------------------


@skip_if_no_live
@pytest.mark.parametrize(
    ("alias", "stat_type"),
    [
        (load_nfl_ngs_passing, "passing"),
        (load_nfl_ngs_rushing, "rushing"),
        (load_nfl_ngs_receiving, "receiving"),
    ],
)
def test_deprecated_ngs_alias_warns_and_returns_data(alias, stat_type):
    with pytest.warns(DeprecationWarning, match=alias.__name__):
        df = alias(seasons=[2024])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0

    # Column parity check vs the unified loader for the same stat_type.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        unified = load_nfl_nextgen_stats(seasons=[2024], stat_type=stat_type)
    assert set(df.columns) == set(unified.columns)


# ---------------------------------------------------------------------------
# Deprecated PFR aliases — emit DeprecationWarning + still return data
# ---------------------------------------------------------------------------


@skip_if_no_live
@pytest.mark.parametrize(
    ("alias", "stat_type", "summary_level", "needs_seasons"),
    [
        (load_nfl_pfr_pass, "pass", "season", False),
        (load_nfl_pfr_rush, "rush", "season", False),
        (load_nfl_pfr_rec, "rec", "season", False),
        (load_nfl_pfr_def, "def", "season", False),
        (load_nfl_pfr_weekly_pass, "pass", "week", True),
        (load_nfl_pfr_weekly_rush, "rush", "week", True),
        (load_nfl_pfr_weekly_rec, "rec", "week", True),
        (load_nfl_pfr_weekly_def, "def", "week", True),
    ],
)
def test_deprecated_pfr_alias_warns_and_returns_data(alias, stat_type, summary_level, needs_seasons):
    with pytest.warns(DeprecationWarning, match=alias.__name__):
        df = alias(seasons=[2024]) if needs_seasons else alias()
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert df.width > 0

    # Column parity check vs the unified loader for the same combo.
    seasons_arg = [2024] if needs_seasons else None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        if seasons_arg is not None:
            unified = load_nfl_pfr_advstats(
                seasons=seasons_arg,
                stat_type=stat_type,
                summary_level=summary_level,
            )
        else:
            # season-level alias passes no seasons → unified must read the
            # full combined parquet to be column-equivalent. We pass an
            # explicit list with the full coverage range to force the same
            # underlying URL but accept the filter no-ops on the column set.
            unified = load_nfl_pfr_advstats(
                seasons=[2024],
                stat_type=stat_type,
                summary_level=summary_level,
            )
    assert set(df.columns) == set(unified.columns)
