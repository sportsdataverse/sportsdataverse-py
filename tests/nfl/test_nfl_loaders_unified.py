"""Smoke tests for the unified NFL nextgen-stats and PFR-advstats loaders.

These two loaders consolidate the 3 per-stat-type NGS loaders and 8
per-stat-type PFR advstats loaders into 2 unified loaders that mirror
nflreadpy's API shape:

- ``load_nfl_nextgen_stats(seasons, stat_type=...)``
- ``load_nfl_pfr_advstats(seasons, stat_type=..., summary_level=...)``

The 11 per-type functions were removed in 0.1.5 (``test_removed_loaders.py``
proves they are gone). These tests verify:

1. The unified loaders return non-empty ``pl.DataFrame`` for every valid
   ``stat_type`` / ``summary_level`` combination.
2. Invalid ``stat_type`` / ``summary_level`` raise ``ValueError``.

Column-name assertions are intentionally avoided — the upstream nflverse
parquet schemas drift and we don't want spurious failures.
"""

from __future__ import annotations


import polars as pl
import pytest

from sportsdataverse.nfl import (
    load_nfl_nextgen_stats,
    load_nfl_pfr_advstats,
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
