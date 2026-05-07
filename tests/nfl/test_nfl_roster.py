"""Smoke test for sportsdataverse.nfl.load_nfl_schedule.

The previous version of this file ran ``load_nfl_schedule(seasons=range(2016, 2017))``
at MODULE-IMPORT time, which:

1. Always hit the network on test collection — even when the live-test gate
   was off — so a clean ``pytest --collect-only`` would 404 before any test
   ran.
2. Pinned to season 2016, which has since 404'd on the underlying
   nflverse-data asset.

Refactored so the call lives inside a gated test using ``@skip_if_no_live``,
and the season is a recent one. Mirrors the pattern in ``test_nfl_pbp.py``
and ``test_cfb_pbp.py``.
"""

from __future__ import annotations

import polars as pl

import sportsdataverse as sdv
from tests.conftest import skip_if_no_live


@skip_if_no_live
def test_load_nfl_schedule_returns_polars_for_recent_season():
    df = sdv.nfl.load_nfl_schedule(seasons=[2024])
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    # Schedule frames carry game_id + week + team identifiers across both
    # historical formats; assert a small core set rather than the full
    # column list (upstream adds columns over time).
    expected = {"game_id", "season", "week"}
    missing = expected - set(df.columns)
    assert not missing, f"missing core columns: {missing}"
