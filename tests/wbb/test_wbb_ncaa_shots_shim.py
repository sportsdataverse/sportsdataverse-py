"""Smoke tests for the wbb box-score + shots shims (orchestrator-written).

The heavy validation lives in tests/wbb/test_wbb_ncaa_box_stats_parity.py and
tests/wbb/test_wbb_ncaa_shots_parity.py (which exercise the shared cores with
the quarters ``period_model`` directly); these tests only pin the shim
bindings — delegation identity and the (4, 600, 300) bind in the driver.
"""

from __future__ import annotations

import polars as pl
from polars.testing import assert_frame_equal

from sportsdataverse.mbb.mbb_ncaa_box_stats import parse_ncaa_bb_box
from sportsdataverse.mbb.mbb_ncaa_shots import parse_ncaa_bb_shots
from sportsdataverse.wbb.wbb_ncaa_box_stats import ncaa_wbb_box_scores
from sportsdataverse.wbb.wbb_ncaa_shots import (
    ncaa_wbb_join_pbp_shots,
    ncaa_wbb_shot_locations,
)
from tests.mbb._bigballr_oracle import HTML_DIR, load_oracle_pbp


class _FakeFetcher:
    def fetch_game_box(self, gid: object) -> str:
        return (HTML_DIR / f"box_{gid}.html").read_text(encoding="utf-8")

    def fetch_game_individual_stats(self, gid: object) -> str:
        return (HTML_DIR / f"individual_stats_{gid}.html").read_text(encoding="utf-8")


def test_wbb_box_scores_shim_delegates() -> None:
    got = ncaa_wbb_box_scores(["5722355"], fetcher=_FakeFetcher())
    expected = parse_ncaa_bb_box(
        (HTML_DIR / "individual_stats_5722355.html").read_text(encoding="utf-8"),
        "5722355",
    )
    assert isinstance(got, pl.DataFrame)
    assert_frame_equal(got, expected)


def test_wbb_shot_locations_binds_quarter_model() -> None:
    got = ncaa_wbb_shot_locations(["5728709"], fetcher=_FakeFetcher())
    expected = parse_ncaa_bb_shots(
        (HTML_DIR / "box_5728709.html").read_text(encoding="utf-8"),
        "5728709",
        period_model=(4, 600, 300),
    )
    assert isinstance(got, pl.DataFrame)
    assert_frame_equal(got, expected)
    # quarters bind, not halves: the 1-OT game must reach past 2400s
    assert got["game_seconds"].max() > 2400


def test_wbb_join_pbp_shots_delegates() -> None:
    pbp = load_oracle_pbp("wbb").filter(pl.col("game_id") == "5728709")
    shots = ncaa_wbb_shot_locations(["5728709"], fetcher=_FakeFetcher())
    joined = ncaa_wbb_join_pbp_shots(pbp, shots)
    assert isinstance(joined, pl.DataFrame)
    assert joined.height == pbp.height
