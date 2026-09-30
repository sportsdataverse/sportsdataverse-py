"""Situation-neutral reads the score-and-clock win probability (``wp_before_naive``) in CFB.

Real fixture: Ohio State at Illinois, 2025 (401752865). The pregame line puts Ohio State's
0-0 opening snaps at a spread-aware ``wp_before`` of 0.82-0.86, outside 20-80%, so the
"neutral" split dropped a tied first quarter. The scoreboard-only ``wp_before_naive``
reads 0.38-0.45 there. NFL keeps ``wp_before`` until its owner decides.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.tendencies import tendencies

FIX = Path(__file__).parent / "fixtures" / "cfb_2025_401752865_neutral_wp.parquet"
NEUTRAL = ["plays_neutral", "passes_neutral", "pass_rate_neutral", "drive_seconds_neutral", "drive_plays_neutral"]


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    return pl.read_parquet(FIX)


def _neutral_snaps(plays: pl.DataFrame, wp: str) -> pl.DataFrame:
    clock = pl.col("clock.minutes") * 60 + pl.col("clock.seconds")
    last_two = pl.col("period").is_in([2, 4]) & (clock <= 120)
    return (
        plays.filter(pl.col("scrimmage_play") & ~pl.col("penalty_no_play"))
        .filter(pl.col(wp).is_between(0.2, 0.8) & (pl.col("period") <= 4) & ~last_two)
        .group_by("pos_team")
        .len()
    )


def test_fixture_opens_outside_the_spread_aware_band(plays):
    first = plays.filter(pl.col("scrimmage_play")).sort("game_play_number").row(0, named=True)
    assert first["wp_before"] > 0.8 and 0.2 < first["wp_before_naive"] < 0.8


def test_cfb_neutral_counts_score_and_clock_snaps(plays):
    got = tendencies(plays, league="cfb").select("pos_team", "plays_neutral")
    want = _neutral_snaps(plays, "wp_before_naive")
    j = got.join(want, on="pos_team")
    assert j.height == 2 and (j["plays_neutral"] == j["len"]).all()


def test_cfb_neutral_ignores_the_spread_aware_wp(plays):
    flat = plays.with_columns(wp_before=pl.lit(0.5))  # every snap "neutral" by the spread-aware WP
    assert (
        tendencies(plays, league="cfb")
        .select("pos_team", *NEUTRAL)
        .equals(tendencies(flat, league="cfb").select("pos_team", *NEUTRAL))
    )


def test_nfl_neutral_still_reads_wp_before(plays):
    for wp in (None, 0.5):
        p = plays if wp is None else plays.with_columns(wp_before=pl.lit(wp))
        got = tendencies(p, league="nfl").select("pos_team", "plays_neutral")
        j = got.join(_neutral_snaps(p, "wp_before"), on="pos_team", how="left").fill_null(0)
        assert j.height == 2 and (j["plays_neutral"] == j["len"]).all(), wp
