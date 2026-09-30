"""Situation-neutral reads the score-and-clock win probability (``wp_before_naive``) in both leagues.

Real fixtures: Ohio State at Illinois, CFB 2025 (401752865). The pregame line puts Ohio
State's 0-0 opening snaps at a spread-aware ``wp_before`` of 0.82-0.86, outside 20-80%, so
the "neutral" split dropped a tied first quarter. The scoreboard-only ``wp_before_naive``
reads 0.38-0.45 there. NFL 2025, Raiders vs Texans (401772805): Las Vegas's 0-0 opening
snaps sit at ``wp_before`` 0.12-0.14 vs ``wp_before_naive`` 0.46-0.50 (nflfastR's ``wp``);
the spread-aware WP leaves 19 of 102 snaps neutral, the naive one 93.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.tendencies import tendencies

FIX = Path(__file__).parent / "fixtures" / "cfb_2025_401752865_neutral_wp.parquet"
NFL_FIX = Path(__file__).parent / "fixtures" / "nfl_2025_401772805_neutral_wp.parquet"
NEUTRAL = ["plays_neutral", "passes_neutral", "pass_rate_neutral", "drive_seconds_neutral", "drive_plays_neutral"]


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    return pl.read_parquet(FIX)


@pytest.fixture(scope="module")
def nfl_plays() -> pl.DataFrame:
    return pl.read_parquet(NFL_FIX)


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


def test_nfl_fixture_opens_outside_the_spread_aware_band(nfl_plays):
    first = nfl_plays.filter(pl.col("scrimmage_play")).sort("game_play_number").row(0, named=True)
    assert first["wp_before"] < 0.2 and 0.2 < first["wp_before_naive"] < 0.8


def test_nfl_neutral_counts_score_and_clock_snaps(nfl_plays):
    got = tendencies(nfl_plays, league="nfl").select("pos_team", "plays_neutral")
    j = got.join(_neutral_snaps(nfl_plays, "wp_before_naive"), on="pos_team")
    assert j.height == 2 and (j["plays_neutral"] == j["len"]).all()


def test_nfl_neutral_ignores_the_spread_aware_wp(nfl_plays):
    flat = nfl_plays.with_columns(wp_before=pl.lit(0.5))
    assert (
        tendencies(nfl_plays, league="nfl")
        .select("pos_team", *NEUTRAL)
        .equals(tendencies(flat, league="nfl").select("pos_team", *NEUTRAL))
    )
