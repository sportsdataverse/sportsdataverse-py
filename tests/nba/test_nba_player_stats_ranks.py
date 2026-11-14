"""Gated producer ranks on ``player_stats_advanced`` (GOP #415 Q24: nearby ranks need a minimum).

Real captures (``tests/fixtures/bb_impact_ranks/README.md``): the 2025-26 NBA and 2025 WNBA
``player_stats_advanced`` release rows (both season types), columns only dropped.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import (
    ADVANCED_LOWER_IS_BETTER,
    ADVANCED_RANKED,
    advanced_min_poss,
    nba_player_stats_advanced_context,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_impact_ranks"
MPJ = 1629008


@pytest.fixture(scope="module")
def nba() -> pl.DataFrame:
    return pl.read_parquet(FIX / "nba_player_stats_advanced_2026.parquet")


@pytest.fixture(scope="module")
def ranked(nba) -> pl.DataFrame:
    return nba_player_stats_advanced_context(nba)


def test_gates() -> None:
    assert advanced_min_poss("00") == 1600.0 and advanced_min_poss("10") == 800.0
    with pytest.raises(ValueError):
        advanced_min_poss("20")


def test_only_players_over_the_possessions_gate_are_ranked(nba, ranked) -> None:
    rs = ranked.filter(pl.col("season_type") == "Regular Season")
    for m in ADVANCED_RANKED:
        has = rs.filter(pl.col(f"{m}_rank").is_not_null())
        assert has["poss"].min() >= 1600, m
        assert has.height == rs.filter((pl.col("poss") >= 1600) & pl.col(m).is_finite()).height == 333, m
        assert has[f"{m}_pct"].null_count() == 0
    # the API ranked all 582; a 20-possession call-up no longer sits among the starters
    assert nba.filter(pl.col("season_type") == "Regular Season")["net_rating_rank"].null_count() == 0


def test_direction_and_percentile(ranked) -> None:
    q = ranked.filter((pl.col("season_type") == "Regular Season") & (pl.col("poss") >= 1600))
    for m in ADVANCED_RANKED:
        best = q[m].min() if m in ADVANCED_LOWER_IS_BETTER else q[m].max()
        assert q.filter(pl.col(f"{m}_rank") == pl.col(f"{m}_rank").min())[m].to_list()[0] == best, m
    mpj = q.filter(pl.col("player_id") == MPJ).row(0, named=True)
    # his net rating among the 333 qualifiers (average rank on ties), and its percentile
    above = q.filter(pl.col("net_rating") > mpj["net_rating"]).height
    ties = q.filter(pl.col("net_rating") == mpj["net_rating"]).height
    assert mpj["net_rating_rank"] == above + (ties + 1) / 2
    assert mpj["net_rating_pct"] == pytest.approx(100 * (334 - mpj["net_rating_rank"]) / 334)


def test_other_ranks_stay_the_apis_and_rerunning_changes_nothing(nba, ranked) -> None:
    assert ranked["gp_rank"].to_list() == nba["gp_rank"].to_list()
    assert ranked.columns == nba.columns + [f"{m}_pct" for m in ADVANCED_RANKED]
    assert nba_player_stats_advanced_context(ranked).equals(ranked)


def test_playoffs_rank_on_their_own_run(ranked) -> None:
    po = ranked.filter(pl.col("season_type") == "Playoffs")
    n = po.filter(pl.col("poss") >= 1600).height
    assert n < 10  # no playoff run reaches a regular season's gate: no playoff cohort
    assert po["net_rating_rank"].null_count() == po.height


def test_wnba_gate() -> None:
    w = nba_player_stats_advanced_context(
        pl.read_parquet(FIX / "wnba_player_stats_advanced_2025.parquet"), league_id="10"
    ).filter(pl.col("season_type") == "Regular Season")
    has = w.filter(pl.col("ts_pct_rank").is_not_null())
    assert has["poss"].min() >= 800 and has.height == 122
