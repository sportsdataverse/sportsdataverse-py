"""Ranks on ``current_impact`` (GOP #413's Season vs Current view): within each date, among players who have played.

Real fixtures (README in ``tests/fixtures/bb_current_impact``): the 2013-14 regular-season possessions from
April 1 and 2014-15's before November 8, and the window priors built at those dates. Two dates: opening week
(2014-10-29, few players have played) and 2014-11-05.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba import CURRENT_IMPACT_RANKED, nba_current_impact, nba_current_impact_context

_CI = Path(__file__).resolve().parents[1] / "fixtures" / "bb_current_impact"
EARLY, LATER = dt.date(2014, 10, 29), dt.date(2014, 11, 5)


@pytest.fixture(scope="module")
def current() -> pl.DataFrame:
    poss = pl.read_parquet(_CI / "nba_poss_2014_2015_slice.parquet")
    priors = pl.read_parquet(_CI / "nba_window_priors_2014_2015_slice.parquet")
    # the producer publishes this season's possessions only (0 for last season's)
    cur = nba_current_impact(poss, priors, [EARLY, LATER])
    return cur.with_columns(
        pl.when(pl.col("season") == 2015).then(pl.col(c)).otherwise(0).alias(c) for c in ("off_poss", "def_poss")
    )


@pytest.fixture(scope="module")
def ranked(current) -> pl.DataFrame:
    return nba_current_impact_context(current)


def test_ranked_within_each_date_among_players_who_have_played(current, ranked) -> None:
    for day in (EARLY, LATER):
        d = ranked.filter(pl.col("as_of") == day)
        played = d.filter((pl.col("off_poss") + pl.col("def_poss")) > 0)
        assert d.filter(pl.col("current_impact_rank").is_not_null()).height == played.height > 10
        assert played.filter(pl.col("off_poss") > 0).height == d["o_current_impact_rank"].is_not_null().sum()
        assert d.filter((pl.col("off_poss") + pl.col("def_poss")) == 0)["current_impact_rank"].null_count() == (
            d.height - played.height
        )
        for m in CURRENT_IMPACT_RANKED:
            top = d.filter(pl.col(f"{m}_rank") == 1.0)
            assert top[m].item() == d.filter(pl.col(f"{m}_rank").is_not_null())[m].max(), (day, m)
    # one cohort per date: the same player ranks against a bigger field a week later
    early_n = ranked.filter(pl.col("as_of") == EARLY)["current_impact_rank"].count()
    later_n = ranked.filter(pl.col("as_of") == LATER)["current_impact_rank"].count()
    assert early_n < later_n


def test_columns_pct_and_idempotent(current, ranked) -> None:
    suffixed = [f"{m}_n" for m in CURRENT_IMPACT_RANKED] + [
        f"{m}{s}" for s in ("_rank", "_pct") for m in CURRENT_IMPACT_RANKED
    ]
    assert ranked.columns == current.columns + suffixed
    d = ranked.filter((pl.col("as_of") == LATER) & pl.col("current_impact_rank").is_not_null())
    n = d.height
    assert (d["current_impact_pct"] - 100 * (n + 1 - d["current_impact_rank"]) / (n + 1)).abs().max() < 1e-9
    assert nba_current_impact_context(ranked).equals(ranked)


def test_no_one_has_played_is_unranked(current) -> None:
    none = nba_current_impact_context(
        current.with_columns(pl.lit(0, pl.Int64).alias("off_poss"), pl.lit(0, pl.Int64).alias("def_poss"))
    )
    assert none["current_impact_rank"].null_count() == none.height


@pytest.mark.parametrize("col", ["off_poss", "def_poss", "as_of", "current_impact"])
def test_missing_column_raises_keyerror(current, col) -> None:
    with pytest.raises(KeyError):
        nba_current_impact_context(current.drop(col))
