"""Baseline-oracle driver tests on the real market fixtures.

Ordering gates encode RESEARCH expectations: the vig-removed close must
beat the home rule and coin flip on Brier in both leagues; a default Elo
must beat both naive baselines but lose to the close. Never lower these —
an inversion means a builder bug, not a threshold problem.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.baselines import baseline_probs, score_baselines
from sportsdataverse.wexp.elo import EloConfig, elo_ratings
from sportsdataverse.wexp.oracle_market import (
    cfb_market_oracle_from_lines,
    nfl_market_oracle_from_schedule,
)

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"


@pytest.fixture(scope="module")
def nfl_probs() -> pl.DataFrame:
    oracle = nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))
    elo = elo_ratings(
        oracle.select(
            "game_id",
            "season",
            "week",
            "home_team",
            "away_team",
            "neutral_site",
            "home_margin",
            "home_win",
        ),
        EloConfig(),
    )
    return baseline_probs(oracle, elo=elo)


@pytest.fixture(scope="module")
def cfb_probs() -> pl.DataFrame:
    oracle = cfb_market_oracle_from_lines(
        pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet"),
        pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet"),
    )
    return baseline_probs(oracle)


def test_baseline_prob_columns(nfl_probs):
    for c in ("p_coin_flip", "p_home_rule", "p_market_close", "p_market_open", "p_elo"):
        assert c in nfl_probs.columns
    assert nfl_probs["p_coin_flip"].unique().to_list() == [0.5]
    # home rule: constant home rate at home, 0.5 at neutral
    neutral = nfl_probs.filter(pl.col("neutral_site") == True)  # noqa: E712
    if neutral.height:
        assert neutral["p_home_rule"].unique().to_list() == [0.5]
    # NFL has no opens: p_market_open all null (never imputed)
    assert nfl_probs["p_market_open"].null_count() == nfl_probs.height


def test_cfb_open_probs_only_where_open_exists(cfb_probs):
    got = cfb_probs.filter(pl.col("p_market_open").is_not_null())
    assert got.height == cfb_probs.filter(pl.col("spread_open").is_not_null()).height
    assert got.height >= 640  # observed 660 of 783 (2015 100%, 2024 68.9%)


def test_leaderboard_ordering_nfl(nfl_probs, tmp_path):
    rows = score_baselines(nfl_probs, path=tmp_path / "lb.parquet")
    lb = pl.read_parquet(tmp_path / "lb.parquet")
    assert lb.height == rows.height
    pooled = rows.filter((pl.col("season") == -1) & (pl.col("metric") == "brier"))
    brier = {r["model_id"]: r["value"] for r in pooled.iter_rows(named=True)}
    # market ceiling ordering (RESEARCH §9.3): close < elo < home rule < coin
    assert brier["market_close"] < brier["elo"]
    assert brier["elo"] < brier["home_rule"]
    assert brier["home_rule"] < brier["coin_flip"]


def test_leaderboard_ordering_cfb(cfb_probs):
    rows = score_baselines(cfb_probs)
    # partial market coverage emits lined rows too; pin the slice explicitly
    pooled = rows.filter((pl.col("season") == -1) & (pl.col("metric") == "brier") & (pl.col("week_slice") == "all"))
    brier = {r["model_id"]: r["value"] for r in pooled.iter_rows(named=True)}
    assert brier["market_close"] < brier["home_rule"] < brier["coin_flip"]
    # open exists for CFB and must be worse than (or ~equal to) close, never better
    assert brier["market_open"] >= brier["market_close"] - 0.005


def test_score_baselines_per_season_rows(nfl_probs):
    rows = score_baselines(nfl_probs)
    seasons = set(rows["season"].to_list())
    assert {-1, 2009, 2020, 2024} <= seasons
    # every scored model carries n on each row
    assert rows.filter(pl.col("n") <= 0).height == 0
