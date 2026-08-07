"""Market-oracle tests on REAL captured fixtures (tests/fixtures/wexp/).

Floors are set from observed values with margin (observed numbers noted
inline). NEVER lower a floor to make a run pass — debug the builder
(sign convention, abbr resolution, devig, join direction) instead.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.oracle_market import (
    cfb_market_oracle_from_lines,
    nfl_market_oracle_from_schedule,
)
from sportsdataverse.wexp.scoring import brier_score, winner_accuracy

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"

ORACLE_COLUMNS = [
    "league",
    "game_id",
    "season",
    "week",
    "season_type",
    "home_team",
    "away_team",
    "neutral_site",
    "fbs_vs_fbs",
    "home_margin",
    "home_win",
    "spread_close",
    "total_close",
    "ml_home_close",
    "ml_away_close",
    "spread_open",
    "p_close_spread",
    "p_close_ml",
    "p_close",
]


@pytest.fixture(scope="module")
def nfl_oracle() -> pl.DataFrame:
    sch = pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet")
    return nfl_market_oracle_from_schedule(sch)


@pytest.fixture(scope="module")
def cfb_oracle() -> pl.DataFrame:
    lines = pl.read_parquet(FIXDIR / "cfb_line_odds_sample.parquet")
    sch = pl.read_parquet(FIXDIR / "cfb_schedule_sample.parquet")
    return cfb_market_oracle_from_lines(lines, sch)


def _scored(oracle: pl.DataFrame) -> tuple:
    sc = oracle.drop_nulls(["home_win", "p_close"])
    return sc["home_win"].to_numpy(), sc["p_close"].to_numpy()


def test_nfl_oracle_contract(nfl_oracle):
    assert nfl_oracle.columns == ORACLE_COLUMNS
    # 821 fixture games (2009/2020/2024); every game gets a p_close
    assert nfl_oracle.height == 821
    assert nfl_oracle["p_close"].null_count() == 0
    assert nfl_oracle.schema["game_id"] == pl.Utf8
    assert nfl_oracle.schema["season"] == pl.Int32
    assert nfl_oracle["p_close"].min() > 0.0
    assert nfl_oracle["p_close"].max() < 1.0


def test_nfl_close_is_a_sane_oracle(nfl_oracle):
    y, p = _scored(nfl_oracle)
    assert len(y) == 820  # one 2009 tie is unscoreable
    # observed brier 0.2015, acc 0.6890 on the 3-season fixture
    assert brier_score(y, p) <= 0.22
    assert winner_accuracy(y, p) >= 0.64


def test_nfl_spread_sign_matches_ml_favorite(nfl_oracle):
    ag = nfl_oracle.drop_nulls(["spread_close", "p_close_ml"]).filter(pl.col("spread_close") != 0)
    assert ag.height >= 800  # observed 807
    rate = ag.select(((pl.col("spread_close") > 0) == (pl.col("p_close_ml") > 0.5)).mean()).item()
    assert rate >= 0.98  # observed 0.9950 — home-positive convention holds


def test_cfb_oracle_contract_and_coverage(cfb_oracle):
    assert cfb_oracle.columns == ORACLE_COLUMNS
    # min-size guard: 783 of the 800 sampled archive games resolve
    assert cfb_oracle.height >= 780
    assert cfb_oracle["p_close"].null_count() == 0
    assert cfb_oracle.schema["game_id"] == pl.Utf8
    # moneyline consensus resolves for most games (observed 584)
    assert cfb_oracle["p_close_ml"].is_not_null().sum() >= 570


def test_cfb_close_is_a_sane_oracle(cfb_oracle):
    y, p = _scored(cfb_oracle)
    # observed brier 0.1528, acc 0.7995
    assert brier_score(y, p) <= 0.17
    assert winner_accuracy(y, p) >= 0.74


def test_cfb_spread_sign_matches_ml_favorite(cfb_oracle):
    ag = cfb_oracle.drop_nulls(["spread_close", "p_close_ml"]).filter(pl.col("spread_close") != 0)
    assert ag.height >= 570  # observed 581
    rate = ag.select(((pl.col("spread_close") > 0) == (pl.col("p_close_ml") > 0.5)).mean()).item()
    assert rate >= 0.98  # observed 0.9983 after the |price|>=100 garbage filter


def test_cfb_open_lines_era_coverage(cfb_oracle):
    cov = cfb_oracle.group_by("season").agg(open_pct=pl.col("spread_open").is_not_null().mean()).sort("season")
    by = {r["season"]: r["open_pct"] for r in cov.iter_rows(named=True)}
    # game-level open coverage (any book) — observed 1.00 (2015), 0.689 (2024)
    assert by[2015] >= 0.95
    assert by[2024] >= 0.60
