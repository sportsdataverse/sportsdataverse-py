"""Market-oracle tests on REAL captured fixtures (tests/fixtures/wexp/).

Floors are set from observed values with margin (observed numbers noted
inline). NEVER lower a floor to make a run pass — debug the builder
(sign convention, abbr resolution, devig, join direction) instead.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.market import logit_blend
from sportsdataverse.wexp.oracle_market import (
    cfb_market_oracle_from_lines,
    nfl_market_oracle_from_schedule,
)
from sportsdataverse.wexp.scoring import brier_score, ece, winner_accuracy


def _assert_blend_applied(oracle: pl.DataFrame) -> None:
    """The 70/30 blend must CHANGE p_close vs both inputs (no silent no-op),
    and must match the unit-tested scalar logit_blend on a sampled row."""
    both = oracle.drop_nulls(["p_close_spread", "p_close_ml"]).filter(pl.col("p_close_spread") != pl.col("p_close_ml"))
    assert both.height > 0
    assert both.select((pl.col("p_close") != pl.col("p_close_spread")).all()).item()
    assert both.select((pl.col("p_close") != pl.col("p_close_ml")).all()).item()
    row = both.row(0, named=True)
    assert row["p_close"] == pytest.approx(
        logit_blend(row["p_close_spread"], row["p_close_ml"], weight_a=0.7), abs=1e-12
    )


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
    _assert_blend_applied(nfl_oracle)


def test_nfl_close_is_a_sane_oracle(nfl_oracle):
    y, p = _scored(nfl_oracle)
    assert len(y) == 820  # one 2009 tie is unscoreable
    # observed brier 0.2015, acc 0.6890, ece 0.0415 on the 3-season fixture
    assert brier_score(y, p) <= 0.22
    assert winner_accuracy(y, p) >= 0.64
    assert ece(y, p) <= 0.07


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
    _assert_blend_applied(cfb_oracle)


def test_cfb_close_is_a_sane_oracle(cfb_oracle):
    y, p = _scored(cfb_oracle)
    # observed brier 0.1528, acc 0.7995, ece 0.0732 (biased first-400 sample)
    assert brier_score(y, p) <= 0.17
    assert winner_accuracy(y, p) >= 0.74
    assert ece(y, p) <= 0.11


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
