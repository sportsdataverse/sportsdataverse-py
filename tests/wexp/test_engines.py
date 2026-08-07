"""Ridge vintage engine + Axis E map tests on the real NFL fixture.

Observed at gate-setting time (2026-08-06, 821-game fixture, seasons
2009/2020/2024): lam=10 margin_normal brier 0.2239 / acc 0.6425 on n=772
(weeks 2+); isotonic brier 0.2276 on n=672 (iso_min_fit=100 warm-up).
Floors carry margin off those values — never lower them to pass.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.backtest import run_backtest
from sportsdataverse.wexp.engines import ratings_predictor, ridge_margin_vintages
from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule
from sportsdataverse.wexp.store import VintageStore

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"

LAM = 10.0


@pytest.fixture(scope="module")
def nfl_oracle() -> pl.DataFrame:
    return nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))


@pytest.fixture(scope="module")
def store(nfl_oracle) -> VintageStore:
    s = VintageStore()
    s.register("ridge", ridge_margin_vintages(nfl_oracle, lam=LAM), entity_key="team_id")
    return s


def test_vintage_table_shape(nfl_oracle, store):
    vint = store.table("ridge")
    # every vintage carries the full 32-team league (observed: always 32)
    assert vint.group_by("season", "as_of_week").agg(pl.len())["len"].unique().to_list() == [32]
    # week 1 emits no vintage (no prior completed games in-season)
    assert vint.filter(pl.col("as_of_week") == 1).height == 0
    assert vint.schema["team_id"] == pl.Utf8
    assert vint.schema["as_of_week"] == pl.Int32


def test_vintage_builder_ignores_future_games(nfl_oracle):
    """A vintage at as_of_week W must be invariant to games in weeks >= W."""
    season = nfl_oracle.filter(pl.col("season") == 2024)
    tampered = season.with_columns(
        pl.when(pl.col("week") >= 10)
        .then(pl.col("home_margin") * 3 + 7)
        .otherwise(pl.col("home_margin"))
        .alias("home_margin")
    )
    a = ridge_margin_vintages(season, lam=LAM).filter(pl.col("as_of_week") <= 10)
    b = ridge_margin_vintages(tampered, lam=LAM).filter(pl.col("as_of_week") <= 10)
    assert a.height == b.height > 0
    assert a.sort("as_of_week", "team_id").equals(b.sort("as_of_week", "team_id"))


def test_ridge_margin_normal_backtest(nfl_oracle, store):
    probs, rows = run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="ridge_margin", store=store)
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    n = rows.filter(pl.col("season") == -1)["n"][0]
    assert n >= 770  # observed 772 (821 minus week-1 games); min-size guard
    assert pooled["brier"] < 0.24  # observed 0.2239; clearly beats coin flip 0.25
    assert pooled["winner_accuracy"] > 0.62  # observed 0.6425
    # week-1 games are uncovered (no vintage) and stay null, never imputed
    week1 = probs.filter((pl.col("week") == 1) & (pl.col("season_type") == "REG"))
    assert week1["p_home"].null_count() == week1.height


def test_ridge_isotonic_backtest(nfl_oracle, store):
    _, rows = run_backtest(nfl_oracle, ratings_predictor("ridge", wp_map="isotonic"), model_id="ridge_iso", store=store)
    pooled = {r["metric"]: r["value"] for r in rows.filter(pl.col("season") == -1).iter_rows(named=True)}
    n = rows.filter(pl.col("season") == -1)["n"][0]
    assert n >= 650  # observed 672 (iso_min_fit=100 warm-up per season)
    assert pooled["brier"] < 0.24  # observed 0.2276


def test_unknown_wp_map_refused():
    with pytest.raises(ValueError, match="wp_map"):
        ratings_predictor("ridge", wp_map="monte_carlo")


def test_predictor_requires_store(nfl_oracle):
    with pytest.raises(ValueError, match="VintageStore"):
        run_backtest(nfl_oracle, ratings_predictor("ridge"), model_id="ridge_margin", store=None)
