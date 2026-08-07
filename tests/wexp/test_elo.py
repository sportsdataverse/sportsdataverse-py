"""Unit + real-data tests for the general Elo engine (wexp.elo).

Real-data sanity floors are observed-with-margin on the NFL fixture
(2009/2020/2024 seasons); never lower them — debug the engine instead.
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.wexp.elo import EloConfig, elo_ratings
from sportsdataverse.wexp.oracle_market import nfl_market_oracle_from_schedule
from sportsdataverse.wexp.scoring import brier_score, winner_accuracy

FIXDIR = Path(__file__).resolve().parents[1] / "fixtures" / "wexp"


@pytest.fixture(scope="module")
def nfl_games() -> pl.DataFrame:
    oracle = nfl_market_oracle_from_schedule(pl.read_parquet(FIXDIR / "nfl_schedule_sample.parquet"))
    return oracle.select(
        "game_id",
        "season",
        "week",
        "home_team",
        "away_team",
        "neutral_site",
        "home_margin",
        "home_win",
    )


def test_elo_output_contract_and_bounds(nfl_games):
    out = elo_ratings(nfl_games, EloConfig())
    assert out.height == nfl_games.height
    for c in ("home_elo_pre", "away_elo_pre", "p_home"):
        assert c in out.columns
    assert out["p_home"].null_count() == 0
    assert out["p_home"].min() > 0.0
    assert out["p_home"].max() < 1.0
    # week-1 season-2009 games start at init +/- HFA only
    wk1 = out.filter((pl.col("season") == 2009) & (pl.col("week") == 1))
    assert wk1["home_elo_pre"].to_list() == [1500.0] * wk1.height


def test_elo_prefers_home_and_learns(nfl_games):
    out = elo_ratings(nfl_games, EloConfig())
    # HFA pushes week-1 p_home above 0.5 (non-neutral games)
    wk1 = out.filter((pl.col("season") == 2009) & (pl.col("week") == 1) & (pl.col("neutral_site") == False))  # noqa: E712
    assert wk1["p_home"].min() > 0.5
    # after mid-season, ratings separate: accuracy beats the home rule on wk5+
    late = out.filter(pl.col("week") >= 5).drop_nulls(["home_win", "p_home"])
    y, p = late["home_win"].to_numpy(), late["p_home"].to_numpy()
    # observed acc 0.6313, brier 0.2186 (defaults, no tuning) on the fixture
    assert winner_accuracy(y, p) >= 0.58
    assert brier_score(y, p) <= 0.24


def test_elo_no_future_leak(nfl_games):
    """Poisoning the LAST game's result must not change any earlier pre-game rating."""
    base = elo_ratings(nfl_games, EloConfig())
    poisoned = nfl_games.sort("season", "week").with_columns(
        pl.when(pl.int_range(pl.len()) == pl.len() - 1)
        .then(pl.lit(-70.0))
        .otherwise(pl.col("home_margin"))
        .alias("home_margin")
    )
    out2 = elo_ratings(poisoned, EloConfig())
    a = base.sort("season", "week", "game_id").head(base.height - 1)
    b = out2.sort("season", "week", "game_id").head(base.height - 1)
    assert a["home_elo_pre"].to_list() == b["home_elo_pre"].to_list()
    assert a["p_home"].to_list() == b["p_home"].to_list()


def test_elo_season_carryover_reverts_toward_mean(nfl_games):
    full = elo_ratings(nfl_games, EloConfig(carryover=0.0))
    # with carryover=0 every season restarts at init: week-1 ratings all 1500
    wk1_2020 = full.filter((pl.col("season") == 2020) & (pl.col("week") == 1))
    assert wk1_2020["home_elo_pre"].to_list() == [1500.0] * wk1_2020.height
    # with carryover=1 the 2020 openers differ from init
    sticky = elo_ratings(nfl_games, EloConfig(carryover=1.0))
    wk1s = sticky.filter((pl.col("season") == 2020) & (pl.col("week") == 1))
    assert wk1s["home_elo_pre"].to_list() != [1500.0] * wk1s.height


def test_elo_neutral_site_gets_no_hfa():
    games = pl.DataFrame(
        {
            "game_id": ["a", "b"],
            "season": pl.Series([2023, 2023], dtype=pl.Int32),
            "week": pl.Series([1, 1], dtype=pl.Int32),
            "home_team": ["X", "Y"],
            "away_team": ["Z", "W"],
            "neutral_site": [True, False],
            "home_margin": [3.0, 3.0],
            "home_win": pl.Series([1, 1], dtype=pl.Int8),
        }
    )
    out = elo_ratings(games, EloConfig())
    p = {r["game_id"]: r["p_home"] for r in out.iter_rows(named=True)}
    assert p["a"] == pytest.approx(0.5)  # equal ratings, no HFA
    assert p["b"] > 0.5
