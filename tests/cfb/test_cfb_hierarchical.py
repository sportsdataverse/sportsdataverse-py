"""The opt-in hierarchical pregame path, pinned to the gated reference model on real games.

``expected_ratings.parquet`` is the cfbfastR-cfb-data stage 34 reference (``hierarchical.build_corpus`` +
``fit_asof(method="eb")``), run on the committed real 2023-2024 schedule with the same as-of corpus rule
(see the fixture README). The port must reproduce it, and ``cfb_predict_games`` must serve it only when asked.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb.cfb_hierarchical import HIER_MARGIN_SD, cfb_hierarchical_ratings

FIX = Path(__file__).parents[1] / "fixtures" / "cfb_hierarchical"
SCHED = pl.read_parquet(FIX / "schedule_2023_2024.parquet")
EXPECTED = pl.read_parquet(FIX / "expected_ratings.parquet")


@pytest.mark.parametrize(("season", "t"), [(2023, 6), (2024, 10), (2024, 101)])
def test_port_reproduces_the_reference_model(season, t):
    got = cfb_hierarchical_ratings(SCHED, season, t, first_season=2023)
    exp = EXPECTED.filter((pl.col("season") == season) & (pl.col("asof_t") == t))
    assert got.height == exp.height
    j = exp.join(got, on="team_id", suffix="_got")
    assert j.height == exp.height
    for c in ("theta", "hfa", "tau_team", "sigma_obs"):
        np.testing.assert_allclose(j[f"{c}_got"].to_numpy(), j[c].to_numpy(), rtol=0, atol=1e-6, err_msg=c)


def test_later_games_cannot_move_an_as_of_rating():
    """The corpus is games before the slot: future results must not reach theta."""
    base = cfb_hierarchical_ratings(SCHED, 2024, 10, first_season=2023)
    slot = pl.when(pl.col("season_type") == "postseason").then(pl.col("week") + 100).otherwise(pl.col("week"))
    poisoned = SCHED.with_columns(
        pl.when((pl.col("season") == 2024) & (slot >= 10))
        .then(pl.col("home_points") + 60)
        .otherwise(pl.col("home_points"))
        .alias("home_points")
    )
    again = cfb_hierarchical_ratings(poisoned, 2024, 10, first_season=2023)
    assert base.equals(again)


def test_first_slot_refuses_instead_of_guessing():
    first = int(SCHED.filter((pl.col("season") == 2024) & (pl.col("season_type") == "regular"))["week"].min())
    with pytest.raises(ValueError, match="no completed 2024 games before slot"):
        cfb_hierarchical_ratings(SCHED, 2024, first, first_season=2023)


def test_missing_chain_start_refuses():
    with pytest.raises(ValueError, match="carryover chain"):
        cfb_hierarchical_ratings(SCHED.filter(pl.col("season") == 2024), 2024, 10, first_season=2023)


def _games_and_ratings(hier: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    slate = SCHED.filter(
        (pl.col("season") == 2024)
        & (pl.col("week") == 10)
        & (pl.col("season_type") == "regular")
        & (pl.col("fbs_participant") == True)  # noqa: E712
    )
    games = slate.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("home_id").cast(pl.Int64).cast(pl.Utf8).alias("home_team_id"),
        pl.col("away_id").cast(pl.Int64).cast(pl.Utf8).alias("away_team_id"),
        pl.col("neutral_site").cast(pl.Boolean),
    )
    # a closed-form ratings frame covering every team, on the fitted adj_net spread (0.2259) so the
    # scale guard stays quiet; it feeds the default path and exp_total
    ratings = hier.select(
        "team_id",
        (pl.col("theta") * 0.2259 / pl.col("theta").std()).alias("adj_net"),
        pl.lit(0.0).alias("adj_off_epa"),
        pl.lit(0.0).alias("adj_def_epa"),
        pl.lit(70.0).alias("off_pace"),
        pl.lit(9).alias("games"),
        pl.lit(70.0).alias("prior_off_pace"),
    )
    return games, ratings


def test_predict_games_serves_hierarchical_only_when_asked():
    hier = cfb_hierarchical_ratings(SCHED, 2024, 10, first_season=2023)
    games, ratings = _games_and_ratings(hier)
    closed = cfb_predict_games(games, ratings)
    served = cfb_predict_games(games, ratings, model="hierarchical", hierarchical=hier)
    th = dict(zip(hier["team_id"].to_list(), hier["theta"].to_list()))
    hfa = float(hier["hfa"][0])
    exp = np.array(
        [
            th[h] - th[a] + (0.0 if n else hfa)
            for h, a, n in zip(games["home_team_id"], games["away_team_id"], games["neutral_site"])
        ]
    )
    np.testing.assert_allclose(served["exp_margin"].to_numpy(), exp, atol=1e-9)
    np.testing.assert_allclose(served["home_win_prob"].to_numpy(), norm.cdf(exp / HIER_MARGIN_SD), atol=1e-12)
    # the default path is untouched: it serves the closed form, not the hierarchical margin
    assert not np.allclose(closed["exp_margin"].to_numpy(), exp)
    assert closed.equals(cfb_predict_games(games, ratings, model="closed_form"))
    np.testing.assert_allclose(served["exp_total"].to_numpy(), closed["exp_total"].to_numpy())


def test_predict_games_never_falls_back():
    hier = cfb_hierarchical_ratings(SCHED, 2024, 10, first_season=2023)
    games, ratings = _games_and_ratings(hier)
    with pytest.raises(ValueError, match="needs the cfb_hierarchical_ratings frame"):
        cfb_predict_games(games, ratings, model="hierarchical")
    gone = games["home_team_id"][0]
    with pytest.raises(ValueError, match="no hierarchical strength"):
        cfb_predict_games(games, ratings, model="hierarchical", hierarchical=hier.filter(pl.col("team_id") != gone))
    with pytest.raises(ValueError, match="without model='hierarchical'"):
        cfb_predict_games(games, ratings, hierarchical=hier)
    with pytest.raises(ValueError, match="model must be"):
        cfb_predict_games(games, ratings, model="bayes")


def test_integer_keyed_ratings_join_the_string_ids():
    """cfb_ratings_weekly carries Int64 team ids; the hierarchical String ids must meet them."""
    hier = cfb_hierarchical_ratings(SCHED, 2024, 10, first_season=2023)
    games, ratings = _games_and_ratings(hier)
    as_str = cfb_predict_games(games, ratings, model="hierarchical", hierarchical=hier)
    games_i = games.with_columns(pl.col("home_team_id", "away_team_id").cast(pl.Int64))
    ratings_i = ratings.with_columns(pl.col("team_id").cast(pl.Int64))
    as_int = cfb_predict_games(games_i, ratings_i, model="hierarchical", hierarchical=hier)
    np.testing.assert_array_equal(as_int["exp_margin"].to_numpy(), as_str["exp_margin"].to_numpy())


def test_fixture_exercises_the_filters():
    """Non-FBS, unplayed and null-conference rows are present, so the corpus filters are tested."""
    assert SCHED.filter(pl.col("fbs_participant") != True).height > 0  # noqa: E712
    assert SCHED.filter(pl.col("home_points").is_null()).height > 0
    assert SCHED.filter(pl.col("home_conference").is_null() | pl.col("away_conference").is_null()).height > 0
