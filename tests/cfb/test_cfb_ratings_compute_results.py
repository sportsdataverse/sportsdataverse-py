"""cfb_ratings_compute_results: R parity on the shared 2025 fixture, plus the generator contract."""

from __future__ import annotations

import functools
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.cfb.cfb_prediction_constants import get_constants
from sportsdataverse.cfb.cfb_simulations import (
    _ratings_expected_margin,
    cfb_ratings_compute_results,
    cfb_simulations,
)

FIXTURE = Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_2025_ratings"


def _variant(ratings: pl.DataFrame, rating_type: str, variant: str) -> pl.DataFrame:
    """cfbseedR's ratings_fixture_variant() (tests/testthat/helper.R)."""
    out = ratings.select("team", pl.col(rating_type).alias("rating"), "games")
    if variant == "half":
        out = out.with_columns(pl.col("games") // 2)
    elif variant == "quarter":
        out = out.with_columns(pl.col("games") // 4)
    elif variant == "missing":
        out = out.filter(pl.col("team") != "Georgia Southern").with_columns(
            pl.when(pl.col("team") == "Navy").then(None).otherwise(pl.col("games")).alias("games")
        )
    return out


@pytest.mark.parametrize("rating_type", ["points", "adj_epa", "raw_epa"])
@pytest.mark.parametrize("variant", ["full", "half", "quarter", "missing"])
def test_expected_margins_match_cfbseedr(rating_type: str, variant: str) -> None:
    ratings = pl.read_csv(FIXTURE / "ratings.csv")
    games = pl.read_csv(FIXTURE / "games.csv")
    want = pl.read_csv(FIXTURE / "expected.csv").filter(
        (pl.col("rating_type") == rating_type) & (pl.col("variant") == variant)
    )
    assert want.height == games.height == 47
    assert want["home_team"].to_list() == games["home_team"].to_list()
    exp, sd = _ratings_expected_margin(
        games["home_team"].to_list(),
        games["away_team"].to_list(),
        games["neutral"].to_numpy(),
        _variant(ratings, rating_type, variant),
        rating_type=rating_type,
    )
    np.testing.assert_allclose(exp, want["expected"].to_numpy(), rtol=0, atol=1e-9)
    np.testing.assert_allclose(sd, want["margin_sd"].to_numpy(), rtol=0, atol=1e-9)


def test_adj_epa_defaults_are_the_live_prediction_constants() -> None:
    c = get_constants("modern")
    r = pl.DataFrame({"team": ["A", "B"], "rating": [0.1, 0.0]})
    exp, sd = _ratings_expected_margin(["A"], ["B"], np.array([0]), r, rating_type="adj_epa")
    assert exp[0] == pytest.approx(c.slope_by_games["8-20"] * 0.1 + c.hfa_points)
    assert sd[0] == pytest.approx(c.margin_sd)


def test_buckets_take_the_largest_lower_bound_and_nan_takes_the_top() -> None:
    r = pl.DataFrame({"team": ["A", "B", "C"], "rating": [1.0, 0.0, 0.0], "games": [3.5, 9.0, 1.0]})
    scale = {"0-3": 1.0, "6-20": 4.0}
    sd = {"2-3": 11.0, "6-20": 14.0}
    # A-B: min 3.5 -> "0-3" (gap -> lower); A-C: min 1 -> below every sd bucket -> bottom; A-Z: Z unrated, A's 3.5
    exp, s = _ratings_expected_margin(
        ["A", "A", "A"], ["B", "C", "Z"], np.array([1, 1, 1]), r, scale=scale, home_field=0.0, margin_sd=sd
    )
    assert exp.tolist() == [1.0, 1.0, 1.0]
    assert s.tolist() == [11.0, 11.0, 11.0]
    no_games = r.drop("games")
    exp2, s2 = _ratings_expected_margin(
        ["A"], ["B"], np.array([1]), no_games, scale=scale, home_field=0.0, margin_sd=sd
    )
    assert (exp2[0], s2[0]) == (4.0, 14.0)


def test_bad_inputs_raise() -> None:
    games = ["A"], ["B"], np.array([0])
    with pytest.raises(ValueError, match="rating_type"):
        _ratings_expected_margin(*games, pl.DataFrame({"team": ["A"], "rating": [1.0]}), rating_type="elo")
    with pytest.raises(ValueError, match="missing"):
        _ratings_expected_margin(*games, pl.DataFrame({"team": ["A", "B"], "rating": [1.0, None]}))
    with pytest.raises(ValueError, match="non-negative"):
        _ratings_expected_margin(*games, pl.DataFrame({"team": ["A"], "rating": [1.0], "games": ["8"]}))
    with pytest.raises(ValueError, match="lo-hi"):
        _ratings_expected_margin(*games, pl.DataFrame({"team": ["A"], "rating": [1.0]}), scale={"a": 1.0})
    with pytest.raises(ValueError, match="at least 0"):
        _ratings_expected_margin(*games, pl.DataFrame({"team": ["A"], "rating": [1.0]}), margin_sd=-1.0)


def _toy() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    teams = pl.DataFrame({"sim": [1, 1, 1], "team": ["A", "B", "C"], "conference": ["X", "X", "X"]})
    games = pl.DataFrame(
        {
            "sim": [1, 1, 1],
            "week": [1, 2, 2],
            "game_type": ["REG", "REG", "POST"],
            "home_team": ["A", "A", "B"],
            "away_team": ["B", "C", "C"],
            "result": [3.0, None, None],
            "neutral": [0, 0, 1],
        }
    )
    ratings = pl.DataFrame({"team": ["A", "B", "C"], "rating": [7.0, 0.0, -7.0], "games": [8, 8, 8]})
    return teams, games, ratings


def test_fills_only_this_weeks_unplayed_games_and_teams_pass_through() -> None:
    teams, games, ratings = _toy()
    out = cfb_ratings_compute_results(teams, games, 2, ratings=ratings, rng=np.random.default_rng(1))
    assert out["teams"] is teams
    res = out["games"]["result"].to_list()
    assert res[0] == 3.0 and None not in res
    assert all(float(x).is_integer() for x in res)
    assert out["games"].columns == games.columns
    same = cfb_ratings_compute_results(teams, games, 2, ratings=ratings, rng=np.random.default_rng(1))
    assert same["games"].equals(out["games"])


def test_no_noise_gives_the_rounded_expected_margin_and_postseason_ties_rebreak_by_one() -> None:
    teams, games, ratings = _toy()
    out = cfb_ratings_compute_results(teams, games, 2, ratings=ratings, margin_sd=0.0, rng=np.random.default_rng(0))
    # A-C at home: 1.0628 * 14 + 2.376 = 17.25 -> 18; B-C neutral: 1.0628 * 7 = 7.44 -> 8
    assert out["games"]["result"].to_list()[1:] == [18.0, 8.0]
    even = ratings.with_columns(pl.lit(0.0).alias("rating"))
    ties = [
        cfb_ratings_compute_results(
            teams, games, 2, ratings=even, margin_sd=0.0, home_field=0.0, rng=np.random.default_rng(s)
        )["games"]["result"].to_list()[2]
        for s in range(20)
    ]
    assert set(ties) == {-1.0, 1.0}


def test_runs_inside_cfb_simulations() -> None:
    games = pl.read_csv(Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy" / "toy_games.csv")
    teams = pl.read_csv(Path(__file__).parent.parent / "fixtures" / "seedr" / "cfb_toy" / "toy_teams.csv")
    games = games.with_columns(pl.when(pl.col("week") >= 3).then(None).otherwise(pl.col("result")).alias("result"))
    ratings = pl.DataFrame({"team": teams["team"], "rating": np.linspace(10, -10, teams.height)})
    cr = functools.partial(cfb_ratings_compute_results, ratings=ratings, rating_type="points")
    sims = cfb_simulations(games, teams, compute_results=cr, simulations=4, playoff_seeds=4, seed=3)
    assert sims["games"]["result"].null_count() == 0


def test_nan_constants_raise_and_float_team_ids_match_as_integers() -> None:
    r = pl.DataFrame({"team": ["A", "B"], "rating": [1.0, 0.0]})
    games = ["A"], ["B"], np.array([0])
    with pytest.raises(ValueError, match="number"):
        _ratings_expected_margin(*games, r, margin_sd=float("nan"))
    with pytest.raises(ValueError, match="NaN"):
        _ratings_expected_margin(*games, r, scale={"0-3": float("nan")})
    with pytest.raises(ValueError, match="finite"):
        _ratings_expected_margin(*games, r, home_field=float("nan"))
    with pytest.raises(ValueError, match="lo-hi"):
        _ratings_expected_margin(*games, r, scale={"0-x": 1.0})
    ids = pl.DataFrame({"team": [87.0, 99.0], "rating": [1.0, 0.0]})
    exp, _ = _ratings_expected_margin(["87"], ["99"], np.array([1]), ids, scale=10.0, home_field=0.0)
    assert exp.tolist() == [10.0]
