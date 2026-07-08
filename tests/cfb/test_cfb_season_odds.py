"""Season Monte-Carlo tests (T2.1 Phase 4).

Task 4.1 exercises the ratings-driven ``compute_results`` closure against the
``cfb_simulations`` engine contract (fills each unplayed week-``week_num`` game's
``result`` = sampled home margin), with a seeded rng for determinism.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results


def test_ratings_compute_results_fills_and_favors_strong_team() -> None:
    """The closure returns {teams, games} and fills the target week's result."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.35, -0.35]})
    cr = make_ratings_compute_results(ratings)
    teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
    games = pl.DataFrame(
        {"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"], "neutral": [0], "result": [None]}
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(0))
    assert set(out.keys()) == {"teams", "games"}
    assert out["games"].filter(pl.col("week") == 1)["result"].item() is not None


def test_strong_home_team_wins_on_average() -> None:
    """A (>> B) at home wins the large majority of a 400-sim batch of the same game."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.35, -0.35]})
    cr = make_ratings_compute_results(ratings)
    n = 400
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": list(range(1, n + 1)),
            "week": [1] * n,
            "home_team": ["A"] * n,
            "away_team": ["B"] * n,
            "neutral": [0] * n,
            "result": [None] * n,
        }
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(1))
    res = out["games"]["result"]
    assert res.null_count() == 0
    assert res.mean() > 15.0  # A ~25pt favorite -> strongly positive home margin
    assert (res > 0).mean() > 0.85


def test_only_target_week_is_filled() -> None:
    """Games in other weeks keep their null result; teams pass through unchanged."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.2, -0.2]})
    cr = make_ratings_compute_results(ratings)
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": [1, 1],
            "week": [1, 2],
            "home_team": ["A", "A"],
            "away_team": ["B", "B"],
            "neutral": [0, 0],
            "result": [None, None],
        }
    )
    out = cr(teams, games, 1, rng=np.random.default_rng(0))
    g = out["games"]
    assert g.filter(pl.col("week") == 1)["result"].item() is not None
    assert g.filter(pl.col("week") == 2)["result"].item() is None
    assert out["teams"].equals(teams)


def test_neutral_site_drops_home_field() -> None:
    """Equal teams: a home game favors home; a neutral one is a coin flip (mean ~0)."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.0, 0.0]})
    cr = make_ratings_compute_results(ratings)
    n = 600
    base = {
        "sim": list(range(1, n + 1)),
        "week": [1] * n,
        "home_team": ["A"] * n,
        "away_team": ["B"] * n,
        "result": [None] * n,
    }
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    home = cr(teams, pl.DataFrame({**base, "neutral": [0] * n}), 1, rng=np.random.default_rng(2))
    neut = cr(teams, pl.DataFrame({**base, "neutral": [1] * n}), 1, rng=np.random.default_rng(2))
    assert home["games"]["result"].mean() > neut["games"]["result"].mean()
    assert abs(neut["games"]["result"].mean()) < 2.0


def test_postseason_games_never_tie() -> None:
    """A POST game re-breaks a sampled 0 (single-elim can't tie)."""
    ratings = pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.0, 0.0]})
    cr = make_ratings_compute_results(ratings)
    n = 500
    teams = pl.DataFrame({"sim": [1], "team": ["A"], "conference": ["X"]})
    games = pl.DataFrame(
        {
            "sim": list(range(1, n + 1)),
            "week": [15] * n,
            "game_type": ["POST"] * n,
            "home_team": ["A"] * n,
            "away_team": ["B"] * n,
            "neutral": [1] * n,
            "result": [None] * n,
        }
    )
    out = cr(teams, games, 15, rng=np.random.default_rng(3))
    assert (out["games"]["result"] == 0).sum() == 0
