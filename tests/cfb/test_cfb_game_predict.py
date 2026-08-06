"""Closed-form pregame predictor tests (T2.1 Task 2.1).

Every assertion is hand-computable from the seeded ``CFB_CONSTANTS["modern"]``
so the math is pinned independently of any fitted values.
"""

from __future__ import annotations

import math
import warnings

import numpy as np
import polars as pl
import pytest
from scipy.stats import norm

from sportsdataverse.cfb.cfb_game_predict import (
    assert_rating_scale,
    cfb_predict_games,
    predict_margin,
    predict_total,
    slope_for_games,
    win_prob_from_margin,
)
from sportsdataverse.cfb.cfb_prediction_constants import get_constants

_C = get_constants("modern")


def _ratings_frame() -> pl.DataFrame:
    """Three teams with hand-picked adjusted ratings + pace (team_id as Utf8)."""
    return pl.DataFrame(
        {
            "team_id": ["1", "2", "3"],
            "adj_net": [0.30, 0.10, -0.20],
            "adj_off_epa": [0.20, 0.05, -0.10],
            "adj_def_epa": [-0.10, 0.00, 0.15],
            "off_pace": [70.0, 66.0, 60.0],
        }
    )


def _schedule_frame() -> pl.DataFrame:
    """Three games, the last on a neutral field (team_id keys as Utf8)."""
    return pl.DataFrame(
        {
            "game_id": [101, 102, 103],
            "home_team_id": ["1", "2", "1"],
            "away_team_id": ["2", "3", "3"],
            "neutral_site": [False, False, True],
        }
    )


def test_predict_margin_neutral_carries_no_hfa() -> None:
    """On a neutral field the margin is the scaled rating differential, no HFA."""
    assert predict_margin(0.30, 0.10, neutral=True) == _C.net_points_scale * (0.30 - 0.10)


def test_predict_margin_home_adds_hfa() -> None:
    """A home game adds HFA in POINTS, outside the rating scale.

    The previous form multiplied an EPA-scale ``2 * hfa_epa`` by
    ``net_points_scale``, tying the two together so a change to either silently
    moved the home-field advantage. Adding points to points keeps them
    independent.
    """
    expected = _C.net_points_scale * (0.30 - 0.10) + _C.hfa_points
    assert predict_margin(0.30, 0.10, neutral=False) == expected


def test_home_minus_neutral_is_exactly_hfa_points() -> None:
    """The semantic invariant, independent of any fitted value.

    Asserts the RELATIONSHIP rather than an arithmetic restatement, so a refit
    of the constants cannot break it -- only a change in meaning can.
    """
    home = predict_margin(0.30, 0.10, neutral=False)
    neutral = predict_margin(0.30, 0.10, neutral=True)
    assert home - neutral == pytest.approx(_C.hfa_points)


def test_slope_curve_is_monotone_in_games_played() -> None:
    """More games behind a rating -> a larger multiplier.

    Errors-in-variables: a noisier predictor attenuates the OLS slope toward
    zero, so an as-of rating built on two games must be trusted less than one
    built on twelve. A non-monotone curve would mean the fit found noise.
    """
    slopes = [slope_for_games(g) for g in (1, 4, 6, 9)]
    assert slopes == sorted(slopes)
    assert slopes[0] < slopes[-1] / 2  # the early-season discount is substantial


def test_slope_without_games_played_is_the_flat_scale() -> None:
    """Omitting games_played falls back to the average slope, not to zero."""
    assert slope_for_games(None) == _C.net_points_scale


def test_rating_scale_gate_passes_on_fitted_scale_and_fires_on_a_rescale() -> None:
    """The gate must be silent on correct data and loud on drifted data.

    A gate that fires on correct input is worse than none: it trains readers to
    ignore it. Both directions are asserted for that reason.
    """
    rng = np.random.default_rng(0)
    ok = pl.DataFrame({"adj_net": rng.normal(0, 0.2646, 400)})
    drifted = pl.DataFrame({"adj_net": rng.normal(0, 0.2646 * 2.2, 400)})

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning becomes a failure
        assert_rating_scale(ok)

    with pytest.warns(UserWarning, match="adj_net sd"):
        assert_rating_scale(drifted)


def test_win_prob_equal_strength_neutral_is_half() -> None:
    """Equal teams on a neutral field are a coin flip."""
    m = predict_margin(0.20, 0.20, neutral=True)
    assert win_prob_from_margin(m) == 0.5


def test_win_prob_symmetric_home_beats_half() -> None:
    """Equal teams, home field -> win prob = Phi(hfa_points / sigma) > 0.5."""
    m = predict_margin(0.20, 0.20, neutral=False)
    p = win_prob_from_margin(m)
    assert p == float(norm.cdf(_C.hfa_points / _C.margin_sd))
    assert p > 0.5


def test_win_prob_is_monotonic_in_margin() -> None:
    """A bigger expected margin never lowers the win probability."""
    assert win_prob_from_margin(-14.0) < win_prob_from_margin(0.0) < win_prob_from_margin(14.0)


def test_predict_total_offense_beats_defense() -> None:
    """Two strong offenses outscore two strong defenses.

    Strong offense = high ``adj_off_epa``; strong defense = low (very negative)
    ``adj_def_epa`` (fewer EPA allowed). The offensive matchup must yield a
    larger expected total than the defensive one.
    """
    off_heavy = predict_total(0.30, 0.05, 0.30, 0.05, game_pace=66.0)  # offenses elite
    def_heavy = predict_total(-0.05, -0.30, -0.05, -0.30, game_pace=66.0)  # defenses elite
    assert off_heavy > def_heavy


def test_predict_total_is_finite_and_positive() -> None:
    """A league-average matchup produces a sane, positive total."""
    t = predict_total(0.0, 0.0, 0.0, 0.0, game_pace=66.0)
    assert math.isfinite(t)
    assert t > 0.0


def test_predict_total_rises_with_pace() -> None:
    """A faster game (more expected plays) yields a higher total, ratings equal."""
    slow = predict_total(0.1, 0.0, 0.1, 0.0, game_pace=55.0)
    fast = predict_total(0.1, 0.0, 0.1, 0.0, game_pace=80.0)
    assert fast > slow


def test_cfb_predict_games_matches_scalars() -> None:
    """Every vectorized row equals the scalar predictors on the same inputs."""
    ratings = _ratings_frame()
    games = _schedule_frame()
    out = cfb_predict_games(games, ratings)

    assert out.columns == [
        "game_id",
        "home_team_id",
        "away_team_id",
        "neutral_site",
        "exp_margin",
        "home_win_prob",
        "exp_total",
    ]
    assert out.height == 3

    by_team = {r["team_id"]: r for r in ratings.iter_rows(named=True)}
    league_avg = ratings["off_pace"].mean()  # cfb_predict_games derives the pace factor from this
    for row in out.iter_rows(named=True):
        h, a = by_team[row["home_team_id"]], by_team[row["away_team_id"]]
        neutral = row["neutral_site"]
        game_pace = h["off_pace"] * a["off_pace"] / league_avg
        exp_m = predict_margin(h["adj_net"], a["adj_net"], neutral=neutral)
        exp_t = predict_total(
            h["adj_off_epa"], h["adj_def_epa"], a["adj_off_epa"], a["adj_def_epa"], game_pace=game_pace
        )
        assert row["exp_margin"] == pytest.approx(exp_m)
        assert row["home_win_prob"] == pytest.approx(win_prob_from_margin(exp_m))
        assert row["exp_total"] == pytest.approx(exp_t)


def test_cfb_predict_games_neutral_row_drops_hfa() -> None:
    """The neutral game's margin is exactly the rating differential (no HFA)."""
    out = cfb_predict_games(_schedule_frame(), _ratings_frame())
    neutral = out.filter(pl.col("neutral_site") == True)  # noqa: E712
    assert neutral.height == 1
    # game 103: home team "1" (adj_net 0.30) vs away "3" (adj_net -0.20), no HFA
    assert neutral["exp_margin"][0] == pytest.approx(_C.net_points_scale * (0.30 - (-0.20)))


def test_cfb_predict_games_dtype_mismatch_raises() -> None:
    """A join-key dtype mismatch trips the guard instead of silently missing."""
    games = _schedule_frame().with_columns(pl.col("home_team_id").cast(pl.Int64))
    with pytest.raises(AssertionError):
        cfb_predict_games(games, _ratings_frame())


def test_cfb_predict_games_return_as_pandas() -> None:
    """``return_as_pandas=True`` yields a pandas frame with the same columns."""
    import pandas as pd

    out = cfb_predict_games(_schedule_frame(), _ratings_frame(), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)
    assert list(out.columns)[:4] == ["game_id", "home_team_id", "away_team_id", "neutral_site"]
