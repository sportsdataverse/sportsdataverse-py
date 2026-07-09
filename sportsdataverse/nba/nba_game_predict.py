"""Closed-form pregame game predictions (margin / win prob / total) -- models ②③.

Phase 2 of the NBA/WNBA/G-League prediction & market stack: turns the
opponent-adjusted team ratings (:func:`~sportsdataverse.nba.nba_team_ratings.nba_team_ratings`)
into per-game expected margin, home win probability, and expected total via
the standard efficiency-margin closed forms. All league-specific numbers
(home-court advantage, margin sigma, pace/rating anchors) come from
:func:`sportsdataverse.nba.nba_prediction_constants.get_constants`, so the
functions are league-agnostic (``league_id="00"``/``"10"``/``"20"``).

*(T7.2-shared)*: :func:`expected_possessions`, :func:`predict_margin`,
:func:`win_prob_from_margin`, and :func:`predict_total` are the identical
closed forms used by the MBB (``mbb_game_predict``) and CFB
(``cfb_game_predict``) sibling spines -- a prime candidate for the future
cross-league infra factor-out (T7.2). Pregame win probability is the
closed-form co-product of the spread model, not a separate trained model
(mirrors the MBB/CFB design decision).
"""

from __future__ import annotations

from typing import Literal, Union, overload

import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "expected_possessions",
    "nba_predict_games",
    "predict_margin",
    "predict_total",
    "win_prob_from_margin",
]


def expected_possessions(home_pace: float, away_pace: float, *, league_id: str = "00") -> float:
    """Expected possessions for a matchup (Pythagorean-tempo blend).

    Args:
        home_pace: Home team's adjusted pace (possessions/game).
        away_pace: Away team's adjusted pace.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League -- selects
            the league's baseline pace.

    Returns:
        Expected possessions for the matchup.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import expected_possessions
            expected_possessions(100.0, 98.0)
    """
    return home_pace * away_pace / get_constants(league_id).avg_pace


def predict_margin(
    home_net: float,
    away_net: float,
    *,
    home_pace: float,
    away_pace: float,
    neutral: bool = False,
    league_id: str = "00",
) -> float:
    """Expected home-minus-away margin from two adjusted net ratings.

    The AdjNet difference (points/100 possessions) is scaled by the
    matchup's :func:`expected_possessions` before the home-court advantage
    is added.

    Args:
        home_net: Home team's adjusted net rating (``adj_net_rtg``).
        away_net: Away team's adjusted net rating.
        home_pace: Home team's adjusted pace.
        away_pace: Away team's adjusted pace.
        neutral: True for a neutral-site game (no home-court advantage).
        league_id: ``"00"``/``"10"``/``"20"`` -- selects the fitted HFA.

    Returns:
        Expected margin in points (positive favors the home team).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import predict_margin
            predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
    """
    c = get_constants(league_id)
    poss = expected_possessions(home_pace, away_pace, league_id=league_id)
    hfa = 0.0 if neutral else c.hfa
    return float((home_net - away_net) * poss / 100.0 + hfa)


def win_prob_from_margin(exp_margin: float, *, league_id: str = "00") -> float:
    """Home win probability from an expected margin (normal-CDF closed form).

    Args:
        exp_margin: Expected home-minus-away margin in points.
        league_id: ``"00"``/``"10"``/``"20"`` -- selects the fitted margin sigma.

    Returns:
        Probability the home team wins, in ``(0, 1)``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
            win_prob_from_margin(5.0)
    """
    return float(norm.cdf(exp_margin / get_constants(league_id).margin_sd))


def predict_total(
    home_off: float,
    home_def: float,
    away_off: float,
    away_def: float,
    home_pace: float,
    away_pace: float,
    *,
    league_id: str = "00",
) -> float:
    """Expected total points from adjusted ratings and paces.

    Expected possessions come from :func:`expected_possessions`; each side's
    expected points per 100 possessions blend its offense with the
    opponent's defense (``0.5 * (off + opp_def)``).

    Args:
        home_off: Home adjusted offensive rating (points/100 poss).
        home_def: Home adjusted defensive rating.
        away_off: Away adjusted offensive rating.
        away_def: Away adjusted defensive rating.
        home_pace: Home team's adjusted pace.
        away_pace: Away team's adjusted pace.
        league_id: ``"00"``/``"10"``/``"20"`` -- selects the pace anchor.

    Returns:
        Expected combined points scored by both teams.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import predict_total
            predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0)
    """
    poss = expected_possessions(home_pace, away_pace, league_id=league_id)
    home_pts = 0.5 * (home_off + away_def) * poss / 100.0
    away_pts = 0.5 * (away_off + home_def) * poss / 100.0
    return float(home_pts + away_pts)


@overload
def nba_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Vectorized pregame predictions for a schedule of games.

    Joins the ratings frame twice (home/away) and applies the closed-form
    :func:`predict_margin` / :func:`win_prob_from_margin` / :func:`predict_total`
    math column-wise.

    Args:
        games: One row per game with ``game_id``, ``home_team_id``,
            ``away_team_id`` and optionally ``neutral_site`` (missing column
            means every game is a true home game). Team-id dtypes must match
            ``ratings['team_id']`` exactly.
        ratings: One row per team with ``team_id, adj_off_rtg, adj_def_rtg,
            adj_net_rtg, adj_pace`` (the :func:`~sportsdataverse.nba.nba_team_ratings.nba_team_ratings`
            output for one season/as-of date).
        league_id: ``"00"``/``"10"``/``"20"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per input game: ``game_id, home_team_id, away_team_id,
        exp_margin, home_win_prob, exp_total``. Games whose teams are
        missing from ``ratings`` carry nulls.

    Raises:
        ValueError: If a join-key dtype mismatches ``ratings['team_id']``, or
            if ``ratings`` has duplicate ``team_id`` rows (e.g. multiple
            seasons passed at once).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import nba_predict_games
            from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
            preds = nba_predict_games(games, nba_team_ratings(2024))
    """
    for key in ("home_team_id", "away_team_id"):
        if games.schema[key] != ratings.schema["team_id"]:
            raise ValueError(
                f"join-key dtype mismatch: games[{key!r}] is {games.schema[key]} "
                f"but ratings['team_id'] is {ratings.schema['team_id']}"
            )
    if ratings.get_column("team_id").n_unique() != ratings.height:
        raise ValueError("ratings must have one row per team_id (pass a single season/as-of frame)")

    c = get_constants(league_id)
    out = games if "neutral_site" in games.columns else games.with_columns(pl.lit(False).alias("neutral_site"))
    for side in ("home", "away"):
        rat = ratings.select(
            pl.col("team_id").alias(f"{side}_team_id"),
            pl.col("adj_off_rtg").alias(f"{side}_adj_off_rtg"),
            pl.col("adj_def_rtg").alias(f"{side}_adj_def_rtg"),
            pl.col("adj_net_rtg").alias(f"{side}_adj_net_rtg"),
            pl.col("adj_pace").alias(f"{side}_adj_pace"),
        )
        out = out.join(rat, on=f"{side}_team_id", how="left")

    hfa = pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa)  # noqa: E712
    poss = pl.col("home_adj_pace") * pl.col("away_adj_pace") / c.avg_pace
    out = out.with_columns(
        ((pl.col("home_adj_net_rtg") - pl.col("away_adj_net_rtg")) * poss / 100.0 + hfa).alias("exp_margin"),
        (
            (0.5 * (pl.col("home_adj_off_rtg") + pl.col("away_adj_def_rtg"))) * poss / 100.0
            + (0.5 * (pl.col("away_adj_off_rtg") + pl.col("home_adj_def_rtg"))) * poss / 100.0
        ).alias("exp_total"),
    )
    wp = norm.cdf(out.get_column("exp_margin").to_numpy() / c.margin_sd)
    out = out.with_columns(
        pl.when(pl.col("exp_margin").is_not_null())
        .then(pl.Series("home_win_prob", wp, dtype=pl.Float64))
        .otherwise(None)
        .alias("home_win_prob")
    )
    result = out.select("game_id", "home_team_id", "away_team_id", "exp_margin", "home_win_prob", "exp_total")
    return result.to_pandas() if return_as_pandas else result
