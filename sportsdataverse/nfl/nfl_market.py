"""Closed-form pregame spread / total / win-prob + market edge (model 2 of T4.2).

Reads the native ratings from :mod:`sportsdataverse.nfl.nfl_ratings` and the
fitted era constants from :mod:`sportsdataverse.nfl.nfl_prediction_constants`.
No bundled artifact and no market input: the predictions are pure functions of
the native ratings. ``market_edge`` (native minus market) is a **display
output** derived from a caller-supplied odds frame -- odds never feed
``exp_margin`` / ``exp_total`` / ``home_win_prob``.
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nfl.nfl_prediction_constants import get_constants

__all__ = ["nfl_predict_games", "predict_margin", "predict_total", "win_prob_from_margin"]


def predict_margin(home_adj_net: float, away_adj_net: float, neutral: bool, *, era: str = "modern") -> float:
    """Expected home scoring margin from two net ratings.

    ``points_per_net * (home_adj_net - away_adj_net)`` plus the era HFA on
    non-neutral fields.

    Args:
        home_adj_net: Home team's ``adj_net`` (EPA/play units).
        away_adj_net: Away team's ``adj_net``.
        neutral: True drops the home-field advantage.
        era: Constants era key (default ``"modern"``).

    Returns:
        Expected home margin in points (positive = home favored).

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import predict_margin
            predict_margin(0.10, -0.05, False)
    """
    cfg = get_constants(era)
    return cfg.points_per_net * (home_adj_net - away_adj_net) + (0.0 if neutral else cfg.hfa)


def win_prob_from_margin(exp_margin: float, *, era: str = "modern") -> float:
    """Home win probability from an expected margin (Gaussian margin model).

    Args:
        exp_margin: Expected home margin in points.
        era: Constants era key (supplies ``margin_sd``).

    Returns:
        ``Phi(exp_margin / margin_sd)`` in ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import win_prob_from_margin
            win_prob_from_margin(3.0)
    """
    cfg = get_constants(era)
    return float(norm.cdf(exp_margin / cfg.margin_sd))


def predict_total(
    home_adj_off: float,
    home_adj_def: float,
    away_adj_off: float,
    away_adj_def: float,
    *,
    era: str = "modern",
) -> float:
    """Expected combined point total from the four efficiency components.

    ``avg_total + total_scale * (home_adj_off + away_adj_def + away_adj_off +
    home_adj_def)``. The four ratings are **summed** because each side's
    scoring rises with its own offense and with the opponent's EPA-*allowed*
    (``adj_def`` is lower = better defense) -- same semantics as the shipped
    CFB analog. (The plan text wrote this with a minus; that sign flips a
    good defense into raising the total, so the analog's sum is used.)

    Args:
        home_adj_off: Home ``adj_off_epa``.
        home_adj_def: Home ``adj_def_epa`` (lower = better defense).
        away_adj_off: Away ``adj_off_epa``.
        away_adj_def: Away ``adj_def_epa``.
        era: Constants era key.

    Returns:
        Expected combined total in points.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_market import predict_total
            predict_total(0.10, -0.02, 0.05, 0.01)
    """
    cfg = get_constants(era)
    matchup_sum = home_adj_off + away_adj_def + away_adj_off + home_adj_def
    return cfg.avg_total + cfg.total_scale * matchup_sum


_PREDICT_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "home_team_id": pl.Utf8,
    "away_team_id": pl.Utf8,
    "neutral_site": pl.Boolean,
    "exp_margin": pl.Float64,
    "home_win_prob": pl.Float64,
    "exp_total": pl.Float64,
    "market_edge": pl.Float64,
}


@overload
def nfl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = ...,
    odds: pl.DataFrame | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def nfl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = ...,
    odds: pl.DataFrame | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def nfl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    era: str = "modern",
    odds: pl.DataFrame | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Vectorized pregame predictions (+ display-only market edge) per game.

    Joins ``ratings`` twice (home/away) onto the schedule and computes the
    three closed-form predictions. ``odds`` is **display-only**: it feeds
    ``market_edge = exp_margin - close_spread_home`` and never the
    predictions themselves (the binding non-market boundary).

    Args:
        games: One row per game: ``game_id`` (Utf8), ``home_team_id`` /
            ``away_team_id`` (Utf8 team abbreviations), ``neutral_site``
            (Boolean).
        ratings: The :func:`sportsdataverse.nfl.nfl_ratings.nfl_ratings`
            output (needs ``team_id``, ``adj_off_epa``, ``adj_def_epa``,
            ``adj_net``).
        era: Constants era key.
        odds: Optional market frame (``game_id``, ``close_spread_home`` --
            the market's expected home margin, positive = home favored).
            Games absent from ``odds`` get a null ``market_edge``.
        return_as_pandas: If True, returns a pandas DataFrame.

    Returns:
        One row per input game: ``game_id`` / ``home_team_id`` /
        ``away_team_id`` (Utf8), ``neutral_site`` (Boolean), ``exp_margin`` /
        ``home_win_prob`` / ``exp_total`` / ``market_edge`` (Float64;
        ``market_edge`` null without odds). Zero-row, correctly-typed on
        empty input.

    Raises:
        AssertionError: If a join-key dtype disagrees (``home_team_id`` /
            ``away_team_id`` vs ``ratings.team_id``, or ``game_id`` vs
            ``odds.game_id``).

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_ratings
            from sportsdataverse.nfl.nfl_market import nfl_predict_games
            ratings = nfl_ratings(2023)
            preds = nfl_predict_games(games, ratings)
            preds.sort("home_win_prob", descending=True).head()

        With a market edge (display only)::

            preds = nfl_predict_games(games, ratings, odds=odds)
    """
    cfg = get_constants(era)
    if games.height == 0:
        empty = pl.DataFrame(schema=_PREDICT_OUTPUT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    games = games.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("neutral_site").cast(pl.Boolean),
    )
    assert games.schema["home_team_id"] == ratings.schema["team_id"]
    assert games.schema["away_team_id"] == ratings.schema["team_id"]

    rating_cols = ratings.select("team_id", "adj_off_epa", "adj_def_epa", "adj_net")
    out = (
        games.join(
            rating_cols.rename({"adj_off_epa": "home_off", "adj_def_epa": "home_def", "adj_net": "home_net"}),
            left_on="home_team_id",
            right_on="team_id",
            how="left",
        )
        .join(
            rating_cols.rename({"adj_off_epa": "away_off", "adj_def_epa": "away_def", "adj_net": "away_net"}),
            left_on="away_team_id",
            right_on="team_id",
            how="left",
        )
        .with_columns(
            exp_margin=(
                cfg.points_per_net * (pl.col("home_net") - pl.col("away_net"))
                + pl.when(pl.col("neutral_site") == True)  # noqa: E712
                .then(pl.lit(0.0))
                .otherwise(pl.lit(cfg.hfa))
            ).cast(pl.Float64),
            exp_total=(
                cfg.avg_total
                + cfg.total_scale * (pl.col("home_off") + pl.col("away_def") + pl.col("away_off") + pl.col("home_def"))
            ).cast(pl.Float64),
        )
    )
    win_prob = norm.cdf(out["exp_margin"].to_numpy() / cfg.margin_sd)
    out = out.with_columns(pl.Series("home_win_prob", win_prob).cast(pl.Float64))

    if odds is not None:
        odds_frame = odds.select(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("close_spread_home").cast(pl.Float64),
        )
        assert out.schema["game_id"] == odds_frame.schema["game_id"]
        out = out.join(odds_frame, on="game_id", how="left").with_columns(
            market_edge=(pl.col("exp_margin") - pl.col("close_spread_home")).cast(pl.Float64)
        )
    else:
        out = out.with_columns(market_edge=pl.lit(None).cast(pl.Float64))

    out = out.select(*_PREDICT_OUTPUT_SCHEMA.keys())
    return out.to_pandas() if return_as_pandas else out
