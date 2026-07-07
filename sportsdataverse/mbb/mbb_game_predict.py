"""Closed-form pregame game predictions (margin / win prob / total).

Phase 2 of the MBB/WBB prediction & tournament stack: turns the
opponent-adjusted team ratings (``mbb_team_ratings``) into per-game expected
margin, home win probability, and expected total via the standard
efficiency-margin closed forms. All league-specific numbers (home-court
advantage, margin sigma, tempo/efficiency anchors) come from
:func:`sportsdataverse.mbb.mbb_prediction_constants.get_constants`, so the
functions are league-agnostic (``league="mens"`` / ``"womens"``).
"""

from __future__ import annotations

import json
import math
from functools import lru_cache
from importlib.resources import files
from typing import Any, Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.mbb.mbb_prediction_constants import get_constants

__all__ = [
    "in_game_features",
    "mbb_in_game_win_prob",
    "mbb_predict_games",
    "predict_margin",
    "predict_total",
    "win_prob_from_margin",
]


@lru_cache(maxsize=None)
def _load_wp_artifact(league: str) -> dict[str, Any]:
    """Bundled in-game WP artifact: ``.ubj`` xgboost booster or ``.json`` logistic."""
    name = get_constants(league).in_game_wp_artifact
    path = files("sportsdataverse.mbb") / "models" / name
    if name.endswith(".json"):
        return {"kind": "logistic", **json.loads(path.read_text(encoding="utf-8"))}
    try:
        import xgboost as xgb  # noqa: PLC0415 -- optional dep, only the tree artifact needs it
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "mbb_in_game_win_prob requires xgboost for the bundled tree artifact; "
            "install it via 'pip install sportsdataverse[models]'"
        ) from exc
    booster = xgb.Booster()
    booster.load_model(bytearray(path.read_bytes()))
    return {"kind": "xgboost", "booster": booster, "features": booster.feature_names}


def in_game_features(pbp: pl.DataFrame, pregame_home_prob: float) -> pl.DataFrame:
    """Per-play in-game win-probability features from a ``load_mbb_pbp`` frame.

    Args:
        pbp: Play-by-play frame with ``start_game_seconds_remaining``,
            ``home_score``, ``away_score``, ``team_id`` (event team) and
            ``home_team_id`` (the ``load_mbb_pbp`` schema).
        pregame_home_prob: The pregame home win probability (e.g. from
            :func:`win_prob_from_margin`), encoded as a constant logit column.

    Returns:
        One row per input play: ``score_diff`` (home - away), ``sec_left``
        (clipped at 0 -- overtime plays count as 0 seconds left),
        ``sqrt_sec_left``, ``pregame_logit``, ``home_has_ball`` (``Int8``;
        dead-ball / unknown-team plays are 0).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import in_game_features
            from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
            pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
            feats = in_game_features(pbp, 0.62)
    """
    logit = math.log(pregame_home_prob / (1.0 - pregame_home_prob))
    return pbp.select(
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("score_diff"),
        pl.col("start_game_seconds_remaining").cast(pl.Float64).clip(lower_bound=0.0).alias("sec_left"),
        pl.col("start_game_seconds_remaining").cast(pl.Float64).clip(lower_bound=0.0).sqrt().alias("sqrt_sec_left"),
        pl.lit(logit, dtype=pl.Float64).alias("pregame_logit"),
        (pl.col("team_id") == pl.col("home_team_id")).fill_null(False).cast(pl.Int8).alias("home_has_ball"),
    )


@overload
def mbb_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-play home win probability from the bundled in-game logistic.

    Scores :func:`in_game_features` through the committed artifact
    (``sportsdataverse/mbb/models``, trained on the season before the pregame
    gate season so the calibration backtest stays out-of-sample).

    Args:
        pbp: Play-by-play for ONE game in the ``load_mbb_pbp`` schema
            (``start_game_seconds_remaining``, ``home_score``, ``away_score``,
            ``team_id``, ``home_team_id``).
        pregame_home_prob: Pregame home win probability (e.g. from
            :func:`win_prob_from_margin`).
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per play: the five feature columns plus ``home_win_prob``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
            from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
            pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
            wp = mbb_in_game_win_prob(pbp, 0.62)
    """
    art = _load_wp_artifact(league)
    feats = in_game_features(pbp, pregame_home_prob)
    X = feats.select(art["features"]).to_numpy()
    if art["kind"] == "xgboost":
        p = art["booster"].inplace_predict(X)
    else:
        z = X @ np.asarray(art["coef"], dtype=float) + float(art["intercept"])
        p = 1.0 / (1.0 + np.exp(-z))
    out = feats.with_columns(pl.Series("home_win_prob", p).cast(pl.Float64))
    return out.to_pandas() if return_as_pandas else out


def predict_margin(
    home_adj_em: float,
    away_adj_em: float,
    neutral: bool = False,
    *,
    league: str = "mens",
) -> float:
    """Expected home-minus-away margin from two adjusted efficiency margins.

    The AdjEM difference is scaled by the league's fitted ``em_scale`` (AdjEM
    is per-100-possessions; a game margin scales by roughly tempo/100, further
    attenuated for as-of estimation noise) before the home-court advantage is
    added.

    Args:
        home_adj_em: Home team's adjusted efficiency margin (points / 100 poss).
        away_adj_em: Away team's adjusted efficiency margin.
        neutral: True for a neutral-site game (no home-court advantage).
        league: ``"mens"`` or ``"womens"`` (selects the fitted em_scale / HFA).

    Returns:
        Expected margin in points (positive favors the home team).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import predict_margin
            predict_margin(20.0, 10.0)
    """
    c = get_constants(league)
    hfa = 0.0 if neutral else c.hfa
    return float(c.em_scale * (home_adj_em - away_adj_em) + hfa)


def win_prob_from_margin(exp_margin: float, *, league: str = "mens") -> float:
    """Home win probability from an expected margin (normal-CDF closed form).

    Args:
        exp_margin: Expected home-minus-away margin in points.
        league: ``"mens"`` or ``"womens"`` (selects the fitted margin sigma).

    Returns:
        Probability the home team wins, in ``(0, 1)``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin
            win_prob_from_margin(5.0)
    """
    return float(norm.cdf(exp_margin / get_constants(league).margin_sd))


def predict_total(
    home_adj_o: float,
    home_adj_d: float,
    away_adj_o: float,
    away_adj_d: float,
    home_tempo: float,
    away_tempo: float,
    *,
    league: str = "mens",
) -> float:
    """Expected total points from adjusted efficiencies and tempos.

    Expected possessions are ``home_tempo * away_tempo / avg_tempo``; each
    side's expected points per 100 possessions blend its offense with the
    opponent's defense (``0.5 * (off + opp_def)``).

    Args:
        home_adj_o: Home adjusted offensive efficiency (points / 100 poss).
        home_adj_d: Home adjusted defensive efficiency.
        away_adj_o: Away adjusted offensive efficiency.
        away_adj_d: Away adjusted defensive efficiency.
        home_tempo: Home adjusted tempo (possessions / game).
        away_tempo: Away adjusted tempo.
        league: ``"mens"`` or ``"womens"`` (selects the tempo anchor).

    Returns:
        Expected combined points scored by both teams.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import predict_total
            predict_total(110.0, 95.0, 105.0, 100.0, 68.0, 66.0)
    """
    c = get_constants(league)
    poss = home_tempo * away_tempo / c.avg_tempo
    home_pts = 0.5 * (home_adj_o + away_adj_d) * poss / 100.0
    away_pts = 0.5 * (away_adj_o + home_adj_d) * poss / 100.0
    return float(home_pts + away_pts)


@overload
def mbb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Vectorized pregame predictions for a schedule of games.

    Joins the ratings frame twice (home / away) and applies the closed-form
    :func:`predict_margin` / :func:`win_prob_from_margin` /
    :func:`predict_total` math column-wise.

    Args:
        games: One row per game with ``game_id``, ``home_team_id``,
            ``away_team_id`` and optionally ``neutral_site`` (missing column
            means every game is a true home game). Team-id dtypes must match
            ``ratings['team_id']`` exactly.
        ratings: One row per team with ``team_id, adj_o, adj_d, adj_em,
            adj_tempo`` (the :func:`mbb_team_ratings` output for one season /
            as-of date).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per input game: ``game_id, home_team_id, away_team_id,
        exp_margin, home_win_prob, exp_total``. Games whose teams are missing
        from ``ratings`` carry nulls.

    Raises:
        ValueError: If a join-key dtype mismatches ``ratings['team_id']``, or
            if ``ratings`` has duplicate ``team_id`` rows (e.g. multiple
            seasons passed at once).

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
            from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
            preds = mbb_predict_games(games, mbb_team_ratings([2024]))
    """
    for key in ("home_team_id", "away_team_id"):
        if games.schema[key] != ratings.schema["team_id"]:
            raise ValueError(
                f"join-key dtype mismatch: games[{key!r}] is {games.schema[key]} "
                f"but ratings['team_id'] is {ratings.schema['team_id']}"
            )
    if ratings.get_column("team_id").n_unique() != ratings.height:
        raise ValueError("ratings must have one row per team_id (pass a single season / as-of frame)")

    c = get_constants(league)
    out = games if "neutral_site" in games.columns else games.with_columns(pl.lit(False).alias("neutral_site"))
    for side in ("home", "away"):
        rat = ratings.select(
            pl.col("team_id").alias(f"{side}_team_id"),
            pl.col("adj_o").alias(f"{side}_adj_o"),
            pl.col("adj_d").alias(f"{side}_adj_d"),
            pl.col("adj_em").alias(f"{side}_adj_em"),
            pl.col("adj_tempo").alias(f"{side}_adj_tempo"),
        )
        out = out.join(rat, on=f"{side}_team_id", how="left")

    hfa = pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(c.hfa)  # noqa: E712
    poss = pl.col("home_adj_tempo") * pl.col("away_adj_tempo") / c.avg_tempo
    out = out.with_columns(
        (c.em_scale * (pl.col("home_adj_em") - pl.col("away_adj_em")) + hfa).alias("exp_margin"),
        (
            (0.5 * (pl.col("home_adj_o") + pl.col("away_adj_d")) + 0.5 * (pl.col("away_adj_o") + pl.col("home_adj_d")))
            * poss
            / 100.0
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
