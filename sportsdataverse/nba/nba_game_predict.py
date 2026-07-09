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

import json
import math
from functools import lru_cache
from importlib.resources import files
from typing import Any, Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nba.nba_prediction_constants import get_constants

__all__ = [
    "expected_possessions",
    "in_game_features",
    "nba_in_game_win_prob",
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


# --- Model ④: native in-game win probability (trained logistic + bundled artifact) ---
#
# NOTE (gate scope): the plan's concurrent oracle was the stats.nba.com
# ``winprobabilitypbp`` HOME_PCT feed. That endpoint is DEAD (HTTP 500 on every
# correctly-formed request; hoopR's own nba_winprobabilitypbp() is
# lifecycle::deprecate_stop()-ed as of 3.0.0, replaced by nba_playbyplayv3()).
# So gate (b) "MAE vs native winprobabilitypbp" is unobtainable; this model is
# gated ONLY on gate (a) -- per-time-bucket realized-outcome calibration. See
# tests/fixtures/nba_prediction/README.md + the SDD ledger for the retirement note.

_IN_GAME_FEATURES = ["score_diff", "sqrt_sec_left", "pregame_logit", "home_has_ball"]


def in_game_features(pbp: pl.DataFrame, pregame_home_prob: float) -> pl.DataFrame:
    """Per-play in-game win-probability features from a ``load_nba_pbp`` frame.

    Args:
        pbp: Play-by-play frame with ``start_game_seconds_remaining``,
            ``home_score``, ``away_score``, ``team_id`` (event team) and
            ``home_team_id`` (the ``load_nba_pbp`` schema).
        pregame_home_prob: The pregame home win probability (e.g. from
            :func:`win_prob_from_margin`), encoded as a constant logit column.
            Clipped to ``[1e-6, 1 - 1e-6]`` so a saturated CDF (exact 0/1)
            cannot crash the logit.

    Returns:
        One row per input play: ``score_diff`` (home - away), ``sec_left``
        (clipped at 0 -- overtime plays count as 0 seconds left),
        ``sqrt_sec_left``, ``pregame_logit``, ``home_has_ball`` (``Int8``;
        dead-ball / unknown-team plays are 0).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import in_game_features
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            feats = in_game_features(pbp, 0.62)
    """
    p = min(max(pregame_home_prob, 1e-6), 1.0 - 1e-6)  # norm.cdf saturates to exact 0/1 for extreme margins
    logit = math.log(p / (1.0 - p))
    return pbp.select(
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("score_diff"),
        pl.col("start_game_seconds_remaining").cast(pl.Float64).clip(lower_bound=0.0).alias("sec_left"),
        pl.col("start_game_seconds_remaining").cast(pl.Float64).clip(lower_bound=0.0).sqrt().alias("sqrt_sec_left"),
        pl.lit(logit, dtype=pl.Float64).alias("pregame_logit"),
        (pl.col("team_id") == pl.col("home_team_id")).fill_null(False).cast(pl.Int8).alias("home_has_ball"),
    )


@lru_cache(maxsize=None)
def _load_in_game_artifact(league_id: str = "00") -> dict[str, Any]:
    """Bundled per-``league_id`` in-game-WP artifact (no first-use download).

    Detects the model kind by the ``in_game_wp_artifact`` extension in
    LEAGUE_CONSTANTS: ``.ubj`` -> an xgboost booster (the shipped NBA model,
    escalated from the plain logistic which failed the calibration gate);
    ``.json`` -> a plain logistic coefficient block keyed by ``league_id``.
    """
    name = get_constants(league_id).in_game_wp_artifact
    path = files("sportsdataverse.nba") / "models" / name
    if name.endswith(".json"):
        return {"kind": "logistic", **json.loads(path.read_text(encoding="utf-8"))[league_id]}
    try:
        import xgboost as xgb  # noqa: PLC0415 -- optional dep, only the tree artifact needs it
    except ImportError as exc:  # pragma: no cover - exercised only when xgboost is absent
        raise ImportError(
            "nba_in_game_win_prob needs xgboost for the bundled tree artifact; "
            "install via 'pip install sportsdataverse[models]' or 'pip install xgboost'"
        ) from exc
    booster = xgb.Booster()
    booster.load_model(bytearray(path.read_bytes()))
    return {"kind": "xgboost", "booster": booster, "features": booster.feature_names}


@overload
def nba_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league_id: str = "00",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-play home win probability from the bundled in-game model.

    Scores :func:`in_game_features` through the committed artifact
    (``sportsdataverse/nba/models/nba_in_game_wp.ubj`` for NBA -- a shallow
    xgboost booster, trained on 2022-23 so the 2023-24 calibration backtest
    stays out-of-sample; escalated from a plain logistic that failed the
    per-bucket calibration gate).

    Gate note: the plan's concurrent oracle (stats.nba.com
    ``winprobabilitypbp`` HOME_PCT) is a dead endpoint, so this model is
    validated ONLY on realized-outcome calibration, not against a native WP
    feed. See the fixtures README + SDD ledger.

    Args:
        pbp: Play-by-play for ONE game in the ``load_nba_pbp`` schema
            (``start_game_seconds_remaining``, ``home_score``, ``away_score``,
            ``team_id``, ``home_team_id``).
        pregame_home_prob: Pregame home win probability (e.g. from
            :func:`win_prob_from_margin`).
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League (selects
            the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per play: the five feature columns plus ``home_win_prob``.

    Raises:
        ImportError: If the bundled artifact is an xgboost ``.ubj`` and
            ``xgboost`` is not installed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
            from sportsdataverse.nba.nba_loaders import load_nba_pbp
            pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
            wp = nba_in_game_win_prob(pbp, 0.62)
    """
    art = _load_in_game_artifact(league_id)
    feats = in_game_features(pbp, pregame_home_prob)
    X = feats.select(art["features"]).to_numpy()
    if art["kind"] == "xgboost":
        p = art["booster"].inplace_predict(X)
    else:
        z = X @ np.asarray(art["coef"], dtype=float) + float(art["intercept"])
        p = 1.0 / (1.0 + np.exp(-z))
    out = feats.with_columns(pl.Series("home_win_prob", p).cast(pl.Float64))
    return out.to_pandas() if return_as_pandas else out
