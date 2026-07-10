"""NHL/PWHL game market -- closed-form pregame spread/total/win-prob + market
edge (Phase 2), plus the trained in-game win probability (Phase 3).

Model (2) of the T5.3 prediction spine. The pregame trio is a **closed
form** (no trained artifact, fully reproducible): blend each team's own-side
:mod:`nhl_team_ratings` offense with the opponent's defense, split home-ice
symmetrically, and convert the resulting margin to a win probability via
Phi(margin / sigma) -- the same T7.2-shared Phi-margin core the NFL/CFB/MBB
prediction spines use, but with a **deliberately wide, fitted** ``margin_sd``
(hockey is low-event-count / high-variance; see the design spec Sec 3.2(b)).
Because an NHL final margin is never exactly 0 (OT/shootout always decide a
winner), Phi(margin/sigma) is a faithful P(home win) with no tie mass to model.

``market_edge`` is display/analysis only -- the closing puck line is never
fed back into the rating (model ①) or the closed-form margin/total.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_market import nhl_predict_games
        from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

        ratings = nhl_team_ratings(2023)
        preds = nhl_predict_games(games, ratings)
        print(preds.sort("home_win_prob", descending=True).head())

See Also:
    * `nflfastR`_ -- the shared Phi-margin power-rating core this Phase-2 layer mirrors.
    * `nhl-api-py`_ -- companion NHL Python client.

.. _nflfastR: https://www.nflfastr.com
.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import json
import math
from functools import lru_cache
from importlib.resources import files as _resource_files
from typing import Literal, Optional, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nhl.nhl_prediction_constants import get_constants

_PREDICT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "neutral_site": pl.Boolean,
    "exp_margin": pl.Float64,
    "home_win_prob": pl.Float64,
    "exp_total": pl.Float64,
    "market_edge": pl.Float64,
}


def expected_goals(
    adj_xgf_home: float,
    adj_xga_home: float,
    adj_xgf_away: float,
    adj_xga_away: float,
    neutral: bool,
    *,
    league: str = "nhl",
) -> tuple[float, float]:
    """Per-team expected goals, blending own offense with opponent defense.

    Args:
        adj_xgf_home: home team's opponent-adjusted xG-for rate.
        adj_xga_home: home team's opponent-adjusted xG-against rate.
        adj_xgf_away: away team's opponent-adjusted xG-for rate.
        adj_xga_away: away team's opponent-adjusted xG-against rate.
        neutral: whether the game is at a neutral site (drops HFA).
        league: resolves ``hfa`` via :func:`get_constants`.

    Returns:
        A ``(eg_home, eg_away)`` tuple of expected goals.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import expected_goals
            expected_goals(2.8, 2.2, 2.5, 2.4, False)
    """
    hfa = get_constants(league).hfa
    side = 0.0 if neutral else hfa / 2.0
    eg_home = 0.5 * (adj_xgf_home + adj_xga_away) + side
    eg_away = 0.5 * (adj_xgf_away + adj_xga_home) - side
    return eg_home, eg_away


def predict_margin(
    adj_xgf_home: float,
    adj_xga_home: float,
    adj_xgf_away: float,
    adj_xga_away: float,
    neutral: bool,
    *,
    league: str = "nhl",
) -> float:
    """Expected home-minus-away goal margin.

    Args:
        adj_xgf_home: home team's opponent-adjusted xG-for rate.
        adj_xga_home: home team's opponent-adjusted xG-against rate.
        adj_xgf_away: away team's opponent-adjusted xG-for rate.
        adj_xga_away: away team's opponent-adjusted xG-against rate.
        neutral: whether the game is at a neutral site.
        league: resolves ``hfa`` via :func:`get_constants`.

    Returns:
        ``eg_home - eg_away``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import predict_margin
            predict_margin(2.8, 2.2, 2.5, 2.4, False)
    """
    eg_home, eg_away = expected_goals(adj_xgf_home, adj_xga_home, adj_xgf_away, adj_xga_away, neutral, league=league)
    return eg_home - eg_away


def win_prob_from_margin(exp_margin: float, *, league: str = "nhl") -> float:
    """Convert an expected goal margin to a home win probability via Phi(margin/sigma).

    Args:
        exp_margin: expected home-minus-away goal margin.
        league: resolves ``margin_sd`` via :func:`get_constants`.

    Returns:
        ``P(home win)`` in (0, 1).

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import win_prob_from_margin
            win_prob_from_margin(0.35)
    """
    margin_sd = get_constants(league).margin_sd
    return float(norm.cdf(exp_margin / margin_sd))


def predict_total(
    adj_xgf_home: float,
    adj_xga_home: float,
    adj_xgf_away: float,
    adj_xga_away: float,
    neutral: bool,
    *,
    league: str = "nhl",
) -> float:
    """Expected total goals, variance-corrected by the fitted ``total_scale``.

    The raw ``eg_home + eg_away`` sum is built from opponent-adjusted,
    **shrunk** ratings, which systematically compress the total's spread
    below the real-world variance (confirmed at fitting time: the OLS slope
    of realized total on the raw sum is ~1.91, not 1.0). ``total_scale``
    corrects for that: the raw total's deviation from the league-average
    total is stretched by ``total_scale`` before adding back the league mean.

    Args:
        adj_xgf_home: home team's opponent-adjusted xG-for rate.
        adj_xga_home: home team's opponent-adjusted xG-against rate.
        adj_xgf_away: away team's opponent-adjusted xG-for rate.
        adj_xga_away: away team's opponent-adjusted xG-against rate.
        neutral: whether the game is at a neutral site.
        league: resolves ``hfa``/``avg_total_goals``/``total_scale`` via :func:`get_constants`.

    Returns:
        The variance-corrected expected combined goal total for the game.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import predict_total
            predict_total(2.8, 2.2, 2.5, 2.4, False)
    """
    eg_home, eg_away = expected_goals(adj_xgf_home, adj_xga_home, adj_xgf_away, adj_xga_away, neutral, league=league)
    const = get_constants(league)
    raw_total = eg_home + eg_away
    return const.avg_total_goals + const.total_scale * (raw_total - const.avg_total_goals)


@overload
def nhl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = ...,
    odds: Optional[pl.DataFrame] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = ...,
    odds: Optional[pl.DataFrame] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_predict_games(
    games: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "nhl",
    odds: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Vectorized pregame margin/win-prob/total (+ market edge) over a schedule.

    Args:
        games: a schedule-shaped frame with ``game_id``, ``home_team``,
            ``away_team``, ``neutral_site`` (``home_team``/``away_team`` must
            share :func:`nhl_team_ratings`'s ``team`` dtype -- asserted below).
        ratings: the output of :func:`sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`
            (``team``, ``adj_xgf``, ``adj_xga``).
        league: resolves HFA/sigma via :func:`get_constants`.
        odds: optional frame with ``game_id``, ``close_puck_line_home``; when
            supplied, ``market_edge = exp_margin - close_puck_line_home``.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per game.

        |col_name      |type   |
        |:-------------|:------|
        |game_id        |String |
        |home_team      |String |
        |away_team      |String |
        |neutral_site   |Boolean|
        |exp_margin     |Float64|
        |home_win_prob  |Float64|
        |exp_total      |Float64|
        |market_edge    |Float64|

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import nhl_predict_games
            preds = nhl_predict_games(games, ratings)
            print(preds.sort("home_win_prob", descending=True).head())
    """
    if games.is_empty() or ratings.is_empty():
        return _empty_predictions(return_as_pandas)

    assert games.schema["home_team"] == ratings.schema["team"], (
        f"home_team dtype {games.schema['home_team']} != ratings team dtype {ratings.schema['team']}"
    )
    assert games.schema["away_team"] == ratings.schema["team"], (
        f"away_team dtype {games.schema['away_team']} != ratings team dtype {ratings.schema['team']}"
    )

    const = get_constants(league)
    home_ratings = ratings.select(
        pl.col("team").alias("home_team"),
        pl.col("adj_xgf").alias("h_adj_xgf"),
        pl.col("adj_xga").alias("h_adj_xga"),
    )
    away_ratings = ratings.select(
        pl.col("team").alias("away_team"),
        pl.col("adj_xgf").alias("a_adj_xgf"),
        pl.col("adj_xga").alias("a_adj_xga"),
    )
    joined = games.join(home_ratings, on="home_team", how="left").join(away_ratings, on="away_team", how="left")

    side = pl.when(pl.col("neutral_site") == True).then(0.0).otherwise(const.hfa / 2.0)  # noqa: E712
    eg_home = 0.5 * (pl.col("h_adj_xgf") + pl.col("a_adj_xga")) + side
    eg_away = 0.5 * (pl.col("a_adj_xgf") + pl.col("h_adj_xga")) - side
    exp_margin = eg_home - eg_away
    raw_total = eg_home + eg_away
    # total_scale variance-corrects the shrinkage-compressed raw sum -- see predict_total.
    exp_total = const.avg_total_goals + const.total_scale * (raw_total - const.avg_total_goals)

    out = joined.with_columns(exp_margin.alias("exp_margin"), exp_total.alias("exp_total"))
    # Vectorized win prob: win_prob_from_margin is just norm.cdf(m/sigma), which
    # scipy computes over the whole column at once -- no per-row UDF.
    out = out.with_columns(pl.Series("home_win_prob", norm.cdf(out["exp_margin"].to_numpy() / const.margin_sd)))

    if odds is not None and not odds.is_empty():
        assert out.schema["game_id"] == odds.schema["game_id"], (
            f"game_id dtype {out.schema['game_id']} != odds game_id dtype {odds.schema['game_id']}"
        )
        out = out.join(odds.select("game_id", "close_puck_line_home"), on="game_id", how="left")
        out = out.with_columns((pl.col("exp_margin") - pl.col("close_puck_line_home")).alias("market_edge"))
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("market_edge"))

    out = out.select(
        "game_id", "home_team", "away_team", "neutral_site", "exp_margin", "home_win_prob", "exp_total", "market_edge"
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _empty_predictions(return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    out = pl.DataFrame(schema=_PREDICT_SCHEMA)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


_IN_GAME_FEATURES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "score_diff": pl.Int32,
    "sec_remaining": pl.Float64,
    "sqrt_sec_remaining": pl.Float64,
    "strength_diff": pl.Int32,
    "home_goalie_pulled": pl.Int8,
    "away_goalie_pulled": pl.Int8,
    "pregame_logit": pl.Float64,
}


def in_game_features(pbp: pl.DataFrame, pregame_home_prob: float) -> pl.DataFrame:
    """Per-play in-game win-probability features from game state.

    Reads the ``load_nhl_pbp_full`` schema (``home_score``/``away_score``,
    ``game_seconds_remaining``, ``home_skaters``/``away_skaters``,
    ``home_goalie_in``/``away_goalie_in``). A live feed can populate the same
    five features via a documented column map from
    :func:`sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_pbp`
    (``homeScore``/``awayScore``/``timeRemaining``/``situationCode``).

    Args:
        pbp: a play-by-play frame shaped like ``load_nhl_pbp_full``.
        pregame_home_prob: the model (2) pregame home win probability
            (e.g. from :func:`win_prob_from_margin`), converted to a logit
            and carried as a constant per-play anchor feature.

    Returns:
        A polars DataFrame, one row per play.

        |col_name           |type   |
        |:-------------------|:------|
        |score_diff          |Int32  |
        |sec_remaining       |Float64|
        |sqrt_sec_remaining  |Float64|
        |strength_diff       |Int32  |
        |home_goalie_pulled  |Int8   |
        |away_goalie_pulled  |Int8   |
        |pregame_logit       |Float64|

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import in_game_features, win_prob_from_margin
            pregame_p = win_prob_from_margin(0.3)
            feats = in_game_features(pbp, pregame_home_prob=pregame_p)
    """
    if pbp.is_empty():
        return pl.DataFrame(schema=_IN_GAME_FEATURES_SCHEMA)

    logit = _log_odds(pregame_home_prob)
    return pbp.select(
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Int32).alias("score_diff"),
        pl.col("game_seconds_remaining").cast(pl.Float64).alias("sec_remaining"),
        pl.col("game_seconds_remaining").cast(pl.Float64).sqrt().alias("sqrt_sec_remaining"),
        (pl.col("home_skaters") - pl.col("away_skaters")).cast(pl.Int32).alias("strength_diff"),
        # Null goalie_in (a small pbp coverage gap, not a real empty-net state)
        # defaults to "goalie in net" (1) so it never masquerades as a pull.
        (pl.col("home_goalie_in").fill_null(1) == 0).cast(pl.Int8).alias("home_goalie_pulled"),
        (pl.col("away_goalie_in").fill_null(1) == 0).cast(pl.Int8).alias("away_goalie_pulled"),
        pl.lit(logit).alias("pregame_logit"),
    )


def _log_odds(p: float) -> float:
    """``log(p / (1 - p))`` -- the logit transform used to anchor the in-game model."""
    return math.log(p / (1 - p))


@lru_cache(maxsize=8)
def _load_in_game_wp_artifact(filename: str) -> dict:
    """Load the bundled in-game-WP logistic coefficients (cached, no first-use download).

    Resolves ``sportsdataverse/nhl/models/<filename>`` via
    :func:`importlib.resources.files` (never ``pkg_resources``), mirroring
    the ``cfb/models`` / ``nfl/models`` resource-loading convention already
    used elsewhere in the package.
    """
    path = _resource_files("sportsdataverse").joinpath(f"nhl/models/{filename}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


@overload
def nhl_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nhl_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nhl_in_game_win_prob(
    pbp: pl.DataFrame,
    pregame_home_prob: float,
    *,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-play live home win probability from the bundled in-game logistic.

    Builds :func:`in_game_features` from ``pbp``, scores them with the
    committed logistic (``sportsdataverse/nhl/models/<league>_in_game_wp.json``
    -- no first-use download; the coefficients are trained offline by
    ``dev/nhl_prediction/train_in_game_wp.py`` and committed), and returns
    ``sigmoid(coef . features + intercept)``.

    Args:
        pbp: a play-by-play frame shaped like ``load_nhl_pbp_full``.
        pregame_home_prob: the model (2) pregame home win probability anchor.
        league: resolves the bundled artifact filename via :func:`get_constants`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per play, with a single
        ``home_win_prob: Float64`` column.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_market import nhl_in_game_win_prob, win_prob_from_margin

            pregame_p = win_prob_from_margin(0.3)
            wp = nhl_in_game_win_prob(pbp, pregame_home_prob=pregame_p)
            print(wp.tail())
    """
    if pbp.is_empty():
        out = pl.DataFrame(schema={"home_win_prob": pl.Float64})
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    artifact_name = get_constants(league).in_game_wp_artifact
    artifact = _load_in_game_wp_artifact(artifact_name)
    feats = in_game_features(pbp, pregame_home_prob=pregame_home_prob)
    X = feats.select(artifact["features"]).to_numpy().astype(float)
    coef = np.asarray(artifact["coef"], dtype=float)
    logits = X @ coef + float(artifact["intercept"])
    probs = 1.0 / (1.0 + np.exp(-logits))

    out = pl.DataFrame({"home_win_prob": probs})
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
