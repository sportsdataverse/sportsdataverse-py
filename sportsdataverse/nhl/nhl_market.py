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
from collections.abc import Callable
from functools import lru_cache, partial
from importlib.resources import files as _resource_files
from typing import Literal, Optional, TypeVar, Union, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.special import gammaln
from scipy.stats import norm

from sportsdataverse.nhl.nhl_prediction_constants import LeagueConstants, get_constants

_PREDICT_SCHEMA: dict[str, pl.PolarsDataType] = {
    # Int64, the dtype the NHL / ESPN schedule parsers give game_id since task 5.2: an empty slate stacks with a
    # full one (a full slate passes the caller's game_id through).
    "game_id": pl.Int64,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "neutral_site": pl.Boolean,
    "exp_margin": pl.Float64,
    "home_win_prob": pl.Float64,
    "exp_total": pl.Float64,
    "market_edge": pl.Float64,
    "home_reg_win_prob": pl.Float64,
    "reg_tie_prob": pl.Float64,
    "away_reg_win_prob": pl.Float64,
    "exp_home_reg_goals": pl.Float64,
    "exp_away_reg_goals": pl.Float64,
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


_Total = TypeVar("_Total", float, pl.Expr)


def _scale_total(raw_total: _Total, const: LeagueConstants) -> _Total:
    """Stretch an even-strength raw total around its league mean onto the all-goals scale.

    Shared by :func:`predict_total` (floats) and :func:`nhl_predict_games` (polars
    expressions) so the two cannot drift apart.
    """
    return const.avg_total_goals + const.total_scale * (raw_total - 2.0 * const.avg_xgf)


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
    corrects for that: the raw total's deviation from its own league mean is
    stretched by ``total_scale`` before adding back the league-average total.
    The ratings are even-strength xG, so that mean is ``2 * avg_xgf``, not
    ``avg_total_goals``: the gap between the two is the power-play,
    short-handed and empty-net scoring the ratings leave out.

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
    return _scale_total(raw_total, const)


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
        A polars (or pandas) DataFrame, one row per game. The five regulation columns come from
        :func:`regulation_goal_means`: the 60-minute score model whose final-score win probability and
        expected total equal ``home_win_prob`` and ``exp_total``.

        |col_name           |type   |
        |:------------------|:------|
        |game_id            |Int64  |
        |home_team          |String |
        |away_team          |String |
        |neutral_site       |Boolean|
        |exp_margin         |Float64|
        |home_win_prob      |Float64|
        |exp_total          |Float64|
        |market_edge        |Float64|
        |home_reg_win_prob  |Float64|
        |reg_tie_prob       |Float64|
        |away_reg_win_prob  |Float64|
        |exp_home_reg_goals |Float64|
        |exp_away_reg_goals |Float64|

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
    exp_total = _scale_total(raw_total, const)

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

    out = _with_regulation_outcomes(out, league).select(list(_PREDICT_SCHEMA))
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def _with_regulation_outcomes(out: pl.DataFrame, league: str) -> pl.DataFrame:
    """Append the regulation 3-way and per-team regulation goal means to a prediction frame.

    Rows whose ``home_win_prob`` or ``exp_total`` is missing or non-finite (a team with no rating) get nulls.
    """
    p = out["home_win_prob"].cast(pl.Float64).fill_null(float("nan")).to_numpy()
    total = out["exp_total"].cast(pl.Float64).fill_null(float("nan")).to_numpy()
    ok = np.isfinite(p) & np.isfinite(total)
    cols = {name: np.full(out.height, np.nan) for name in _REGULATION_COLUMNS}
    if ok.any():
        lam_home, lam_away = regulation_goal_means(p[ok], total[ok], league=league)
        home, tie, away, _ = _grid_summary(_regulation_grid(lam_home, lam_away, get_constants(league)))
        for name, values in zip(_REGULATION_COLUMNS, (home, tie, away, lam_home, lam_away), strict=True):
            cols[name][ok] = values
    return out.with_columns(pl.Series(name, values).fill_nan(None) for name, values in cols.items())


_REGULATION_COLUMNS = (
    "home_reg_win_prob",
    "reg_tie_prob",
    "away_reg_win_prob",
    "exp_home_reg_goals",
    "exp_away_reg_goals",
)

# ponytail: per-side regulation goal cap. P(X > 15) is 3e-5 at the largest regulation mean on the 2023-24 ..
# 2025-26 holdout (4.68) and 7e-5 at 5; the solver works on this truncated, renormalised grid, so the win
# probability and total it matches are exact for the grid. Raise the cap if means above ~5 become common.
_MAX_REG_GOALS = 15
_GOALS = np.arange(_MAX_REG_GOALS + 1)
_LOG_FACT = gammaln(_GOALS + 1.0)
_HOME_AHEAD = np.tril(np.ones((_MAX_REG_GOALS + 1, _MAX_REG_GOALS + 1)), -1)  # [home, away] with home > away
_REG_TOTAL = _GOALS[:, None] + _GOALS[None, :]


def _poisson_pmf(lam: np.ndarray) -> np.ndarray:
    """Poisson pmf at 0..15 goals for each mean, shape ``(n, 16)``."""
    log_lam = np.log(np.maximum(lam, 1e-12))[:, None]
    return np.exp(_GOALS[None, :] * log_lam - lam[:, None] - _LOG_FACT[None, :])


def _regulation_grid(lam_home: np.ndarray, lam_away: np.ndarray, const: LeagueConstants) -> np.ndarray:
    """``P(home = i, away = j)`` after 60 minutes, shape ``(n, 16, 16)``.

    Two independent Poissons with every tied cell scaled by ``1 + reg_draw_inflation``, then renormalised.
    Independent Poissons put ~16% of NHL games level after regulation; ~22% are (2022-23 .. 2025-26).
    """
    grid = _poisson_pmf(lam_home)[:, :, None] * _poisson_pmf(lam_away)[:, None, :]
    grid[:, _GOALS, _GOALS] *= 1.0 + const.reg_draw_inflation
    return grid / grid.sum(axis=(1, 2), keepdims=True)


def _grid_summary(grid: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """``(P(home ahead), P(tied), P(away ahead), E[regulation goals])`` of a regulation grid."""
    home = (grid * _HOME_AHEAD).sum(axis=(1, 2))
    tie = np.einsum("nii->n", grid)
    away = 1.0 - home - tie
    return home, tie, away, (grid * _REG_TOTAL).sum(axis=(1, 2))


def regulation_goal_means(
    home_win_prob: np.ndarray, exp_total: np.ndarray, *, league: str = "nhl"
) -> tuple[np.ndarray, np.ndarray]:
    """Per-team regulation goal means consistent with a game's win probability and expected total.

    The 60-minute score is a draw-inflated pair of Poissons (see ``reg_draw_inflation``). A game level
    after regulation is decided by one more goal (overtime or the shootout winner, both of which add
    exactly one goal to the final score), won by the home side with probability
    ``P(home ahead) / (P(home ahead) + P(away ahead))``. The two means are solved by bisection so that
    the model's final-score home win probability equals ``home_win_prob`` and its expected final total
    equals ``exp_total``. ``exp_total`` is used as given, so the regulation means inherit its centring
    (``avg_total_goals`` / ``total_scale``). A pair the grid cannot reach (an ``exp_total`` below about 1,
    say) gets ``NaN`` means rather than the nearest bracket.

    Args:
        home_win_prob: final-score home win probabilities in (0, 1), e.g. ``nhl_predict_games``'s column.
        exp_total: expected final-score total goals, same length.
        league: resolves ``reg_draw_inflation`` via :func:`get_constants`.

    Returns:
        A ``(home_means, away_means)`` tuple of float arrays.

    Raises:
        ValueError: if ``league`` has no constants row.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_market import regulation_goal_means
            regulation_goal_means(np.array([0.55]), np.array([6.1]))
    """
    const = get_constants(league)
    p = np.clip(np.asarray(home_win_prob, dtype=float), 1e-6, 1 - 1e-6)
    target = np.asarray(exp_total, dtype=float)
    mu, share = np.maximum(target - 0.25, 0.5), np.full(p.shape, 0.5)
    for _ in range(8):  # alternate the two monotone 1-D solves; the coupling is weak, so this converges fast
        share = _bisect(partial(_home_prob_at_share, mu=mu, const=const), p, 1e-4, 1 - 1e-4)
        mu = _bisect(partial(_total_at_mean, share=share, const=const), target, 0.05, 20.0)
    lam_home, lam_away = mu * share, mu * (1 - share)
    reached = (np.abs(_final_home_prob(lam_home, lam_away, const) - p) < 1e-6) & (
        np.abs(_final_total(lam_home, lam_away, const) - target) < 1e-6
    )
    return np.where(reached, lam_home, np.nan), np.where(reached, lam_away, np.nan)


def _home_prob_at_share(share: np.ndarray, mu: np.ndarray, const: LeagueConstants) -> np.ndarray:
    return _final_home_prob(mu * share, mu * (1 - share), const)


def _total_at_mean(mu: np.ndarray, share: np.ndarray, const: LeagueConstants) -> np.ndarray:
    return _final_total(mu * share, mu * (1 - share), const)


def _final_home_prob(lam_home: np.ndarray, lam_away: np.ndarray, const: LeagueConstants) -> np.ndarray:
    """``P(home ahead) + P(tied) * q`` with ``q = P(home ahead) / (P(home ahead) + P(away ahead))``.

    That is ``ahead / (ahead + behind)``, in which the draw inflation and the grid's normalisation cancel, so
    it needs only the two marginal pmfs (``_grid_summary`` of ``_regulation_grid`` gives the same number).
    """
    ph, pa = _poisson_pmf(lam_home), _poisson_pmf(lam_away)
    ahead = (ph * (np.cumsum(pa, axis=1) - pa)).sum(axis=1)
    behind = (pa * (np.cumsum(ph, axis=1) - ph)).sum(axis=1)
    return ahead / (ahead + behind)


def _final_total(lam_home: np.ndarray, lam_away: np.ndarray, const: LeagueConstants) -> np.ndarray:
    """``E[regulation goals] + P(tied)`` of ``_regulation_grid``, from the marginal pmfs."""
    ph, pa = _poisson_pmf(lam_home), _poisson_pmf(lam_away)
    theta = const.reg_draw_inflation
    mass_h, mass_a, tied = ph.sum(axis=1), pa.sum(axis=1), ph * pa
    norm_ = mass_h * mass_a + theta * tied.sum(axis=1)
    goals = (ph @ _GOALS) * mass_a + (pa @ _GOALS) * mass_h + 2.0 * theta * (tied @ _GOALS)
    return (goals + (1.0 + theta) * tied.sum(axis=1)) / norm_


def _bisect(
    f: Callable[[np.ndarray], np.ndarray], target: np.ndarray, lo: float, hi: float, iters: int = 50
) -> np.ndarray:
    """Vectorised bisection for an increasing ``f``: the ``x`` in ``[lo, hi]`` with ``f(x) == target``."""
    lo_a, hi_a = np.full(target.shape, lo), np.full(target.shape, hi)
    for _ in range(iters):
        mid = 0.5 * (lo_a + hi_a)
        below = f(mid) < target
        lo_a, hi_a = np.where(below, mid, lo_a), np.where(below, hi_a, mid)
    return 0.5 * (lo_a + hi_a)


def _final_score_grid(preds: pl.DataFrame, league: str) -> tuple[pl.Series, np.ndarray, np.ndarray]:
    """Final-score grid ``P(home = i, away = j)`` and its beyond-regulation part, shape ``(n, 17, 17)``.

    Rows without a finite ``home_win_prob`` / ``exp_total``, or whose pair the grid cannot reach, are dropped;
    their ``game_id`` is not returned.
    """
    size = _MAX_REG_GOALS + 2
    if preds.is_empty():
        return pl.Series("game_id", [], dtype=pl.Int64), np.zeros((0, size, size)), np.zeros((0, size, size))
    preds = preds.filter(pl.col("home_win_prob").is_finite() & pl.col("exp_total").is_finite())
    const = get_constants(league)
    lam_home, lam_away = regulation_goal_means(
        preds["home_win_prob"].to_numpy(), preds["exp_total"].to_numpy(), league=league
    )
    reached = np.isfinite(lam_home)
    preds, lam_home, lam_away = preds.filter(pl.Series(reached)), lam_home[reached], lam_away[reached]
    reg = _regulation_grid(lam_home, lam_away, const)
    home, _, away, _ = _grid_summary(reg)
    q = (home / (home + away))[:, None]
    ties = np.einsum("nii->ni", reg)
    beyond = np.zeros((preds.height, size, size))
    beyond[:, _GOALS + 1, _GOALS] = q * ties
    beyond[:, _GOALS, _GOALS + 1] = (1.0 - q) * ties
    final = beyond.copy()
    final[:, : size - 1, : size - 1] += reg
    final[:, _GOALS, _GOALS] = 0.0
    return preds["game_id"], final, beyond


def nhl_score_distribution(
    preds: pl.DataFrame,
    *,
    top_n: int = 10,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Most likely final scores per game, from the regulation score model behind ``nhl_predict_games``.

    A final score counts overtime and shootout wins as one goal, as the NHL scores them. Summed over every
    scoreline, the home-win probability equals the row's ``home_win_prob`` and the mean total equals its
    ``exp_total`` (see :func:`regulation_goal_means`).

    Args:
        preds: a frame with ``game_id``, ``home_win_prob`` and ``exp_total``, e.g. :func:`nhl_predict_games`.
            Rows where either value is missing are skipped.
        top_n: how many scorelines to keep per game, most likely first (at least 1).
        league: resolves ``reg_draw_inflation`` via :func:`get_constants`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, ``top_n`` rows per game. ``prob_beyond_regulation`` is the part of
        ``prob`` from games tied after 60 minutes (a one-goal score only). ``game_id`` keeps the input's
        dtype (Int64 from :func:`nhl_predict_games`).

        |col_name               |type   |
        |:----------------------|:------|
        |game_id                |Int64  |
        |rank                   |Int64  |
        |home_goals             |Int64  |
        |away_goals             |Int64  |
        |prob                   |Float64|
        |prob_beyond_regulation |Float64|

    Raises:
        ValueError: if ``top_n`` is below 1 or ``league`` has no constants row.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_market import nhl_score_distribution
            preds = pl.DataFrame({"game_id": [2025020001], "home_win_prob": [0.55], "exp_total": [6.2]})
            nhl_score_distribution(preds, top_n=5)
    """
    if top_n < 1:
        raise ValueError(f"top_n must be at least 1, got {top_n}")
    game_ids, final, beyond = _final_score_grid(preds, league)
    n, size = final.shape[0], final.shape[1]
    flat = final.reshape(n, size * size)
    order = np.argsort(-flat, axis=1, kind="stable")[:, :top_n]
    k = order.shape[1]
    out = pl.DataFrame(
        {
            "game_id": game_ids.gather(np.repeat(np.arange(n), k)),
            "rank": pl.Series(np.tile(np.arange(1, k + 1), n), dtype=pl.Int64),
            "home_goals": pl.Series((order // size).ravel(), dtype=pl.Int64),
            "away_goals": pl.Series((order % size).ravel(), dtype=pl.Int64),
            "prob": pl.Series(np.take_along_axis(flat, order, axis=1).ravel(), dtype=pl.Float64),
            "prob_beyond_regulation": pl.Series(
                np.take_along_axis(beyond.reshape(n, size * size), order, axis=1).ravel(), dtype=pl.Float64
            ),
        }
    )
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


def nhl_total_goals_distribution(
    preds: pl.DataFrame,
    *,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Distribution of the final-score total goals per game, from the same model as :func:`nhl_score_distribution`.

    Args:
        preds: a frame with ``game_id``, ``home_win_prob`` and ``exp_total``, e.g. :func:`nhl_predict_games`.
            Rows where either value is missing are skipped.
        league: resolves ``reg_draw_inflation`` via :func:`get_constants`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per game and total from 0 to 32 goals. ``prob_over`` is
        ``P(total > total_goals)``, the over probability for a line of ``total_goals + 0.5``. Per game the
        probabilities sum to 1 and their mean is the row's ``exp_total``. ``game_id`` keeps the input's dtype.

        |col_name    |type   |
        |:-----------|:------|
        |game_id     |Int64  |
        |total_goals |Int64  |
        |prob        |Float64|
        |prob_over   |Float64|

    Raises:
        ValueError: if ``league`` has no constants row.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_market import nhl_total_goals_distribution
            preds = pl.DataFrame({"game_id": [2025020001], "home_win_prob": [0.55], "exp_total": [6.2]})
            totals = nhl_total_goals_distribution(preds)
            totals.filter(pl.col("total_goals") == 5)  # prob_over == P(over 5.5)
    """
    game_ids, final, _ = _final_score_grid(preds, league)
    n, size = final.shape[0], final.shape[1]
    cell_total = (np.arange(size)[:, None] + np.arange(size)[None, :]).ravel()
    probs = final.reshape(n, size * size) @ np.eye(2 * size - 1)[cell_total]  # sum each total's cells
    width = probs.shape[1]
    out = pl.DataFrame(
        {
            "game_id": game_ids.gather(np.repeat(np.arange(n), width)),
            "total_goals": pl.Series(np.tile(np.arange(width), n), dtype=pl.Int64),
            "prob": pl.Series(probs.ravel(), dtype=pl.Float64),
            "prob_over": pl.Series(np.clip(1.0 - np.cumsum(probs, axis=1), 0.0, 1.0).ravel(), dtype=pl.Float64),
        }
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
    ``tools/models/nhl_prediction/train_in_game_wp.py`` and committed), and returns
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
