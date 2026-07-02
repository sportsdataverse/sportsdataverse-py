"""DARKO-style player projection: per-player Kalman filter + empirical aging curve."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Generator, List, Tuple

import numpy as np
import polars as pl
from scipy.optimize import minimize


@dataclass
class AgingCurve:
    """Empirical aging deltas: ``delta_by_age[a]`` = expected rating change aging a -> a+1."""

    delta_by_age: Dict[int, float] = field(default_factory=dict)

    def delta(self, age: float) -> float:
        """Aging drift for a player of (rounded) ``age``; 0.0 outside the fitted range."""
        return float(self.delta_by_age.get(int(round(age)), 0.0))


def fit_aging_curve(panel: pl.DataFrame, ages: pl.DataFrame, *, smooth: int = 3) -> AgingCurve:
    """Fit the aging curve by the delta method: avg YoY rating change grouped by starting age.

    Args:
        panel: ``player_id``, ``season``, ``rating`` (per-player-season ratings).
        ages: ``player_id``, ``season``, ``age``.
        smooth: Odd window for a centered moving average over ages (1 = no smoothing).

    Returns:
        An ``AgingCurve`` mapping each integer starting age to its mean YoY delta.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_darko import fit_aging_curve

            panel = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "rating": [10.0, 11.0]})
            ages = pl.DataFrame({"player_id": [1, 1], "season": [2020, 2021], "age": [24.0, 25.0]})
            curve = fit_aging_curve(panel, ages, smooth=1)
            print(curve.delta(24))  # ~1.0
    """
    df = panel.join(ages, on=["player_id", "season"], how="inner").sort(["player_id", "season"])
    # consecutive-season pairs per player
    nxt = df.with_columns(
        pl.col("season").shift(-1).over("player_id").alias("season_next"),
        pl.col("rating").shift(-1).over("player_id").alias("rating_next"),
    ).filter(pl.col("season_next") == pl.col("season") + 1)
    nxt = nxt.with_columns(
        (pl.col("rating_next") - pl.col("rating")).alias("delta"),
        pl.col("age").round(0).cast(pl.Int64).alias("age_int"),
    )
    grp = nxt.group_by("age_int").agg(pl.col("delta").mean().alias("mean_delta")).sort("age_int")
    ages_arr = grp["age_int"].to_list()
    deltas = np.array(grp["mean_delta"].to_list(), dtype=np.float64)
    if smooth > 1 and len(deltas) >= smooth:
        kern = np.ones(smooth) / smooth
        deltas = np.convolve(deltas, kern, mode="same")
    return AgingCurve(delta_by_age={int(a): float(d) for a, d in zip(ages_arr, deltas)})


def _kalman_filter(
    ratings: np.ndarray,
    ages: np.ndarray,
    weights: np.ndarray,
    aging_curve: AgingCurve,
    q: float,
    obs_base: float,
    *,
    p0: float = 100.0,
) -> Tuple[float, float, np.ndarray, np.ndarray]:
    """Run the per-player Kalman filter over an ordered season rating series.

    State: latent skill; drift = ``aging_curve.delta(age[t-1])``, process var ``q``;
    observation ``ratings[t]`` with variance ``obs_base / weights[t]``.

    Args:
        ratings: Ordered per-season ratings for one player (shape ``(n,)``).
        ages: Age at the start of each season (shape ``(n,)``).
        weights: Observation weight for each season (shape ``(n,)``); higher = more reliable.
        aging_curve: Fitted ``AgingCurve`` supplying per-age expected drift.
        q: Process (state transition) variance — controls how fast skill can drift.
        obs_base: Base observation variance; effective variance = ``obs_base / weights[t]``.
        p0: Initial state variance (default 100.0 — diffuse prior).

    Returns:
        ``(s_final, P_final, s_preds, innov_vars)`` — end-of-series filtered skill + variance,
        and the one-step-ahead predictions ``s_preds[t]`` (for t>=1) + their innovation
        variances ``innov_vars[t]`` (``P_pred + r_t``), used by the MLE fit + the validator.
        Indices 0 of ``s_preds`` and ``innov_vars`` are ``np.nan`` (no prediction for first obs).
    """
    n = len(ratings)
    s = float(ratings[0])
    P = float(p0)
    s_preds: np.ndarray = np.full(n, np.nan, dtype=np.float64)
    innov_vars: np.ndarray = np.full(n, np.nan, dtype=np.float64)
    for t in range(1, n):
        s_pred = s + aging_curve.delta(float(ages[t - 1]))
        P_pred = P + q
        r_t = obs_base / max(float(weights[t]), 1e-9)
        s_preds[t] = s_pred
        innov_vars[t] = P_pred + r_t
        k_gain = P_pred / (P_pred + r_t)
        s = s_pred + k_gain * (float(ratings[t]) - s_pred)
        P = (1.0 - k_gain) * P_pred
    return s, P, s_preds, innov_vars


def _forecast(
    s_final: float,
    P_final: float,
    age_last: float,
    aging_curve: AgingCurve,
    q: float,
) -> Tuple[float, float]:
    """Forecast next season: skill + aging drift, SD = sqrt(P_final + q).

    Args:
        s_final: Filtered skill estimate at the end of the observed series.
        P_final: Filtered state variance at the end of the observed series.
        age_last: Player age at the last observed season.
        aging_curve: Fitted ``AgingCurve`` supplying per-age expected drift.
        q: Process variance — controls how fast skill can drift.

    Returns:
        ``(projected_rating, projected_sd)`` — one-season-ahead skill forecast and its
        standard deviation (``sqrt(P_final + q)``).
    """
    projected = s_final + aging_curve.delta(age_last)
    sd = float(np.sqrt(P_final + q))
    return float(projected), sd


def _player_series(
    panel: pl.DataFrame,
    ages: pl.DataFrame,
) -> Generator[Tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray], None, None]:
    """Yield ``(player_id, seasons, ratings, ages, weights)`` per player, season-ordered."""
    w = "weight" if "weight" in panel.columns else None
    df = panel.join(ages, on=["player_id", "season"], how="inner").sort(["player_id", "season"])
    for (pid,), g in df.group_by("player_id", maintain_order=True):
        yield (
            int(pid),
            g["season"].to_numpy(),
            g["rating"].to_numpy().astype(np.float64),
            g["age"].to_numpy().astype(np.float64),
            (g[w].to_numpy().astype(np.float64) if w else np.ones(g.height)),
        )


def _neg_loglik(
    log_params: np.ndarray,
    series: List[Tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray]],
    aging_curve: AgingCurve,
) -> float:
    """Negative one-step-ahead Gaussian log-likelihood over all players (for MLE)."""
    q = float(np.exp(log_params[0]))
    obs_base = float(np.exp(log_params[1]))
    ll = 0.0
    for _pid, _seasons, ratings, player_ages, weights in series:
        if len(ratings) < 2:
            continue
        _s, _P, s_preds, innov_vars = _kalman_filter(ratings, player_ages, weights, aging_curve, q, obs_base)
        for t in range(1, len(ratings)):
            v = innov_vars[t]
            resid = ratings[t] - s_preds[t]
            ll += -0.5 * (np.log(2.0 * np.pi * v) + resid * resid / v)
    return -ll


def _fit_noise_params(
    panel: pl.DataFrame,
    ages: pl.DataFrame,
    aging_curve: AgingCurve,
    *,
    defaults: Tuple[float, float] = (0.25, 1.0),
) -> Tuple[float, float]:
    """MLE-fit ``(q, obs_base)`` by maximizing one-step-ahead forecast log-likelihood.

    Deterministic (no RNG). Falls back to ``defaults`` if optimization fails/non-finite.

    Args:
        panel: ``player_id``, ``season``, ``rating`` (+ optional ``weight``) panel.
        ages: ``player_id``, ``season``, ``age``.
        aging_curve: Fitted ``AgingCurve`` supplying per-age expected drift.
        defaults: Fallback ``(q, obs_base)`` when optimization fails or produces non-finite values.

    Returns:
        ``(q, obs_base)`` — MLE-fitted process and base observation variance parameters.
    """
    series = list(_player_series(panel, ages))
    x0 = np.log(np.array(defaults, dtype=np.float64))
    try:
        res = minimize(_neg_loglik, x0, args=(series, aging_curve), method="Nelder-Mead")
        if res.success and np.all(np.isfinite(res.x)):
            return float(np.exp(res.x[0])), float(np.exp(res.x[1]))
    except Exception:
        pass
    return defaults


def nba_darko(
    panel: pl.DataFrame,
    ages: pl.DataFrame,
    *,
    aging_curve: "AgingCurve | None" = None,
    process_var: "float | None" = None,
    obs_base: "float | None" = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Project each player's next-season rating via a per-player Kalman filter + aging curve.

    Args:
        panel: ``player_id, season, rating`` (+ optional ``weight``) — a multi-season rating panel.
        ages: ``player_id, season, age`` (from ``nba_player_ages``).
        aging_curve: an ``AgingCurve``; fitted from ``panel`` if None.
        process_var: Kalman process variance ``q``; MLE-fit from ``panel`` if None.
        obs_base: Kalman base observation variance; MLE-fit from ``panel`` if None.
        return_as_pandas: return pandas instead of polars.

    Returns:
        ``player_id, last_season, forecast_season, filtered_skill, projected_rating, projected_sd``.

    Example:
        Project next season from a multi-season adj-RAPM panel::

            from sportsdataverse.nba import nba_darko, nba_player_ages
            proj = nba_darko(rating_panel, ages_panel)
            print(proj.sort("projected_rating", descending=True).head())
    """
    curve = aging_curve if aging_curve is not None else fit_aging_curve(panel, ages)
    if process_var is None or obs_base is None:
        q_fit, o_fit = _fit_noise_params(panel, ages, curve)
        q = process_var if process_var is not None else q_fit
        ob = obs_base if obs_base is not None else o_fit
    else:
        q = process_var
        ob = obs_base
    rows: List[Dict[str, object]] = []
    for pid, seasons, ratings, ages_seq, weights in _player_series(panel, ages):
        s_final, P_final, _sp, _iv = _kalman_filter(ratings, ages_seq, weights, curve, q, ob)
        proj, sd = _forecast(s_final, P_final, float(ages_seq[-1]), curve, q)
        last_season = int(seasons[-1])
        rows.append(
            {
                "player_id": pid,
                "last_season": last_season,
                "forecast_season": last_season + 1,
                "filtered_skill": float(s_final),
                "projected_rating": proj,
                "projected_sd": sd,
            }
        )
    out = pl.DataFrame(
        rows,
        schema={
            "player_id": pl.Int64,
            "last_season": pl.Int64,
            "forecast_season": pl.Int64,
            "filtered_skill": pl.Float64,
            "projected_rating": pl.Float64,
            "projected_sd": pl.Float64,
        },
    )
    return out.to_pandas() if return_as_pandas else out
