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


def _lag1_autocorr(
    series: "List[Tuple[int, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]",
) -> float:
    """Pooled raw lag-1 autocorrelation of ratings across all players.

    Uses raw (not per-player demeaned) pairs so the cross-sectional persistence signal
    (between-player skill variance) is included in the correlation.  High autocorr
    indicates strong persistent skill; near-zero indicates pure noise.
    """
    xs: List[float] = []
    ys: List[float] = []
    for _pid, _seasons, ratings, _ages, _weights in series:
        for t in range(len(ratings) - 1):
            xs.append(float(ratings[t]))
            ys.append(float(ratings[t + 1]))
    if len(xs) < 2:
        return 0.5
    x_arr, y_arr = np.array(xs), np.array(ys)
    sx, sy = float(np.std(x_arr)), float(np.std(y_arr))
    if sx < 1e-12 or sy < 1e-12:
        return 0.5
    return float(np.corrcoef(x_arr, y_arr)[0, 1])


def _fit_noise_params(
    panel: pl.DataFrame,
    ages: pl.DataFrame,
    aging_curve: AgingCurve,
    *,
    defaults: Tuple[float, float] = (0.25, 1.0),
    _rho_min: float = 0.05,
) -> Tuple[float, float]:
    """MLE-fit ``(q, obs_base)`` by maximizing one-step-ahead forecast log-likelihood.

    A moment-based lower bound on ``q`` is applied after MLE to prevent the degenerate
    ``q → 0`` solution on low-persistence panels.  The bound uses the pooled raw lag-1
    autocorrelation ``ρ`` of the ratings: ``q_min = obs_base * (1 − ρ) / ρ`` (clamped to
    ``[_rho_min, 0.99]``).  On panels with strong cross-sectional skill persistence
    (``ρ ≈ 1``) the bound is near zero and the MLE value dominates.  On pure-noise panels
    (``ρ ≈ 0``) the bound is large, correctly reflecting that process noise ≈ observation
    noise when ratings carry no carry-forward information.

    Deterministic (no RNG). Falls back to ``defaults`` if optimization fails/non-finite.

    Args:
        panel: ``player_id``, ``season``, ``rating`` (+ optional ``weight``) panel.
        ages: ``player_id``, ``season``, ``age``.
        aging_curve: Fitted ``AgingCurve`` supplying per-age expected drift.
        defaults: Fallback ``(q, obs_base)`` when optimization fails or produces non-finite values.
        _rho_min: Internal floor on the lag-1 autocorrelation used for the ``q`` lower bound.

    Returns:
        ``(q, obs_base)`` — fitted process and base observation variance parameters.
    """
    series = list(_player_series(panel, ages))
    x0 = np.log(np.array(defaults, dtype=np.float64))
    q_mle, ob = defaults
    try:
        res = minimize(_neg_loglik, x0, args=(series, aging_curve), method="Nelder-Mead")
        if res.success and np.all(np.isfinite(res.x)):
            q_mle, ob = float(np.exp(res.x[0])), float(np.exp(res.x[1]))
    except Exception:
        pass
    # Moment-based lower bound: prevent q collapsing to zero on low-persistence panels.
    rho = _lag1_autocorr(series)
    rho_clamped = max(_rho_min, min(0.99, rho))
    q_min = ob * (1.0 - rho_clamped) / rho_clamped
    return max(q_mle, q_min), ob


@dataclass(frozen=True)
class ForecastResult:
    """Forecast-accuracy metrics: predicted-vs-actual next-season rating over held-out transitions."""

    forecast_rmse: float
    forecast_corr: float
    baseline_rmse: float
    n_forecasts: int


def darko_forecast_accuracy(
    panel: pl.DataFrame,
    ages: pl.DataFrame,
    *,
    aging_curve: "AgingCurve | None" = None,
    process_var: "float | None" = None,
    obs_base: "float | None" = None,
    min_history: int = 1,
) -> ForecastResult:
    """Holdout forecast accuracy: for each transition, forecast N+1 from history <= N vs actual.

    For each player and each split at index ``t`` (prefix seasons ``0..t`` used to forecast
    season ``t+1``), run the Kalman filter on the prefix then forecast; the baseline is
    carry-forward (``ratings[t]``).  Global ``aging_curve`` and ``(q, obs_base)`` are fit on
    the full panel (low-dim parameters — standard practice; the holdout is on each player's
    rating-history prefix).

    Args:
        panel: ``player_id``, ``season``, ``rating`` (+ optional ``weight``) panel.
        ages: ``player_id``, ``season``, ``age``.
        aging_curve: Fitted ``AgingCurve``; fit from ``panel`` if None.
        process_var: Kalman process variance ``q``; MLE-fit from ``panel`` if None.
        obs_base: Kalman base observation variance; MLE-fit from ``panel`` if None.
        min_history: Minimum prefix length before a forecast is scored (default 1).

    Returns:
        ``ForecastResult`` with ``forecast_rmse`` / ``forecast_corr`` vs the actual next-season
        rating, ``baseline_rmse`` = carry-forward RMSE, and ``n_forecasts`` (total held-out
        transitions across all players).

    Example:
        Quick start — evaluate projection quality on a rating panel::

            from sportsdataverse.nba.nba_darko import darko_forecast_accuracy
            res = darko_forecast_accuracy(rating_panel, ages_panel)
            print(res.forecast_rmse, res.baseline_rmse, res.forecast_corr)

        Pass pre-fitted params to skip the global MLE step::

            from sportsdataverse.nba.nba_darko import fit_aging_curve, _fit_noise_params
            curve = fit_aging_curve(panel, ages)
            q, ob = _fit_noise_params(panel, ages, curve)
            res = darko_forecast_accuracy(panel, ages, aging_curve=curve, process_var=q, obs_base=ob)

        See Also:
            * `nba_darko`_ -- one-shot next-season projection for all players
            * `hoopR`_ -- Men's basketball R package

        .. _nba_darko: sportsdataverse.nba.nba_darko.nba_darko
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    curve = aging_curve if aging_curve is not None else fit_aging_curve(panel, ages)
    if process_var is None or obs_base is None:
        q_fit, o_fit = _fit_noise_params(panel, ages, curve)
        q = process_var if process_var is not None else q_fit
        ob = obs_base if obs_base is not None else o_fit
    else:
        q, ob = process_var, obs_base

    preds: List[float] = []
    actuals: List[float] = []
    base: List[float] = []
    for _pid, _seasons, ratings, ages_seq, weights in _player_series(panel, ages):
        for t in range(min_history, len(ratings) - 1):
            s_f, P_f, _sp, _iv = _kalman_filter(ratings[: t + 1], ages_seq[: t + 1], weights[: t + 1], curve, q, ob)
            proj, _sd = _forecast(s_f, P_f, float(ages_seq[t]), curve, q)
            preds.append(proj)
            actuals.append(float(ratings[t + 1]))
            base.append(float(ratings[t]))

    if not preds:
        return ForecastResult(float("nan"), float("nan"), float("nan"), 0)

    p, a, b = np.array(preds), np.array(actuals), np.array(base)
    rmse = float(np.sqrt(np.mean((p - a) ** 2)))
    base_rmse = float(np.sqrt(np.mean((b - a) ** 2)))
    corr = float(np.corrcoef(p, a)[0, 1]) if p.size > 1 and np.std(p) > 0 else 0.0
    return ForecastResult(rmse, corr, base_rmse, int(p.size))


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
