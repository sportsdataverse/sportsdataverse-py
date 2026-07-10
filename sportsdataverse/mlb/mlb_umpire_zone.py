"""MLB umpire strike-zone / bias model (T6.4, model ③ -- the spine's sole
Statcast bridge).

A logistic fit on standardized zone coordinates gives P(called strike |
pitch location); the per-umpire mean residual (observed - expected
called-strike rate) is the bias metric. This is the **only** model in
the game-state spine that consumes Baseball Savant pitch location
(:func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`);
everything else in this spine is statsapi-native.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant (baseballsavant.mlb.com) -- source of the
      ``plate_x``/``plate_z``/``sz_top``/``sz_bot`` pitch-tracking
      coordinates this module fits on.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
import polars as pl
from scipy.optimize import minimize

_CALLED_STRIKE_PROB_SCHEMA = {"called_strike_prob": pl.Float64}
_BIAS_SCHEMA = {
    "umpire_id": pl.Utf8,
    "n_called": pl.Int64,
    "obs_strike_rate": pl.Float64,
    "exp_strike_rate": pl.Float64,
    "bias": pl.Float64,
}
_CALLED_PITCH_DESCRIPTIONS = ("called_strike", "ball")
_L2_PENALTY = 1e-4


def _zone_features(pitches: pl.DataFrame) -> "np.ndarray":
    """Standardized zone-coordinate feature matrix for the logistic fit.

    Args:
        pitches: Frame with ``plate_x``, ``plate_z``, ``sz_top``, ``sz_bot``.

    Returns:
        np.ndarray: shape ``(n, 7)``, columns
        ``[plate_x, plate_x**2, z_norm, z_norm**2, plate_x*z_norm, |plate_x|, |z_norm-0.5|]``,
        where ``z_norm = (plate_z - sz_bot) / (sz_top - sz_bot)``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_umpire_zone import _zone_features
            X = _zone_features(pitches)
    """
    px = pitches["plate_x"].to_numpy().astype(float)
    pz = pitches["plate_z"].to_numpy().astype(float)
    sz_top = pitches["sz_top"].to_numpy().astype(float)
    sz_bot = pitches["sz_bot"].to_numpy().astype(float)
    denom = np.where((sz_top - sz_bot) == 0, np.nan, sz_top - sz_bot)
    z_norm = (pz - sz_bot) / denom
    z_norm = np.nan_to_num(z_norm, nan=0.5)
    return np.column_stack([px, px**2, z_norm, z_norm**2, px * z_norm, np.abs(px), np.abs(z_norm - 0.5)])


def _called_pitches(pitches: pl.DataFrame) -> pl.DataFrame:
    return pitches.filter(
        pl.col("description").is_in(_CALLED_PITCH_DESCRIPTIONS)
        & pl.col("plate_x").is_not_null()
        & pl.col("plate_z").is_not_null()
        & pl.col("sz_top").is_not_null()
        & pl.col("sz_bot").is_not_null()
    )


def fit_zone_model(pitches: pl.DataFrame) -> Dict[str, Any]:
    """Fit a logistic P(called strike | zone coordinates) on called pitches.

    Compute-on-demand -- **no artifact is bundled or cached to disk.**
    L2-regularized (``1e-4``) mean log-loss, minimized via
    ``scipy.optimize.minimize(method="L-BFGS-B")``.

    Args:
        pitches: Frame of pitches with ``description`` (filtered to
            ``{"called_strike", "ball"}``), ``plate_x``, ``plate_z``,
            ``sz_top``, ``sz_bot``.

    Returns:
        dict: ``{"coef": list[float] (7,), "intercept": float, "features": list[str]}``.
        ``{"coef": [], "intercept": 0.0, "features": [...]}`` if fewer
        than 2 called pitches are available (degenerate fit).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_umpire_zone import fit_zone_model
            model = fit_zone_model(pitches)
    """
    feature_names = ["plate_x", "plate_x_sq", "z_norm", "z_norm_sq", "plate_x_z_norm", "abs_plate_x", "abs_z_dev"]
    called = _called_pitches(pitches)
    if called.height < 2:
        return {"coef": [0.0] * len(feature_names), "intercept": 0.0, "features": feature_names}

    X = _zone_features(called)
    y = (called["description"] == "called_strike").cast(pl.Int8).to_numpy().astype(float)

    def _neg_log_loss(theta: "np.ndarray") -> float:
        w, b = theta[:-1], theta[-1]
        z = X @ w + b
        p = 1.0 / (1.0 + np.exp(-z))
        p = np.clip(p, 1e-12, 1 - 1e-12)
        loss = -np.mean(y * np.log(p) + (1 - y) * np.log(1 - p))
        return loss + _L2_PENALTY * float(np.sum(w**2))

    theta0 = np.zeros(X.shape[1] + 1)
    res = minimize(_neg_log_loss, theta0, method="L-BFGS-B")
    return {"coef": res.x[:-1].tolist(), "intercept": float(res.x[-1]), "features": feature_names}


def _score(pitches: pl.DataFrame, model: Dict[str, Any]) -> "np.ndarray":
    X = _zone_features(pitches)
    coef = np.asarray(model["coef"], dtype=float)
    z = X @ coef + model["intercept"]
    return 1.0 / (1.0 + np.exp(-z))


def mlb_umpire_called_strike_prob(
    pitches: pl.DataFrame,
    *,
    model: Optional[Dict[str, Any]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """P(called strike) per pitch from the zone logistic.

    Args:
        pitches: Frame with ``plate_x``, ``plate_z``, ``sz_top``, ``sz_bot``
            (one row per pitch, not required to be called pitches only).
        model: Pre-fit model dict from :func:`fit_zone_model`; fits on
            ``pitches`` itself when ``None`` (using only its called pitches).
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per input pitch.

        | Column | Type | Description |
        |---|---|---|
        | called_strike_prob | Float64 | P(called strike \\| pitch location) |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_called_strike_prob
            prob = mlb_umpire_called_strike_prob(pitches)
    """
    if pitches is None or pitches.height == 0:
        out = pl.DataFrame(schema=_CALLED_STRIKE_PROB_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    fitted = model or fit_zone_model(pitches)
    probs = _score(pitches, fitted)
    out = pl.DataFrame({"called_strike_prob": probs})
    return out.to_pandas() if return_as_pandas else out


def mlb_umpire_bias(
    pitches: pl.DataFrame,
    *,
    model: Optional[Dict[str, Any]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per-umpire called-strike bias residual (observed minus expected).

    Args:
        pitches: Called pitches with ``umpire_id``, ``description``,
            ``plate_x``, ``plate_z``, ``sz_top``, ``sz_bot``.
        model: Pre-fit model dict from :func:`fit_zone_model`; fits on
            ``pitches`` itself when ``None``.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        pl.DataFrame: one row per umpire.

        | Column | Type | Description |
        |---|---|---|
        | umpire_id | Utf8 | Umpire identifier |
        | n_called | Int64 | Called pitches observed for this umpire |
        | obs_strike_rate | Float64 | Realized called-strike rate |
        | exp_strike_rate | Float64 | Mean model-predicted called-strike probability |
        | bias | Float64 | obs_strike_rate - exp_strike_rate (positive = strike-generous) |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_umpire_zone import mlb_umpire_bias
            bias = mlb_umpire_bias(pitches)
    """
    if pitches is None or pitches.height == 0 or "umpire_id" not in pitches.columns:
        return pl.DataFrame(schema=_BIAS_SCHEMA).to_pandas() if return_as_pandas else pl.DataFrame(schema=_BIAS_SCHEMA)
    called = _called_pitches(pitches)
    if called.height == 0:
        return pl.DataFrame(schema=_BIAS_SCHEMA).to_pandas() if return_as_pandas else pl.DataFrame(schema=_BIAS_SCHEMA)
    fitted = model or fit_zone_model(called)
    called = called.with_columns(
        (pl.col("description") == "called_strike").cast(pl.Float64).alias("observed_strike"),
        pl.Series("exp_prob", _score(called, fitted)),
    )
    out = (
        called.group_by("umpire_id")
        .agg(
            pl.len().alias("n_called"),
            pl.col("observed_strike").mean().alias("obs_strike_rate"),
            pl.col("exp_prob").mean().alias("exp_strike_rate"),
        )
        .with_columns((pl.col("obs_strike_rate") - pl.col("exp_strike_rate")).alias("bias"))
        .sort("umpire_id")
    )
    return out.to_pandas() if return_as_pandas else out
