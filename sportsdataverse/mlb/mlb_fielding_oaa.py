"""Native outs-above-average range model (T6.3, model (3)).

Owns the batted-ball trajectory feature extraction
(:func:`bip_trajectory_features`), a standalone empirical catch-probability
surface utility (:func:`catch_prob_surface`), and the public entry point
(:func:`mlb_fielding_oaa`). The entry point fits a **per-position smooth
logistic** P(out | landing distance, launch angle, exit velocity, spray
angle) -- exit velocity + launch angle together proxy the hang time that,
with landing distance and direction, drives catch difficulty -- then
``oaa = sum(is_out - p_catch)``. The per-position logistic replaced a coarse
empirical (distance x spray x launch-angle) bin surface, roughly halving the
gap to Savant's leaderboard (full-season Pearson ~0.40 -> ~0.60).

**Documented approximation:** the public Statcast feed lacks fielder *start*
coordinates, so range is inferred from landing location + a launch-parameter
hang-time proxy rather than distance actually covered -- this is why the model
cannot reach Savant's design target (which uses proprietary fielder tracking);
see the module's oracle test docstring for the full rationale.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant outs-above-average / catch-probability leaderboards --
      concurrent-validity oracles
      (:func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_outs_above_average`,
      :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_catch_probability`).

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import numpy as np
import polars as pl
from scipy.optimize import minimize

if TYPE_CHECKING:
    import pandas as pd

#: Statcast ``events`` values counted as a fielding out for the OAA label.
OUT_EVENTS = [
    "field_out",
    "force_out",
    "grounded_into_double_play",
    "double_play",
    "sac_fly",
    "fielders_choice_out",
    "sac_fly_double_play",
]

#: Savant's home-plate origin for the ``hc_x``/``hc_y`` spray-angle transform.
_HC_X_ORIGIN = 125.42
_HC_Y_ORIGIN = 198.27

_SURFACE_SCHEMA = {
    "position": pl.Int64,
    "dist_b": pl.Int64,
    "spray_b": pl.Int64,
    "la_bin": pl.Int64,
    "p_catch": pl.Float64,
    "n": pl.Int64,
}

_OAA_SCHEMA = {
    "fielder_id": pl.Utf8,
    "position": pl.Int64,
    "opportunities": pl.Int64,
    "oaa": pl.Float64,
}


def bip_trajectory_features(bip: "pl.DataFrame") -> "pl.DataFrame":
    """Add spray angle / hit distance / launch-angle bin / out label / position.

    ``spray_angle = atan2(hc_x - 125.42, 198.27 - hc_y)`` (Savant's standard
    ``hc_x``/``hc_y`` transform, home plate at the origin, positive = toward
    first base).

    Args:
        bip: Balls-in-play frame (``hc_x``, ``hc_y``, ``hit_distance_sc``,
            ``launch_angle``, ``events``, ``hit_location``).

    Returns:
        pl.DataFrame: ``bip`` with added ``spray_angle`` (Float64),
            ``hit_dist`` (Float64), ``la_bin`` (Int64), ``is_out`` (Int8),
            ``position`` (Int64).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_fielding_oaa import bip_trajectory_features
            feats = bip_trajectory_features(bip)
    """
    if bip.height == 0:
        return bip
    return bip.with_columns(
        pl.arctan2(pl.col("hc_x") - _HC_X_ORIGIN, _HC_Y_ORIGIN - pl.col("hc_y")).alias("spray_angle"),
        pl.col("hit_distance_sc").cast(pl.Float64).alias("hit_dist"),
        (pl.col("launch_angle") / 5).floor().cast(pl.Int64).alias("la_bin"),
        pl.col("events").is_in(OUT_EVENTS).fill_null(False).cast(pl.Int8).alias("is_out"),
        pl.col("hit_location").cast(pl.Int64, strict=False).alias("position"),
    )


def catch_prob_surface(
    bip: "pl.DataFrame", *, dist_bin: float = 10.0, spray_bin: float = 0.1, alpha: float = 2.0
) -> "pl.DataFrame":
    """Empirical catch-probability surface over ``(position, distance, spray, launch angle)``.

    Rate per bin is Laplace-smoothed: ``(outs + alpha) / (n + 2 * alpha)``.

    Args:
        bip: Balls-in-play frame (see :func:`bip_trajectory_features`).
        dist_bin: Bin width for hit distance, in feet. Defaults to ``10.0``.
        spray_bin: Bin width for spray angle, in radians. Defaults to ``0.1``.
        alpha: Laplace smoothing strength. Defaults to ``2.0``.

    Returns:
        pl.DataFrame: one row per observed ``(position, dist_b, spray_b, la_bin)``.

        | Column | Type | Description |
        |---|---|---|
        | position | Int64 | Responsible fielder position (Savant ``hit_location``, 1-9) |
        | dist_b | Int64 | Hit-distance bin index |
        | spray_b | Int64 | Spray-angle bin index |
        | la_bin | Int64 | Launch-angle bin index (hang-time proxy) |
        | p_catch | Float64 | Laplace-smoothed empirical out (catch) probability |
        | n | Int64 | Balls in play observed in this bin |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_fielding_oaa import catch_prob_surface
            surface = catch_prob_surface(bip, alpha=2.0)
    """
    if bip.height == 0:
        return pl.DataFrame(schema=_SURFACE_SCHEMA)
    f = bip_trajectory_features(bip).with_columns(
        (pl.col("hit_dist") / dist_bin).floor().cast(pl.Int64).alias("dist_b"),
        (pl.col("spray_angle") / spray_bin).floor().cast(pl.Int64).alias("spray_b"),
    )
    return (
        f.group_by(["position", "dist_b", "spray_b", "la_bin"])
        .agg(pl.col("is_out").sum().alias("k"), pl.len().alias("n"))
        .with_columns(((pl.col("k") + alpha) / (pl.col("n") + 2 * alpha)).alias("p_catch"))
        .select("position", "dist_b", "spray_b", "la_bin", "p_catch", "n")
        .sort("position", "dist_b", "spray_b", "la_bin")
    )


def _oaa_feature_matrix(f: "pl.DataFrame") -> "np.ndarray":
    """Per-position catch logistic features: landing distance (+ cubic), launch
    angle, exit velocity, spray angle, and distance x (angle / |spray|)
    interactions -- exit velocity x launch angle proxy hang time."""
    dist = f["hit_dist"].to_numpy().astype(float)
    la = f["launch_angle"].to_numpy().astype(float)
    spray = f["spray_angle"].to_numpy().astype(float)
    if "launch_speed" in f.columns:
        ev = f["launch_speed"].to_numpy().astype(float)
        m = float(np.nanmean(ev)) if np.isfinite(np.nanmean(ev)) else 0.0
        ev = np.nan_to_num(ev, nan=m)
    else:
        ev = np.zeros(len(dist))
    return np.column_stack(
        [dist, dist**2, dist**3, la, la**2, ev, ev**2, np.abs(spray), spray, spray**2, dist * la, dist * np.abs(spray)]
    )


def _fit_catch_logistic(x: "np.ndarray", y: "np.ndarray", l2: float) -> "np.ndarray":
    """L2-regularized logistic P(out | features); returns fitted P for each row of ``x``."""
    xs = (x - x.mean(0)) / (x.std(0) + 1e-9)

    def _neg_log_loss(theta: "np.ndarray") -> float:
        z = xs @ theta[:-1] + theta[-1]
        p = np.clip(1.0 / (1.0 + np.exp(-z)), 1e-9, 1 - 1e-9)
        return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)) + l2 * np.sum(theta[:-1] ** 2))

    res = minimize(_neg_log_loss, np.zeros(x.shape[1] + 1), method="L-BFGS-B")
    z = xs @ res.x[:-1] + res.x[-1]
    return np.clip(1.0 / (1.0 + np.exp(-z)), 1e-9, 1 - 1e-9)


def _direction_expr(scored: "pl.DataFrame") -> "pl.Expr":
    """Classify each ball in play as ``in`` / ``back`` / ``lateral`` for the fielder.

    **Documented approximation.** Savant splits directional OAA against the
    fielder's tracked START position; the public feed has no start coordinates
    (the same ceiling the module docstring names), so the position's own MEDIAN
    landing spot stands in for where that position normally plays. Depth
    deviation is ``hit_dist - median(hit_dist)`` for the position; lateral
    deviation is the arc length ``hit_dist * (spray_angle - median(spray_angle))``
    so both legs are in feet and comparable. The larger leg names the direction:
    lateral when the sideways move dominates, else ``back`` (deeper than normal
    depth) or ``in`` (shallower).

    Savant's own ``leaderboard/outfield_directional_outs_above_average``
    (wrapped as
    :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_outfield_directional_oaa`)
    is the concurrent-validity oracle for the outfield rows of this split.

    The split is a strict PARTITION of the scored balls in play -- it re-groups
    the same ``out_gain`` values and never re-fits -- so per-direction OAA sums
    exactly to the undirected OAA for every fielder-position.
    """
    med_dist = pl.col("hit_dist").median().over("position")
    med_spray = pl.col("spray_angle").median().over("position")
    depth = pl.col("hit_dist") - med_dist
    lateral = pl.col("hit_dist") * (pl.col("spray_angle") - med_spray)
    return (
        pl.when(lateral.abs() >= depth.abs())
        .then(pl.lit("lateral"))
        .when(depth > 0)
        .then(pl.lit("back"))
        .otherwise(pl.lit("in"))
        .alias("direction")
    )


def mlb_fielding_oaa(
    bip: "pl.DataFrame",
    *,
    l2: float = 1e-4,
    min_fit: int = 50,
    by_direction: bool = False,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-fielder outs above average from a per-position catch-probability logistic.

    ``oaa = sum(is_out - p_catch)`` per ``(fielder_id, position)``, where
    ``p_catch`` is a **smooth per-position logistic** P(out | landing
    distance, launch angle, exit velocity, spray angle) (exit velocity x
    launch angle proxy the hang time). A position with fewer than ``min_fit``
    balls in play falls back to its mean out rate. This replaced a coarse
    empirical bin surface, roughly halving the gap to Savant's leaderboard
    (full-season Pearson ~0.40 -> ~0.60). The fielder id is resolved
    dynamically from the responsible position's ``fielder_{position}`` column
    (cast ``Utf8`` at the boundary).

    Args:
        bip: Balls-in-play frame with ``hc_x``/``hc_y``, ``hit_distance_sc``,
            ``launch_angle``, ``launch_speed``, ``hit_location``, ``events``,
            and the ``fielder_1``..``fielder_9`` responsible-player columns.
            MiLB input (e.g.
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`)
            runs through the same function -- there is no Savant OAA
            leaderboard oracle for MiLB.
        l2: L2 penalty for the per-position logistic. Defaults to ``1e-4``.
        min_fit: Minimum balls in play for a position to fit its own
            logistic; below this the position's mean out rate is used.
            Defaults to ``50``.
        by_direction: Split each fielder-position into ``in`` / ``back`` /
            ``lateral`` buckets, using the position's own median landing spot
            as a stand-in for the fielder start coordinates the public feed
            lacks (a documented approximation). The split re-groups the same
            scored balls in play, so the three rows sum exactly to the
            undirected ``oaa``. Defaults to ``False``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per ``(fielder_id, position)``.

        | Column | Type | Description |
        |---|---|---|
        | fielder_id | Utf8 | Responsible fielder's MLBAM id |
        | position | Int64 | Position (Savant ``hit_location``, 1-9) |
        | direction | Utf8 | ``in``/``back``/``lateral`` -- only when ``by_direction=True`` |
        | opportunities | Int64 | Balls in play charged to this fielder |
        | oaa | Float64 | Sum of (out - expected catch probability) |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_fielding_oaa import mlb_fielding_oaa
            oaa = mlb_fielding_oaa(bip)

        Per-direction splits (sum to the undirected OAA)::

            mlb_fielding_oaa(bip, by_direction=True)

        Pipeline next step (one line)::

            oaa.filter(pl.col("opportunities") >= 100).sort("oaa", descending=True)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant outs-above-average leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if bip.height == 0:
        out = pl.DataFrame(schema=_OAA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    f = bip_trajectory_features(bip)

    # Resolve the responsible fielder id dynamically from fielder_{position}.
    fielder_cols = [c for c in f.columns if c.startswith("fielder_") and c[len("fielder_") :].isdigit()]
    if fielder_cols:
        f = f.with_columns(
            pl.coalesce(
                [pl.when(pl.col("position") == int(c[len("fielder_") :])).then(pl.col(c)) for c in fielder_cols]
            )
            .cast(pl.Int64, strict=False)
            .cast(pl.Utf8)
            .alias("fielder_id")
        )
    else:
        f = f.with_columns(pl.lit(None, dtype=pl.Utf8).alias("fielder_id"))

    f = f.filter(
        pl.col("fielder_id").is_not_null()
        & pl.col("position").is_not_null()
        & pl.col("hit_dist").is_not_null()
        & pl.col("launch_angle").is_not_null()
        & pl.col("spray_angle").is_not_null()
    )
    if f.height == 0:
        out = pl.DataFrame(schema=_OAA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    # Per-position smooth logistic P(out | trajectory). Expected out probability
    # is deliberately position-scoped (an infielder's and a center fielder's
    # catch surfaces are unrelated); the fielder's own outs above that surface
    # are the OAA signal.
    x = _oaa_feature_matrix(f)
    y = f["is_out"].to_numpy().astype(float)
    pos = f["position"].to_numpy()
    p_catch: np.ndarray = np.empty(len(y), dtype=float)
    for pv in np.unique(pos):
        mask = pos == pv
        p_catch[mask] = _fit_catch_logistic(x[mask], y[mask], l2) if mask.sum() >= min_fit else y[mask].mean()

    scored = f.with_columns(pl.Series("p_catch", p_catch)).with_columns(
        (pl.col("is_out") - pl.col("p_catch")).alias("out_gain")
    )

    keys = ["fielder_id", "position"]
    if by_direction:
        scored = scored.with_columns(_direction_expr(scored))
        keys = ["fielder_id", "position", "direction"]

    out = (
        scored.group_by(keys)
        .agg(pl.len().alias("opportunities"), pl.col("out_gain").sum().alias("oaa"))
        .sort("oaa", descending=True)
        .select(*keys, "opportunities", "oaa")
    )
    return out.to_pandas() if return_as_pandas else out
