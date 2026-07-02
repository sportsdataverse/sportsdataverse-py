"""DARKO-style player projection: per-player Kalman filter + empirical aging curve."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import numpy as np
import polars as pl


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
