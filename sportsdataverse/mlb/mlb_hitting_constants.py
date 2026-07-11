"""Shared grid config, value maps, spray/split helpers, and validation metrics for
the MLB hitting model spine (season-agnostic algorithm; per-season empirical
constants -- see ``CLAUDE.md`` / the T6.2 design doc).

Every model in ``sportsdataverse.mlb.mlb_expected_stats`` /
``mlb_swing_decision`` / ``mlb_expected_home_runs`` / ``mlb_batter_projection``
imports from this module rather than re-implementing the grid config, spray
angle, leakage-boundary split, or validation metrics.

Methodology references (cited, not copied):
  * Baseball Savant ``estimated_woba_using_speedangle`` -- EV+LA outcome estimate,
    reproduced here as a from-scratch binned empirical grid.
  * Bill Petti's spray-angle formula from Statcast ``hc_x``/``hc_y``.
  * Tango/Lichtman delta-method aging curve; Marcel the Monkey projection system.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import polars as pl
from scipy.stats import rankdata

from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search


@dataclass(frozen=True)
class GridConfig:
    """EV x LA x spray binning configuration for the hitting-spine grids.

    These are the *seeded* starting widths; Task 1.4's sweep
    (``dev/mlb_hitting/fit_grid.py``) confirms or revises them against the
    Savant concurrent-validity oracle and records the chosen values here.
    """

    ev_width: float = 2.0
    ev_min: float = 20.0
    ev_max: float = 120.0
    la_width: float = 2.0
    la_min: float = -90.0
    la_max: float = 90.0
    spray_width: float = 5.0
    min_n: int = 25


GRID = GridConfig()

#: Per-cell value maps: {stat name -> the batted-ball column whose cell-mean
#: is that stat's prediction}. ``ba``/``slg`` use derived indicator columns
#: (``_hit``/``_total_bases``) added by ``mlb_expected_stats._add_value_columns``.
VALUE_MAPS: dict[str, str] = {"woba": "woba_value", "ba": "_hit", "slg": "_total_bases"}

HIT_EVENTS: set[str] = {"single", "double", "triple", "home_run"}
TOTAL_BASES: dict[str, int] = {"single": 1, "double": 2, "triple": 3, "home_run": 4}

#: ``description`` values that count as a swing decision (Task 2.1).
SWING_DESCRIPTIONS: set[str] = {
    "swinging_strike",
    "swinging_strike_blocked",
    "foul",
    "foul_tip",
    "hit_into_play",
    "foul_bunt",
    "missed_bunt",
}
#: ``description`` values that count as a take decision (Task 2.1).
TAKE_DESCRIPTIONS: set[str] = {"ball", "called_strike", "blocked_ball", "hit_by_pitch", "pitchout"}


def spray_angle(hc_x: pl.Expr, hc_y: pl.Expr, stand: pl.Expr) -> pl.Expr:
    """Signed spray angle in degrees (+ = pull) from Statcast hit coordinates.

    ``atan2(hc_x - 125.42, 198.27 - hc_y) * 180/pi``; mirrored for left-handed
    batters so positive is always the pull side (Bill Petti's public formula).

    Args:
        hc_x: Statcast hit-coordinate x expression.
        hc_y: Statcast hit-coordinate y expression.
        stand: Batter handedness expression (``"L"``/``"R"``).

    Returns:
        A polars expression for the signed spray angle in degrees.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mlb.mlb_hitting_constants import spray_angle

            df = pl.DataFrame({"hc_x": [125.42], "hc_y": [98.27], "stand": ["R"]})
            df.with_columns(spray_angle(pl.col("hc_x"), pl.col("hc_y"), pl.col("stand")).alias("sa"))
    """
    raw = pl.arctan2(hc_x - 125.42, 198.27 - hc_y) * (180.0 / np.pi)
    return pl.when(stand == "L").then(-raw).otherwise(raw)


def as_of_seasons_split(player_seasons: pl.DataFrame, target_season: int) -> pl.DataFrame:
    """Leakage boundary: keep only seasons strictly before ``target_season``.

    Every backward-looking feature build (e.g. the Marcel projection in
    ``mlb_batter_projection``) must route its history through this helper so a
    projection for season *Y* never sees season *Y* (or later) data.

    Args:
        player_seasons: Frame with an integer ``season`` column.
        target_season: The season being projected/evaluated.

    Returns:
        The input frame filtered to ``season < target_season``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.mlb.mlb_hitting_constants import as_of_seasons_split

            ps = pl.DataFrame({"batter": [1, 1, 1], "season": [2021, 2022, 2023]})
            as_of_seasons_split(ps, target_season=2023)
    """
    return player_seasons.filter(pl.col("season") < target_season)


def spearman_corr(a: "np.ndarray[Any, Any]", b: "np.ndarray[Any, Any]") -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The Spearman rank correlation coefficient.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_hitting_constants import spearman_corr

            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: "np.ndarray[Any, Any]", b: "np.ndarray[Any, Any]") -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array of values.
        b: Second array of values (same length as ``a``).

    Returns:
        The mean absolute difference ``mean(abs(a - b))``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_hitting_constants import mae

            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def brier_score(y_true: "np.ndarray[Any, Any]", p_pred: "np.ndarray[Any, Any]") -> float:
    """Brier score (mean squared error of a probability forecast).

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted probability array (same length as ``y_true``).

    Returns:
        The Brier score ``mean((p_pred - y_true) ** 2)``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_hitting_constants import brier_score

            brier_score(np.array([1, 0]), np.array([0.75, 0.25]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def calibration_table(y_true: "np.ndarray[Any, Any]", p_pred: "np.ndarray[Any, Any]", n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare mean predicted vs. mean actual.

    Args:
        y_true: Binary outcome array (0/1).
        p_pred: Predicted probability array (same length as ``y_true``).
        n_bins: Number of equal-width probability bins.

    Returns:
        A polars DataFrame with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` -- one row per non-empty bin.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_hitting_constants import calibration_table

            rng = np.random.default_rng(0)
            calibration_table(rng.integers(0, 2, 200), rng.random(200), n_bins=10)
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(pl.col("p").mean().alias("mean_pred"), pl.col("y").mean().alias("mean_actual"), pl.len().alias("n"))
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )


def pull_statcast_season(
    season: int,
    *,
    puller: Callable[..., pl.DataFrame] = mlb_statcast_search,
    player_type: str = "batter",
) -> pl.DataFrame:
    """Whole-season Statcast pull via the shipped date-chunked search (shared with T6.1).

    Args:
        season: Four-digit season year.
        puller: Injectable date-range puller -- defaults to
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`;
            pass ``mlb_statcast_search_minors``/``_wbc`` to reuse this pipeline
            for the minors/WBC search routes.
        player_type: ``"batter"`` or ``"pitcher"`` (Savant's search parameter).

    Returns:
        A polars DataFrame of every pitch from Jan 1 to Dec 1 of ``season``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_hitting_constants import pull_statcast_season

            season_pitches = pull_statcast_season(2024)
    """
    return puller(f"{season}-01-01", f"{season}-12-01", player_type=player_type)
