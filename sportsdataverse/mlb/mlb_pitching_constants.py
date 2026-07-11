"""MLB pitching-spine shared constants, fitted coefficients, and validation metrics.

Single import home for the seven-model pitching stack (T6.1): league-baseline
lookups, fitted OLS/penalty coefficients, the ``+``-scale normalization scale,
bundled-artifact paths, and the validation metric primitives (Spearman/RMSE/MAE/
calibration table) every model's oracle gate uses. Also owns :func:`as_of_split`,
the single leakage boundary shared by the injury-risk model (Phase 8) and the
out-of-sample skill checks in Phases 2/4.

The ``LEAGUE_BASELINES`` seed values below are **placeholders overwritten by the
committed fitting scripts** (``dev/mlb_pitching/fit_stuff_plus.py``,
``fit_command_plus.py``, ``fit_era_siera.py``, ``fit_fatigue.py``) as each phase
lands — see each field's docstring note for its fitting task.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import polars as pl
from scipy.stats import rankdata

# --- bundled-artifact paths (Phase 2 / Phase 3 write these .ubj files) ---
STUFF_PLUS_ARTIFACT = "mlb_stuff_plus.ubj"
COMMAND_PLUS_ARTIFACT = "mlb_command_plus.ubj"

#: "+"-scale: 100 = league average, one league SD of predicted run value maps
#: to this many "+"-scale points (sign inverted: negative run value is good).
PLUS_SCALE: float = 10.0

#: Task 2.2 fitting output (``dev/mlb_pitching/fit_stuff_plus.py``, 2023 season,
#: 30-pitcher real corpus, 71,012 pitches after dropna) — centers ``stuff_plus``
#: at 100 by construction.
STUFF_LEAGUE_MEAN_RV: float = -0.00139173015486449
STUFF_LEAGUE_SD_RV: float = 0.022356726229190826

#: Task 3.1 fitting output (``dev/mlb_pitching/fit_command_plus.py``) — seeded
#: the same way as the Stuff+ pair above.
COMMAND_LEAGUE_MEAN_RV: float = 0.0
COMMAND_LEAGUE_SD_RV: float = 0.1


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two equal-length arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        float: The Spearman rank correlation coefficient in ``[-1, 1]``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_pitching_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def rmse(a: np.ndarray, b: np.ndarray) -> float:
    """Root-mean-squared error between two equal-length arrays.

    Args:
        a: Predicted (or first) array.
        b: Actual (or second) array (same length as ``a``).

    Returns:
        float: The RMSE.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_pitching_constants import rmse
            rmse(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    return float(np.sqrt(np.mean((a - b) ** 2)))


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two equal-length arrays.

    Args:
        a: Predicted (or first) array.
        b: Actual (or second) array (same length as ``a``).

    Returns:
        float: The MAE.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_pitching_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    return float(np.mean(np.abs(a - b)))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare to observed outcome rate per bin.

    Args:
        y_true: Binary (0/1) outcome array.
        p_pred: Predicted probability array (same length as ``y_true``).
        n_bins: Number of equal-width probability bins (default 10).

    Returns:
        polars.DataFrame: Columns ``bin_mid``, ``mean_pred``, ``mean_actual``,
        ``n`` — one row per non-empty bin (at most ``n_bins`` rows).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_pitching_constants import calibration_table
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


def as_of_split(pitches: pl.DataFrame, cutoff_date: dt.date, *, date_col: str = "game_date") -> pl.DataFrame:
    """Leakage boundary: rows strictly before ``cutoff_date``.

    The single as-of-date leakage boundary used across the pitching spine —
    features for game/event ``G`` must use only rows with
    ``date_col < cutoff_date`` (never ``<=``). Shared by :mod:`mlb_pitch_injury`
    (Phase 8) and the Phase 2/4 out-of-sample skill checks.

    Args:
        pitches: A frame carrying ``date_col``.
        cutoff_date: The exclusive upper bound.
        date_col: Name of the date column (default ``"game_date"``).

    Returns:
        polars.DataFrame: Rows with ``date_col < cutoff_date``.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.mlb.mlb_pitching_constants import as_of_split
            as_of_split(pitches, dt.date(2024, 6, 15))
    """
    return pitches.filter(pl.col(date_col) < cutoff_date)


@dataclass(frozen=True)
class PitchingConstants:
    """Per-season league baselines + fitted coefficients for the pitching spine.

    Attributes:
        league_woba: League-average wOBA allowed (xERA parametric conversion).
        woba_scale: wOBA-to-runs-per-PA scale divisor (xERA parametric conversion).
        league_era: League-average ERA (xERA parametric conversion baseline).
        pa_per_9: League-average plate appearances per 9 innings (xERA scaling).
        siera_coef: SIERA-like OLS coefficients
            ``[b0, b1_k_pct, b2_bb_pct, b3_gb_pct, b4_gb_pct_sq, b5_k_gb_interact]``
            — seeded here, overwritten by Task 4.2's
            ``dev/mlb_pitching/fit_era_siera.py``.
        tto_penalty: Times-through-order run-value penalty per TTO level
            ``[tto1, tto2, tto3]`` (``tto1`` is the reference, typically ``0.0``)
            — seeded here, overwritten by Task 5.2's
            ``dev/mlb_pitching/fit_fatigue.py``.
    """

    league_woba: float
    woba_scale: float
    league_era: float
    pa_per_9: float
    siera_coef: List[float]
    tto_penalty: List[float]


#: Seeded from published league references (2024 MLB context); Tasks 4.2 / 5.2
#: overwrite ``siera_coef`` / ``tto_penalty`` with fitted numbers from their
#: committed ``dev/mlb_pitching/fit_*.py`` scripts.
LEAGUE_BASELINES: Dict[int, PitchingConstants] = {
    2021: PitchingConstants(
        league_woba=0.312,
        woba_scale=1.25,
        league_era=4.26,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=[0.0, 0.005, 0.010],
    ),
    2022: PitchingConstants(
        league_woba=0.309,
        woba_scale=1.25,
        league_era=3.96,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=[0.0, 0.005, 0.010],
    ),
    2023: PitchingConstants(
        league_woba=0.318,
        woba_scale=1.25,
        league_era=4.33,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=[0.0, 0.005, 0.010],
    ),
    2024: PitchingConstants(
        league_woba=0.310,
        woba_scale=1.25,
        league_era=4.15,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=[0.0, 0.005, 0.010],
    ),
}


def get_baselines(season: int) -> PitchingConstants:
    """Resolve a season's :class:`PitchingConstants`, falling back to the nearest committed season.

    Args:
        season: MLB season year, e.g. ``2024``.

    Returns:
        PitchingConstants: The committed baseline for ``season``, or the
        nearest committed season if ``season`` is not in ``LEAGUE_BASELINES``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitching_constants import get_baselines
            get_baselines(2024).league_era
    """
    if season in LEAGUE_BASELINES:
        return LEAGUE_BASELINES[season]
    nearest = min(LEAGUE_BASELINES, key=lambda yr: abs(yr - season))
    return LEAGUE_BASELINES[nearest]
