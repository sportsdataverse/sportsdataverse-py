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
from sportsdataverse._common.metrics import (
    calibration_table as calibration_table,
    mae as mae,
    spearman_corr as spearman_corr,
)

# --- bundled-artifact paths (Phase 2 / Phase 3 write these .ubj files) ---
STUFF_PLUS_ARTIFACT = "mlb_stuff_plus.ubj"
COMMAND_PLUS_ARTIFACT = "mlb_command_plus.ubj"

#: "+"-scale: 100 = league average, one league SD of predicted run value maps
#: to this many "+"-scale points (sign inverted: negative run value is good).
PLUS_SCALE: float = 10.0

#: Centering reference for ``stuff_plus``. The model is TRAINED on
#: ``dev/mlb_pitching/fit_stuff_plus.py``'s 2023-season, 30-pitcher corpus, but
#: that corpus is itself workhorse-selected (top-N by pitch volume in one
#: probe week) and is NOT representative of the general league -- centering
#: the "+"-scale to the training set's own mean run-value shifted a genuinely
#: held-out 2024 population's average to ~97.6, outside the ±0.5 calibration
#: target. These constants are instead the mean/sd of ``stuff_rv_hat`` scored
#: over ``tests/fixtures/mlb_pitching/pitcher_holdout_season_2024.parquet``
#: (15 real, NON-training pitchers, full 2024 season, 38,699 pitches after
#: dropna) -- a broader, out-of-training reference population. See
#: ``dev/mlb_pitching/recalibrate_stuff_plus.py`` for the recomputation.
#: Spearman-based oracle legs are unaffected by this choice (rank correlation
#: is invariant to any linear recentering with ``sd_rv > 0``).
STUFF_LEAGUE_MEAN_RV: float = 0.0008117794641293585
STUFF_LEAGUE_SD_RV: float = 0.022242603823542595

#: Same centering-reference reasoning as ``STUFF_LEAGUE_MEAN_RV``/``_SD_RV``
#: above: computed from ``dev/mlb_pitching/fit_command_plus.py``'s trained
#: (unchanged) booster re-scored over the held-out
#: ``pitcher_holdout_season_2024.parquet`` population (38,803 pitches after
#: dropna), not the workhorse-selected training corpus. Retrained once after
#: the fixed-categorical-encoding fix (see ``_CATEGORICAL_CODE_MAPS`` in
#: ``mlb_command_plus.py``) -- the model's ``pitch_type``/``stand``/
#: ``p_throws`` codes must be identical between train time and score time.
COMMAND_LEAGUE_MEAN_RV: float = -0.0009078294970095158
COMMAND_LEAGUE_SD_RV: float = 0.0403415784239769


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


#: Task 5.2 fitting output (``dev/mlb_pitching/fit_fatigue.py``, OLS on the
#: real ``pitcher_season_pitches_2023_sample.parquet`` fixture, 13,996 pitches
#: after dropna): ``run_value ~ C(times_through_order) + cum_pitches_game +
#: velo_drop_from_start``, TTO=1 reference. Monotone increasing as designed
#: (fatigue -> more offense-favorable run value at higher TTO). This is the
#: workload/velocity-CONTROLLED marginal effect of TTO, so it is larger in
#: magnitude than :func:`sportsdataverse.mlb.mlb_pitch_fatigue.tto_penalty_table`'s
#: raw, unconditional per-TTO mean-run-value gap (0.0029 / 0.0045 on the same
#: fixture) -- TTO and cumulative pitch count are correlated, so the
#: unconditional gap partly reflects workload, not pure "seeing the pitcher a
#: third time" familiarity. The Task 5.2 gate (in
#: ``tests/mlb/test_mlb_pitch_fatigue.py``) validates the raw table's
#: monotonicity/magnitude directly against its own observed values, per the
#: plan's Task 5.1/5.2 split.
TTO_PENALTY_FITTED: List[float] = [0.0, 0.024451416002495174, 0.0436238547119437]

#: Seeded from published league references (2024 MLB context) for
#: ``siera_coef`` (Task 4.2 has not yet overwritten it with a fitted OLS);
#: ``tto_penalty`` uses the Task 5.2 fitted value above.
LEAGUE_BASELINES: Dict[int, PitchingConstants] = {
    2021: PitchingConstants(
        league_woba=0.312,
        woba_scale=1.25,
        league_era=4.26,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=TTO_PENALTY_FITTED,
    ),
    2022: PitchingConstants(
        league_woba=0.309,
        woba_scale=1.25,
        league_era=3.96,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=TTO_PENALTY_FITTED,
    ),
    2023: PitchingConstants(
        league_woba=0.318,
        woba_scale=1.25,
        league_era=4.33,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=TTO_PENALTY_FITTED,
    ),
    2024: PitchingConstants(
        league_woba=0.310,
        woba_scale=1.25,
        league_era=4.15,
        pa_per_9=38.0,
        siera_coef=[6.0, -12.0, 8.0, -3.0, 2.0, 1.0],
        tto_penalty=TTO_PENALTY_FITTED,
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
