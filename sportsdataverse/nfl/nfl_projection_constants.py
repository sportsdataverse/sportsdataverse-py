"""Projection-spine shared constants, scoring formats, validation metrics, and the
as-of-date leakage split (position-agnostic).

Methodology references (no code copied): Tom Tango's "Marcel the Monkey" forecaster;
Pro-Football-Reference Approximate Value; the WOPR formula (``1.5*target_share +
0.7*air_yards_share``). Cited here per the project's methodology-attribution
convention.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Tuple

import numpy as np
import polars as pl
from sportsdataverse._common.metrics import (
    brier_score as brier_score,
    log_loss_score as log_loss_score,
    mae as mae,
    spearman_corr as spearman_corr,
)

# ---------------------------------------------------------------------------
# Scoring formats
# ---------------------------------------------------------------------------

SCORING_PPR: Dict[str, float] = {
    "passing_yards": 0.04,
    "passing_tds": 4.0,
    "interceptions": -2.0,
    "rushing_yards": 0.1,
    "rushing_tds": 6.0,
    "receptions": 1.0,
    "receiving_yards": 0.1,
    "receiving_tds": 6.0,
    "fumbles_lost": -2.0,
}
SCORING_HALF: Dict[str, float] = {**SCORING_PPR, "receptions": 0.5}
SCORING_STANDARD: Dict[str, float] = {**SCORING_PPR, "receptions": 0.0}


# ---------------------------------------------------------------------------
# Per-position fitted constants
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionConstants:
    """Fitted per-position-group constants for the projection spine.

    Attributes:
        recency_weights (tuple[float, ...]): Marcel recency weights, most
            recent season first.
        shrinkage_k (float): Regression-to-position-mean constant; shrink
            weight is ``k / (k + reliability)``.
        min_volume (float): Minimum season volume for aging-curve pairs.
        aging_base_age (float): Reference (peak) age for the position.
        base_availability (float): Position base availability rate in [0, 1].
        fp_calibration (tuple[float, float]): ``(intercept, slope)`` of the
            per-position fantasy-points calibration ``a + b * raw_proj_fp``.
        aging_damping (float): Fitted weight in [0, 1] on the aging-curve
            ratio (applied as ``1 + damping * (ratio - 1)``); 0 disables the
            curve where its holdout signal is noise-dominated.
    """

    recency_weights: Tuple[float, ...] = (5.0, 4.0, 3.0)
    shrinkage_k: float = 8.0
    min_volume: float = 40.0
    aging_base_age: float = 26.0
    base_availability: float = 0.85
    fp_calibration: Tuple[float, float] = (0.0, 1.0)
    aging_damping: float = 1.0


# recency_weights / aging_damping / shrinkage_k fitted jointly by
# dev/nfl_projection/fit_shrinkage.py (2026-07-08, fold-based revision after the
# oracle-gate review): grid over weight/damping candidates x
# scipy.optimize.minimize_scalar on k (bounded 0.01-400), minimizing POOLED
# AS-OF FOLD MAE(proj_ppg, realized_ppg) on fold targets 2022 (hist 2020-21)
# and 2023 (hist 2020-22) — the 2024 holdout was NOT touched during fitting.
# Single out-of-sample 2024 evaluation (MAE vs last-season carry-forward MAE):
#   QB w=(3,1,0) d=0.0 k=338.99 -> spearman 0.6146, MAE 2.4949 (carry 3.8881)
#   RB w=(3,1,0) d=0.0 k=2.02   -> spearman 0.7240, MAE 2.9397 (carry 2.8356; RED)
#   WR w=(3,1,0) d=1.0 k=3.26   -> spearman 0.6599, MAE 3.0176 (carry 3.3225)
#   TE w=(1,0,0) d=0.0 k=0.78   -> spearman 0.7285, MAE 2.0338 (carry 2.1147)
# RB is the one position whose Marcel blend does NOT beat single-season
# carry-forward out-of-sample (documented xfail in the oracle gate).
# aging_damping=0 where the 4-season delta-method curve is noise-dominated on
# the folds (curve still fit + exposed; the damping gates its application).
# base_availability fitted by dev/nfl_projection/fit_availability.py
# (2026-07-08): all-player mean availability_rate per position over the
# committed 2021-2023 snap corpus (population-consistent EB prior; minimizes
# holdout games MAE 4.0192): QB 0.4746 (n=252), RB 0.5976 (n=496),
# WR 0.6397 (n=737), TE 0.6883 (n=414). FINDING: the plan's "RB < QB" sanity
# expectation is INVERTED in this data at every conditioning tried (all-player
# AND per-team-primary QB 0.8407 < RB 0.9081); the snap-count crosswalk was
# investigated per plan and verified clean (star QBs exact vs weekly stats,
# 0/247 QB seasons missing, 84% within-1-game agreement) — QB is a
# winner-take-all position, so snap-derived availability folds benching/depth
# churn into QB unavailability. The EB prior strength lives in
# nfl_availability.EB_PRIOR_SEASONS (fitted 0.485).
# fp_calibration fitted by dev/nfl_projection/fit_fantasy_calibration.py
# (2026-07-08): numpy.polyfit deg-1 of realized season fantasy points on raw
# projected fantasy points, pooled over as-of folds target=2022 (hist 2020-21)
# and target=2023 (hist 2020-22), players with >= 8 realized games:
#   QB (138.9270, 0.3833) n=66; RB (50.0169, 0.6174) n=150;
#   WR (20.2288, 0.8215) n=244; TE (26.0575, 0.6898) n=122 (refit after the
#   fold-based Marcel-constant revision).
# The calibration is a per-position monotone (linear) level correction — it
# fixes systematic totals bias; rank metrics (Spearman gates) are unaffected.
POSITION_CONSTANTS: Dict[str, PositionConstants] = {
    "QB": PositionConstants(
        recency_weights=(3.0, 1.0, 0.0),
        shrinkage_k=338.99,
        min_volume=100.0,
        aging_base_age=27.0,
        base_availability=0.4746,
        aging_damping=0.0,
        fp_calibration=(138.9270, 0.3833),
    ),
    "RB": PositionConstants(
        recency_weights=(3.0, 1.0, 0.0),
        shrinkage_k=2.02,
        min_volume=60.0,
        aging_base_age=25.0,
        base_availability=0.5976,
        aging_damping=0.0,
        fp_calibration=(50.0169, 0.6174),
    ),
    "WR": PositionConstants(
        recency_weights=(3.0, 1.0, 0.0),
        shrinkage_k=3.26,
        min_volume=40.0,
        aging_base_age=26.0,
        base_availability=0.6397,
        aging_damping=1.0,
        fp_calibration=(20.2288, 0.8215),
    ),
    "TE": PositionConstants(
        recency_weights=(1.0, 0.0, 0.0),
        shrinkage_k=0.78,
        min_volume=30.0,
        aging_base_age=27.0,
        base_availability=0.6883,
        aging_damping=0.0,
        fp_calibration=(26.0575, 0.6898),
    ),
    "DEFAULT": PositionConstants(),
}


def get_position_constants(pos_group: str) -> PositionConstants:
    """Look up the fitted constants for a position group.

    Unknown / fringe position groups (nflverse ships ``LS``, ``SPEC``, ``OL``,
    etc.) resolve to the documented ``DEFAULT`` bucket rather than raising.

    Args:
        pos_group (str): Position group key (``"QB"``, ``"RB"``, ``"WR"``,
            ``"TE"``, or anything else for the default bucket).

    Returns:
        PositionConstants: The per-position constants record.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_projection_constants import get_position_constants
            get_position_constants("RB").shrinkage_k
    """
    return POSITION_CONSTANTS.get(pos_group, POSITION_CONSTANTS["DEFAULT"])


@dataclass(frozen=True)
class ProjectionConfig:
    """Projection-spine configuration knobs.

    Attributes:
        team_games (int): Regular-season team games (17 from 2021).
        min_realized_games (int): Realized-games floor for oracle joins.
        scoring (dict[str, float]): Default scoring format (PPR).
    """

    team_games: int = 17
    min_realized_games: int = 8
    scoring: Dict[str, float] = field(default_factory=lambda: dict(SCORING_PPR))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Decile (or ``n_bins``) calibration table for probability predictions.

    Args:
        y_true (np.ndarray): Binary (or rate-valued) outcomes.
        p_pred (np.ndarray): Predicted probabilities in ``[0, 1]``.
        n_bins (int): Number of equal-width probability bins.

    Returns:
        pl.DataFrame: Columns ``bin_mid:Float64, mean_pred:Float64,
        mean_actual:Float64, n:Int64``, one row per non-empty bin.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_projection_constants import calibration_table
            tbl = calibration_table(np.array([1, 0, 1]), np.array([0.8, 0.2, 0.7]))
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(
            pl.col("p").mean().alias("mean_pred"),
            pl.col("y").mean().alias("mean_actual"),
            pl.len().alias("n"),
        )
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", pl.col("n").cast(pl.Int64))
    )


def as_of_season_split(df: pl.DataFrame, target_season: int, *, season_col: str = "season") -> pl.DataFrame:
    """Leakage boundary: only seasons strictly before ``target_season`` are visible.

    Args:
        df (pl.DataFrame): Frame carrying a season column.
        target_season (int): The season being projected; excluded along with
            everything after it.
        season_col (str): Name of the season column.

    Returns:
        pl.DataFrame: Rows with ``season < target_season``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_projection_constants import as_of_season_split
            hist = as_of_season_split(pl.DataFrame({"season": [2022, 2023, 2024]}), 2024)
    """
    return df.filter(pl.col(season_col) < target_season)
