"""Shot-value spine: per-league constants + validation metrics (league-agnostic).

The shot-value models (``nba_shot_value``) are one league-agnostic core switched
by ``league_id`` (``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League). Every
league-specific number — court geometry, shooter-talent shrinkage ``k`` —
lives here keyed by ``league_id`` so no algorithm function hard-codes a
men's/women's value. The validation metrics (points calibration, split-half
reliability, MAE, points-per-shot) back the oracle gates.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


def points_calibration_error(exp_points: np.ndarray, actual_points: np.ndarray) -> float:
    """Relative gap between total expected and total actual points.

    Args:
        exp_points: Per-shot expected points (``xpoints``).
        actual_points: Per-shot realized points (``shot_made_flag * shot_value``).

    Returns:
        ``|Σexp − Σactual| / Σactual`` (``0.0`` when the actual total is 0).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import points_calibration_error
            points_calibration_error(np.array([2.0]), np.array([2.0]))
    """
    total = float(np.sum(actual_points))
    if total == 0.0:
        return 0.0
    return abs(float(np.sum(exp_points)) - total) / total


def split_half_reliability(first_half: np.ndarray, second_half: np.ndarray) -> float:
    """Pearson correlation between paired per-player halves.

    Args:
        first_half: Per-player metric on one half of the shots.
        second_half: Per-player metric on the other half (same player order).

    Returns:
        Pearson ``r`` (``nan`` when fewer than two players).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import split_half_reliability
            split_half_reliability(np.array([1.0, 2.0, 3.0]), np.array([2.0, 4.0, 6.0]))
    """
    a, b = np.asarray(first_half, dtype=float), np.asarray(second_half, dtype=float)
    if a.size < 2:
        return float("nan")
    return float(np.corrcoef(a, b)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two aligned arrays.

    Args:
        a: First array.
        b: Second array.

    Returns:
        ``mean(|a − b|)``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def pps(points: np.ndarray, attempts: np.ndarray) -> float:
    """Points per shot: ``Σpoints / Σattempts``.

    Args:
        points: Per-group points.
        attempts: Per-group attempts.

    Returns:
        ``Σpoints / Σattempts`` (``0.0`` when there are no attempts).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import pps
            pps(np.array([2.0, 0.0, 3.0]), np.array([1.0, 1.0, 1.0]))
    """
    n = float(np.sum(attempts))
    return float(np.sum(points)) / n if n else 0.0


@dataclass(frozen=True)
class CourtGeometry:
    """Per-league court constants used by the zone/geometry logic.

    Attributes:
        rim_radius_ft: Radius of the restricted-area / rim zone (feet).
        corner3_loc_x_abs: ``|loc_x|`` (tenths-of-foot stats.nba.com units)
            at/above which a baseline shot is a corner three.
        three_point_radius_ft: Three-point arc radius (feet).
    """

    rim_radius_ft: float
    corner3_loc_x_abs: int
    three_point_radius_ft: float


@dataclass(frozen=True)
class ShotValueConfig:
    """Runtime knobs for the shot-value orchestrators.

    Attributes:
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        min_attempts_talent: Minimum shots for a stable talent estimate.
    """

    league_id: str = "00"
    min_attempts_talent: int = 50


# NBA (== G-League court) and WNBA geometry. Corner-3 loc_x and the 3pt radius
# differ between the men's and women's court; the rim radius is shared.
LEAGUE_COURT: "dict[str, CourtGeometry]" = {
    "00": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=220, three_point_radius_ft=23.75),
    "20": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=220, three_point_radius_ft=23.75),
    "10": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=200, three_point_radius_ft=22.13),
}

# ``"00"`` fitted split-half on the 2022-23 fixture
# (dev/nba_shot_value/fit_shrinkage_k.py, 2026-07-08): cross-half reliability
# 0.699 raw → 0.707 shrunk. G-League reuses it; ``"10"`` is the Phase-5
# women's fit (seeded until captured).
TALENT_SHRINKAGE_K: "dict[str, float]" = {"00": 70.1, "20": 70.1, "10": 60.0}

# stats.nba.com ``shot_zone_basic`` → canonical collapsed zone.
ZONE_COLLAPSE: "dict[str, str]" = {
    "Restricted Area": "rim",
    "In The Paint (Non-RA)": "paint_non_ra",
    "Mid-Range": "mid_range",
    "Left Corner 3": "corner_3",
    "Right Corner 3": "corner_3",
    "Above the Break 3": "above_break_3",
    "Backcourt": "backcourt",
}


def get_court(league_id: str) -> CourtGeometry:
    """Court geometry for a league.

    Args:
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.

    Returns:
        The frozen :class:`CourtGeometry` for that league.

    Raises:
        ValueError: Unknown ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import get_court
            get_court("00").corner3_loc_x_abs
    """
    try:
        return LEAGUE_COURT[league_id]
    except KeyError as exc:
        raise ValueError(f"unknown league_id {league_id!r}; expected one of {sorted(LEAGUE_COURT)}") from exc


def get_shrinkage_k(league_id: str) -> float:
    """Shooter-talent shrinkage ``k`` for a league.

    Args:
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.

    Returns:
        The pseudo-attempt shrinkage constant (fitted split-half).

    Raises:
        ValueError: Unknown ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import get_shrinkage_k
            get_shrinkage_k("00")
    """
    try:
        return TALENT_SHRINKAGE_K[league_id]
    except KeyError as exc:
        raise ValueError(f"unknown league_id {league_id!r}; expected one of {sorted(TALENT_SHRINKAGE_K)}") from exc
