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
from sportsdataverse._common.metrics import (
    mae as mae,
)


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


# NBA (== G-League court) and WNBA geometry for the CURRENT arc. Corner-3 loc_x
# and the 3pt radius differ between the men's and women's court; the rim radius
# is shared. Older seasons used shorter arcs -- see COURT_ERAS / get_court(season=).
LEAGUE_COURT: "dict[str, CourtGeometry]" = {
    "00": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=220, three_point_radius_ft=23.75),
    "20": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=220, three_point_radius_ft=23.75),
    "10": CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=200, three_point_radius_ft=22.13),
}

# Three-point arc eras, keyed by the FIRST season (END year: 1995 = 1994-95)
# the geometry applies to; a season maps to the latest era at or before it.
# Seasons before the first key use the first era (the NBA's 1979-80 arc equals
# the current one). Sources: sdv-internal-refs rules/nba.yaml
# nba-1995-three-point-line-shortened / nba-1998-three-point-line-restored
# (uniform 22 ft, 1994-95 through 1996-97, so the corner equals the arc);
# rules/wnba.yaml wnba-2004-three-point-line-20-6 (19 ft 9 in 1997-2003,
# 20 ft 6.25 in 2004-2012) and wnba-2013-three-point-line-fiba (22 ft 1.75 in,
# corner 200 tenths, 2013+). The G League has always used the NBA court.
COURT_ERAS: "dict[str, dict[int, CourtGeometry]]" = {
    "00": {
        1980: LEAGUE_COURT["00"],
        1995: CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=220, three_point_radius_ft=22.0),
        1998: LEAGUE_COURT["00"],
    },
    "20": {2002: LEAGUE_COURT["20"]},
    "10": {
        1997: CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=198, three_point_radius_ft=19.75),
        2004: CourtGeometry(rim_radius_ft=4.0, corner3_loc_x_abs=200, three_point_radius_ft=20.52),
        2013: LEAGUE_COURT["10"],
    },
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


def get_court(league_id: str, season: "int | None" = None) -> CourtGeometry:
    """Court geometry for a league, for the three-point arc in force that season.

    Args:
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        season: Season as the END year for the NBA / G League (``1996`` =
            1995-96) or the calendar year for the WNBA. ``None`` returns the
            current geometry.

    Returns:
        The frozen :class:`CourtGeometry` for that league and era
        (:data:`COURT_ERAS`).

    Raises:
        ValueError: Unknown ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value_constants import get_court
            get_court("00").corner3_loc_x_abs
            get_court("00", season=1996).three_point_radius_ft  # 22.0
            get_court("10", season=2010).three_point_radius_ft  # 20.52
    """
    try:
        eras = COURT_ERAS[league_id]
    except KeyError as exc:
        raise ValueError(f"unknown league_id {league_id!r}; expected one of {sorted(LEAGUE_COURT)}") from exc
    if season is None:
        return LEAGUE_COURT[league_id]
    starts = sorted(eras)
    applicable = [yr for yr in starts if yr <= season]
    return eras[applicable[-1] if applicable else starts[0]]


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
