"""Shot-quality rule + fitted constants (league-agnostic engine, per-league values).

Arc radii are NCAA rule constants: the men's three-point line moved to
22 ft 1.75 in for the 2019-20 season, the women's for 2021-22. Published
zone baselines are *methodology* references (Hoop-Math / Barttorvik
shot-zone efficiency splits) used only as oracle tolerance targets — no
code from those sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field

ARC_OLD_FT = 20.75  # 20 ft 9 in
ARC_NEW_FT = 22.15  # 22 ft 1.75 in


@dataclass(frozen=True)
class ShotQualityConstants:
    """Per-league rule + fitted constants for the shot-quality spine.

    Attributes:
        arc_radius_by_season: ``{(first_season, last_season): radius_ft}``
            eras for the three-point arc.
        paint_radius_ft: Outer edge of the paint/floater range (beyond the
            rim zone, inside this = ``paint``).
        rim_radius_ft: Radius of the ``rim`` zone.
        corner_x_ft: ``|x|`` at/beyond this (with a small ``y``) = corner 3.
        corner_y_ft: Baseline band height for the corner-3 test.
        shrink_k_zone: Empirical-Bayes cell-toward-zone-mean shrinkage
            (pseudo-attempts; refined by the Phase-1 fit).
        shrink_k_talent: Shooter-talent shrinkage (fitted in Phase 3; 0.0
            until fit).
    """

    arc_radius_by_season: "dict[tuple[int, int], float]" = field(default_factory=dict)
    paint_radius_ft: float = 15.0
    rim_radius_ft: float = 4.0
    corner_x_ft: float = 21.0
    corner_y_ft: float = 9.0
    shrink_k_zone: float = 100.0
    shrink_k_talent: float = 0.0


LEAGUE_CONSTANTS: "dict[str, ShotQualityConstants]" = {
    "mens": ShotQualityConstants(
        arc_radius_by_season={(2009, 2019): ARC_OLD_FT, (2020, 2100): ARC_NEW_FT},
        # split-half fit on the 2025 train fixture (dev/mbb_shot_quality/
        # fit_talent_k.py, 2026-07-08): MSE 0.0187 at k vs 0.0337 unshrunk
        shrink_k_talent=233.2,
    ),
    "womens": ShotQualityConstants(
        arc_radius_by_season={(2009, 2021): ARC_OLD_FT, (2022, 2100): ARC_NEW_FT},
        # split-half fit on the 2026 wbb train fixture (dev/mbb_shot_quality/
        # fit_talent_k.py, 2026-07-08): MSE 0.0105 at k vs 0.0163 unshrunk
        shrink_k_talent=92.4,
    ),
}

# methodology references (Hoop-Math / Barttorvik zone splits) — oracle
# tolerance targets only, cited in the oracle test
PUBLISHED_ZONE_BASELINES: "dict[str, dict[str, float]]" = {
    "mens": {"rim": 0.62, "paint": 0.40, "mid": 0.36, "corner3": 0.38, "abovebreak3": 0.34},
    "womens": {"rim": 0.55, "paint": 0.38, "mid": 0.35, "corner3": 0.33, "abovebreak3": 0.30},
}


# OBSERVED national make rates, captured 2026-07-08 by summing every player
# row of barttorvik.com getadvstats (mens CSV year=2025, n_att 2P=406,493
# 3P=260,289; womens JSON year=2026 -- era-matched to the wbb shots-release
# floor -- n_att 2P=435,079 3P=223,495). These are the hard external
# calibration anchors for the shot-quality oracle gates
# (PUBLISHED_ZONE_BASELINES above are literature ESTIMATES, sanity-band only).
BART_NATIONAL_SPLITS: "dict[str, dict[str, float]]" = {
    "mens": {"fg2_pct": 0.5082, "fg3_pct": 0.3376},
    "womens": {"fg2_pct": 0.4547, "fg3_pct": 0.3087},
}


def get_constants(league: str) -> ShotQualityConstants:
    """League constants bundle for the shot-quality spine.

    Args:
        league: ``"mens"`` or ``"womens"``.

    Returns:
        The frozen :class:`ShotQualityConstants` for that league.

    Raises:
        ValueError: Unknown league.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants
            get_constants("mens").rim_radius_ft
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"unknown league {league!r}; expected one of {list(LEAGUE_CONSTANTS)}") from exc


def three_point_radius(league: str, season: int) -> float:
    """Three-point arc radius (feet) for a league x season.

    Args:
        league: ``"mens"`` or ``"womens"``.
        season: Season-ending year (e.g. ``2020`` = 2019-20).

    Returns:
        Arc radius in feet.

    Raises:
        ValueError: Unknown league or unconfigured season.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shot_quality_constants import three_point_radius
            three_point_radius("mens", 2020)
    """
    for (lo, hi), radius in get_constants(league).arc_radius_by_season.items():
        if lo <= season <= hi:
            return radius
    raise ValueError(f"no arc radius configured for {league} season {season}")
