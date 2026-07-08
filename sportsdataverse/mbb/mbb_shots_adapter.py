"""Adapter: NCAA HTML shot events + ESPN ``load_mbb_shots`` -> one canonical shot frame.

The two shot sources disagree on coordinates (ESPN grid units vs feet),
identifiers, and vocabulary; every downstream shot-quality model consumes
the single canonical frame this module produces (see the plan's schema:
``game_id, season, team_id, shooter_id, shot_x, shot_y, dist_ft, shot_zone,
shot_type, made, point_value, period, sec_left, source``).
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants, three_point_radius

__all__ = [
    "classify_point_value",
    "classify_zone_geometry",
    "classify_zone_type",
]


def classify_point_value(dist_ft: float, x: float, y: float, *, league: str, season: int) -> int:
    """2 or 3 from basket-relative geometry (arc radius + corner band).

    Args:
        dist_ft: Euclidean distance from the basket, feet.
        x: Lateral offset from the basket, feet (baseline direction).
        y: Distance up-court from the basket, feet.
        league: ``"mens"`` or ``"womens"``.
        season: Season-ending year (selects the arc era).

    Returns:
        ``3`` at/beyond the arc or in the corner band, else ``2``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import classify_point_value
            classify_point_value(24.0, 0.0, 24.0, league="mens", season=2020)
    """
    c = get_constants(league)
    arc = three_point_radius(league, season)
    is_corner3 = abs(x) >= c.corner_x_ft and abs(y) <= c.corner_y_ft
    return 3 if (dist_ft >= arc or is_corner3) else 2


def classify_zone_geometry(dist_ft: float, x: float, y: float, *, league: str, season: int) -> str:
    """Shot zone from geometry: ``rim | paint | mid | corner3 | abovebreak3``.

    Args:
        dist_ft: Euclidean distance from the basket, feet.
        x: Lateral offset from the basket, feet.
        y: Distance up-court from the basket, feet.
        league: ``"mens"`` or ``"womens"``.
        season: Season-ending year (selects the arc era).

    Returns:
        One of ``rim``, ``paint``, ``mid``, ``corner3``, ``abovebreak3``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_geometry
            classify_zone_geometry(2.0, 0.0, 2.0, league="mens", season=2020)
    """
    c = get_constants(league)
    if classify_point_value(dist_ft, x, y, league=league, season=season) == 3:
        return "corner3" if (abs(x) >= c.corner_x_ft and abs(y) <= c.corner_y_ft) else "abovebreak3"
    if dist_ft <= c.rim_radius_ft:
        return "rim"
    if dist_ft <= c.paint_radius_ft:
        return "paint"
    return "mid"


_RIM_WORDS = ("dunk", "layup", "lay up", "tip", "close", "putback")


def classify_zone_type(type_text: "str | None") -> "str | None":
    """Collapse a source shot-type label to ``rim | arc3 | jump``.

    Note: the 2025+ ESPN shots release carries NO three-point marker in
    ``type_text`` (vocabulary is JumpShot/LayUpShot/DunkShot/TipShot), so
    ``arc3`` typically comes from geometry/score_value there; the branch
    exists for sources that do label threes.

    Args:
        type_text: Source label (e.g. ``"DunkShot"``); ``None`` passes
            through.

    Returns:
        ``rim``, ``arc3``, ``jump``, or ``None`` for null input.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import classify_zone_type
            classify_zone_type("DunkShot")
    """
    if type_text is None:
        return None
    t = type_text.lower()
    if any(w in t for w in _RIM_WORDS):
        return "rim"
    if "three" in t or "3pt" in t or "3-pt" in t:
        return "arc3"
    return "jump"
