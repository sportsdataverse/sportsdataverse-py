"""Adapter: NCAA HTML shot events + ESPN ``load_mbb_shots`` -> one canonical shot frame.

The two shot sources disagree on coordinates (ESPN grid units vs feet),
identifiers, and vocabulary; every downstream shot-quality model consumes
the single canonical frame this module produces (see the plan's schema:
``game_id, season, team_id, shooter_id, shot_x, shot_y, dist_ft, shot_zone,
shot_type, made, point_value, period, sec_left, source``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants, three_point_radius

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sportsdataverse.mbb.mbb_ncaa_models import ShotEvent

__all__ = [
    "CANONICAL_SHOT_SCHEMA",
    "classify_point_value",
    "classify_zone_geometry",
    "classify_zone_type",
    "shot_events_to_frame",
]

# the one shot schema every downstream shot-quality model consumes
CANONICAL_SHOT_SCHEMA: "dict[str, pl.DataType]" = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "shooter_id": pl.Utf8,
    "shot_x": pl.Float64,
    "shot_y": pl.Float64,
    "dist_ft": pl.Float64,
    "shot_zone": pl.Utf8,
    "shot_type": pl.Utf8,
    "made": pl.Boolean,
    "point_value": pl.Int8,
    "period": pl.Int64,
    "sec_left": pl.Float64,
    "source": pl.Utf8,
}


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
    if "three" in t or "3pt" in t or "3-pt" in t:
        return "arc3"
    if any(w in t for w in _RIM_WORDS):
        return "rim"
    if "jump" in t or "shot" in t:
        return "jump"
    return None


def shot_events_to_frame(
    events: list[ShotEvent],
    *,
    season: int,
    league: str = "mens",
) -> pl.DataFrame:
    """Flatten NCAA HTML :class:`ShotEvent` objects to the canonical frame.

    The NCAA SVG shot maps carry location + made/miss but no shot-type label
    (``shot_type = "unknown"``); ``point_value``/``shot_zone`` come from the
    geometry classifiers. The parser-phase ``pts`` field is the MADE flag
    (1/0), not the point value.

    Args:
        events: Parsed shot events (``create_shot_event_data`` output).
        season: Season-ending year the events belong to.
        league: ``"mens"`` or ``"womens"``.

    Returns:
        The canonical shot frame (``CANONICAL_SHOT_SCHEMA``); empty input
        returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import shot_events_to_frame
            df = shot_events_to_frame(events, season=2025)
    """
    if not events:
        return pl.DataFrame(schema=CANONICAL_SHOT_SCHEMA)
    rows = []
    for e in events:
        x, y, dist = float(e.loc.x), float(e.loc.y), float(e.dist)
        rows.append(
            {
                "game_id": None,
                "season": season,
                "team_id": str(e.team.team) if e.team is not None else None,
                "shooter_id": str(e.player.code) if e.player is not None else None,
                "shot_x": x,
                "shot_y": y,
                "dist_ft": dist,
                "shot_zone": classify_zone_geometry(dist, x, y, league=league, season=season),
                "shot_type": "unknown",
                "made": e.pts == 1,
                "point_value": classify_point_value(dist, x, y, league=league, season=season),
                "period": None,
                "sec_left": None,
                "source": "ncaa",
            }
        )
    return pl.DataFrame(rows, schema=CANONICAL_SHOT_SCHEMA)
