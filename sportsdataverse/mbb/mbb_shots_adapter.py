"""Adapter: NCAA HTML shot events + ESPN ``load_mbb_shots`` -> one canonical shot frame.

The two shot sources disagree on coordinates (ESPN grid units vs feet),
identifiers, and vocabulary; every downstream shot-quality model consumes
the single canonical frame this module produces (see the plan's schema:
``game_id, season, team_id, shooter_id, shot_x, shot_y, dist_ft, shot_zone,
shot_type, made, point_value, period, sec_left, source``).
"""

from __future__ import annotations

import math

from typing import TYPE_CHECKING

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants, three_point_radius

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sportsdataverse.mbb.mbb_ncaa_models import ShotEvent

__all__ = [
    "CANONICAL_SHOT_SCHEMA",
    "classify_point_value",
    "classify_zone_geometry",
    "classify_zone_type",
    "espn_shots_to_canonical",
    "fit_espn_court_scale",
    "mbb_shot_data",
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


def period_and_sec_left(minute: float, *, league: str, season: int) -> "tuple[int | None, float | None]":
    """Derive ``(period, sec_left)`` from a shot's ascending game-clock minute.

    ``ShotEvent.min`` is ascending ELAPSED minutes for the whole game, so the
    period has to be recovered from the era's period schedule -- and the era is
    not a constant.

    **WBB is HALVES before season 2016**, quarters from 2016; MBB has always
    been halves. :func:`~sportsdataverse.mbb.mbb_ncaa_stints.start_time_from_period`
    takes a BOOLEAN ``is_women_game`` and unconditionally assumes quarters for
    women, so using it here would silently label a pre-2016 WBB game's first
    half as "quarter 1" and put ``sec_left`` on a 10-minute clock that ran for
    20. The season-aware model is resolved through
    :func:`~sportsdataverse.scrape.ncaa.parse.wbb_period_model` instead.

    Args:
        minute: Ascending elapsed game time in minutes (``ShotEvent.min``).
        league: ``"mbb"`` or ``"wbb"``.
        season: Season-ending year.

    Returns:
        ``(period, sec_left)`` -- 1-indexed period and seconds remaining IN that
        period. ``(None, None)`` when ``minute`` is missing or negative, so a
        bad clock stays unresolved rather than becoming a confident wrong period.

    **Seconds-left-in-PERIOD, matching the ESPN path.** This is not an
    assumption: :func:`espn_shots_to_canonical` builds ``sec_left`` from
    ESPN's ``clock_display_value`` (``"12:34"`` -> ``12*60 + 34``), which is the
    DESCENDING clock within the current period, paired with ``period_number``.
    Both sources land in the same frame distinguished only by ``source``, so a
    consumer filtering across ESPN and NCAA rows would silently mix two
    definitions if these disagreed.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import period_and_sec_left
            period_and_sec_left(25.0, league="mbb", season=2024)   # (2, 900.0)
            period_and_sec_left(25.0, league="wbb", season=2024)   # (3, 300.0)
            period_and_sec_left(25.0, league="wbb", season=2014)   # (2, 900.0) -- halves era
    """
    if minute is None:
        return None, None
    try:
        m = float(minute)
    except (TypeError, ValueError):
        return None, None
    # NaN passes `m < 0` and then explodes in the floor division; +inf
    # overflows. Both must honour the (None, None) contract, not crash.
    if not math.isfinite(m) or m < 0:
        return None, None

    # This module speaks "mens"/"womens"; the release/league layer speaks
    # "mbb"/"wbb". Accept BOTH -- matching only one vocabulary would silently
    # give women's games the men's halves schedule and mislabel every quarter.
    if str(league).lower() in ("wbb", "womens", "women", "w"):
        from sportsdataverse.scrape.ncaa.parse import wbb_period_model

        periods, reg_seconds, ot_seconds = wbb_period_model(str(season))
    else:
        periods, reg_seconds, ot_seconds = 2, 1200, 300

    reg_len = reg_seconds / 60.0
    ot_len = ot_seconds / 60.0
    reg_total = periods * reg_len

    # A period BOUNDARY belongs to the period that just ended, at 0 seconds --
    # a buzzer-beater at 0:00 of the first half is period 1 / 0.0, not period 2
    # with a full clock. `ceil` puts the boundary on the closing period;
    # `m == 0` is the only tip-off case and needs the explicit branch.
    if m == 0.0:
        return 1, round(reg_len * 60.0, 1)

    if m <= reg_total:
        period = math.ceil(m / reg_len)
        start = (period - 1) * reg_len
        return period, round((reg_len - (m - start)) * 60.0, 1)

    over = m - reg_total
    ot_idx = math.ceil(over / ot_len)
    period = periods + ot_idx
    start = reg_total + (ot_idx - 1) * ot_len
    return period, round((ot_len - (m - start)) * 60.0, 1)


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
        # the NCAA ShotLocation frame is (x = up-court, y = lateral) -- the
        # OPPOSITE of the canonical schema (shot_x = lateral, shot_y =
        # up-court, matching the ESPN width axis); swap at ingestion
        x, y, dist = float(e.loc.y), float(e.loc.x), float(e.dist)
        # e.team is the team UNDER ANALYSIS; the shooter's team follows is_off
        shooting = e.team if e.is_off else e.opponent
        rows.append(
            {
                "game_id": None,
                "season": season,
                "team_id": str(shooting.team.name) if shooting is not None else None,
                "shooter_id": str(e.player.code) if e.player is not None else None,
                "shot_x": x,
                "shot_y": y,
                "dist_ft": dist,
                "shot_zone": classify_zone_geometry(dist, x, y, league=league, season=season),
                "shot_type": "unknown",
                "made": e.pts == 1,
                "point_value": classify_point_value(dist, x, y, league=league, season=season),
                **dict(
                    zip(
                        ("period", "sec_left"),
                        period_and_sec_left(e.min, league=league, season=season),
                    )
                ),
                "source": "ncaa",
            }
        )
    return pl.DataFrame(rows, schema=CANONICAL_SHOT_SCHEMA)


_COORD_SENTINEL = 1_000.0  # |coordinate| beyond this is int32-sentinel garbage
_FALLBACK_FEET_PER_UNIT = 1.0  # raw grid is ~feet (court width 0-50)


def fit_espn_court_scale(espn: pl.DataFrame, *, league: str, season: int) -> "tuple[float, float, float]":
    """Fit the ESPN raw-coordinate court scale: ``(origin_x, origin_y, feet_per_unit)``.

    The release's ``coordinate_{x,y}_raw`` grid is basket-anchored half-court
    (width 0-50, rim cluster near ``(25, 2)``). Origin = median raw
    coordinates of made rim-type shots; ``feet_per_unit`` = arc radius /
    median unit-distance of made threes from that origin -- fitted, not
    guessed, so a units change in the release shows up as a scale shift.

    Args:
        espn: ``load_mbb_shots``-shaped frame.
        league: ``"mens"`` or ``"womens"``.
        season: Season-ending year (selects the arc radius).

    Returns:
        ``(origin_x, origin_y, feet_per_unit)``; documented fallbacks
        ``(25.0, 2.0, 1.0)`` when either calibration subset is empty.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shots_adapter import fit_espn_court_scale
            scale = fit_espn_court_scale(espn, league="mens", season=2025)
    """
    valid = espn.filter(
        pl.col("coordinate_x_raw").is_not_null()
        & pl.col("coordinate_y_raw").is_not_null()
        & (pl.col("coordinate_x_raw").abs() < _COORD_SENTINEL)
        & (pl.col("coordinate_y_raw").abs() < _COORD_SENTINEL)
    )
    rim = valid.filter(
        pl.col("type_text").str.contains("(?i)dunk|layup|lay up|tip") & (pl.col("scoring_play") == True)  # noqa: E712
    )
    if rim.is_empty():
        return (25.0, 2.0, _FALLBACK_FEET_PER_UNIT)
    ox = float(rim.get_column("coordinate_x_raw").median())
    oy = float(rim.get_column("coordinate_y_raw").median())
    threes = valid.filter((pl.col("score_value") == 3) & (pl.col("scoring_play") == True))  # noqa: E712
    if threes.is_empty():
        return (ox, oy, _FALLBACK_FEET_PER_UNIT)
    unit_dist = threes.select(
        ((pl.col("coordinate_x_raw") - ox) ** 2 + (pl.col("coordinate_y_raw") - oy) ** 2).sqrt().alias("d")
    ).get_column("d")
    med = float(unit_dist.median())
    if med <= 0:
        return (ox, oy, _FALLBACK_FEET_PER_UNIT)
    return (ox, oy, three_point_radius(league, season) / med)


def espn_shots_to_canonical(
    espn: pl.DataFrame,
    *,
    league: str,
    season: int,
    scale: "tuple[float, float, float] | None" = None,
) -> pl.DataFrame:
    """ESPN ``load_mbb_shots`` frame -> the canonical shot frame.

    Field-goal attempts only (free throws and sentinel-coordinate rows are
    dropped). ``point_value`` comes from ``score_value`` -- the release
    populates it on misses too, and its ``type_text`` carries NO three-point
    marker, so ``arc3`` is value-derived. Coordinates are re-based to the
    fitted basket origin and scaled to feet.

    Args:
        espn: ``load_mbb_shots``-shaped frame.
        league: ``"mens"`` or ``"womens"``.
        season: Season-ending year.
        scale: Optional pre-fitted ``(origin_x, origin_y, feet_per_unit)``;
            fitted from ``espn`` when ``None``.

    Returns:
        The canonical shot frame (``CANONICAL_SHOT_SCHEMA``); empty input
        returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_loaders import load_mbb_shots
            from sportsdataverse.mbb.mbb_shots_adapter import espn_shots_to_canonical
            df = espn_shots_to_canonical(load_mbb_shots([2025]), league="mens", season=2025)
    """
    if espn.is_empty():
        return pl.DataFrame(schema=CANONICAL_SHOT_SCHEMA)
    fga = espn.filter(
        (pl.col("type_text").str.contains("(?i)free throw") == False)  # noqa: E712
        & pl.col("score_value").is_in([2, 3])
        & pl.col("coordinate_x_raw").is_not_null()
        & pl.col("coordinate_y_raw").is_not_null()
        & (pl.col("coordinate_x_raw").abs() < _COORD_SENTINEL)
        & (pl.col("coordinate_y_raw").abs() < _COORD_SENTINEL)
    )
    if fga.is_empty():
        return pl.DataFrame(schema=CANONICAL_SHOT_SCHEMA)
    ox, oy, fpu = scale if scale is not None else fit_espn_court_scale(espn, league=league, season=season)

    c = get_constants(league)
    out = fga.with_columns(
        ((pl.col("coordinate_x_raw") - ox) * fpu).alias("shot_x"),
        ((pl.col("coordinate_y_raw") - oy) * fpu).alias("shot_y"),
    ).with_columns(((pl.col("shot_x") ** 2 + pl.col("shot_y") ** 2).sqrt()).alias("dist_ft"))
    is_rim_type = pl.col("type_text").str.contains("(?i)dunk|layup|lay up|tip")
    out = out.with_columns(
        pl.when(pl.col("score_value") == 3)
        .then(pl.lit("arc3"))
        .when(is_rim_type)
        .then(pl.lit("rim"))
        .otherwise(pl.lit("jump"))
        .alias("shot_type"),
        # zone: value-confident threes split corner/above-break by geometry;
        # twos by rim type then radial distance
        pl.when(pl.col("score_value") == 3)
        .then(
            pl.when((pl.col("shot_x").abs() >= c.corner_x_ft) & (pl.col("shot_y").abs() <= c.corner_y_ft))
            .then(pl.lit("corner3"))
            .otherwise(pl.lit("abovebreak3"))
        )
        .when(is_rim_type | (pl.col("dist_ft") <= c.rim_radius_ft))
        .then(pl.lit("rim"))
        .when(pl.col("dist_ft") <= c.paint_radius_ft)
        .then(pl.lit("paint"))
        .otherwise(pl.lit("mid"))
        .alias("shot_zone"),
        (
            pl.col("clock_display_value").str.extract(r"^(\d+):", 1).cast(pl.Float64) * 60
            + pl.col("clock_display_value").str.extract(r":(\d+)", 1).cast(pl.Float64)
        ).alias("sec_left"),
    )
    result = out.select(
        pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("athlete_id_1").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("shooter_id"),
        pl.col("shot_x").cast(pl.Float64),
        pl.col("shot_y").cast(pl.Float64),
        pl.col("dist_ft").cast(pl.Float64),
        pl.col("shot_zone").cast(pl.Utf8),
        pl.col("shot_type").cast(pl.Utf8),
        pl.col("scoring_play").cast(pl.Boolean).alias("made"),
        pl.col("score_value").cast(pl.Int8).alias("point_value"),
        pl.col("period_number").cast(pl.Int64).alias("period"),
        pl.col("sec_left").cast(pl.Float64),
        pl.lit("espn").alias("source"),
    )
    assert dict(result.schema) == dict(CANONICAL_SHOT_SCHEMA)
    return result


def mbb_shot_data(
    seasons: "int | list[int]",
    *,
    source: str = "espn",
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Season(s) of shots in the canonical frame (the spine's data entry point).

    ``source="espn"`` loads the sportsdataverse-data shots release
    (``load_mbb_shots`` / ``load_wbb_shots``) and canonicalizes it. The NCAA
    HTML path is per-game, not per-season -- parse with
    ``create_shot_event_data`` and flatten via :func:`shot_events_to_frame`
    instead (``source="ncaa"`` raises with that pointer).

    Args:
        seasons: A season (e.g. ``2025``) or list of seasons.
        source: ``"espn"`` (the only batch source).
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The canonical shot frame; seasons the release doesn't cover are
        skipped, and no coverage at all returns the zero-row schema.

    Raises:
        ValueError: ``source`` is not ``"espn"``.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_shot_data
            shots = mbb_shot_data(2025)

        Pipeline next step (one line)::

            shots.group_by("shot_zone").agg(pl.col("made").mean()).sort("shot_zone")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if source != "espn":
        raise ValueError(
            f"source {source!r} is not batch-loadable; for NCAA HTML games parse with "
            "create_shot_event_data and flatten via shot_events_to_frame"
        )
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_shots as _loader  # noqa: PLC0415
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_shots as _loader  # noqa: PLC0415

    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    frames = []
    for s in seasons_list:
        try:
            raw = _loader([s])
        except Exception:  # noqa: BLE001 - release floor varies by league (wbb: 2026)
            continue
        if raw.is_empty():
            continue
        frames.append(espn_shots_to_canonical(raw, league=league, season=s))
    out = pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=CANONICAL_SHOT_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
