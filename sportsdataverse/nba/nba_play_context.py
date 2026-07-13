"""NBA/WNBA play-context engine — a Cleaning the Glass recreation.

Classifies **how each possession started** (the pbpstats start-type taxonomy,
zone-split) and **what context each play happened in** (CTG's halfcourt /
transition / putback / miscellaneous), then applies CTG's default garbage-time
and heave filters and rolls the result up into CTG's Play-Context table.

This module is strictly **additive** on the possession engine: boundaries,
second-chance detection and the box-score-reconciled possession frame come from
:mod:`sportsdataverse.nba.nba_possessions` (a faithful pbpstats port) and are
never recomputed here.

Methodology provenance
----------------------
Every definition traces to CTG's own published guide pages, captured verbatim in
``sdv-internal-refs/cleaningtheglass/`` (``METHODOLOGY.md`` + ``captures/guide_glossary.md``);
the engineering mapping lives in that repo's ``RECREATION.md``. Where CTG does
not publish a threshold, the knob is exposed and defaulted to the closest
documented public convention (see
:mod:`sportsdataverse.nba.nba_play_context_constants`):

* **Transition** — CTG says only "starting at the beginning of a possession and
  only ending once the defense is set". Default: hoop-math's 10-second
  initial-play rule (``transition_seconds``).
* **Putback** — CTG says "within a few seconds of the rebound". Default:
  pbpstats' 2-second same-player unassisted-two rule (``putback_seconds``).
* **Garbage time / heaves** — CTG publishes these exactly; reproduced faithfully.

CTG precedence rule reproduced here: a **transition putback stays transition**
("if a team comes down in transition and misses a shot but gets a putback, that
putback is classified as part of the overall transition event").
"""

from __future__ import annotations

from typing import Any, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.nba import nba_play_context_constants as C
from sportsdataverse.nba.nba_possessions import build_possessions

__all__ = [
    "LINEUP_PLAY_CONTEXT_SCHEMA",
    "PLAYER_PLAY_CONTEXT_SCHEMA",
    "PLAY_CONTEXT_POSSESSIONS_SCHEMA",
    "PLAY_CONTEXT_SHOTS_SCHEMA",
    "add_ctg_shot_zones",
    "add_play_context",
    "add_start_type_detail",
    "add_transition",
    "build_play_context_shots",
    "flag_garbage_time",
    "flag_heave_possessions",
    "lineup_play_context",
    "nba_play_context",
    "player_play_context",
    "starters_on_court_counts",
    "team_play_context",
]

#: Columns appended to the possession frame by :func:`add_play_context`.
PLAY_CONTEXT_POSSESSIONS_SCHEMA: dict[str, pl.DataType] = {
    "possession_start_type_detail": pl.Utf8,
    "possession_start_type_ctg": pl.Utf8,
    "seconds_to_first_play": pl.Float64,
    "is_transition": pl.Boolean,
    "transition_source": pl.Utf8,
    "possession_context": pl.Utf8,
    "is_heave_possession": pl.Boolean,
    "is_garbage_time": pl.Boolean,
    "garbage_time_basis": pl.Utf8,
}

#: The Play-Context metric columns every rollup emits (team / lineup / player-on-off).
#: Kept in one place so the three tables cannot drift apart.
_CONTEXT_METRIC_SCHEMA: dict[str, pl.DataType] = {
    "poss": pl.Int64,
    "points": pl.Int64,
    "pts_per_100": pl.Float64,
    "transition_poss": pl.Int64,
    "transition_points": pl.Int64,
    "transition_freq": pl.Float64,
    "transition_pts_per_100": pl.Float64,
    "non_transition_pts_per_100": pl.Float64,
    "transition_pts_added_per_100": pl.Float64,
    "halfcourt_poss": pl.Int64,
    "halfcourt_pts_per_100": pl.Float64,
    "freq_off_steal": pl.Float64,
    "freq_off_live_rebound": pl.Float64,
}

#: Schema of :func:`lineup_play_context` — one row per 5-man offensive lineup.
LINEUP_PLAY_CONTEXT_SCHEMA: dict[str, pl.DataType] = {
    "offense_team_id": pl.Int64,
    "lineup_id": pl.Utf8,
    **{f"off_player_{i}": pl.Int64 for i in range(1, 6)},
    **_CONTEXT_METRIC_SCHEMA,
}

#: Schema of :func:`player_play_context` — the offensive half of CTG's On/Off page.
#: ``on_*`` = the team's offense with the player on the floor, ``off_*`` = without
#: them, ``diff_*`` = on minus off (the number CTG actually shows).
PLAYER_PLAY_CONTEXT_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "offense_team_id": pl.Int64,
    "on_poss": pl.Int64,
    "off_poss": pl.Int64,
    "on_points": pl.Int64,
    "off_points": pl.Int64,
    "on_pts_per_100": pl.Float64,
    "off_pts_per_100": pl.Float64,
    "diff_pts_per_100": pl.Float64,
    "on_transition_freq": pl.Float64,
    "off_transition_freq": pl.Float64,
    "diff_transition_freq": pl.Float64,
    "on_transition_pts_per_100": pl.Float64,
    "off_transition_pts_per_100": pl.Float64,
    "diff_transition_pts_per_100": pl.Float64,
    "on_halfcourt_pts_per_100": pl.Float64,
    "off_halfcourt_pts_per_100": pl.Float64,
    "diff_halfcourt_pts_per_100": pl.Float64,
    "on_transition_pts_added_per_100": pl.Float64,
    "off_transition_pts_added_per_100": pl.Float64,
}

#: The offensive lineup columns :func:`attach_possession_lineups` appends.
_OFF_PLAYER_COLS: list[str] = [f"off_player_{i}" for i in range(1, 6)]

#: Schema of the per-shot frame from :func:`build_play_context_shots`.
PLAY_CONTEXT_SHOTS_SCHEMA: dict[str, pl.DataType] = {
    "game_id": pl.Utf8,
    "possession_number": pl.Int64,
    "order_index": pl.Int64,
    "period": pl.Int64,
    "team_id": pl.Int64,
    "person_id": pl.Int64,
    "shot_value": pl.Int64,
    "shot_made": pl.Boolean,
    "ctg_shot_zone": pl.Utf8,
    "is_assisted": pl.Boolean,
    "is_putback": pl.Boolean,
    "is_second_chance_shot": pl.Boolean,
    "shot_context": pl.Utf8,
}

# ---------------------------------------------------------------------------
# Shot zones (CTG taxonomy)
# ---------------------------------------------------------------------------


def add_ctg_shot_zones(enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Append CTG's shot-location zone (``ctg_shot_zone``) to an enhanced PBP frame.

    CTG's zones differ from the official NBA zones emitted by
    :func:`~sportsdataverse.nba.nba_shot_zones.add_shot_zones`: CTG splits the
    midrange at the free-throw-line distance rather than at the paint boundary.

    * ``at_rim`` — shot distance < 4 ft ("Shots within 4 feet of the basket").
    * ``short_mid`` — 4 ft <= distance < 14 ft ("outside of 4 feet, but inside of
      ~14 feet (the free throw line distance)").
    * ``long_mid`` — >= 14 ft, inside the arc.
    * ``corner_3`` — a three "below the break" (``|x_legacy| >= 220`` and
      ``y_legacy <= 87.5``).
    * ``arc_3`` — any other three (CTG's "non-corner three").

    Args:
        enhanced_pbp: Frame from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.

    Returns:
        The input frame with a ``ctg_shot_zone`` Utf8 column appended (null on
        non-field-goal rows). Empty input returns a zero-row frame carrying the
        column — never raises.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_play_context import add_ctg_shot_zones
            pbp = add_ctg_shot_zones(enhanced_pbp_from_payload(payload))
            print(pbp.filter(pl.col("ctg_shot_zone").is_not_null())["ctg_shot_zone"].value_counts())

        See Also:
            * `hoopR`_ -- R sister package with the same NBA surface.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if enhanced_pbp.is_empty():
        return enhanced_pbp.with_columns(pl.lit(None, dtype=pl.Utf8).alias("ctg_shot_zone"))

    x = pl.col("x_legacy").abs()
    is_fg = pl.col("is_field_goal") == 1
    is_three = pl.col("shot_value") == 3

    zone = (
        pl.when(~is_fg)
        .then(None)
        .when(is_three & (x >= C.CORNER_THREE_ABS_X) & (pl.col("y_legacy") <= C.CORNER_THREE_MAX_Y))
        .then(pl.lit("corner_3"))
        .when(is_three)
        .then(pl.lit("arc_3"))
        .when(_shot_distance_ft() < C.RIM_DISTANCE_FT)
        .then(pl.lit("at_rim"))
        .when(_shot_distance_ft() < C.SHORT_MID_DISTANCE_FT)
        .then(pl.lit("short_mid"))
        .otherwise(pl.lit("long_mid"))
        .alias("ctg_shot_zone")
    )
    return enhanced_pbp.with_columns(zone)


def _shot_distance_ft() -> pl.Expr:
    """Exact shot distance in feet, from the legacy coordinates.

    **Do not use the v3 ``shot_distance`` column for zone boundaries.** It is
    ``Int64`` — the feed rounds distance to whole feet — so a shot released 3.6 ft
    from the rim is reported as ``4`` and falls on the wrong side of CTG's 4-foot
    ``at_rim`` boundary. The legacy coordinates are exact (tenths of a foot, rim at
    the origin), so ``sqrt(x^2 + y^2) / 10`` recovers the true distance.

    Measured against the pbpstats-live oracle (which reads the decimal distance off
    the CDN feed), the rounded column mis-binned every 3.5-3.9 ft shot as
    ``short_mid`` — 8 of the 22 residual start-type mismatches on the three
    committed fixtures, all in the single most important zone.

    Falls back to the rounded column only when a coordinate is null.
    """
    coord = ((pl.col("x_legacy").cast(pl.Float64) ** 2 + pl.col("y_legacy").cast(pl.Float64) ** 2).sqrt()) / 10.0
    return (
        pl.when(pl.col("x_legacy").is_null() | pl.col("y_legacy").is_null())
        .then(pl.col("shot_distance").cast(pl.Float64))
        .otherwise(coord)
    )


def _row_distance_ft(row: dict) -> Optional[float]:
    """Row-wise twin of :func:`_shot_distance_ft` (exact distance from coordinates)."""
    x, y = row.get("x_legacy"), row.get("y_legacy")
    if x is None or y is None:
        d = row.get("shot_distance")
        return float(d) if d is not None else None
    return ((float(x) ** 2 + float(y) ** 2) ** 0.5) / 10.0


def _zone_of_row(row: dict) -> Optional[str]:
    """CTG zone for a single shot row (the row-wise twin of :func:`add_ctg_shot_zones`)."""
    if not row.get("is_field_goal"):
        return None
    if (row.get("shot_value") or 0) == 3:
        x = abs(row.get("x_legacy") or 0)
        y = row.get("y_legacy") or 0
        if x >= C.CORNER_THREE_ABS_X and y <= C.CORNER_THREE_MAX_Y:
            return "corner_3"
        return "arc_3"
    dist = _row_distance_ft(row)
    if dist is None:
        return None
    if dist < C.RIM_DISTANCE_FT:
        return "at_rim"
    if dist < C.SHORT_MID_DISTANCE_FT:
        return "short_mid"
    return "long_mid"


# ---------------------------------------------------------------------------
# Start-type taxonomy
# ---------------------------------------------------------------------------


def _is_blocked(pbp_rows: list[dict], miss_pos: int, stop_pos: int) -> bool:
    """True if a BLOCK companion row sits between the miss and its rebound.

    Real-capture shape (verified on ``tests/fixtures/nba_engine``): a block is a
    companion row with ``event_type == "other"`` whose description reads
    ``"<Name> BLOCK (N BLK)"``, sharing the missed shot's ``seconds_remaining``.
    """
    miss_clock = pbp_rows[miss_pos].get("seconds_remaining")
    for pos in range(miss_pos + 1, min(stop_pos + 1, len(pbp_rows))):
        row = pbp_rows[pos]
        if (row.get("event_type") or "") != "other":
            continue
        if row.get("seconds_remaining") != miss_clock:
            continue
        if "BLOCK" in (row.get("description") or "").upper():
            return True
    return False


def _rebounded_shot_pos(pbp_rows: list[dict], rebound_pos: int) -> Optional[int]:
    """Index of the missed shot / missed FT that the rebound at ``rebound_pos`` collected."""
    for pos in range(rebound_pos - 1, -1, -1):
        et = pbp_rows[pos].get("event_type") or ""
        if et in ("missed_shot", "free_throw"):
            return pos
        if et in ("made_shot", "turnover", "period"):
            return None
    return None


def _start_type_detail(
    coarse: str,
    prev_end_row: Optional[dict],
    pbp_rows: list[dict],
    pos_by_order: dict[int, int],
) -> tuple[str, str]:
    """Refine the engine's coarse start type into (fine start type, CTG bucket).

    Faithful to pbpstats ``possession.py:206-242``. The coarse type already
    encodes the validated period-start / timeout / steal / team-rebound
    decisions (see
    :func:`~sportsdataverse.nba.nba_possessions._possession_start_type`), so this
    only *refines* the made/missed branches with the shot's CTG zone.
    """
    if coarse == C.START_TYPE_TIMEOUT:
        return C.START_TYPE_TIMEOUT, "off_timeout"
    if coarse == C.START_TYPE_LIVE_BALL_TURNOVER:
        return C.START_TYPE_LIVE_BALL_TURNOVER, "off_steal"
    if prev_end_row is None or coarse == C.START_TYPE_DEADBALL:
        return C.START_TYPE_DEADBALL, "off_deadball"

    et = prev_end_row.get("event_type") or ""

    if coarse == "OffMadeShot":
        if et == "free_throw":
            return C.START_TYPE_FT_MAKE, "off_made"
        zone = _zone_of_row(prev_end_row)
        if zone is None:
            return C.START_TYPE_DEADBALL, "off_deadball"
        return f"Off{C.START_TYPE_ZONE_STEMS[zone]}Make", "off_made"

    if coarse == "OffMissedShot":
        # The boundary event is the defensive rebound; find the shot it collected.
        rb_order = prev_end_row.get("order_index")
        rb_pos = pos_by_order.get(int(rb_order)) if rb_order is not None else None
        if rb_pos is None:
            return C.START_TYPE_DEADBALL, "off_deadball"
        shot_pos = _rebounded_shot_pos(pbp_rows, rb_pos)
        if shot_pos is None:
            return C.START_TYPE_DEADBALL, "off_deadball"
        shot = pbp_rows[shot_pos]
        if (shot.get("event_type") or "") == "free_throw":
            return C.START_TYPE_FT_MISS, "off_live_rebound"
        zone = _zone_of_row(shot)
        if zone is None:
            return C.START_TYPE_DEADBALL, "off_deadball"
        stem = C.START_TYPE_ZONE_STEMS[zone]
        if _is_blocked(pbp_rows, shot_pos, rb_pos):
            return f"Off{stem}Block", "off_live_rebound"
        return f"Off{stem}Miss", "off_live_rebound"

    return C.START_TYPE_DEADBALL, "off_deadball"


def add_start_type_detail(possessions: pl.DataFrame, enhanced_pbp: pl.DataFrame) -> pl.DataFrame:
    """Append the full pbpstats start-type taxonomy to a possession frame.

    Upgrades the engine's coarse 5-value ``possession_start_type`` into:

    * ``possession_start_type_detail`` — the zone-split pbpstats vocabulary:
      ``Off{AtRim|ShortMidRange|LongMidRange|Corner3|Arc3}{Make|Miss|Block}``,
      ``OffFTMake`` / ``OffFTMiss``, ``OffLiveBallTurnover``, ``OffTimeout``,
      ``OffDeadball``.
    * ``possession_start_type_ctg`` — the coarse bucket CTG reports on:
      ``off_made`` / ``off_live_rebound`` / ``off_steal`` / ``off_deadball`` /
      ``off_timeout`` (see :data:`~nba_play_context_constants.CTG_START_BUCKETS`).

    Precedence (pbpstats): period start > timeout > previous boundary event. A
    **team rebound** (``person_id == 0``) is a dead-ball start even though a
    rebound row exists; a **timeout** beats a made basket (an after-timeout
    possession is ``OffTimeout``, not ``OffMadeShot``).

    Args:
        possessions: Frame from
            :func:`~sportsdataverse.nba.nba_possessions.build_possessions`.
        enhanced_pbp: The enhanced PBP frame those possessions were built from.

    Returns:
        ``possessions`` with the two columns appended. Empty input returns a
        zero-row frame carrying them — never raises.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possessions import build_possessions
            from sportsdataverse.nba.nba_play_context import add_start_type_detail
            poss = add_start_type_detail(build_possessions(pbp), pbp)
            print(poss["possession_start_type_ctg"].value_counts())

        See Also:
            * `pbpstats`_ -- the reference possession parser this ports.

        .. _pbpstats: https://github.com/dblackrun/pbpstats
    """
    if possessions.is_empty():
        return possessions.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("possession_start_type_detail"),
            pl.lit(None, dtype=pl.Utf8).alias("possession_start_type_ctg"),
        )

    pbp_rows: list[dict] = enhanced_pbp.sort("order_index").to_dicts()
    pos_by_order = {r["order_index"]: i for i, r in enumerate(pbp_rows)}

    poss = possessions.sort("possession_number")
    prows = poss.to_dicts()
    end_by_number = {r["possession_number"]: r.get("end_order_index") for r in prows}

    details: list[str] = []
    buckets: list[str] = []
    for r in prows:
        prev_end_order = end_by_number.get(r["possession_number"] - 1)
        prev_end_row = (
            pbp_rows[pos_by_order[prev_end_order]]
            if prev_end_order is not None and prev_end_order in pos_by_order
            else None
        )
        # The first possession of a period is always a dead-ball start (pbpstats).
        coarse = r.get("possession_start_type") or C.START_TYPE_DEADBALL
        if (r.get("number_in_period") or 0) == 1:
            coarse = C.START_TYPE_DEADBALL
            prev_end_row = None
        detail, bucket = _start_type_detail(coarse, prev_end_row, pbp_rows, pos_by_order)
        details.append(detail)
        buckets.append(bucket)

    return poss.with_columns(
        pl.Series("possession_start_type_detail", details, dtype=pl.Utf8),
        pl.Series("possession_start_type_ctg", buckets, dtype=pl.Utf8),
    )


# ---------------------------------------------------------------------------
# Transition / halfcourt
# ---------------------------------------------------------------------------


def add_transition(
    possessions: pl.DataFrame,
    enhanced_pbp: pl.DataFrame,
    *,
    transition_seconds: float = C.DEFAULT_TRANSITION_SECONDS,
    variant: str = C.DEFAULT_TRANSITION_VARIANT,
) -> pl.DataFrame:
    """Flag possessions that started in transition, and time their initial play.

    CTG defines transition as beginning at the possession start and ending "once
    the defense is set", **without publishing a seconds threshold**. We therefore
    time the possession's *initial play* — its first shot attempt, trip to the
    line, or turnover (CTG's own definition of a "play") — and call the
    possession transition when that play lands within ``transition_seconds``.

    Variants (:data:`~nba_play_context_constants.TRANSITION_VARIANTS`):

    * ``hoop_math`` (default) — any non-timeout start type qualifies.
    * ``haslametrics`` — steal starts only (conservative).
    * ``bigballr`` — the previous possession must have ended live
      (``off_made`` / ``off_live_rebound`` / ``off_steal``); a dead-ball start can
      never be transition.

    The first possession of a period is never transition. After a timeout the
    defense is set by construction, so ``off_timeout`` never qualifies under any
    variant.

    Args:
        possessions: Frame carrying ``possession_start_type_ctg`` (i.e. the output
            of :func:`add_start_type_detail`).
        enhanced_pbp: The enhanced PBP frame the possessions were built from.
        transition_seconds: Initial-play cutoff. Default 10.0 (hoop-math).
            Calibrate against Synergy transition frequency (league mean ~15-16%).
        variant: One of :data:`~nba_play_context_constants.TRANSITION_VARIANTS`.

    Returns:
        ``possessions`` with ``seconds_to_first_play`` (Float64, null when the
        possession had no play), ``is_transition`` (Boolean), ``transition_source``
        (Utf8: ``steal`` / ``live_rebound`` / ``made`` / ``deadball``; null when not
        transition) and ``possession_context`` (Utf8: ``transition`` / ``halfcourt``
        / ``misc``) appended.

    Raises:
        ValueError: If ``variant`` is not a known transition variant.

    Example:
        Quick start::

            poss = add_transition(add_start_type_detail(poss, pbp), pbp)
            print(poss["is_transition"].mean())          # transition frequency

        Tune the knob against the Synergy oracle::

            poss8 = add_transition(poss, pbp, transition_seconds=8.0)
    """
    if variant not in C.TRANSITION_VARIANTS:
        raise ValueError(f"variant must be one of {C.TRANSITION_VARIANTS}, got {variant!r}")

    empty_cols = (
        pl.lit(None, dtype=pl.Float64).alias("seconds_to_first_play"),
        pl.lit(None, dtype=pl.Boolean).alias("is_transition"),
        pl.lit(None, dtype=pl.Utf8).alias("transition_source"),
        pl.lit(None, dtype=pl.Utf8).alias("possession_context"),
    )
    if possessions.is_empty():
        return possessions.with_columns(*empty_cols)

    pbp_rows = enhanced_pbp.sort("order_index").to_dicts()

    # First play-ending event per possession, by order-index window.
    first_play_clock: dict[int, float] = {}
    poss_rows = possessions.sort("possession_number").to_dicts()
    for r in poss_rows:
        lo, hi = r.get("start_order_index"), r.get("end_order_index")
        if lo is None or hi is None:
            continue
        for row in pbp_rows:
            oi = row.get("order_index")
            if oi is None or oi < lo:
                continue
            if oi > hi:
                break
            if (row.get("event_type") or "") in C.PLAY_ENDING_EVENT_TYPES:
                first_play_clock[r["possession_number"]] = row.get("seconds_remaining")
                break

    _SOURCE = {
        "off_steal": "steal",
        "off_live_rebound": "live_rebound",
        "off_made": "made",
        "off_deadball": "deadball",
    }

    secs: list[Optional[float]] = []
    trans: list[bool] = []
    source: list[Optional[str]] = []
    context: list[str] = []

    # The possession's START is the moment the ball changed hands -- i.e. the clock
    # of the event that ENDED the previous possession -- NOT `start_seconds_remaining`,
    # which is the clock of this possession's first *row*. Those differ: after a
    # defensive rebound the group's first row is frequently the shot itself, so
    # measuring from it makes almost every possession look like transition (85% vs
    # the Synergy ~15% oracle -- the bug this reference fixes). Same subtlety
    # documented in `nba_possessions._count_as_possession`.
    prev_end_clock: dict[int, float] = {}
    for r in poss_rows:
        end_clock = r.get("end_seconds_remaining")
        if end_clock is not None:
            prev_end_clock[r["possession_number"] + 1] = float(end_clock)

    for r in poss_rows:
        bucket = r.get("possession_start_type_ctg") or "off_deadball"
        # A period's opening possession has no prior possession in that period, so
        # the ball-change reference is the period start itself -- use the group's own
        # first-row clock (these are never transition; see below).
        if (r.get("number_in_period") or 0) == 1:
            start_clock = r.get("start_seconds_remaining")
        else:
            start_clock = prev_end_clock.get(r["possession_number"], r.get("start_seconds_remaining"))
        play_clock = first_play_clock.get(r["possession_number"])
        elapsed = float(start_clock) - float(play_clock) if start_clock is not None and play_clock is not None else None
        secs.append(elapsed)

        if variant == "haslametrics":
            eligible = bucket == "off_steal"
        elif variant == "bigballr":
            eligible = bucket in C.LIVE_START_BUCKETS
        else:  # hoop_math
            eligible = bucket not in C.NON_TRANSITION_START_BUCKETS

        # A period's opening possession is never transition.
        if (r.get("number_in_period") or 0) == 1:
            eligible = False

        is_trans = bool(eligible and elapsed is not None and elapsed <= transition_seconds)
        trans.append(is_trans)
        source.append(_SOURCE.get(bucket) if is_trans else None)
        context.append("transition" if is_trans else ("halfcourt" if elapsed is not None else "misc"))

    return possessions.sort("possession_number").with_columns(
        pl.Series("seconds_to_first_play", secs, dtype=pl.Float64),
        pl.Series("is_transition", trans, dtype=pl.Boolean),
        pl.Series("transition_source", source, dtype=pl.Utf8),
        pl.Series("possession_context", context, dtype=pl.Utf8),
    )


# ---------------------------------------------------------------------------
# CTG default filters — heaves + garbage time
# ---------------------------------------------------------------------------


def flag_heave_possessions(possessions: pl.DataFrame) -> pl.DataFrame:
    """Flag CTG's "projected heave possessions" (excluded from CTG stats by default).

    CTG (exact): "possessions that start with **4 or fewer seconds on the game
    clock at the end of one of the first three quarters**." Q4/OT are exempt — a
    late Q4 possession is a real possession.

    Args:
        possessions: Any frame with ``period`` and ``start_seconds_remaining``.

    Returns:
        ``possessions`` with a Boolean ``is_heave_possession`` column appended.

    Example:
        Quick start::

            poss = flag_heave_possessions(poss)
            clean = poss.filter(pl.col("is_heave_possession") == False)
    """
    if possessions.is_empty():
        return possessions.with_columns(pl.lit(None, dtype=pl.Boolean).alias("is_heave_possession"))

    return possessions.with_columns(
        (
            pl.col("period").is_in(list(C.HEAVE_PERIODS))
            & (pl.col("start_seconds_remaining") <= C.HEAVE_POSSESSION_SECONDS)
        )
        .fill_null(False)
        .alias("is_heave_possession")
    )


def flag_garbage_time(
    possessions: pl.DataFrame,
    enhanced_pbp: pl.DataFrame,
    *,
    starters_on_court: Optional[dict[int, int]] = None,
) -> pl.DataFrame:
    """Flag CTG garbage time (excluded from CTG stats by default).

    CTG (exact): "the game has to be in the **4th quarter**, the score
    differential has to be **>= 25 for minutes 12-9, >= 20 for minutes 9-6, and
    >= 10 for the remainder of the quarter**. Additionally, there have to be **two
    or fewer starters on the floor combined between the two teams**. Importantly,
    the game can never go back to being non-garbage time, or this clock resets."

    The margin x minutes bands are reproduced exactly, evaluated on the score at
    each possession's start. The reset semantics fall out of that per-possession
    evaluation: if the trailing team claws back inside the band's threshold, the
    condition stops holding and those possessions are NOT garbage time (CTG's own
    "comeback is not counted as garbage time" example); if the lead re-expands,
    the flag turns back on.

    **The starters clause is applied only when ``starters_on_court`` is supplied**
    — it needs lineup + box ``START_POSITION`` data this frame does not carry.
    Without it the flag is the **margin-only superset** of CTG's definition (it can
    flag a blowout stretch in which the starters are still on the floor), and
    ``garbage_time_basis`` records which rule was actually used. This is a
    deliberate, documented divergence — do not read a ``margin_only`` flag as
    CTG-exact.

    Args:
        possessions: Frame with ``period``, ``start_seconds_remaining`` and
            ``start_order_index``.
        enhanced_pbp: The enhanced PBP frame (supplies the running score).
        starters_on_court: Optional map ``possession_number -> number of starters
            on the floor across BOTH teams``. When given, a possession is garbage
            time only if that count is
            <= :data:`~nba_play_context_constants.GARBAGE_TIME_MAX_STARTERS`.

    Returns:
        ``possessions`` with Boolean ``is_garbage_time`` and Utf8
        ``garbage_time_basis`` (``"margin_and_starters"`` or ``"margin_only"``)
        appended.

    Example:
        Quick start (margin-only superset)::

            poss = flag_garbage_time(poss, pbp)
            print(poss.filter(pl.col("is_garbage_time") == True).height)

        CTG-exact, with the starters clause::

            poss = flag_garbage_time(poss, pbp, starters_on_court=starters_by_possession)
    """
    basis = "margin_and_starters" if starters_on_court is not None else "margin_only"
    if possessions.is_empty():
        return possessions.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("is_garbage_time"),
            pl.lit(basis, dtype=pl.Utf8).alias("garbage_time_basis"),
        )

    # Running score at each possession's start. The v3 feed ships score_home /
    # score_away as Utf8 and populates them only on scoring rows, so cast to Int64
    # at the boundary (never diff the raw strings) and forward-fill the gaps.
    score = (
        enhanced_pbp.sort("order_index")
        .with_columns(
            pl.col("score_home").cast(pl.Int64, strict=False).forward_fill().fill_null(0).alias("_sh"),
            pl.col("score_away").cast(pl.Int64, strict=False).forward_fill().fill_null(0).alias("_sa"),
        )
        .select("order_index", "_sh", "_sa")
    )
    margin_by_order = {r["order_index"]: abs((r["_sh"] or 0) - (r["_sa"] or 0)) for r in score.to_dicts()}

    flags: list[bool] = []
    for r in possessions.sort("possession_number").to_dicts():
        if (r.get("period") or 0) != C.GARBAGE_TIME_PERIOD:
            flags.append(False)
            continue
        clock = r.get("start_seconds_remaining")
        margin = margin_by_order.get(r.get("start_order_index"), 0)
        hit = False
        for high, low, min_margin in C.GARBAGE_TIME_BANDS:
            if clock is not None and low < float(clock) <= high and margin >= min_margin:
                hit = True
                break
        if hit and starters_on_court is not None:
            hit = starters_on_court.get(r["possession_number"], 10) <= C.GARBAGE_TIME_MAX_STARTERS
        flags.append(hit)

    return possessions.sort("possession_number").with_columns(
        pl.Series("is_garbage_time", flags, dtype=pl.Boolean),
        pl.lit(basis, dtype=pl.Utf8).alias("garbage_time_basis"),
    )


# ---------------------------------------------------------------------------
# Shot-level context (putbacks / second chance)
# ---------------------------------------------------------------------------


def build_play_context_shots(
    possessions: pl.DataFrame,
    enhanced_pbp: pl.DataFrame,
    *,
    putback_seconds: float = C.DEFAULT_PUTBACK_SECONDS,
) -> pl.DataFrame:
    """Build the per-shot frame carrying CTG's play context.

    CTG assigns context **per play**, not per possession: one possession can
    contain a transition miss, a halfcourt reset and a putback. This frame is the
    play-level view — one row per field-goal attempt.

    * ``is_putback`` — pbpstats ``field_goal.py:112-144``: an **unassisted 2-point**
      attempt whose preceding event is a **real offensive rebound by the same
      player**, within ``putback_seconds``. A three is never a putback.
    * ``is_second_chance_shot`` — the shot follows an offensive rebound earlier in
      the same possession.
    * ``shot_context`` — ``transition`` / ``putback`` / ``halfcourt``. **Transition
      wins over putback**, reproducing CTG exactly: "if a team comes down in
      transition and misses a shot but gets a putback, that putback is classified
      as part of the overall transition event."

    Args:
        possessions: Frame from :func:`add_transition` (needs ``is_transition``).
        enhanced_pbp: The enhanced PBP frame the possessions were built from.
        putback_seconds: Rebound-to-shot window. Default 2.0 (pbpstats).

    Returns:
        Polars DataFrame with schema :data:`PLAY_CONTEXT_SHOTS_SCHEMA` — one row
        per field-goal attempt. Empty input returns the zero-row schema.

    Example:
        Quick start::

            shots = build_play_context_shots(poss, pbp)
            print(shots.group_by("shot_context").len())
            print(shots.filter(pl.col("is_putback") == True).height)
    """
    if possessions.is_empty() or enhanced_pbp.is_empty():
        return pl.DataFrame(schema=PLAY_CONTEXT_SHOTS_SCHEMA)

    pbp_rows = add_ctg_shot_zones(enhanced_pbp.sort("order_index")).to_dicts()
    trans_by_poss = {
        r["possession_number"]: bool(r.get("is_transition"))
        for r in possessions.select("possession_number", "is_transition").to_dicts()
    }
    # order-index window -> possession number
    windows = [
        (r["start_order_index"], r["end_order_index"], r["possession_number"], r["offense_team_id"])
        for r in possessions.sort("possession_number").to_dicts()
        if r.get("start_order_index") is not None and r.get("end_order_index") is not None
    ]

    def _poss_of(order_index: int) -> tuple[Optional[int], Optional[int]]:
        for lo, hi, num, off in windows:
            if lo <= order_index <= hi:
                return num, off
        return None, None

    records: list[dict[str, Any]] = []
    for i, row in enumerate(pbp_rows):
        et = row.get("event_type") or ""
        if et not in ("made_shot", "missed_shot"):
            continue
        oi = row.get("order_index")
        poss_num, _off_team = _poss_of(oi)
        if poss_num is None:
            continue

        assisted = et == "made_shot" and "AST" in (row.get("description") or "").upper()

        # Putback: walk back past co-clock fouls / companion rows to the previous
        # substantive event; it must be this player's own offensive rebound,
        # within the window.
        is_putback = False
        if (row.get("shot_value") or 0) == 2 and not assisted:
            for j in range(i - 1, -1, -1):
                prev = pbp_rows[j]
                pet = prev.get("event_type") or ""
                if pet in ("foul", "other", "replay", "substitution"):
                    continue
                if pet == "rebound" and prev.get("person_id") and prev.get("person_id") == row.get("person_id"):
                    reb_clock = prev.get("seconds_remaining")
                    shot_clock = row.get("seconds_remaining")
                    if (
                        reb_clock is not None
                        and shot_clock is not None
                        and 0 <= (float(reb_clock) - float(shot_clock)) <= putback_seconds
                    ):
                        is_putback = True
                break

        # Second chance: an offensive rebound (by the offense) earlier in this possession.
        second_chance = False
        for j in range(i - 1, -1, -1):
            prev = pbp_rows[j]
            pnum, _ = _poss_of(prev.get("order_index"))
            if pnum != poss_num:
                break
            if (prev.get("event_type") or "") == "rebound" and prev.get("team_id") == row.get("team_id"):
                second_chance = True
                break

        in_transition = trans_by_poss.get(poss_num, False)
        if in_transition:
            ctx = "transition"  # CTG: transition wins over putback
        elif is_putback:
            ctx = "putback"
        else:
            ctx = "halfcourt"

        records.append(
            {
                "game_id": row.get("game_id"),
                "possession_number": poss_num,
                "order_index": oi,
                "period": row.get("period"),
                "team_id": row.get("team_id"),
                "person_id": row.get("person_id"),
                "shot_value": row.get("shot_value"),
                "shot_made": et == "made_shot",
                "ctg_shot_zone": row.get("ctg_shot_zone"),
                "is_assisted": assisted,
                "is_putback": is_putback,
                "is_second_chance_shot": second_chance,
                "shot_context": ctx,
            }
        )

    if not records:
        return pl.DataFrame(schema=PLAY_CONTEXT_SHOTS_SCHEMA)
    return pl.DataFrame(records, schema=PLAY_CONTEXT_SHOTS_SCHEMA)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def add_play_context(
    enhanced_pbp: pl.DataFrame,
    *,
    transition_seconds: float = C.DEFAULT_TRANSITION_SECONDS,
    transition_variant: str = C.DEFAULT_TRANSITION_VARIANT,
    starters_on_court: Optional[dict[int, int]] = None,
) -> pl.DataFrame:
    """Build possessions and enrich them with the full CTG play-context surface.

    One call: :func:`~sportsdataverse.nba.nba_possessions.build_possessions` ->
    :func:`add_start_type_detail` -> :func:`add_transition` ->
    :func:`flag_heave_possessions` -> :func:`flag_garbage_time`.

    The CTG filter columns are **flags, not filters** — nothing is dropped. Apply
    CTG's defaults yourself::

        ctg = poss.filter(
            (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
        )

    Args:
        enhanced_pbp: Frame from
            :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`.
        transition_seconds: Transition initial-play cutoff (default 10.0).
        transition_variant: See :func:`add_transition`.
        starters_on_court: Optional starters-on-floor counts; see
            :func:`flag_garbage_time`.

    Returns:
        The possession frame (``POSSESSIONS_SCHEMA``) plus every column in
        :data:`PLAY_CONTEXT_POSSESSIONS_SCHEMA`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            from sportsdataverse.nba.nba_play_context import add_play_context
            poss = add_play_context(enhanced_pbp_from_payload(payload))
            print(poss["possession_start_type_ctg"].value_counts())
    """
    poss = build_possessions(enhanced_pbp)
    poss = add_start_type_detail(poss, enhanced_pbp)
    poss = add_transition(poss, enhanced_pbp, transition_seconds=transition_seconds, variant=transition_variant)
    poss = flag_heave_possessions(poss)
    poss = flag_garbage_time(poss, enhanced_pbp, starters_on_court=starters_on_court)
    return poss


# ---------------------------------------------------------------------------
# Aggregation — CTG's Play Context table
# ---------------------------------------------------------------------------


def team_play_context(
    possessions: pl.DataFrame,
    *,
    league_non_transition_ppp: Optional[float] = None,
    apply_ctg_filters: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Roll possessions up into CTG's team Play-Context table.

    Reproduces the offensive half of CTG's ``/stats/league/context`` page.

    Columns: ``poss``, ``points``, ``pts_per_100``, ``transition_poss``,
    ``transition_points``, ``transition_freq``, ``transition_pts_per_100``
    (CTG's "Eff"), ``non_transition_pts_per_100``, ``transition_pts_added_per_100``
    (CTG's "Pts+/Poss"), plus ``halfcourt_*`` twins and per-source transition
    frequencies (``freq_off_steal`` / ``freq_off_live_rebound``).

    **Pts+/Poss** is the subtle one. CTG: "CTG takes a team's points per
    possession that starts with transition, and subtracts out **what an average
    team does** in a possession that did not start with transition. ... We take the
    result and multiply it by the team's transition frequency."::

        pts_added_per_100 = (team_transition_ppp - LEAGUE_avg_non_transition_ppp) * freq

    The **league-average** baseline (not the team's own) is deliberate — it stops a
    great halfcourt offense from deflating its own transition value. When
    ``league_non_transition_ppp`` is not supplied it is computed from the frame
    itself, which is only meaningful on a **season-wide** frame; on a single game
    that "league average" is just the two teams in it. Pass the season value
    explicitly for CTG-comparable numbers.

    Args:
        possessions: Frame from :func:`add_play_context`.
        league_non_transition_ppp: League-average points per 100 possessions on
            non-transition-start possessions. Computed from the frame when omitted.
        apply_ctg_filters: Drop garbage-time, heave and non-counting possessions
            first (CTG's default view). Set ``False`` for the unfiltered totals.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``offense_team_id``. Empty input returns a zero-row frame.

    Example:
        Quick start::

            ctx = team_play_context(add_play_context(pbp))
            print(ctx.select("offense_team_id", "transition_freq", "transition_pts_added_per_100"))

        Season-comparable Pts+/Poss::

            ctx = team_play_context(season_poss, league_non_transition_ppp=104.8)
    """
    df = possessions
    if df.is_empty():
        out = pl.DataFrame(schema={"offense_team_id": pl.Int64, **_CONTEXT_METRIC_SCHEMA})
        return out.to_pandas() if return_as_pandas else out

    df = _ctg_filtered(df) if apply_ctg_filters else df
    league_non_transition_ppp = _league_non_transition_ppp(df, league_non_transition_ppp)
    out = _context_rates(_context_counts(df, ["offense_team_id"]), league_non_transition_ppp).sort("offense_team_id")
    return out.to_pandas() if return_as_pandas else out


# ---------------------------------------------------------------------------
# Shared aggregation core — team / lineup / player all roll up the same way
# ---------------------------------------------------------------------------


def _ctg_filtered(df: pl.DataFrame) -> pl.DataFrame:
    """CTG's default view: drop garbage time, heaves, and non-counting possessions."""
    return df.filter(
        (pl.col("count_as_possession") == True)  # noqa: E712
        & (pl.col("is_garbage_time") == False)  # noqa: E712
        & (pl.col("is_heave_possession") == False)  # noqa: E712
    )


def _league_non_transition_ppp(df: pl.DataFrame, supplied: Optional[float]) -> float:
    """The Pts+/Poss baseline: league-average PPP on non-transition-start possessions.

    Computed from the frame when not supplied — only meaningful on a season-wide
    frame (on a single game the "league" is just the two teams in it). Every
    rollup in one call shares ONE baseline, so on/off diffs stay comparable.
    """
    if supplied is not None:
        return supplied
    nt = df.filter(pl.col("is_transition") == False)  # noqa: E712
    return 100.0 * nt["points"].sum() / nt.height if nt.height else 0.0


def _context_counts(df: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """Raw (additive) play-context counts grouped by *by*.

    Counts only — no rates. Keeping the additive layer separate is what lets
    :func:`player_play_context` derive the OFF side by simple subtraction
    (team total - on-court), which is exactly what makes the on/off partition
    identity exact rather than approximate.
    """
    is_trans = pl.col("is_transition") == True  # noqa: E712
    # Every count is cast to Int64 explicitly: pl.len() and Boolean.sum() are UInt32,
    # which (a) disagrees with the declared zero-row schema and (b) would wrap on the
    # subtraction that derives the OFF side in player_play_context.
    return df.group_by(by).agg(
        pl.len().cast(pl.Int64).alias("poss"),
        pl.col("points").sum().cast(pl.Int64).alias("points"),
        is_trans.sum().cast(pl.Int64).alias("transition_poss"),
        pl.col("points").filter(is_trans).sum().cast(pl.Int64).alias("transition_points"),
        (~is_trans).sum().cast(pl.Int64).alias("halfcourt_poss"),
        pl.col("points").filter(~is_trans).sum().cast(pl.Int64).alias("halfcourt_points"),
        (pl.col("transition_source") == "steal").sum().cast(pl.Int64).alias("_trans_steal"),
        (pl.col("transition_source") == "live_rebound").sum().cast(pl.Int64).alias("_trans_reb"),
    )


def _context_rates(counts: pl.DataFrame, league_non_transition_ppp: float) -> pl.DataFrame:
    """Turn :func:`_context_counts` output into the CTG metric columns."""
    return (
        counts.with_columns(
            # poss can be 0 on the subtraction-derived OFF side (a player who never
            # sits: off_poss = team_poss - on_poss = 0). Guard every poss-denominator
            # rate to a null rather than emit inf/NaN into the output (and into the
            # on-minus-off diff). Matches the transition/halfcourt guards below.
            pl.when(pl.col("poss") > 0)
            .then(100.0 * pl.col("points") / pl.col("poss"))
            .otherwise(None)
            .alias("pts_per_100"),
            pl.when(pl.col("poss") > 0)
            .then(pl.col("transition_poss") / pl.col("poss"))
            .otherwise(None)
            .alias("transition_freq"),
            pl.when(pl.col("transition_poss") > 0)
            .then(100.0 * pl.col("transition_points") / pl.col("transition_poss"))
            .otherwise(None)
            .alias("transition_pts_per_100"),
            pl.when(pl.col("halfcourt_poss") > 0)
            .then(100.0 * pl.col("halfcourt_points") / pl.col("halfcourt_poss"))
            .otherwise(None)
            .alias("non_transition_pts_per_100"),
            pl.when(pl.col("poss") > 0)
            .then(pl.col("_trans_steal") / pl.col("poss"))
            .otherwise(None)
            .alias("freq_off_steal"),
            pl.when(pl.col("poss") > 0)
            .then(pl.col("_trans_reb") / pl.col("poss"))
            .otherwise(None)
            .alias("freq_off_live_rebound"),
        )
        .with_columns(
            ((pl.col("transition_pts_per_100") - league_non_transition_ppp) * pl.col("transition_freq")).alias(
                "transition_pts_added_per_100"
            ),
            pl.col("non_transition_pts_per_100").alias("halfcourt_pts_per_100"),
        )
        .drop("_trans_steal", "_trans_reb", "halfcourt_points")
    )


def starters_on_court_counts(
    possessions: pl.DataFrame,
    starters: dict[int, list[int]],
) -> dict[int, int]:
    """Count, per possession, how many **starters** are on the floor across BOTH teams.

    This supplies the second half of CTG's garbage-time rule — "there have to be
    **two or fewer starters on the floor combined between the two teams**" — which
    :func:`flag_garbage_time` cannot evaluate on its own (the possession frame does
    not carry who is on the floor).

    Feed the result straight back in::

        poss = attach_possession_lineups(build_possessions(enh), oncourt, enh, home_team_id=home)
        counts = starters_on_court_counts(poss, _starters_from_boxscore_v3(box))
        ctx = add_play_context(enh, starters_on_court=counts)   # now CTG-exact

    The possession numbering is stable for a given enhanced-PBP frame, so counts
    derived from a lineup-attached frame key correctly into a frame rebuilt from the
    same PBP.

    Args:
        possessions: Possession frame with the ten on-court columns
            ``off_player_1..5`` **and** ``def_player_1..5`` (from
            :func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`).
        starters: ``{team_id: [player_id, ...]}`` — e.g. from
            :func:`~sportsdataverse.nba.nba_lineups._starters_from_boxscore_v3`.
            Player ids are matched across both teams' starting fives, so the
            offense/defense split of the lineup columns does not matter.

    Returns:
        ``{possession_number: starters_on_floor}``, each value in ``0..10``.

        An **empty** *starters* map yields all-zero counts, which would make CTG's
        ``<= 2`` clause vacuously true and flag every margin-qualifying possession.
        The counts are reported honestly rather than guessed — do not pass an empty
        map and then read the result as CTG-exact.

    Raises:
        ValueError: when the ``off_player_*`` / ``def_player_*`` columns are absent.

    Example:
        Quick start::

            counts = starters_on_court_counts(poss, _starters_from_boxscore_v3(box))
            print(max(counts.values()))  # 10 at the opening tip
    """
    def_cols = [f"def_player_{i}" for i in range(1, 6)]
    missing = [c for c in _OFF_PLAYER_COLS + def_cols if c not in possessions.columns]
    if missing:
        raise ValueError(
            f"missing on-court columns {missing}: pass a frame through "
            "nba_possessions.attach_possession_lineups() first"
        )
    if possessions.is_empty():
        return {}

    # One flat set of starter ids: a player is a starter or they are not, and the
    # offense/defense column split says nothing about which team they start for.
    starter_ids = {int(pid) for ids in starters.values() for pid in ids}

    counted = possessions.select(
        "possession_number",
        pl.concat_list(_OFF_PLAYER_COLS + def_cols)
        .list.set_intersection(pl.lit(sorted(starter_ids), dtype=pl.List(pl.Int64)))
        .list.len()
        .cast(pl.Int64)
        .alias("n_starters"),
    )
    return dict(zip(counted["possession_number"].to_list(), counted["n_starters"].to_list()))


def _require_lineups(df: pl.DataFrame) -> None:
    """Fail loudly when the 5v5 lineup columns are absent.

    Without ``off_player_1..5`` a lineup/player rollup would silently degenerate
    (an empty group key, or every possession attributed to one bucket), which is
    worse than an error — so this raises rather than returning a plausible-looking
    wrong answer.
    """
    missing = [c for c in _OFF_PLAYER_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"missing lineup columns {missing}: pass a frame through "
            "nba_possessions.attach_possession_lineups() before a lineup/player rollup"
        )


def lineup_play_context(
    possessions: pl.DataFrame,
    *,
    min_poss: int = 0,
    league_non_transition_ppp: Optional[float] = None,
    apply_ctg_filters: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Roll possessions up into a per-5-man-lineup Play-Context table.

    The lineup analogue of :func:`team_play_context`: same metric columns, grouped
    by the five players on the floor **for the offense**. Lineups are identified by
    ``lineup_id`` — the five player ids sorted ascending and hyphen-joined — so the
    same five players always land in the same bucket regardless of slot order.

    Requires the ``off_player_1..5`` columns from
    :func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`
    (which passes the play-context columns through, so the two compose in either
    order).

    Args:
        possessions: Frame from :func:`add_play_context` **with lineups attached**.
        min_poss: Drop lineups below this possession count (CTG's tables carry a
            minimum; 0 keeps everything, which is what the partition identity needs).
        league_non_transition_ppp: Pts+/Poss baseline; see :func:`team_play_context`.
        apply_ctg_filters: Drop garbage-time / heave / non-counting possessions first.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (team, lineup) with :data:`LINEUP_PLAY_CONTEXT_SCHEMA`.
        Empty input returns a zero-row frame with that schema.

    Raises:
        ValueError: when the ``off_player_*`` columns are missing.

    Example:
        Quick start::

            poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
            lu = lineup_play_context(poss, min_poss=25)
            print(lu.sort("pts_per_100", descending=True).head())
    """
    _require_lineups(possessions)
    if possessions.is_empty():
        out = pl.DataFrame(schema=LINEUP_PLAY_CONTEXT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = _ctg_filtered(possessions) if apply_ctg_filters else possessions
    league_non_transition_ppp = _league_non_transition_ppp(df, league_non_transition_ppp)

    # Sort the five ids NUMERICALLY, then cast the list's inner dtype to Utf8 to join.
    # (Casting to Utf8 before sorting would order them lexicographically -- "10" < "9" --
    # which still yields a stable key but scrambles the off_player_* ids parsed back out.)
    df = df.with_columns(
        pl.concat_list(_OFF_PLAYER_COLS).list.sort().cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id")
    )
    counts = _context_counts(df, ["offense_team_id", "lineup_id"])
    out = _context_rates(counts, league_non_transition_ppp)

    # re-attach the five ids (sorted, so they line up with lineup_id)
    ids = out["lineup_id"].str.split("-")
    out = out.with_columns([ids.list.get(i).cast(pl.Int64).alias(f"off_player_{i + 1}") for i in range(5)])
    if min_poss > 0:
        out = out.filter(pl.col("poss") >= min_poss)
    out = out.select(list(LINEUP_PLAY_CONTEXT_SCHEMA)).sort(["offense_team_id", "poss"], descending=[False, True])
    return out.to_pandas() if return_as_pandas else out


def player_play_context(
    possessions: pl.DataFrame,
    *,
    league_non_transition_ppp: Optional[float] = None,
    apply_ctg_filters: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half).

    For each player: their team's offensive play-context **with them on the floor**
    (``on_*``), **without them** (``off_*``), and the on-minus-off difference
    (``diff_*``) — which is the number CTG actually displays.

    The OFF side is derived by **subtraction** (team total minus on-court), not by a
    second scan. That is deliberate: it makes the partition exact by construction —
    ``on_poss + off_poss == team_poss`` and the same for points — so a leak (a
    double-counted possession, a dropped lineup slot) is impossible to hide. The
    test suite asserts that identity directly.

    Like CTG's on/off, this is a **raw** split: no luck adjustment, no opponent
    adjustment, no minutes threshold. It is a descriptive difference, not a causal
    estimate — for that, use the RAPM surface
    (:func:`~sportsdataverse.nba.nba_rapm.nba_rapm`).

    Requires ``off_player_1..5`` from
    :func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.

    Args:
        possessions: Frame from :func:`add_play_context` **with lineups attached**.
        league_non_transition_ppp: Pts+/Poss baseline; see :func:`team_play_context`.
            One baseline is shared across the on and off sides so the diffs are
            comparable.
        apply_ctg_filters: Drop garbage-time / heave / non-counting possessions first.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (player, team) with :data:`PLAYER_PLAY_CONTEXT_SCHEMA`.
        Empty input returns a zero-row frame with that schema.

    Raises:
        ValueError: when the ``off_player_*`` columns are missing.

    Example:
        Quick start::

            poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
            onoff = player_play_context(poss)
            print(onoff.sort("diff_pts_per_100", descending=True).head())

        Who makes their team run?::

            print(onoff.sort("diff_transition_freq", descending=True).head())
    """
    _require_lineups(possessions)
    if possessions.is_empty():
        out = pl.DataFrame(schema=PLAYER_PLAY_CONTEXT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = _ctg_filtered(possessions) if apply_ctg_filters else possessions
    league_ppp = _league_non_transition_ppp(df, league_non_transition_ppp)

    keep = ["game_id", "possession_number", "offense_team_id", "points", "is_transition", "transition_source"]
    long = (
        df.select(keep + _OFF_PLAYER_COLS)
        .unpivot(index=keep, on=_OFF_PLAYER_COLS, variable_name="_slot", value_name="player_id")
        .drop_nulls("player_id")
        # a player occupying two slots of the same possession would double-count
        .unique(subset=["game_id", "possession_number", "player_id"])
    )

    on_counts = _context_counts(long, ["offense_team_id", "player_id"])
    team_counts = _context_counts(df, ["offense_team_id"])

    count_cols = [
        "poss",
        "points",
        "transition_poss",
        "transition_points",
        "halfcourt_poss",
        "halfcourt_points",
        "_trans_steal",
        "_trans_reb",
    ]
    off_counts = on_counts.join(team_counts, on="offense_team_id", how="inner", suffix="_team").select(
        "offense_team_id",
        "player_id",
        *[(pl.col(f"{c}_team") - pl.col(c)).alias(c) for c in count_cols],
    )

    on_rates = _context_rates(on_counts, league_ppp)
    off_rates = _context_rates(off_counts, league_ppp)

    metric_cols = [c for c in _CONTEXT_METRIC_SCHEMA if c not in ("halfcourt_points",)]
    out = on_rates.join(
        off_rates.rename({c: f"off_{c}" for c in metric_cols}),
        on=["offense_team_id", "player_id"],
        how="inner",
    ).rename({c: f"on_{c}" for c in metric_cols})

    out = out.with_columns(
        (pl.col("on_pts_per_100") - pl.col("off_pts_per_100")).alias("diff_pts_per_100"),
        (pl.col("on_transition_freq") - pl.col("off_transition_freq")).alias("diff_transition_freq"),
        (pl.col("on_transition_pts_per_100") - pl.col("off_transition_pts_per_100")).alias(
            "diff_transition_pts_per_100"
        ),
        (pl.col("on_halfcourt_pts_per_100") - pl.col("off_halfcourt_pts_per_100")).alias("diff_halfcourt_pts_per_100"),
    )
    out = out.select(list(PLAYER_PLAY_CONTEXT_SCHEMA)).sort(["offense_team_id", "on_poss"], descending=[False, True])
    return out.to_pandas() if return_as_pandas else out


# ---------------------------------------------------------------------------
# Public fetcher
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str, league_id: str = "00") -> dict:
    """Fetch the raw play-by-play v3 payload (module-level so tests can monkeypatch)."""
    from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

    return nba_stats_playbyplayv3(game_id=game_id, return_parsed=False)


def nba_play_context(
    game_id: str,
    league_id: str = "00",
    *,
    transition_seconds: float = C.DEFAULT_TRANSITION_SECONDS,
    transition_variant: str = C.DEFAULT_TRANSITION_VARIANT,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Fetch one game and return its possessions with the full CTG play-context surface.

    Single live call to ``nba_stats_playbyplayv3``, then
    :func:`add_play_context`. Works for NBA (``league_id="00"``), WNBA and the
    G-League — ``stats.wnba.com`` ships the same play-by-play shapes, and every
    threshold here is league-agnostic.

    Args:
        game_id: Ten-character game identifier (e.g. ``"0022200001"``).
        league_id: League identifier (``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League).
        transition_seconds: Transition initial-play cutoff (default 10.0).
        transition_variant: See :func:`add_transition`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Possession frame with the play-context columns. Empty/malformed payloads
        return a zero-row frame — never raises.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_play_context import nba_play_context
            poss = nba_play_context("0022200001")
            print(poss["possession_start_type_ctg"].value_counts())

        Transition rate for the game::

            import polars as pl
            clean = poss.filter(
                (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
            )
            print(clean["is_transition"].mean())

        See Also:
            * `pbpstats`_ -- the reference possession parser this builds on.
            * `hoopR`_ -- R sister package.

        .. _pbpstats: https://github.com/dblackrun/pbpstats
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload

    payload = _fetch_pbp(game_id, league_id)
    enh = enhanced_pbp_from_payload(payload, league_id=league_id)
    out = add_play_context(enh, transition_seconds=transition_seconds, transition_variant=transition_variant)
    return out.to_pandas() if return_as_pandas else out
