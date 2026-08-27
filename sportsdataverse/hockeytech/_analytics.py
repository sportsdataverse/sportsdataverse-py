"""Pure HockeyTech analytics: frame(s) -> frame. No network (by default).

Corsi/Fenwick caveat: the HockeyTech feed has no missed-shot event, so shot
attempts = shot + blocked_shot + goal. Both metrics are proxies; outputs carry
``corsi_includes_missed = False`` (added in a later task).
"""

from __future__ import annotations

import math
from typing import Any, Optional

import polars as pl

# ---------------------------------------------------------------------------
# PWHL PBP enrichment: clock + coordinate transforms
# Ported faithfully from fastRhockey R/pwhl_pbp.R
# ---------------------------------------------------------------------------

# Period-length in seconds for sec_from_start offset (PWHL: 20 min = 1200 s).
_PERIOD_OFFSET_S = {
    1: 0,
    2: 1200,
    3: 2400,
    4: 3600,
    5: 4800,
}


def add_clock_columns(pbp: pl.DataFrame) -> pl.DataFrame:
    """Add period-clock columns derived from ``time_of_period`` (elapsed MM:SS).

    Ported from ``fastRhockey::pwhl_pbp`` (R/pwhl_pbp.R, lines 498-525).

    ``time_of_period`` is ELAPSED seconds counting UP from ``"0:00"`` at the
    start of each period.  The four columns added are:

    - ``minute_start`` (Int64): elapsed minutes component of ``time_of_period``.
    - ``second_start`` (Int64): elapsed seconds component of ``time_of_period``.
    - ``clock`` (Utf8): remaining time in the period formatted as ``"M:SS"``.
      Computed as ``(19 - minute_start):(60 - second_start)`` with the R
      special-cases:

      * When ``minute_start == 0`` **and** ``second_start == 0`` (start of
        period), ``clock = "20:00"``.
      * When ``second_start == 0`` (but not both zero), ``second = 0``.
      * Edge: ``minute_start = 20`` produces ``minute = -1`` (faithful to R).

    - ``sec_from_start`` (Int64): cumulative game-seconds elapsed since the
      very start of the game.  Within each period it equals
      ``minute_start * 60 + second_start``; a per-period offset is then added:
      period 1 → +0, period 2 → +1200, period 3 → +2400, period 4 → +3600,
      period 5 → +4800.

    The function is idempotent: if the columns already exist they are
    overwritten.  Rows with a null ``time_of_period`` or un-parseable value
    receive null for all four columns.  The input frame is not mutated.

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``time_of_period`` (Utf8, ``"M:SS"``)
        and ``period_of_game`` (Utf8 or castable to Int64) columns.

    Returns
    -------
    pl.DataFrame
        Input frame with the four clock columns appended (or replaced).
    """
    if pbp.height == 0:
        return pbp.with_columns(
            minute_start=pl.lit(None, dtype=pl.Int64),
            second_start=pl.lit(None, dtype=pl.Int64),
            clock=pl.lit(None, dtype=pl.Utf8),
            sec_from_start=pl.lit(None, dtype=pl.Int64),
        )

    # --- parse "M:SS" into minute_start and second_start -------------------
    # Use str.split(":") and index into the resulting list.
    split = pl.col("time_of_period").str.split(":")
    minute_start = split.list.get(0).cast(pl.Int64, strict=False)
    second_start = split.list.get(1).cast(pl.Int64, strict=False)

    # --- clock (remaining time) --------------------------------------------
    # R logic (lines 502-512):
    #   minute = if (19 - minute_start == 19 AND 60 - second_start == 60) 20
    #            else 19 - minute_start
    #   second = if (60 - second_start == 60) 0 else 60 - second_start
    #   second formatted as zero-padded 2-digit string
    #   clock  = paste0(minute, ":", second)
    #
    # Equivalent conditions:
    #   19 - m == 19  ↔  m == 0
    #   60 - s == 60  ↔  s == 0
    m = minute_start
    s = second_start

    clock_minute = pl.when((m == 0) & (s == 0)).then(pl.lit(20)).otherwise(pl.lit(19) - m)
    clock_second_raw = pl.when(s == 0).then(pl.lit(0)).otherwise(pl.lit(60) - s)
    # Zero-pad seconds to two digits: "0" + str if < 10, else str
    clock_second_str = (
        pl.when(clock_second_raw < 10)
        .then(pl.lit("0") + clock_second_raw.cast(pl.Utf8))
        .otherwise(clock_second_raw.cast(pl.Utf8))
    )
    clock_expr = clock_minute.cast(pl.Utf8) + pl.lit(":") + clock_second_str

    # --- sec_from_start ----------------------------------------------------
    # Base: elapsed seconds within period
    base_sfs = m * 60 + s

    # Period offset via case_when (R lines 515-521):
    #   period 2 → +1200, period 3 → +2400, period 4 → +3600, period 5 → +4800
    period_int = pl.col("period_of_game").cast(pl.Int64, strict=False)
    sfs = (
        pl.when(period_int == 2)
        .then(base_sfs + 1200)
        .when(period_int == 3)
        .then(base_sfs + 2400)
        .when(period_int == 4)
        .then(base_sfs + 3600)
        .when(period_int == 5)
        .then(base_sfs + 4800)
        .otherwise(base_sfs)
    )

    return pbp.with_columns(
        minute_start=minute_start.alias("minute_start"),
        second_start=second_start.alias("second_start"),
        clock=clock_expr.alias("clock"),
        sec_from_start=sfs.alias("sec_from_start"),
    )


def add_coord_transforms(pbp: pl.DataFrame) -> pl.DataFrame:
    """Add normalized coordinate columns from raw ``x_coord``/``y_coord``.

    Ported from ``fastRhockey::pwhl_pbp`` (R/pwhl_pbp.R, lines 484-496).

    Raw coordinates (``x_coord``, ``y_coord``) come from the HockeyTech feed
    on an approximately 850×400 canvas.  This function adds ten derived
    columns that map those raw values into various normalized frames used by
    fastRhockey.

    The transform sequence (faithful to the R mutate call, where dplyr's
    ``.data$col`` within a single ``mutate()`` sees values produced by
    earlier assignments in the same call):

    .. code-block:: text

        ox, oy         = raw x_coord, y_coord

        x_coord_original = ox
        y_coord_original = oy

        x_coord_neutral  = ox - 300
        y_coord_neutral  = oy - 150

        x_t  = (ox / 3) - 100                          [R: x_coord = ...]
        y_t  = 42.5 - ((oy * 85 / 300) - 42.5) - 42.5  [R: y_coord = ...]
             = 42.5 - (oy * 85 / 300)                  [simplified]

        x_coord_fixed = x_t / 3
        y_coord_fixed = 42.5 - ((y_t * 85 / 300) - 42.5)

        x_coord_right = if team_id == home_team_id: 100 + (100 - x_t) else x_t
        y_coord_right = if team_id == home_team_id: 42.5 - (y_t - 42.5)  else y_t

        x_coord_vertical = 42.5 - (y_coord_right - 42.5)
        y_coord_vertical = x_coord_right

    Rows with null ``x_coord`` or ``y_coord`` produce null for all ten columns.
    Rows with null ``team_id`` or ``home_team_id`` produce null for
    ``x_coord_right``, ``y_coord_right``, ``x_coord_vertical``,
    ``y_coord_vertical`` (the team-dependent transforms).

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``x_coord`` (Float64),
        ``y_coord`` (Float64), ``team_id`` (Utf8), and ``home_team_id``
        (Utf8 or castable) columns.

    Returns
    -------
    pl.DataFrame
        Input frame with the ten coordinate columns appended (or replaced).
    """
    if pbp.height == 0:
        return pbp.with_columns(
            x_coord_original=pl.lit(None, dtype=pl.Float64),
            y_coord_original=pl.lit(None, dtype=pl.Float64),
            x_coord_neutral=pl.lit(None, dtype=pl.Float64),
            y_coord_neutral=pl.lit(None, dtype=pl.Float64),
            x_coord_fixed=pl.lit(None, dtype=pl.Float64),
            y_coord_fixed=pl.lit(None, dtype=pl.Float64),
            x_coord_right=pl.lit(None, dtype=pl.Float64),
            y_coord_right=pl.lit(None, dtype=pl.Float64),
            x_coord_vertical=pl.lit(None, dtype=pl.Float64),
            y_coord_vertical=pl.lit(None, dtype=pl.Float64),
        )

    # Coerce raw coords to Float64. Incomplete/current-season games whose PBP
    # carries no coordinate-bearing events yield an all-null column that
    # pd.json_normalize -> pl.from_pandas infers as Utf8; dividing a String
    # column raises ``InvalidOperationError: division with 'String' datatypes``.
    # strict=False also rescues feeds that serialize coords as numeric strings.
    ox = pl.col("x_coord").cast(pl.Float64, strict=False)
    oy = pl.col("y_coord").cast(pl.Float64, strict=False)

    # Intermediate transformed coordinates (R lines 489-490):
    #   x_coord = (x_coord / 3) - 100
    #   y_coord = 42.5 - (((y_coord * 85) / 300) - 42.5) - 42.5
    #           = 42.5 - y_coord*85/300 + 42.5 - 42.5
    #           = 42.5 - y_coord*85/300
    x_t = (ox / 3.0) - 100.0
    y_t = 42.5 - (oy * 85.0 / 300.0)

    # x_coord_fixed = .data$x_coord / 3  (uses x_t, the transformed value)
    x_coord_fixed = x_t / 3.0

    # y_coord_fixed = 42.5 - (((.data$y_coord * 85) / 300) - 42.5)
    #               (uses y_t, the transformed value)
    y_coord_fixed = 42.5 - ((y_t * 85.0 / 300.0) - 42.5)

    # Team-dependent right/vertical transforms.
    # ``home_team_id`` is only present after a meta-join (task A2.5b).
    # When absent, treat all rows as away team (passthrough).
    if "home_team_id" in pbp.columns:
        is_home = pl.col("team_id").cast(pl.Utf8) == pl.col("home_team_id").cast(pl.Utf8)
    else:
        is_home = pl.lit(False)

    x_coord_right = pl.when(is_home).then(100.0 + (100.0 - x_t)).otherwise(x_t)
    y_coord_right = pl.when(is_home).then(42.5 - (y_t - 42.5)).otherwise(y_t)

    # Vertical projection uses the right coords (computed above as intermediates)
    # R: x_coord_vertical = 42.5 - (.data$y_coord_right - 42.5)
    #    y_coord_vertical = .data$x_coord_right
    # We compute these as a second with_columns pass to reference the right cols.
    out = pbp.with_columns(
        x_coord_original=ox,
        y_coord_original=oy,
        x_coord_neutral=(ox - 300.0),
        y_coord_neutral=(oy - 150.0),
        x_coord_fixed=x_coord_fixed,
        y_coord_fixed=y_coord_fixed,
        x_coord_right=x_coord_right,
        y_coord_right=y_coord_right,
    )

    return out.with_columns(
        x_coord_vertical=(42.5 - (pl.col("y_coord_right") - 42.5)),
        y_coord_vertical=pl.col("x_coord_right"),
    )


_SCORING_CHANCE_FT = 25.0  # distance threshold from net (feet)
_SHOT_EVENTS = ["shot", "blocked_shot", "goal"]

#: Offensive goal-line x-coordinate (feet) on an NHL-size (200x85 ft) rink. The PWHL
#: plays on NHL-size rinks, so 89.0 is correct for it too -- but it is asserted, not
#: assumed: a caller who passes a mis-scaled ``goal_x`` (e.g. a RAW-feed value) would
#: otherwise silently compute garbage distances. See the guard in
#: :func:`add_shot_distance_angle`.
_NHL_SIZE_RINK_GOAL_X = 89.0
_MAX_PLAUSIBLE_GOAL_X = 110.0  # rink half-length is 100 ft; a goal_x past this is a scale error


def add_shot_distance_angle(pbp: pl.DataFrame, goal_x: float = _NHL_SIZE_RINK_GOAL_X) -> pl.DataFrame:
    """Add ``shot_distance``/``shot_angle`` (feet/degrees) for shot-type events.

    Assumes coordinates are already in a standard rink frame (offensive net at
    +goal_x, y=0). Non-shot rows receive null values for both columns.

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``event``, ``x_coord``, ``y_coord``
        columns. Coordinates must already be in feet in the standard rink frame.
    goal_x:
        X-coordinate of the offensive net in feet. Default is
        :data:`_NHL_SIZE_RINK_GOAL_X` (89.0) -- correct for the PWHL, which plays
        on an NHL-size rink. Asserted to lie within a plausible rink range so a
        mis-scaled value can't silently produce garbage geometry.

    Returns
    -------
    pl.DataFrame
        Input frame with ``shot_distance`` (Float64) and ``shot_angle``
        (Float64, degrees) columns appended.

    Raises
    ------
    ValueError
        If ``goal_x`` is outside ``(0, _MAX_PLAUSIBLE_GOAL_X]`` feet -- a scale error.
    """
    if not 0.0 < goal_x <= _MAX_PLAUSIBLE_GOAL_X:
        raise ValueError(
            f"goal_x={goal_x} ft is outside the plausible rink range (0, {_MAX_PLAUSIBLE_GOAL_X}]; "
            "coordinates must be in standard rink-feet (offensive net near +89 ft), not RAW feed scale."
        )
    if pbp.height == 0:
        return pbp.with_columns(
            shot_distance=pl.lit(None, dtype=pl.Float64),
            shot_angle=pl.lit(None, dtype=pl.Float64),
        )

    # Coerce to Float64 so an all-null (Utf8-inferred) coord column does not
    # raise on the arithmetic below — see add_coord_transforms for context.
    dx = pl.lit(goal_x) - pl.col("x_coord").cast(pl.Float64, strict=False).abs()
    dy = pl.col("y_coord").cast(pl.Float64, strict=False)
    dist = (dx**2 + dy**2).sqrt()
    # pl.arctan2(y, x) -> radians; convert to degrees and take absolute value
    angle = pl.arctan2(dy.abs(), dx).abs() * (180.0 / math.pi)

    is_shot = pl.col("event").is_in(_SHOT_EVENTS)
    return pbp.with_columns(
        shot_distance=pl.when(is_shot).then(dist).otherwise(None),
        shot_angle=pl.when(is_shot).then(angle).otherwise(None),
    )


def scoring_chances(pbp: pl.DataFrame, threshold_ft: float = _SCORING_CHANCE_FT) -> pl.DataFrame:
    """Flag ``scoring_chance`` for shot-type events within ``threshold_ft`` of net.

    Parameters
    ----------
    pbp:
        Play-by-play frame. If ``shot_distance`` is not already present,
        :func:`add_shot_distance_angle` is called automatically.
    threshold_ft:
        Distance threshold in feet. Shots at or inside this distance from the
        net are flagged as scoring chances. Default is 25.0 ft.

    Returns
    -------
    pl.DataFrame
        Input frame with a ``scoring_chance`` (Boolean) column appended.
    """
    if "shot_distance" not in pbp.columns:
        pbp = add_shot_distance_angle(pbp)
    return pbp.with_columns(
        scoring_chance=(pl.col("shot_distance").is_not_null() & (pl.col("shot_distance") <= threshold_ft))
    )


#: Seconds BEFORE a goal instant at which on-ice personnel are evaluated.
#: The HockeyTech shift chart is frequently already rolled to the POST-goal
#: deployment at the goal's own timestamp, so evaluating at eps=0 reads the
#: line that came over the boards after the goal. See :func:`build_on_ice`.
GOAL_EPSILON_S = 2


def build_on_ice(pbp: pl.DataFrame, shifts: pl.DataFrame, goal_epsilon_s: int = GOAL_EPSILON_S) -> pl.DataFrame:
    """Attach ``on_ice_home``/``on_ice_away`` (comma-joined player_ids) per event.

    ``pbp`` must carry integer ``period_of_game`` and ``time_s`` (seconds remaining,
    countdown). A player is on ice iff a shift in that period has
    ``start_s >= time_s > end_s`` -- the end boundary is EXCLUSIVE so a
    line-change instant (outgoing ``end_s`` == incoming ``start_s`` == event
    ``time_s``) is owned only by the incoming shift, not double-counted across
    both lines (which produced impossible 10-v-10 on-ice counts). ``shifts``
    carries ``home`` (1/0).

    **Goal-instant epsilon convention.** HockeyTech shift boundaries are
    unreliable at the exact second of a goal: the chart is often already rolled
    to the post-goal deployment, so an eps=0 lookup returns the line that came
    over the boards *after* the goal rather than the one that was on for it.
    Rows whose ``event`` is ``"goal"`` are therefore evaluated ``goal_epsilon_s``
    seconds BEFORE the goal instant (a larger countdown value), clamped to the
    period's start so an opening-seconds goal still resolves. Pass
    ``goal_epsilon_s=0`` to restore the old at-the-instant behavior. Non-goal
    events are unaffected.

    Args:
        pbp: event frame carrying ``period_of_game`` and ``time_s``.
        shifts: parsed shift frame (``player_id``, ``home``, ``period``,
            ``start_s``, ``end_s``).
        goal_epsilon_s: seconds before a goal instant at which to evaluate
            on-ice personnel. Defaults to :data:`GOAL_EPSILON_S` (2).

    Returns:
        ``pbp`` with two added columns, one row per original event, order preserved.
    """
    if pbp.height == 0 or shifts.height == 0:
        return pbp.with_columns(
            on_ice_home=pl.lit(None, dtype=pl.Utf8),
            on_ice_away=pl.lit(None, dtype=pl.Utf8),
        )

    indexed = pbp.with_row_index("_eidx")

    # Goal-instant epsilon: look up goals `goal_epsilon_s` seconds earlier
    # (countdown clock -> larger value), clamped to the period's own start so a
    # goal in the opening seconds does not fall outside every shift.
    if goal_epsilon_s and "event" in indexed.columns:
        period_start = shifts.group_by("period").agg(pl.col("start_s").max().alias("_period_start_s"))
        indexed = (
            indexed.join(period_start, left_on="period_of_game", right_on="period", how="left")
            .with_columns(
                time_s=pl.when(pl.col("event") == "goal")
                .then(
                    pl.min_horizontal(
                        pl.col("time_s") + goal_epsilon_s,
                        pl.col("_period_start_s").fill_null(pl.col("time_s") + goal_epsilon_s),
                    )
                )
                .otherwise(pl.col("time_s"))
            )
            .drop("_period_start_s")
        )
    # Rename shifts player_id to avoid collision with any player_id column in pbp.
    shifts_sel = shifts.select(
        [
            pl.col("player_id").alias("_shift_pid"),
            pl.col("home"),
            pl.col("period"),
            pl.col("start_s"),
            pl.col("end_s"),
        ]
    )
    # join all shifts in the same period, then filter to those covering the event time
    joined = indexed.join(
        shifts_sel,
        left_on="period_of_game",
        right_on="period",
        how="inner",
    )
    # End boundary EXCLUSIVE (`> end_s`, not `>= end_s`): at a line change the
    # outgoing shift's end_s equals the incoming shift's start_s equals the event
    # time; a closed interval counts BOTH lines (impossible ~10-v-10 counts).
    on = joined.filter((pl.col("start_s") >= pl.col("time_s")) & (pl.col("time_s") > pl.col("end_s")))

    def _side(home_flag: int, name: str) -> pl.DataFrame:
        side = on.filter(pl.col("home") == home_flag)
        if side.height == 0:
            return pl.DataFrame(schema={"_eidx": pl.UInt32, name: pl.Utf8})
        # Cast Int64 -> Utf8 directly (avoids Float64 intermediate and ".0" suffix).
        return side.group_by("_eidx").agg(
            pl.col("_shift_pid").cast(pl.Int64).cast(pl.Utf8).unique().sort().str.join(",").alias(name)
        )

    home_agg = _side(1, "on_ice_home")
    away_agg = _side(0, "on_ice_away")
    out = indexed.join(home_agg, on="_eidx", how="left").join(away_agg, on="_eidx", how="left")
    return out.drop("_eidx")


def add_strength_state(pbp: pl.DataFrame, goalie_ids: "list | set | None" = None) -> pl.DataFrame:
    """Derive ``strength_state`` + skater counts from on-ice ids.

    Requires ``on_ice_home`` / ``on_ice_away`` (comma-joined player ids, from
    :func:`build_on_ice`). ``goalie_ids`` is the set of goalie ``player_id`` s
    (any type; coerced to ``str``) for the game -- typically the goalies from
    ``game_rosters`` (``player_type``) or the pbp ``goalie_id`` column. Goalies
    are stripped from the skater counts. When ``goalie_ids`` is ``None``/empty
    each side is assumed to carry exactly one goalie (``skaters = on-ice - 1``).

    The skater count is **robust to HockeyTech goalie shift-tracking gaps**: a
    pulled goalie (6 skaters, no goalie on ice) reads as ``"6v5"``, and a goalie
    the shift feed omits still leaves the 5 skaters counted correctly. That is
    exactly why an *empty-net* flag is NOT derived here -- detecting an empty net
    needs the goalie's on-ice presence, which the shift feed does not reliably
    carry (~40% false positives observed on real games). Use the authoritative
    goal-level ``empty_net`` field already on the pbp, or derive per-event
    goalie-pulled state from ``goalie_change`` events instead.

    Adds:
      - ``skaters_home`` / ``skaters_away`` (``Int64``): non-goalie on-ice counts.
      - ``strength_state`` (``Utf8``): ``"{skaters_home}v{skaters_away}"`` (home
        perspective; ``"5v5"``, ``"5v4"`` home-PP, ``"4v5"`` home-SH, ``"6v5"``
        home net empty). ``null`` when on-ice is missing.
      - ``strength_state_valid`` (``Boolean``): ``False`` when a count is
        implausible (skaters < 3 or > 6) -- the HockeyTech shift-boundary noise
        that survives the :func:`build_on_ice` fix (~5% of events). Downstream
        should drop/clamp invalid rows rather than trust the count.

    At goal instants the on-ice lists this reads are evaluated a couple of
    seconds BEFORE the goal (see :func:`build_on_ice`'s goal-epsilon
    convention), because the HockeyTech shift chart is typically already rolled
    to the post-goal deployment at the goal's own timestamp. That is what makes
    EV/PP/SH goal attribution derived from ``strength_state`` trustworthy.

    Args:
        pbp: frame carrying ``on_ice_home`` / ``on_ice_away``.
        goalie_ids: goalie ``player_id`` s for the game (coerced to ``str``).

    Returns:
        ``pbp`` with the three columns above appended, order/rows preserved.

    Example:
        Quick start::

            from sportsdataverse.hockeytech._analytics import add_strength_state
            out = add_strength_state(pbp, goalie_ids={"99", "88"})
            out.select("strength_state", "skaters_home", "strength_state_valid")
    """
    gset = [str(g) for g in (goalie_ids or [])]
    have_g = len(gset) > 0
    null_cols = dict(
        skaters_home=pl.lit(None, dtype=pl.Int64),
        skaters_away=pl.lit(None, dtype=pl.Int64),
        strength_state=pl.lit(None, dtype=pl.Utf8),
        strength_state_valid=pl.lit(None, dtype=pl.Boolean),
    )
    if pbp.height == 0 or not {"on_ice_home", "on_ice_away"}.issubset(pbp.columns):
        return pbp.with_columns(**null_cols)

    def _skaters(col: str):
        ids = pl.col(col).str.split(",")
        total = ids.list.len().cast(pl.Int64)
        if have_g:
            # Count goalie ids present via per-goalie list membership -- avoids a
            # polars list expression that a naive pre-commit hook rejects (it
            # text-matches the builtin). On-ice ids are unique so each goalie
            # contributes at most 1.
            goalies = pl.sum_horizontal([ids.list.contains(g).cast(pl.Int64) for g in gset])
        else:
            # unknown goalies: assume exactly one per side (null-safe)
            goalies = pl.when(total.is_null()).then(None).otherwise(pl.lit(1)).cast(pl.Int64)
        return total - goalies

    out = pbp.with_columns(skaters_home=_skaters("on_ice_home"), skaters_away=_skaters("on_ice_away"))
    both = pl.col("skaters_home").is_not_null() & pl.col("skaters_away").is_not_null()
    return out.with_columns(
        strength_state=pl.when(both)
        .then(pl.col("skaters_home").cast(pl.Utf8) + pl.lit("v") + pl.col("skaters_away").cast(pl.Utf8))
        .otherwise(None),
        strength_state_valid=pl.col("skaters_home").is_between(3, 6) & pl.col("skaters_away").is_between(3, 6),
    )


_CORSI_EVENTS = ["shot", "blocked_shot", "goal"]
_FENWICK_EVENTS = ["shot", "goal"]


def corsi_fenwick(pbp: pl.DataFrame) -> pl.DataFrame:
    """Team-level shot-attempt counts (Corsi and Fenwick proxies).

    Corsi = shot + blocked_shot + goal; Fenwick excludes blocked_shot.
    Missed shots are unavailable from the HockeyTech feed, so both metrics
    are proxies — every output row carries ``corsi_includes_missed = False``.

    Returns one row per ``team_id`` with CF/CA/CF% and FF/FA/FF%.

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``event`` and ``team_id`` columns.

    Returns
    -------
    pl.DataFrame
        One row per team with columns: ``team_id``, ``corsi_for``,
        ``corsi_against``, ``corsi_for_pct``, ``fenwick_for``,
        ``fenwick_against``, ``fenwick_for_pct``, ``corsi_includes_missed``.
    """
    empty_schema = {
        "team_id": pl.Int64,
        "corsi_for": pl.Int64,
        "corsi_against": pl.Int64,
        "corsi_for_pct": pl.Float64,
        "fenwick_for": pl.Int64,
        "fenwick_against": pl.Int64,
        "fenwick_for_pct": pl.Float64,
        "corsi_includes_missed": pl.Boolean,
    }
    if pbp.height == 0:
        return pl.DataFrame(schema=empty_schema)

    teams = [t for t in pbp.get_column("team_id").unique().to_list() if t is not None]
    if not teams:
        return pl.DataFrame(schema=empty_schema)

    rows = []
    for t in teams:
        cf = pbp.filter(pl.col("event").is_in(_CORSI_EVENTS) & (pl.col("team_id") == t)).height
        ca = pbp.filter(
            pl.col("event").is_in(_CORSI_EVENTS) & (pl.col("team_id") != t) & pl.col("team_id").is_not_null()
        ).height
        ff = pbp.filter(pl.col("event").is_in(_FENWICK_EVENTS) & (pl.col("team_id") == t)).height
        fa = pbp.filter(
            pl.col("event").is_in(_FENWICK_EVENTS) & (pl.col("team_id") != t) & pl.col("team_id").is_not_null()
        ).height
        rows.append(
            {
                "team_id": t,
                "corsi_for": cf,
                "corsi_against": ca,
                "corsi_for_pct": (cf / (cf + ca)) if (cf + ca) else None,
                "fenwick_for": ff,
                "fenwick_against": fa,
                "fenwick_for_pct": (ff / (ff + fa)) if (ff + fa) else None,
                "corsi_includes_missed": False,
            }
        )
    return pl.DataFrame(rows)


_CORSI_FENWICK_ON_ICE_SCHEMA = {
    "player_id": pl.Utf8,
    "corsi_for": pl.Int64,
    "corsi_against": pl.Int64,
    "corsi_for_pct": pl.Float64,
    "fenwick_for": pl.Int64,
    "fenwick_against": pl.Int64,
    "fenwick_for_pct": pl.Float64,
    "corsi_includes_missed": pl.Boolean,
}


def corsi_fenwick_on_ice(pbp: pl.DataFrame) -> pl.DataFrame:
    """Player-level on-ice shot-attempt metrics (Corsi and Fenwick proxies).

    Computes CF/CA/FF/FA for every player found in the ``on_ice_home`` or
    ``on_ice_away`` column of an *enriched* play-by-play frame.  The enriched
    frame must carry:

    - ``event`` (Utf8): event type.
    - ``team_id`` (Utf8): team that performed the event.
    - ``home_team_id`` (Utf8): the home team for the game.
    - ``on_ice_home`` (Utf8): comma-joined player ids for the home side.
    - ``on_ice_away`` (Utf8): comma-joined player ids for the away side.

    For each shot-attempt event (``shot``, ``blocked_shot``, ``goal``):

    - The event team's side is determined by comparing ``team_id`` to
      ``home_team_id``.  Home side == *for*; away side == *against*.
    - Each on-ice "for" player receives **CF +1** (and **FF +1** if the event
      is ``shot`` or ``goal``, i.e. not a ``blocked_shot``).
    - Each on-ice "against" player receives **CA +1** (and **FA +1** if not
      a ``blocked_shot``).

    Corsi/Fenwick are proxies: the HockeyTech feed has no missed-shot event,
    so every output row carries ``corsi_includes_missed = False``.

    Parameters
    ----------
    pbp:
        Enriched play-by-play frame as returned by ``pwhl_pbp()``.

    Returns
    -------
    pl.DataFrame
        One row per player with columns: ``player_id``, ``corsi_for``,
        ``corsi_against``, ``corsi_for_pct``, ``fenwick_for``,
        ``fenwick_against``, ``fenwick_for_pct``, ``corsi_includes_missed``.
        Empty/missing-columns input returns a zero-row frame with this schema.
    """
    empty = pl.DataFrame(schema=_CORSI_FENWICK_ON_ICE_SCHEMA)

    required = {"event", "team_id", "home_team_id", "on_ice_home", "on_ice_away"}
    if pbp.height == 0 or not required.issubset(pbp.columns):
        return empty

    # Accumulate CF/CA/FF/FA per player_id (string key)
    stats: dict[str, dict[str, int]] = {}

    def _ensure(pid: str) -> None:
        if pid not in stats:
            stats[pid] = {"cf": 0, "ca": 0, "ff": 0, "fa": 0}

    shot_events = {"shot", "blocked_shot", "goal"}
    fenwick_events = {"shot", "goal"}

    for row in pbp.iter_rows(named=True):
        event = row.get("event")
        if event not in shot_events:
            continue

        team_id = row.get("team_id")
        home_team_id = row.get("home_team_id")
        on_ice_home_raw = row.get("on_ice_home")
        on_ice_away_raw = row.get("on_ice_away")

        if team_id is None or home_team_id is None:
            continue
        if on_ice_home_raw is None or on_ice_away_raw is None:
            continue

        is_home_event = str(team_id) == str(home_team_id)
        for_players_raw = on_ice_home_raw if is_home_event else on_ice_away_raw
        against_players_raw = on_ice_away_raw if is_home_event else on_ice_home_raw

        for_players = [p.strip() for p in str(for_players_raw).split(",") if p.strip()]
        against_players = [p.strip() for p in str(against_players_raw).split(",") if p.strip()]

        is_fenwick = event in fenwick_events

        for pid in for_players:
            _ensure(pid)
            stats[pid]["cf"] += 1
            if is_fenwick:
                stats[pid]["ff"] += 1

        for pid in against_players:
            _ensure(pid)
            stats[pid]["ca"] += 1
            if is_fenwick:
                stats[pid]["fa"] += 1

    if not stats:
        return empty

    rows = []
    for pid, s in stats.items():
        cf, ca, ff, fa = s["cf"], s["ca"], s["ff"], s["fa"]
        rows.append(
            {
                "player_id": pid,
                "corsi_for": cf,
                "corsi_against": ca,
                "corsi_for_pct": (cf / (cf + ca)) if (cf + ca) else None,
                "fenwick_for": ff,
                "fenwick_against": fa,
                "fenwick_for_pct": (ff / (ff + fa)) if (ff + fa) else None,
                "corsi_includes_missed": False,
            }
        )
    return pl.DataFrame(rows, schema=_CORSI_FENWICK_ON_ICE_SCHEMA)


def backfill_power_play(df: pl.DataFrame) -> pl.DataFrame:
    """Back-fill ``power_play`` and ``short_handed`` for shot/faceoff events.

    Ported from fastRhockey ``pwhl_pbp.R`` lines 522-565.

    For each penalty event whose ``power_play`` flag is ``"1"``, a PP window
    ``[start_sec, end_sec]`` is derived where:

    - ``start_sec`` = ``sec_from_start`` of the penalty event.
    - ``end_sec``   = ``start_sec + penalty_length * 60``, **truncated** at the
      cumulative time of the first goal scored during that window (mirrors the
      R power-kill-ends-on-goal logic).

    For every ``shot`` or ``faceoff`` event whose ``sec_from_start`` falls
    inside any PP window:

    - If the event's ``team_id`` equals the window's ``advantage_team``,
      ``power_play`` is set to ``"1"`` and ``short_handed`` to ``"0"``.
    - Otherwise ``power_play`` is set to ``"0"`` and ``short_handed`` to
      ``"1"``.

    All other rows are left unchanged.  The function is safe when there are
    zero penalty events (returns ``df`` unmodified after ensuring the two flag
    columns exist).

    Requires ``sec_from_start``, ``event``, ``power_play``,
    ``penalty_length``, ``team_id``, ``home_team_id``, ``away_team_id``
    columns to be present.  ``short_handed`` may be absent — it will be
    created.

    Parameters
    ----------
    df:
        PBP frame **after** :func:`add_clock_columns` has run (so
        ``sec_from_start`` is populated).

    Returns
    -------
    pl.DataFrame
        Input frame with ``power_play`` / ``short_handed`` back-filled for
        shot and faceoff events that occur during an active PP window.
    """
    # Ensure short_handed column exists (may be absent for non-PWHL feeds).
    if "short_handed" not in df.columns:
        df = df.with_columns(short_handed=pl.lit(None, dtype=pl.Utf8))

    # Ensure power_play exists.
    if "power_play" not in df.columns:
        df = df.with_columns(power_play=pl.lit(None, dtype=pl.Utf8))

    if df.height == 0:
        return df

    # Extract penalty rows with power_play == "1" and valid sec_from_start.
    pen_mask = (pl.col("event") == "penalty") & (pl.col("power_play") == "1") & pl.col("sec_from_start").is_not_null()
    pens_df = df.filter(pen_mask)

    if pens_df.height == 0:
        # No PP penalties -- nothing to back-fill.
        return df

    # Derive advantage_team: the team NOT penalized (penalized = team_id on penalty row).
    # advantage_team = away_team_id if penalized==home_team_id, else home_team_id.
    pens_df = pens_df.with_columns(
        advantage_team=pl.when(pl.col("team_id") == pl.col("home_team_id"))
        .then(pl.col("away_team_id"))
        .otherwise(pl.col("home_team_id"))
    )

    # Build penalty interval list: (start_sec, end_sec, advantage_team_id).
    # penalty_length is a string (e.g. "2") representing minutes.
    penalty_length_s = pens_df["penalty_length"].cast(pl.Float64, strict=False) * 60.0
    starts = pens_df["sec_from_start"].cast(pl.Float64, strict=False)
    ends = starts + penalty_length_s
    adv_teams = pens_df["advantage_team"].to_list()
    starts_list = starts.to_list()
    ends_list = ends.to_list()

    # Extract goal times for PP-end truncation.
    goal_times = (
        df.filter(pl.col("event") == "goal")
        .filter(pl.col("sec_from_start").is_not_null())["sec_from_start"]
        .cast(pl.Float64, strict=False)
        .to_list()
    )
    goal_times_sorted = sorted(t for t in goal_times if t is not None)

    # Truncate each penalty window at first goal within it.
    # Mirrors the R loop: for each penalty find the first goal in the PP window;
    # if found, set end_power_play = goal.sec_from_start.
    pen_intervals: list[tuple[float, float, str]] = []
    for i, (s, e, adv) in enumerate(zip(starts_list, ends_list, adv_teams)):
        if s is None or e is None:
            continue
        # Find goals in [s, e] that are after previous penalty's end (if any).
        prev_end = pen_intervals[i - 1][1] if i > 0 and pen_intervals else None
        for g in goal_times_sorted:
            if g >= s and g <= e:
                if prev_end is None or g > prev_end:
                    e = g
                    break
        pen_intervals.append((float(s), float(e), str(adv) if adv is not None else ""))

    # Back-fill shot/faceoff rows using the computed intervals.
    # We do this row-by-row via map_elements for correctness (intervals can overlap).
    event_col = df["event"].to_list()
    sec_col = df["sec_from_start"].cast(pl.Float64, strict=False).to_list()
    team_col = df["team_id"].to_list()
    pp_col = df["power_play"].to_list()
    sh_col = df["short_handed"].to_list()

    new_pp = list(pp_col)
    new_sh = list(sh_col)

    for idx, (ev, sec, team) in enumerate(zip(event_col, sec_col, team_col)):
        if ev not in ("shot", "faceoff"):
            continue
        if sec is None:
            continue
        for ps, pe, adv in pen_intervals:
            if ps <= sec <= pe:
                # This event is during a PP window.
                if str(team) == adv:
                    new_pp[idx] = "1"
                    new_sh[idx] = "0"
                else:
                    new_pp[idx] = "0"
                    new_sh[idx] = "1"
                break  # Use the first matching interval.

    return df.with_columns(
        power_play=pl.Series("power_play", new_pp, dtype=pl.Utf8),
        short_handed=pl.Series("short_handed", new_sh, dtype=pl.Utf8),
    )


def enrich_pbp(
    df: pl.DataFrame,
    league: str,
    game_id: int,
    *,
    meta_payload: Optional[Any] = None,
    shifts_payload: Optional[Any] = None,
    return_as_pandas: bool = False,
) -> Any:
    """Enrich a parsed HockeyTech PBP frame — league-generic.

    Applies the full enrichment pipeline:

    1. Game-meta join (``game_date``, ``game_season``, ``game_season_id``,
       ``home_team``, ``home_team_id``, ``away_team``, ``away_team_id``) from
       ``gc/gamesummary``.
    2. Coordinate transforms (``*_original``, ``*_neutral``, ``*_fixed``,
       ``*_right``, ``*_vertical`` — 10 columns) via
       :func:`add_coord_transforms`.
    3. Clock columns (``minute_start``, ``second_start``, ``clock``,
       ``sec_from_start``) via :func:`add_clock_columns`.
    4. Shot geometry (``shot_distance``, ``shot_angle``, ``scoring_chance``)
       via :func:`add_shot_distance_angle` + :func:`scoring_chances`.
    5. On-ice player tracking (``on_ice_home``, ``on_ice_away``) from
       ``modulekit/gameshifts`` via :func:`build_on_ice`.

    This function is pure when ``meta_payload`` and ``shifts_payload`` are
    injected (no network calls).  When either is ``None`` the corresponding
    feed is fetched via ``hockeytech_api`` using the supplied ``league`` code.
    Callers that need test isolation should fetch both payloads via their own
    (patchable) ``hockeytech_api`` reference and pass them in.

    Parameters
    ----------
    df:
        Raw frame produced by ``parse_pbp``.
    league:
        HockeyTech league code (e.g. ``"pwhl"``, ``"ohl"``, ``"whl"``).
    game_id:
        Numeric game identifier.
    meta_payload:
        Optional pre-fetched ``gc/gamesummary`` JSON dict.  Fetched live when
        ``None``.
    shifts_payload:
        Optional pre-fetched ``modulekit/gameshifts`` JSON dict.  Fetched live
        when ``None``.  Pass ``{}`` to suppress on-ice computation entirely.
    return_as_pandas:
        When ``True`` return a ``pandas.DataFrame``; otherwise return a
        :class:`polars.DataFrame`.

    Returns
    -------
    polars.DataFrame | pandas.DataFrame
        Enriched play-by-play frame.
    """
    # Import here to avoid top-level circular dependency risk.
    # _analytics is imported by pwhl_api which also imports hockeytech_api;
    # keeping this import lazy sidesteps any future circular-import issues.
    from sportsdataverse.hockeytech import _parsers as P
    from sportsdataverse.hockeytech._client import hockeytech_api

    # ------------------------------------------------------------------
    # Step 1: fetch meta if not provided
    # ------------------------------------------------------------------
    if meta_payload is None:
        meta_payload = hockeytech_api(league, "gc", "gamesummary", {"game_id": game_id})

    # ------------------------------------------------------------------
    # Step 2: extract game-meta fields from GC.Gamesummary
    # ------------------------------------------------------------------
    gs_root = (meta_payload if isinstance(meta_payload, dict) else {}).get("GC", {}) or {}
    gs = gs_root.get("Gamesummary", gs_root) or {}
    gs_meta = gs.get("meta") or {}
    home_raw = gs.get("home") or {}
    away_raw = gs.get("visitor") or {}

    home_team: str = str(home_raw.get("name") or home_raw.get("city") or "")
    home_team_id: str = str(gs_meta.get("home_team") or home_raw.get("id") or home_raw.get("team_id") or "")
    away_team: str = str(away_raw.get("name") or away_raw.get("city") or "")
    away_team_id: str = str(gs_meta.get("visiting_team") or away_raw.get("id") or away_raw.get("team_id") or "")

    game_date: str = str(gs_meta.get("date_played") or gs.get("game_date_iso_8601") or gs.get("game_date") or "")
    game_season_raw = game_date[:4] if game_date else None
    game_season: Optional[int] = int(game_season_raw) if game_season_raw and game_season_raw.isdigit() else None
    game_season_id: str = str(gs_meta.get("season_id") or "")

    # ------------------------------------------------------------------
    # Step 3: add game-meta literal columns BEFORE coord transforms
    #   (add_coord_transforms needs home_team_id to compute right/vertical)
    # ------------------------------------------------------------------
    df = df.with_columns(
        game_date=pl.lit(game_date),
        game_season=pl.lit(game_season),
        game_season_id=pl.lit(game_season_id),
        home_team=pl.lit(home_team),
        home_team_id=pl.lit(home_team_id),
        away_team=pl.lit(away_team),
        away_team_id=pl.lit(away_team_id),
    )

    # ------------------------------------------------------------------
    # Step 4: coordinate transforms
    # ------------------------------------------------------------------
    df = add_coord_transforms(df)

    # ------------------------------------------------------------------
    # Step 5: clock columns
    # ------------------------------------------------------------------
    df = add_clock_columns(df)

    # ------------------------------------------------------------------
    # Step 5b: PP / SH back-fill for shot & faceoff events
    #   Ported from fastRhockey R/pwhl_pbp.R lines 522-565.
    #   Requires sec_from_start (Step 5) and home_team_id/away_team_id (Step 3).
    # ------------------------------------------------------------------
    df = backfill_power_play(df)

    # ------------------------------------------------------------------
    # Step 6: shot geometry (use intermediate rink-feet frame)
    # ------------------------------------------------------------------
    geo = df.with_columns(
        x_coord=(pl.col("x_coord_original") / 3.0 - 100.0),
        y_coord=(42.5 - (pl.col("y_coord_original") * 85.0 / 300.0)),
    )
    geo = scoring_chances(add_shot_distance_angle(geo))
    df = df.with_columns(
        shot_distance=geo["shot_distance"],
        shot_angle=geo["shot_angle"],
        scoring_chance=geo["scoring_chance"],
    )

    # ------------------------------------------------------------------
    # Step 7: on-ice player tracking via shifts
    # ------------------------------------------------------------------
    if shifts_payload is None:
        shifts_payload = hockeytech_api(league, "modulekit", "gameshifts", {"game_id": game_id})

    if isinstance(shifts_payload, dict):
        shifts = P.parse_shifts(shifts_payload, game_id=game_id)
    else:
        shifts = pl.DataFrame()

    if df.height > 0 and shifts.height > 0:
        # Derive per-period ceiling from shifts: max(start_s) in each period.
        # For regulation PWHL periods this is ~1200 s; for OT periods it is
        # smaller (300 s for 5-minute OT).  Fallback to 1200 when a period has
        # no shifts at all.
        period_len_df = shifts.group_by("period").agg(pl.col("start_s").max().alias("plen"))
        period_len_map: dict[int, int] = {
            int(row["period"]): int(row["plen"])
            for row in period_len_df.iter_rows(named=True)
            if row["plen"] is not None
        }

        # Build a plen column on df, falling back to 1200 for unknown periods.
        period_int_expr = pl.col("period_of_game").cast(pl.Int64, strict=False)
        # Start with a default of 1200 then override per known period.
        plen_cases = pl.lit(1200, dtype=pl.Int64)
        for period_val, plen_val in period_len_map.items():
            plen_cases = (
                pl.when(period_int_expr == period_val).then(pl.lit(plen_val, dtype=pl.Int64)).otherwise(plen_cases)
            )

        elapsed_s = pl.col("minute_start") * 60 + pl.col("second_start")
        time_s = (plen_cases - elapsed_s).cast(pl.Int64, strict=False)

        df_copy = df.with_columns(
            _period_str=pl.col("period_of_game"),
            period_of_game=pl.col("period_of_game").cast(pl.Int64, strict=False),
            time_s=time_s,
        )
        result = build_on_ice(df_copy, shifts)
        result = result.with_columns(period_of_game=pl.col("_period_str")).drop(["_period_str", "time_s"])
        df = df.with_columns(
            on_ice_home=result["on_ice_home"],
            on_ice_away=result["on_ice_away"],
        )
    else:
        df = df.with_columns(
            on_ice_home=pl.lit(None, dtype=pl.Utf8),
            on_ice_away=pl.lit(None, dtype=pl.Utf8),
        )

    if return_as_pandas:
        return df.to_pandas()
    return df


def per60(value_col: str, toi_seconds_col: str = "toi_seconds") -> pl.Expr:
    """Per-60 rate expression.

    Computes ``value / toi_seconds * 3600``, aliased as ``<value_col>_per60``.

    Parameters
    ----------
    value_col:
        Name of the column containing the count to rate-ify.
    toi_seconds_col:
        Name of the column containing time-on-ice in seconds. Default is
        ``"toi_seconds"``.

    Returns
    -------
    pl.Expr
        A Polars expression suitable for use in ``with_columns`` or ``select``.
    """
    return (pl.col(value_col) / pl.col(toi_seconds_col) * 3600).alias(f"{value_col}_per60")


def player_toi(shifts: pl.DataFrame) -> pl.DataFrame:
    """Compute per-player TOI from a parsed shifts frame.

    The HockeyTech shift feed uses a countdown clock where ``start_s`` is the
    clock value at the start of the shift and ``end_s`` is the clock value at
    the end (``start_s >= end_s``); shift length is therefore
    ``start_s - end_s``.

    Parameters
    ----------
    shifts:
        Shifts frame with at least ``player_id``, ``first_name``,
        ``last_name``, ``start_s``, ``end_s`` columns.

    Returns
    -------
    pl.DataFrame
        One row per player with ``player_id``, ``first_name``, ``last_name``,
        ``toi_seconds`` (Int64), ``num_shifts`` (UInt32), and
        ``avg_shift_s`` (Float64), sorted by ``toi_seconds`` descending.
    """
    if shifts.height == 0:
        return pl.DataFrame(
            schema={
                "player_id": pl.Int64,
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "toi_seconds": pl.Int64,
                "num_shifts": pl.UInt32,
                "avg_shift_s": pl.Float64,
            }
        )
    per_shift = shifts.with_columns(shift_s=(pl.col("start_s") - pl.col("end_s")))
    return (
        per_shift.group_by("player_id", "first_name", "last_name")
        .agg(
            toi_seconds=pl.col("shift_s").sum(),
            num_shifts=pl.len(),
            avg_shift_s=pl.col("shift_s").mean(),
        )
        .sort("toi_seconds", descending=True)
    )
