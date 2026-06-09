"""Pure HockeyTech analytics: frame(s) -> frame. No network.

Corsi/Fenwick caveat: the HockeyTech feed has no missed-shot event, so shot
attempts = shot + blocked_shot + goal. Both metrics are proxies; outputs carry
``corsi_includes_missed = False`` (added in a later task).
"""

from __future__ import annotations

import math

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

    ox = pl.col("x_coord")
    oy = pl.col("y_coord")

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


def add_shot_distance_angle(pbp: pl.DataFrame, goal_x: float = 89.0) -> pl.DataFrame:
    """Add ``shot_distance``/``shot_angle`` (feet/degrees) for shot-type events.

    Assumes coordinates are already in a standard rink frame (offensive net at
    +goal_x, y=0). Non-shot rows receive null values for both columns.

    Parameters
    ----------
    pbp:
        Play-by-play frame with at least ``event``, ``x_coord``, ``y_coord``
        columns. Coordinates must already be in feet in the standard rink frame.
    goal_x:
        X-coordinate of the offensive net in feet. Default is 89.0 (NHL).

    Returns
    -------
    pl.DataFrame
        Input frame with ``shot_distance`` (Float64) and ``shot_angle``
        (Float64, degrees) columns appended.
    """
    if pbp.height == 0:
        return pbp.with_columns(
            shot_distance=pl.lit(None, dtype=pl.Float64),
            shot_angle=pl.lit(None, dtype=pl.Float64),
        )

    dx = pl.lit(goal_x) - pl.col("x_coord").abs()
    dy = pl.col("y_coord")
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


def build_on_ice(pbp: pl.DataFrame, shifts: pl.DataFrame) -> pl.DataFrame:
    """Attach ``on_ice_home``/``on_ice_away`` (comma-joined player_ids) per event.

    ``pbp`` must carry integer ``period_of_game`` and ``time_s`` (seconds remaining,
    countdown). A player is on ice iff a shift in that period has
    ``start_s >= time_s >= end_s``. ``shifts`` carries ``home`` (1/0).
    Returns ``pbp`` with two added columns, one row per original event, order preserved.
    """
    if pbp.height == 0 or shifts.height == 0:
        return pbp.with_columns(
            on_ice_home=pl.lit(None, dtype=pl.Utf8),
            on_ice_away=pl.lit(None, dtype=pl.Utf8),
        )

    indexed = pbp.with_row_index("_eidx")
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
    on = joined.filter((pl.col("start_s") >= pl.col("time_s")) & (pl.col("time_s") >= pl.col("end_s")))

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
