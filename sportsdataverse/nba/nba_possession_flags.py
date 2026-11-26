"""Garbage-time, transition and 2-for-1 flags on the NBA/WNBA ``possessions`` table (BB-D7, BB-D44).

:func:`add_possession_flags` appends three columns to the published possession frame
(``nba_stats_possessions`` / ``wnba_stats_possessions``, or the producer's in-memory
``build_possessions`` + ``attach_possession_lineups`` output) and changes nothing else:
every existing column, value, row and row order is kept.

``is_garbage_time`` uses the same rule as the college flag on ``ncaa_{mbb,wbb}_possessions``
(bigballR's ``get_play_by_play``, ported in
:mod:`sportsdataverse.mbb.mbb_ncaa_game_pbp`). ``is_transition`` reads the possession's
``possession_start_type`` plus the time from the change of possession to its first shot,
within :data:`TRANSITION_SECONDS` (8). hoop-math and bigballR use 10 seconds for college
transition (bigballR also counts a steal or a drawn foul as the first play; this counts
field-goal attempts only); the pros' window is 8 because, of 6, 8 and 10 seconds, it lands
closest to the level of Synergy's ``Transition`` play type and correlates with it better than 10
(Pearson) in every season Synergy covers (see :data:`TRANSITION_SECONDS`). The WNBA, which Synergy does
not cover here, uses the same 8 seconds. The NBA / WNBA EPV boosters' ``transition`` state uses
these 8 seconds too since their 2026-11-17 refit
(:data:`~sportsdataverse.nba.nba_epv.EPV_TRANSITION_SECONDS`).

These are deliberately not the Cleaning the Glass definitions that
:mod:`sportsdataverse.nba.nba_play_context` reproduces as ``is_transition_ctg`` and
``is_garbage_time_ctg`` (4th-quarter margin bands with two or fewer starters, and a
6-second first-play window calibrated to CTG's published rates). The two engines agree on
the idea and differ on the thresholds; the columns here are the ones the possessions table,
the Data API and Game on Paper serve.

Garbage-time details. The on-floor players are the possession's ``off_player_*`` /
``def_player_*``: bigballR reads each event's lineup, the possessions table freezes it at the
possession's start. Regulation is four quarters, or two halves for a game whose first period
runs 20 minutes (the WNBA through 2005). Because any event counts, the possession that starts
garbage time can qualify on its own basket (about one in five do in 2023-24), so filtering on
the flag drops that possession's points too, as the college flag does. A model that needs the
state before a possession should test the margin at its start instead.
"""

from __future__ import annotations

from typing import Final

import numpy as np

import polars as pl

__all__ = [
    "GARBAGE_TIME_BANDS",
    "GARBAGE_TIME_MAX_STARTERS",
    "POSSESSION_FLAGS_SCHEMA",
    "TRANSITION_SECONDS",
    "TRANSITION_START_TYPES",
    "TWO_FOR_ONE_GO_FLOOR",
    "TWO_FOR_ONE_SCHEMA",
    "TWO_FOR_ONE_SHOT_CLOCK",
    "TWO_FOR_ONE_SUMMARY_SCHEMA",
    "TWO_FOR_ONE_WINDOW",
    "add_possession_flags",
    "add_two_for_one_flags",
    "two_for_one_summary",
]

#: The columns :func:`add_possession_flags` appends, in order.
POSSESSION_FLAGS_SCHEMA: dict[str, pl.DataType] = {
    "seconds_to_first_shot": pl.Float64,
    "is_transition": pl.Boolean,
    "is_garbage_time": pl.Boolean,
}

#: bigballR's garbage-time bands (``get_play_by_play``, all_functions.R:1016-1034):
#: ``(seconds left in regulation at or under, absolute margin at or over)``.
#: Overtime is past every floor, so any margin of 15 or more qualifies there.
GARBAGE_TIME_BANDS: Final[tuple[tuple[float, int], ...]] = ((600.0, 25), (300.0, 20), (120.0, 15))

#: bigballR: garbage time also needs three or fewer of the game's ten starters on the floor.
GARBAGE_TIME_MAX_STARTERS: Final[int] = 3

#: Regulation is four quarters (NBA 12 minutes, WNBA 10), or two 20-minute halves for the
#: WNBA through 2005. A game whose first period starts with more than this many seconds is
#: a halves game. The last 10 minutes of regulation always fall in the last regulation period.
_HALF_GAME_FIRST_PERIOD_SECONDS: Final[float] = 900.0

#: The transition window: the first shot comes within 8 seconds of the change of possession.
#: Chosen on a 6 / 8 / 10 grid against Synergy's ``Transition`` play type (regular season,
#: 2015-16 to 2025-26; ``tools/models/nba_transition_window.py``, its output beside it in
#: ``nba_transition_window.csv``) as the window closest to Synergy's level: in every season 8 s
#: flags 0.96-1.22 of Synergy's league share of possessions, 6 s 0.57-0.83 and 10 s 1.34-1.62
#: (college's hoop-math / bigballR window). 8 s also beats 10 s on Pearson over the 30 teams in every season (mean 0.884 against
#: 0.825) and on Spearman in all but 2020-21; 6 s orders the teams slightly better (0.897) but
#: misses the level. On the team-level 2023-24 capture: 8 s gives Pearson 0.892 / Spearman 0.860
#: and 0.97 of Synergy's possessions a game, 10 s 0.844 / 0.788 and 1.29. The other seasons use
#: Synergy's player rows summed per team (2023-24's hold only 37,388 of the team table's ~46,900
#: possessions, so that season is read from the team table).
TRANSITION_SECONDS: Final[float] = 8.0

#: Starts where the ball changed hands live, without a stoppage: after a made shot, a
#: defensive rebound, or a live-ball turnover. ``OffDeadball`` and ``OffTimeout`` never
#: qualify, and neither does a period's first possession.
TRANSITION_START_TYPES: Final[tuple[str, ...]] = ("OffMadeShot", "OffMissedShot", "OffLiveBallTurnover")

_SHOT_EVENT_TYPES: Final[tuple[str, ...]] = ("made_shot", "missed_shot")
_LINEUP_COLS: Final[list[str]] = [f"{side}_player_{i}" for side in ("off", "def") for i in range(1, 6)]
_POSS_COLS: Final[list[str]] = [
    "game_id",
    "period",
    "possession_number",
    "number_in_period",
    "start_order_index",
    "end_order_index",
    "start_seconds_remaining",
    "end_seconds_remaining",
    "possession_start_type",
    *_LINEUP_COLS,
]
_PBP_COLS: Final[list[str]] = ["game_id", "order_index", "event_type", "seconds_remaining", "score_home", "score_away"]


def add_possession_flags(
    possessions: pl.DataFrame,
    pbp: pl.DataFrame,
    *,
    transition_seconds: float = TRANSITION_SECONDS,
) -> pl.DataFrame:
    """Append ``seconds_to_first_shot``, ``is_transition`` and ``is_garbage_time`` to possessions.

    Works for the NBA and the WNBA, on any number of games at once. ``possessions`` is the
    ``nba_stats_possessions`` / ``wnba_stats_possessions`` frame (or the producer's
    ``attach_possession_lineups`` output) and ``pbp`` the play-by-play it was built from:
    the ``nba_stats_pbp`` release or the enhanced pbp frame. The input's columns, values,
    row count and row order are unchanged; only the three columns are appended.

    **seconds_to_first_shot** (Float64): seconds from the change of possession to the
    possession's first field-goal attempt. The change of possession is the clock at the
    previous possession's last event (``start_seconds_remaining`` is the clock at this
    possession's first event, which after a rebound is often the shot itself). A period's
    first possession counts from its own first event. Null when the possession has no field
    goal attempt (turnovers, free-throw-only trips). It can pass 24 when the shot clock
    reset inside the possession (a kicked ball, a foul).

    **is_transition** (Boolean, never null): ``possession_start_type`` is ``OffMadeShot``,
    ``OffMissedShot`` or ``OffLiveBallTurnover``, the possession is not its period's first,
    and ``seconds_to_first_shot <= transition_seconds`` (8 by default, the window that
    tracks Synergy's ``Transition`` play type; college's is 10).

    **is_garbage_time** (Boolean, never null): bigballR's rule, the one behind the college
    flag. A possession triggers it when any of its events has an absolute score margin of
    at least 25 with 10:00 or less left in regulation, at least 20 with 5:00 or less, or at
    least 15 with 2:00 or less (or at least 15 in overtime), with at most 3 of the game's 10
    starters on the floor (the first possession's 10). Once a game triggers, it stays
    garbage time to the end. The module docstring lists where this differs from bigballR
    and why the triggering possession can qualify on its own basket.

    Args:
        possessions: Every possession of each game it holds (the previous possession's
            clock and the first possession's starters are read from it, so do not filter it
            to ``count_as_possession`` or to one team first). Needs ``game_id``, ``period``,
            ``possession_number``, ``number_in_period``, ``start_order_index``,
            ``end_order_index``, ``start_seconds_remaining``, ``end_seconds_remaining``,
            ``possession_start_type`` and ``off_player_1..5`` / ``def_player_1..5``.
        pbp: The play-by-play of those games. Needs ``game_id``, ``order_index``,
            ``event_type``, ``seconds_remaining``, ``score_home`` and ``score_away`` (Utf8 or
            integer; the margin is the running score after each event).
        transition_seconds: The transition window in seconds. Default 8.0 (:data:`TRANSITION_SECONDS`).

    Returns:
        ``possessions`` with :data:`POSSESSION_FLAGS_SCHEMA` appended. An empty frame
        returns zero rows with those columns.

    Raises:
        ValueError: If a needed column is missing, a column the flags would write already
            exists, ``game_id`` differs in dtype between the two frames, or ``pbp`` has no
            event for some possession.

    Example:
        Flag one season::

            from sportsdataverse.nba import load_nba_stats_pbp, load_nba_stats_possessions
            from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags

            # a release that already carries the flags: drop them to re-flag
            raw = load_nba_stats_possessions(2023).drop(*POSSESSION_FLAGS_SCHEMA, strict=False)
            poss = add_possession_flags(raw, load_nba_stats_pbp(2023))
            print(poss.group_by("is_transition").len())

        See Also:
            * `bigballR`_ -- the college garbage-time and transition rules.
            * `hoop-math`_ -- the college 10-second transition window.

        .. _bigballR: https://github.com/jflancer/bigballR
        .. _hoop-math: https://hoop-math.com/
    """
    _check_inputs(possessions, pbp)
    if possessions.is_empty():
        return possessions.with_columns(pl.lit(None, dtype=t).alias(c) for c, t in POSSESSION_FLAGS_SCHEMA.items())

    p = possessions.select(_POSS_COLS).with_row_index("_row")
    ev = pbp.select(_PBP_COLS).sort("game_id", "order_index")
    flags = (
        p.join(_first_shot(p, ev), on="_row", how="left")
        .join(_garbage_possessions(p, ev), on="_row", how="left")
        .sort("game_id", "possession_number")
        .with_columns(
            (
                pl.col("possession_start_type").is_in(TRANSITION_START_TYPES)
                # a guard: a period's first possession is OffDeadball in the engine already
                & (pl.col("number_in_period") > 1)
                & (pl.col("seconds_to_first_shot") <= transition_seconds)
            )
            .fill_null(False)
            .alias("is_transition"),
            (pl.col("_trigger").cast(pl.Int32).cum_sum().over("game_id") > 0).alias("is_garbage_time"),
        )
        .select("_row", *POSSESSION_FLAGS_SCHEMA)
        .sort("_row")
    )
    return possessions.with_columns(flags.drop("_row").get_columns())


def _check_inputs(possessions: pl.DataFrame, pbp: pl.DataFrame) -> None:
    """Fail loudly on a missing column, a column we would overwrite, or a game_id dtype split."""
    for name, frame, cols in (("possessions", possessions, _POSS_COLS), ("pbp", pbp, _PBP_COLS)):
        missing = [c for c in cols if c not in frame.columns]
        if missing:
            raise ValueError(f"{name} is missing columns {missing}")
    clash = [c for c in POSSESSION_FLAGS_SCHEMA if c in possessions.columns]
    if clash:
        raise ValueError(f"possessions already has {clash}; drop them before re-flagging")
    if possessions.schema["game_id"] != pbp.schema["game_id"]:
        raise ValueError(
            f"game_id dtype differs: possessions {possessions.schema['game_id']} vs pbp {pbp.schema['game_id']}"
        )


def _change_clock(p: pl.DataFrame) -> pl.DataFrame:
    """``p`` sorted by game and possession, with ``_ball_clock``: the clock at the change of possession.

    The ball changed hands at the previous possession's last event; a period's first possession
    counts from its own first event. Shared by ``seconds_to_first_shot`` and the 2-for-1 flag, so
    the two can never disagree about when a possession started.
    """
    return p.sort("game_id", "possession_number").with_columns(
        pl.when(pl.col("number_in_period") > 1)
        .then(pl.col("end_seconds_remaining").shift(1).over("game_id"))
        .otherwise(pl.col("start_seconds_remaining"))
        .alias("_ball_clock")
    )


def _first_shot(p: pl.DataFrame, ev: pl.DataFrame) -> pl.DataFrame:
    """``_row -> _ball_clock, seconds_to_first_shot``: from the change of possession to the first FGA."""
    return (
        _change_clock(p)
        .select("_row", "game_id", "start_order_index", "end_order_index", "_ball_clock")
        .sort("game_id", "start_order_index")
        .join_asof(
            ev.filter(pl.col("event_type").is_in(_SHOT_EVENT_TYPES)).select(
                "game_id", pl.col("order_index").alias("_shot_oi"), pl.col("seconds_remaining").alias("_shot_clock")
            ),
            left_on="start_order_index",
            right_on="_shot_oi",
            by="game_id",
            strategy="forward",
            check_sortedness=False,
        )
        .select(
            "_row",
            "_ball_clock",
            pl.when(pl.col("_shot_oi") <= pl.col("end_order_index"))
            .then(pl.col("_ball_clock") - pl.col("_shot_clock"))
            .alias("seconds_to_first_shot"),
        )
    )


def _running_score(col: str) -> pl.Expr:
    """A team's score after each event, per game.

    The v3 feed fills the score on scoring rows only: blank between them since about 2019-20,
    and ``"0"`` on every non-scoring row before that. A game's score never falls, so the
    running maximum carried forward is the score in both eras (it matches the schedule's
    final score for every 1999-00, 2009-10, 2023-24 and 2025-26 game). The one cost: a score
    a replay review later lowers stays at its higher value.
    """
    return (
        pl.col(col).cast(pl.Int64, strict=False).cum_max().over("game_id").forward_fill().over("game_id").fill_null(0)
    )


def _starters_on_floor(p: pl.DataFrame) -> pl.DataFrame:
    """``_row -> _n_starters``: how many of the game's first-possession 10 are on the floor.

    Null when any of the possession's 10 slots is null (an unknown lineup cannot pass the
    starters clause), so that possession never triggers garbage time.
    """
    long = p.select("_row", "game_id", "possession_number", *_LINEUP_COLS).unpivot(
        index=["_row", "game_id", "possession_number"], on=_LINEUP_COLS, value_name="_player"
    )
    starters = (
        long.filter(pl.col("possession_number") == pl.col("possession_number").min().over("game_id"))
        .select("game_id", "_player")
        .drop_nulls()
        .unique()
        .with_columns(pl.lit(1, dtype=pl.Int32).alias("_is_starter"))
    )
    return (
        long.join(starters, on=["game_id", "_player"], how="left")
        .group_by("_row")
        .agg(
            pl.when(pl.col("_player").null_count() == 0)
            .then(pl.col("_is_starter").fill_null(0).sum())
            .alias("_n_starters")
        )
    )


def _garbage_possessions(p: pl.DataFrame, ev: pl.DataFrame) -> pl.DataFrame:
    """``_row -> _trigger``: does any event of the possession meet bigballR's garbage-time test.

    bigballR tests every event (all_functions.R:1016-1034) and the possession table takes the
    max over its events, so an event in the middle of a possession counts even if the next
    basket takes the margin back under the band. Each event is tested with its own running
    margin and clock and the possession's on-floor 10 (``possessions`` freezes the lineup at
    the possession's start; bigballR uses each event's lineup).

    Raises:
        ValueError: If a possession has no event in ``pbp`` (the wrong season, a filtered frame).
    """
    poss = (
        p.select("_row", "game_id", "period", "start_order_index", "end_order_index", "start_seconds_remaining")
        .with_columns(
            # The WNBA played two 20-minute halves through 2005: a 1,200-second first period.
            pl.when(
                pl.col("start_seconds_remaining").filter(pl.col("period") == 1).max().over("game_id")
                > _HALF_GAME_FIRST_PERIOD_SECONDS
            )
            .then(2)
            .otherwise(4)
            .alias("_reg_periods")
        )
        .join(_starters_on_floor(p), on="_row", how="left")
    )
    events = (
        ev.select(
            "game_id",
            "order_index",
            pl.col("seconds_remaining").alias("_secs"),
            (_running_score("score_home") - _running_score("score_away")).abs().alias("_margin"),
        )
        .join_asof(
            poss.sort("game_id", "start_order_index"),
            left_on="order_index",
            right_on="start_order_index",
            by="game_id",
            strategy="backward",
            check_sortedness=False,
        )
        .filter(pl.col("order_index") <= pl.col("end_order_index"))
        .group_by("_row")
        .agg(_garbage_trigger().fill_null(False).any().alias("_trigger"))
    )
    uncovered = p.height - events.height
    if uncovered:
        raise ValueError(f"pbp has no events for {uncovered} of {p.height} possessions; pass the pbp they came from")
    return events


def _garbage_trigger() -> pl.Expr:
    """bigballR's margin x time-left bands plus the starters clause, for one event."""
    secs, m = pl.col("_secs"), pl.col("_margin")
    in_band = pl.any_horizontal(
        (pl.col("period") == pl.col("_reg_periods")) & (secs <= left) & (m >= margin)
        for left, margin in GARBAGE_TIME_BANDS
    ) | ((pl.col("period") > pl.col("_reg_periods")) & (m >= min(b[1] for b in GARBAGE_TIME_BANDS)))
    return in_band & (pl.col("_n_starters") <= GARBAGE_TIME_MAX_STARTERS)


# --- 2-for-1 (BB-D44) -----------------------------------------------------------------------

#: The columns :func:`add_two_for_one_flags` appends, in order.
TWO_FOR_ONE_SCHEMA: dict[str, pl.DataType] = {
    "change_clock": pl.Float64,
    "first_shot_clock": pl.Float64,
    "two_for_one": pl.Utf8,
    "extra_possession": pl.Boolean,
    "sequence_points": pl.Int64,
    "sequence_points_allowed": pl.Int64,
    "sequence_net": pl.Int64,
    "margin_at_change": pl.Int64,
}

#: The window on the change clock, ``(lo, hi]`` seconds left: where teams hurry. Set on NBA regular
#: seasons 2015-16 to 2025-26 (``tools/models/nba_two_for_one_window.py``, its output beside it in
#: ``nba_two_for_one_window.csv``) as the run of 1-second change-clock bins whose go rate beats the
#: ``natural`` rate (a mid-period possession's chance of a first shot that early) by 5 points or more:
#: 0.059 in (33, 34], peaking at 0.257 in (38, 39], 0.064 in (45, 46] and 0.042 in (46, 47]. Below 33
#: teams rarely manage it (a go rate of 0.13 or less); above 46 a normal possession goes anyway. A
#: 3-point threshold gives (32, 47], a 10-point one (34, 42].
TWO_FOR_ONE_WINDOW: Final[tuple[float, float]] = (33.0, 46.0)

#: A possession ``went`` 2-for-1 when its first shot leaves at least this many seconds. Same script and
#: seasons: ``P(extra_possession)`` against the first-shot clock rises from about 0.48 (16-21 s) to
#: about 0.93 (30-40 s) over a ramp (0.57 at 24, 0.64 at 25, 0.69 at 26, 0.75 at 27, 0.82 at 28 s),
#: and 27 is where a step function fits it best (count-weighted least squares over 16-39 s, from the
#: candidates 24-28): pooled, and in 7 of the 11 seasons (26 in 2015-16 and 2016-17, 28 in 2021-22 and
#: 2023-24). A full shot clock plus about 3 seconds for the rebound or inbound.
TWO_FOR_ONE_GO_FLOOR: Final[float] = 27.0

#: The one shot clock the window and floor are calibrated for (NBA, WNBA, G League).
TWO_FOR_ONE_SHOT_CLOCK: Final[float] = 24.0

#: ACC-2026-0075's rules (``source/data_transformations.ipynb`` cell 7): start clock 34-40 s, a
#: first shot within 7 s, an end at 24 s or less for ``held``, and the 4th quarter only within 10.
_MEKONEN_START: Final[tuple[float, float]] = (34.0, 40.0)
_MEKONEN_SHOT_SECONDS: Final[float] = 7.0
_MEKONEN_LATE_MARGIN: Final[int] = 10

_TFO_POSS_COLS: Final[list[str]] = [
    "game_id",
    "period",
    "possession_number",
    "number_in_period",
    "start_order_index",
    "end_order_index",
    "start_seconds_remaining",
    "end_seconds_remaining",
    "offense_team_id",
    "defense_team_id",
    "points",
]
_SEQ_KEYS: Final[list[str]] = ["game_id", "period", "offense_team_id"]


def add_two_for_one_flags(
    possessions: pl.DataFrame,
    pbp: pl.DataFrame,
    *,
    window: tuple[float, float] = TWO_FOR_ONE_WINDOW,
    go_floor: float = TWO_FOR_ONE_GO_FLOOR,
    rules: str = "sdv",
    shot_clock: float = TWO_FOR_ONE_SHOT_CLOCK,
) -> pl.DataFrame:
    """Append the 2-for-1 choice and its end-of-period outcome to possessions (BB-D44).

    Near a period's end, a team that gets the ball with about half a minute left can shoot
    early, leaving more than a shot clock so it gets the ball back for the last shot (two
    possessions to one), or run a normal possession and let the opponent hold for the last
    shot. This marks every possession that faces that choice, which one the team made, and
    what the rest of the period brought. Works for the NBA, WNBA and G League (24-second shot
    clock), on any number of games. Every clock is seconds left in the period.

    **change_clock** (Float64): the clock at the change of possession, the same moment
    ``seconds_to_first_shot`` counts from (the previous possession's last event; a period's
    first possession uses its own first event). **first_shot_clock** (Float64): the clock at the
    possession's first field-goal attempt, null without one. **margin_at_change** (Int64): the
    offense's score minus the defense's at the change of possession, from the pbp running score.
    These three are set on every possession.

    **two_for_one** (Utf8, null outside the window): with ``rules="sdv"`` the window is
    ``window[0] < change_clock <= window[1]``; ``"went"`` when ``first_shot_clock >= go_floor``,
    ``"held"`` when not went and the possession ends with 24 or fewer seconds left (the
    opponent can hold for the last shot), ``"other"`` otherwise (an early turnover or foul
    trip, or an end between 24 and ``go_floor``). ``rules="mekonen"`` reproduces ACC-2026-0075:
    the change clock rounded half-to-even (as numpy does) in 34-40 s, ``"went"`` for a first
    shot within 7 s, ``"held"`` for a later first shot and a rounded end clock of 24 or less,
    and in the 4th quarter only when ``|margin_at_change| <= 10``. The study's "start clock" is
    its possession's first event, and its possessions open with the defensive rebound or steal
    (625 of its 778 rows) that ours give to the previous possession, so it is our change clock
    (``tools/models/nba_two_for_one_parity.py``).

    In the window: **extra_possession** (Boolean), the offense has a later possession in the
    period whose change clock is above 0 (a 0.4-second heave counts); **sequence_points**
    (Int64), the offense's points on this and its later possessions in the period;
    **sequence_points_allowed** (Int64), the defense's points on its possessions after this one
    in the period; **sequence_net** (Int64), the difference. All four are null outside it.

    Args:
        possessions: Every possession of each game (the previous possession's clock and the
            rest of the period are read from it; do not filter it first). Needs ``game_id``,
            ``period``, ``possession_number``, ``number_in_period``, ``start_order_index``,
            ``end_order_index``, ``start_seconds_remaining``, ``end_seconds_remaining``,
            ``offense_team_id``, ``defense_team_id`` and ``points``.
        pbp: The play-by-play of those games. Needs ``game_id``, ``order_index``,
            ``event_type``, ``seconds_remaining``, ``score_home`` and ``score_away``.
        window: ``(lo, hi]`` on the change clock for ``rules="sdv"``. Default
            :data:`TWO_FOR_ONE_WINDOW`.
        go_floor: The first-shot clock at or above which a possession went 2-for-1. Default
            :data:`TWO_FOR_ONE_GO_FLOOR`.
        rules: ``"sdv"`` (default) or ``"mekonen"`` (ACC-2026-0075's definition, for parity).
        shot_clock: Must be 24.0: the window and floor are calibrated for that shot clock only.

    Returns:
        ``possessions`` with :data:`TWO_FOR_ONE_SCHEMA` appended; every input column, value,
        row and row order is unchanged. An empty frame returns zero rows with those columns.

    Raises:
        ValueError: If a needed column is missing, a column it would write already exists,
            ``game_id`` differs in dtype between the frames, ``offense_team_id`` and
            ``defense_team_id`` differ in dtype, ``rules`` is unknown, or ``shot_clock`` is not 24.

    Example:
        Flag one season::

            from sportsdataverse.nba import load_nba_stats_pbp, load_nba_stats_possessions
            from sportsdataverse.nba.nba_possession_flags import TWO_FOR_ONE_SCHEMA, add_two_for_one_flags

            raw = load_nba_stats_possessions(2026).drop(*TWO_FOR_ONE_SCHEMA, strict=False)
            poss = add_two_for_one_flags(raw, load_nba_stats_pbp(2026))
            print(poss.group_by("two_for_one").len())

        The study's definition, for comparison::

            mek = add_two_for_one_flags(raw, load_nba_stats_pbp(2026), rules="mekonen")

        See Also:
            * `NBA-2for1-Analysis`_ -- the 2025-26 study the ``mekonen`` rules reproduce.

        .. _NBA-2for1-Analysis: https://github.com/NathanielMekonen/NBA-2for1-Analysis
    """
    if rules not in ("sdv", "mekonen"):
        raise ValueError(f"rules must be 'sdv' or 'mekonen', not {rules!r}")
    if shot_clock != TWO_FOR_ONE_SHOT_CLOCK:
        raise ValueError(f"the 2-for-1 window is calibrated for a 24-second shot clock only, not {shot_clock}")
    for name, frame, cols in (("possessions", possessions, _TFO_POSS_COLS), ("pbp", pbp, _PBP_COLS)):
        missing = [c for c in cols if c not in frame.columns]
        if missing:
            raise ValueError(f"{name} is missing columns {missing}")
    clash = [c for c in TWO_FOR_ONE_SCHEMA if c in possessions.columns]
    if clash:
        raise ValueError(f"possessions already has {clash}; drop them before re-flagging")
    if possessions.schema["game_id"] != pbp.schema["game_id"]:
        raise ValueError(
            f"game_id dtype differs: possessions {possessions.schema['game_id']} vs pbp {pbp.schema['game_id']}"
        )
    if possessions.schema["offense_team_id"] != possessions.schema["defense_team_id"]:
        # the opponent join matches a possession's defense_team_id to later offense_team_ids
        raise ValueError(
            f"team id dtype differs: offense_team_id {possessions.schema['offense_team_id']} vs "
            f"defense_team_id {possessions.schema['defense_team_id']}"
        )
    if possessions.is_empty():
        return possessions.with_columns(pl.lit(None, dtype=t).alias(c) for c, t in TWO_FOR_ONE_SCHEMA.items())

    p = possessions.select(_TFO_POSS_COLS).with_row_index("_row")
    ev = pbp.select(_PBP_COLS).sort("game_id", "order_index")
    g = (
        p.join(_first_shot(p, ev), on="_row", how="left")
        .join(_margin_at_change(p, ev), on="_row", how="left")
        .with_columns(
            pl.col("_ball_clock").alias("change_clock"),
            (pl.col("_ball_clock") - pl.col("seconds_to_first_shot")).alias("first_shot_clock"),
        )
        .with_columns(_choice(rules, window, go_floor).alias("two_for_one"))
        .sort("game_id", "possession_number")
        .with_columns(
            # per team and period, from this possession to the buzzer
            pl.col("points").cum_sum(reverse=True).over(_SEQ_KEYS).alias("_rcum"),
            (pl.col("change_clock") > 0).cast(pl.Int64).cum_sum(reverse=True).over(_SEQ_KEYS).alias("_n_live"),
        )
    )
    # The defense's points after this possession = the reverse cumsum at its first later possession.
    later = g.select("game_id", "period", pl.col("offense_team_id").alias("_team"), "possession_number", "_rcum").sort(
        "game_id", "period", "_team", "possession_number"
    )
    allowed = (
        g.select("_row", "game_id", "period", pl.col("defense_team_id").alias("_team"), "possession_number")
        .sort("game_id", "period", "_team", "possession_number")
        .join_asof(
            later.rename({"_rcum": "_allowed"}),
            on="possession_number",
            by=["game_id", "period", "_team"],
            strategy="forward",
            allow_exact_matches=False,
            check_sortedness=False,
        )
        .select("_row", "_allowed")
    )
    in_window = pl.col("two_for_one").is_not_null()
    flags = (
        g.join(allowed, on="_row", how="left")
        .with_columns(
            pl.when(in_window)
            .then(pl.col("_n_live") - (pl.col("change_clock") > 0).cast(pl.Int64) > 0)
            .alias("extra_possession"),
            pl.when(in_window).then(pl.col("_rcum")).alias("sequence_points"),
            pl.when(in_window).then(pl.col("_allowed").fill_null(0)).alias("sequence_points_allowed"),
        )
        .with_columns((pl.col("sequence_points") - pl.col("sequence_points_allowed")).alias("sequence_net"))
        .select("_row", *(pl.col(c).cast(t) for c, t in TWO_FOR_ONE_SCHEMA.items()))
        .sort("_row")
    )
    return possessions.with_columns(flags.drop("_row").get_columns())


def _margin_at_change(p: pl.DataFrame, ev: pl.DataFrame) -> pl.DataFrame:
    """``_row -> margin_at_change``: offense minus defense, after the event before the possession's first.

    The pbp score is home and away and neither frame names the home team, so the side comes
    from the score itself: per game, the offense whose possessions raise the home score is home.
    """
    score = ev.select(
        "game_id", "order_index", (_running_score("score_home") - _running_score("score_away")).alias("_hma")
    )

    def at(on: str, exact: bool) -> pl.DataFrame:
        return (
            p.select("_row", "game_id", on)
            .sort("game_id", on)
            .join_asof(
                score,
                left_on=on,
                right_on="order_index",
                by="game_id",
                strategy="backward",
                allow_exact_matches=exact,
                check_sortedness=False,
            )
            .select("_row", pl.col("_hma").fill_null(0))
        )

    return (
        p.select("_row", "game_id", "offense_team_id")
        .join(at("start_order_index", False), on="_row")
        .join(at("end_order_index", True).rename({"_hma": "_hma_end"}), on="_row")
        .with_columns(
            (pl.col("_hma_end") - pl.col("_hma")).sum().over("game_id", "offense_team_id").sign().alias("_side")
        )
        .select("_row", pl.when(pl.col("_side") != 0).then(pl.col("_hma") * pl.col("_side")).alias("margin_at_change"))
    )


def _choice(rules: str, window: tuple[float, float], go_floor: float) -> pl.Expr:
    """``went`` / ``held`` / ``other``, null outside the window, under the sdv or ACC-2026-0075 rules."""
    c, end = pl.col("change_clock"), pl.col("end_seconds_remaining")
    if rules == "sdv":
        in_window = (c > window[0]) & (c <= window[1])
        went = pl.col("first_shot_clock") >= go_floor
        held = end <= TWO_FOR_ONE_SHOT_CLOCK
    else:
        sts = pl.col("seconds_to_first_shot")
        late_ok = (pl.col("period") != 4) | (pl.col("margin_at_change").abs() <= _MEKONEN_LATE_MARGIN)
        # the study's possession starts at the rebound or steal: our change of possession
        in_window = c.round(0, mode="half_to_even").is_between(*_MEKONEN_START) & late_ok
        went = sts <= _MEKONEN_SHOT_SECONDS
        held = (sts > _MEKONEN_SHOT_SECONDS) & (end.round(0, mode="half_to_even") <= TWO_FOR_ONE_SHOT_CLOCK)
    inw = in_window.fill_null(False)
    return (
        pl.when(inw & went.fill_null(False))
        .then(pl.lit("went"))
        .when(inw & held.fill_null(False))
        .then(pl.lit("held"))
        .when(inw)
        .then(pl.lit("other"))
    )


#: The columns :func:`two_for_one_summary` returns after the ``by`` keys, in order.
TWO_FOR_ONE_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "games": pl.UInt32,
    "n_went": pl.UInt32,
    "n_held": pl.UInt32,
    "n_other": pl.UInt32,
    "go_rate": pl.Float64,
    "extra_rate_went": pl.Float64,
    "extra_rate_held": pl.Float64,
    "extra_rate_1s_went": pl.Float64,
    "extra_rate_1s_held": pl.Float64,
    "net_went": pl.Float64,
    "net_held": pl.Float64,
    "net_other": pl.Float64,
    "net_diff": pl.Float64,
    "net_diff_lo": pl.Float64,
    "net_diff_hi": pl.Float64,
}


def two_for_one_summary(
    flagged: pl.DataFrame,
    *,
    by: tuple[str, ...] = ("offense_team_id",),
    exclude_garbage: bool = True,
    late_margin_max: int = 10,
    n_boot: int = 1000,
    ci: float = 0.90,
    seed: int = 0,
) -> pl.DataFrame:
    """How often teams go 2-for-1, how often it pays the extra possession, and clock-matched net points.

    Descriptive, not causal: whether a team goes depends on score, lineup, opponent and
    timeouts, so ``net_diff`` is not "the value of going 2-for-1".

    Kept: possessions with a ``two_for_one`` value, minus garbage time when
    ``exclude_garbage`` (needs ``is_garbage_time`` from :func:`add_possession_flags`), and in
    period 4 and overtime only when ``|margin_at_change| <= late_margin_max`` (late-game
    fouling is not a 2-for-1 choice).

    Per ``by`` group: ``games``, ``n_went`` / ``n_held`` / ``n_other``, ``go_rate`` (went over
    went plus held), ``extra_rate_*`` (share with ``extra_possession``), ``extra_rate_1s_*`` (the
    sensitivity that counts a later possession only with 1.0 s or more on its change clock),
    mean ``sequence_net`` per choice (``net_*``), and ``net_diff``: went minus held, taken
    within 1-second ``change_clock`` bins and averaged with each bin weighted by its went plus
    held count, so the time each choice starts with does not drive it. ``net_diff_lo`` /
    ``net_diff_hi`` are the ``ci`` interval of a bootstrap that resamples whole games (never
    rows) within the group. ``"other"`` stays out of the comparison.

    Args:
        flagged: :func:`add_two_for_one_flags` output with every possession of each game (the
            1-second sensitivity reads the later possessions). Possessions carry no season
            column: to group by season, add one and pass it in ``by``.
        by: Group columns; ``()`` for one league row. Default ``("offense_team_id",)``.
        exclude_garbage: Drop ``is_garbage_time`` possessions. Default True.
        late_margin_max: Period 4 and overtime keep ``|margin_at_change|`` up to this. Default 10.
        n_boot: Bootstrap replicates. Default 1000.
        ci: Interval coverage. Default 0.90.
        seed: Bootstrap seed. Default 0.

    Returns:
        One row per group: the ``by`` columns then :data:`TWO_FOR_ONE_SUMMARY_SCHEMA`, sorted
        by ``by``. ``net_diff`` is null with no bin holding both choices.

    Raises:
        ValueError: If a needed column is missing.

    Example:
        League line by season::

            from sportsdataverse.nba.nba_possession_flags import two_for_one_summary
            # poss: add_possession_flags then add_two_for_one_flags, plus a season column
            print(two_for_one_summary(poss, by=("season",)))

        Per team::

            two_for_one_summary(poss).sort("go_rate", descending=True).head()

        See Also:
            * `NBA-2for1-Analysis`_ -- the 2025-26 study behind the comparison.

        .. _NBA-2for1-Analysis: https://github.com/NathanielMekonen/NBA-2for1-Analysis
    """
    need = [*by, "game_id", "period", "possession_number", "offense_team_id", *TWO_FOR_ONE_SCHEMA]
    if exclude_garbage:
        need.append("is_garbage_time")
    missing = [c for c in dict.fromkeys(need) if c not in flagged.columns]
    if missing:
        raise ValueError(f"flagged is missing columns {missing}")
    keys = list(by)
    late_ok = (pl.col("period") < 4) | (pl.col("margin_at_change").abs() <= late_margin_max)
    live = (pl.col("change_clock") >= 1.0).cast(pl.Int64)
    w = (
        flagged.sort("game_id", "possession_number")
        .with_columns(
            ((live.cum_sum(reverse=True) - live).over(_SEQ_KEYS) > 0).alias("_extra_1s"),
            pl.col("change_clock").floor().alias("_bin"),
        )
        .filter(pl.col("two_for_one").is_not_null() & late_ok.fill_null(False))
    )
    if exclude_garbage:
        w = w.filter(pl.col("is_garbage_time") == False)  # noqa: E712

    def rate(col: str, choice: str) -> pl.Expr:
        return pl.col(col).filter(pl.col("two_for_one") == choice).cast(pl.Float64).mean()

    n = {c: (pl.col("two_for_one") == c).sum().alias(f"n_{c}") for c in ("went", "held", "other")}
    base = w.group_by(keys or pl.lit(0).alias("_all")).agg(
        pl.col("game_id").n_unique().alias("games"),
        *n.values(),
        *(
            rate(col, c).alias(f"extra_rate{s}_{c}")
            for col, s in (("extra_possession", ""), ("_extra_1s", "_1s"))
            for c in ("went", "held")
        ),
        *(rate("sequence_net", c).alias(f"net_{c}") for c in ("went", "held", "other")),
    )
    base = base.with_columns((pl.col("n_went") / (pl.col("n_went") + pl.col("n_held"))).alias("go_rate"))
    boot = _binned_diff_boot(w.filter(pl.col("two_for_one") != "other"), keys, n_boot, ci, seed)
    out = base.join(boot, on=keys or ["_all"], how="left")
    return out.select(*keys, *(pl.col(c).cast(t) for c, t in TWO_FOR_ONE_SUMMARY_SCHEMA.items())).sort(keys or "games")


def _binned_diff_boot(w: pl.DataFrame, keys: list[str], n_boot: int, ci: float, seed: int) -> pl.DataFrame:
    """Per group: the bin-matched went-minus-held net, and its whole-game bootstrap interval."""

    grp = keys or ["_all"]
    if not keys:
        w = w.with_columns(pl.lit(0).alias("_all"))
    rng = np.random.default_rng(seed)
    rows = []
    for gkey, gw in w.group_by(grp, maintain_order=True):
        # games x (bin, choice) sums and counts; a replicate reweights games by multinomial draws
        cell = gw.group_by("game_id", "_bin", "two_for_one").agg(
            pl.col("sequence_net").sum().alias("s"), pl.len().alias("k")
        )
        games = cell["game_id"].unique().sort()
        cols = cell.select("_bin", "two_for_one").unique().sort("_bin", "two_for_one")
        gi = cell.join(games.to_frame().with_row_index("_g"), on="game_id")
        gi = gi.join(cols.with_row_index("_c"), on=["_bin", "two_for_one"])
        S = np.zeros((games.len(), cols.height))
        K = np.zeros_like(S)
        S[gi["_g"].to_numpy(), gi["_c"].to_numpy()] = gi["s"].to_numpy()
        K[gi["_g"].to_numpy(), gi["_c"].to_numpy()] = gi["k"].to_numpy()
        W = np.vstack(
            [np.ones(games.len()), rng.multinomial(games.len(), np.full(games.len(), 1 / games.len()), n_boot)]
        )
        diff = _diff_from(W @ S, W @ K, cols)
        est, reps = diff[0], diff[1:]
        reps = reps[~np.isnan(reps)]
        lo, hi = np.quantile(reps, [(1 - ci) / 2, (1 + ci) / 2]) if reps.size else (np.nan, np.nan)
        rows.append({**dict(zip(grp, gkey)), "net_diff": est, "net_diff_lo": lo, "net_diff_hi": hi})
    schema = {c: w.schema[c] for c in grp} | {
        "net_diff": pl.Float64,
        "net_diff_lo": pl.Float64,
        "net_diff_hi": pl.Float64,
    }
    return pl.DataFrame(rows, schema=schema).fill_nan(None)


def _diff_from(S: np.ndarray, K: np.ndarray, cols: pl.DataFrame) -> np.ndarray:
    """Rows of (bin, choice) sums and counts -> the count-weighted mean of per-bin went minus held."""

    bins = cols["_bin"].unique().sort().to_list()
    num, den = np.zeros(S.shape[0]), np.zeros(S.shape[0])
    choice = cols["two_for_one"].to_list()
    at = {(b, c): i for i, (b, c) in enumerate(zip(cols["_bin"].to_list(), choice))}
    for b in bins:
        i, j = at.get((b, "went")), at.get((b, "held"))
        if i is None or j is None:
            continue
        both = (K[:, i] > 0) & (K[:, j] > 0)
        d = np.where(both, S[:, i] / np.where(both, K[:, i], 1) - S[:, j] / np.where(both, K[:, j], 1), 0.0)
        wgt = np.where(both, K[:, i] + K[:, j], 0.0)
        num += d * wgt
        den += wgt
    with np.errstate(invalid="ignore", divide="ignore"):
        return num / den
