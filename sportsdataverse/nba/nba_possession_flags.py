"""Garbage-time and transition flags on the NBA/WNBA ``possessions`` table (BB-D7).

:func:`add_possession_flags` appends three columns to the published possession frame
(``nba_stats_possessions`` / ``wnba_stats_possessions``, or the producer's in-memory
``build_possessions`` + ``attach_possession_lineups`` output) and changes nothing else:
every existing column, value, row and row order is kept.

``is_garbage_time`` uses the same rule as the college flag on ``ncaa_{mbb,wbb}_possessions``
(bigballR's ``get_play_by_play``, ported in
:mod:`sportsdataverse.mbb.mbb_ncaa_game_pbp`). ``is_transition`` reads the possession's
``possession_start_type`` plus the time from the change of possession to its first shot,
with the same 10-second window hoop-math and bigballR use for college transition (bigballR
also counts a steal or a drawn foul as the first play; this counts field-goal attempts only).

These are deliberately not the Cleaning the Glass definitions that
:mod:`sportsdataverse.nba.nba_play_context` reproduces (4th-quarter margin bands with
two or fewer starters, and a 6-second first-play window calibrated to CTG's published
rates). The two engines agree on the idea and differ on the thresholds; the columns
here are the ones the possessions table, the Data API and Game on Paper serve.

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

import polars as pl

__all__ = [
    "GARBAGE_TIME_BANDS",
    "GARBAGE_TIME_MAX_STARTERS",
    "POSSESSION_FLAGS_SCHEMA",
    "TRANSITION_SECONDS",
    "TRANSITION_START_TYPES",
    "add_possession_flags",
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

#: hoop-math's and bigballR's transition window: the first shot comes within 10 seconds
#: of the change of possession.
TRANSITION_SECONDS: Final[float] = 10.0

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
    and ``seconds_to_first_shot <= transition_seconds`` (10 by default: hoop-math's and
    bigballR's college window).

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
        transition_seconds: The transition window in seconds. Default 10.0.

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
            from sportsdataverse.nba.nba_possession_flags import add_possession_flags

            poss = add_possession_flags(load_nba_stats_possessions(2023), load_nba_stats_pbp(2023))
            print(poss.group_by("is_transition").len())

        See Also:
            * `bigballR`_ -- the college garbage-time and transition rules.
            * `hoop-math`_ -- the 10-second transition window.

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


def _first_shot(p: pl.DataFrame, ev: pl.DataFrame) -> pl.DataFrame:
    """``_row -> seconds_to_first_shot``: from the change of possession to the first FGA."""
    p = p.sort("game_id", "possession_number").with_columns(
        # The ball changed hands at the previous possession's last event.
        pl.when(pl.col("number_in_period") > 1)
        .then(pl.col("end_seconds_remaining").shift(1).over("game_id"))
        .otherwise(pl.col("start_seconds_remaining"))
        .alias("_ball_clock")
    )
    return (
        p.select("_row", "game_id", "start_order_index", "end_order_index", "_ball_clock")
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
    """``_row -> _n_starters``: how many of the game's first-possession 10 are on the floor."""
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
        .agg(pl.col("_is_starter").fill_null(0).sum().alias("_n_starters"))
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
