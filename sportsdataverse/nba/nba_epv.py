"""Expected possession value (EPV) and EPV added for NBA play-by-play (BB-D24).

EPV is basketball's expected points: the points the team with the ball is expected to score
before the possession ends, given the game state. EPV added is basketball's EPA: what a play
changed that expectation by.

**The state** after each play-by-play event, read as of that event (nothing later in the
possession leaks in):

* ``start_type`` -- how the possession started: the engine's ``possession_start_type``.
* ``period`` (overtimes folded to 5) and ``seconds_remaining`` in the period.
* ``start_margin`` -- the offense's lead when the possession started (before its first event).
  It is the margin at the start, not the garbage-time trigger: a possession can trigger garbage
  time on its own basket (see :mod:`sportsdataverse.nba.nba_possession_flags`).
* ``transition`` -- 0 half court, 1 the transition window is open (a live-ball start that is not
  its period's first, no shot and no dead ball yet, within :data:`TRANSITION_SECONDS` of the
  change of possession), 2 a transition possession whose first shot has been taken (the
  possession's ``is_transition`` flag, known once that shot happens).
* ``last_event`` -- the event just processed (:data:`LAST_EVENTS`); steals, blocks,
  substitutions and replays carry the previous one.
* ``ft_pending`` -- free throws still to come after a free throw ("1 of 2" leaves 1).
* ``shot_zone`` -- the zone of the possession's latest field-goal attempt (:data:`SHOT_ZONES`),
  0 before its first.

The possession's start is a state too (``last_event`` ``start``), at the clock of the change of
possession.

**EPV added** on an event is ``points + epv_after - epv_before``: the offense's points on the
event, plus the EPV after it, minus the EPV before it (the previous event's, or the start's).
When the event ends the possession, ``epv_after`` is minus the opponent's next start value
over an average start, ``-(epv_start_next - start_epv_mean)``, so a live-ball turnover costs
more than a dead-ball one; it is 0 when the period ends. Over a possession the EPV added sums
to ``points - epv_start + epv_after`` of its last event.

**Credit.** A play's EPV added is the offense's; the defense's is its negative. Each event
credits its actor: the offensive actor gets the EPV added, a defensive actor (a defensive
rebounder, a fouler) its negative, and a steal or block credits the stealer or blocker with
the negative of the turnover or missed shot it ended. On-court EPV +/- gives every player on
the floor the offense's EPV added (the defenders its negative): the ten at each action from
the ``nba_stats_game_lineups`` release when it is passed (the loaders do), else the
possession's starting ten. Free throws go on the lineup of the foul that led to them (:func:`~sportsdataverse._common.game_impact.credit_free_throws_to_foul_lineup`,
BB-D6's rule). The release carries no assist or foul-drawn player, so neither is credited.

The model is an XGBoost regression on the remaining points of the possession, fitted by
``tools/models/nba_epv/train_epv.py`` on disjoint train, holdout, confirmation and monitor
seasons (the seasons and gates are in the booster's attributes and in that folder's holdout
reports). It is served from its first training season on; earlier seasons are refused.

Most of the skill is inside the possession (after a miss, a foul, a free throw); possession
starts differ little (about 0.15 points from a dead ball to a live-ball turnover), so the
possession-ending term moves EPV added by tenths of a point, not whole points.
"""

from __future__ import annotations

from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Final

import polars as pl

from sportsdataverse._common.game_impact import credit_free_throws_to_foul_lineup
from sportsdataverse.nba.nba_possession_flags import TRANSITION_SECONDS, TRANSITION_START_TYPES, add_possession_flags

if TYPE_CHECKING:
    import pandas as pd
    import xgboost as xgb

__all__ = [
    "EPV_FEATURES",
    "LAST_EVENTS",
    "SHOT_ZONES",
    "START_TYPES",
    "add_epv",
    "epv_lineup_games",
    "epv_player_games",
    "epv_states",
    "epv_team_games",
    "load_epv_model",
    "nba_epv_lineup_games",
    "nba_epv_player_games",
    "nba_epv_plays",
    "nba_epv_team_games",
]

#: ``possession_start_type`` -> ``start_type`` code (its index).
START_TYPES: Final[tuple[str, ...]] = (
    "OffDeadball",
    "OffTimeout",
    "OffMadeShot",
    "OffMissedShot",
    "OffLiveBallTurnover",
)
#: ``last_event`` codes (the index). ``start`` is the possession's start state. The order is the
#: states' typical remaining points, lowest first (2021-23 NBA), so a tree split on the code
#: groups like states: an offensive foul (worth nothing, the turnover follows) never sits
#: between two fouls that send the offense to the line.
LAST_EVENTS: Final[tuple[str, ...]] = (
    "turnover", "offensive_foul", "made_shot", "def_rebound", "missed_shot", "free_throw", "jump_ball",
    "off_team_rebound", "violation", "timeout", "other_foul", "start", "off_rebound", "shooting_foul",
)  # fmt: skip
_START_EVENT: Final[int] = LAST_EVENTS.index("start")
#: ``shot_zone`` codes: 0 no shot yet, then by ``shot_distance`` in feet (a three is zone 4).
SHOT_ZONES: Final[dict[int, str]] = {
    0: "none",
    1: "rim (<4 ft)",
    2: "short (4-13 ft)",
    3: "mid (14+ ft two)",
    4: "three",
}
#: The model's inputs, in order.
EPV_FEATURES: Final[list[str]] = [
    "start_type", "period", "seconds_remaining", "start_margin", "transition", "last_event", "ft_pending", "shot_zone",
]  # fmt: skip

#: league_id -> booster file. Each booster is served from its first training season on
#: (:func:`_check_seasons`): the older feeds code about a third of their two-point attempts at
#: 0 ft, which would read as rim shots, and the gates cover the training era only.
_LEAGUES: Final[dict[str, str]] = {"00": "nba_epv.ubj"}
#: ``shot_zone`` cut points, in feet: under the first a rim shot, under the second a short one.
_RIM_FEET: Final[int] = 4
_SHORT_FEET: Final[int] = 14
_SHOTS: Final[tuple[str, ...]] = ("made_shot", "missed_shot")
#: Events that close a free-throw trip. Not a rebound: the dead-ball team rebound filed after a missed
#: first free throw leaves the rest of the trip to come (nor substitutions, timeouts or replays).
_LIVE: Final[tuple[str, ...]] = ("made_shot", "missed_shot", "turnover", "jump_ball", "violation", "foul")
#: Dead-ball events: once one happens the defense is set, so the transition window closes (a team
#: rebound, filed when the ball goes out of bounds, is one too).
_DEAD_BALL: Final[tuple[str, ...]] = ("timeout", "foul", "violation", "jump_ball", "substitution", "replay")
_SLOTS: Final[list[str]] = [f"{side}_player_{i}" for side in ("off", "def") for i in range(1, 6)]
_POSS_COLS: Final[list[str]] = [
    "game_id", "period", "possession_number", "number_in_period", "start_order_index", "end_order_index",
    "start_seconds_remaining", "end_seconds_remaining", "possession_start_type", "offense_team_id",
    "defense_team_id", *_SLOTS,
]  # fmt: skip
_PBP_COLS: Final[list[str]] = [
    "season", "game_id", "order_index", "action_number", "period", "seconds_remaining", "event_type", "sub_type",
    "description", "shot_distance", "shot_value", "team_id", "person_id", "location", "score_home", "score_away",
]  # fmt: skip
_KEY: Final[list[str]] = ["game_id", "possession_number"]


def _running(col: str) -> pl.Expr:
    """A side's score after each event (the feed fills scoring rows only; a score never falls)."""
    return (
        pl.col(col).cast(pl.Int64, strict=False).cum_max().over("game_id").forward_fill().over("game_id").fill_null(0)
    )


def _scored_events(pbp: pl.DataFrame) -> pl.DataFrame:
    """The pbp in game order with each side's points on the event, the margin before it and the home team."""
    ev = (
        pbp.select(_PBP_COLS)
        .with_columns(
            pl.col("game_id").cast(pl.Utf8),
            # team events (a team rebound, a team timeout) carry team_id 0 and the team in person_id
            pl.when(pl.col("team_id").fill_null(0) == 0)
            .then(pl.col("person_id"))
            .otherwise(pl.col("team_id"))
            .alias("team_id"),
        )
        .sort("game_id", "order_index")
        .with_columns(_running("score_home").alias("_h"), _running("score_away").alias("_a"))
    )
    home = (
        ev.filter((pl.col("location") == "h") & (pl.col("team_id") > 0))
        .group_by("game_id")
        .agg(pl.col("team_id").mode().first().alias("home_team_id"))
    )
    prev = {c: pl.col(c).shift(1).over("game_id").fill_null(0) for c in ("_h", "_a")}
    return ev.join(home, on="game_id", how="left").with_columns(
        (pl.col("_h") - prev["_h"]).alias("_dh"),
        (pl.col("_a") - prev["_a"]).alias("_da"),
        (prev["_h"] - prev["_a"]).alias("_pre_home_margin"),
    )


def _possession_frame(possessions: pl.DataFrame, pbp: pl.DataFrame) -> pl.DataFrame:
    """Possessions with the change-of-possession clock, the BB-D7 transition flag and the next one's period."""
    unknown = possessions.filter(~pl.col("possession_start_type").is_in(START_TYPES))["possession_start_type"]
    if unknown.len():
        raise ValueError(f"unknown possession_start_type {sorted(unknown.unique().to_list(), key=str)}")
    flags = add_possession_flags(possessions.select(_POSS_COLS), pbp.select(_PBP_COLS))
    return (
        flags.with_columns(pl.col("game_id").cast(pl.Utf8))
        .sort(_KEY)
        .with_columns(
            # the ball changed hands at the previous possession's last event (BB-D7's clock)
            pl.when(pl.col("number_in_period") > 1)
            .then(pl.col("end_seconds_remaining").shift(1).over("game_id"))
            .otherwise(pl.col("start_seconds_remaining"))
            .alias("_ball_clock"),
            (pl.col("period").shift(-1).over("game_id") == pl.col("period")).fill_null(False).alias("_next_in_period"),
            (pl.col("possession_start_type").is_in(TRANSITION_START_TYPES) & (pl.col("number_in_period") > 1)).alias(
                "_live_start"
            ),
            pl.col("possession_start_type")
            .replace_strict(START_TYPES, list(range(len(START_TYPES))), return_dtype=pl.Int32)
            .alias("start_type"),
        )
    )


def _last_event_code() -> pl.Expr:
    """The ``last_event`` code of one event; null for events that carry the previous state."""
    et, st, codes = pl.col("event_type"), pl.col("sub_type").fill_null(""), {e: i for i, e in enumerate(LAST_EVENTS)}
    return (
        pl.when(et == "made_shot")
        .then(codes["made_shot"])
        .when(et == "missed_shot")
        .then(codes["missed_shot"])
        # a team rebound is a dead ball (an inbound follows), a player's a live one (a putback can)
        .when(
            (et == "rebound")
            & (pl.col("team_id") == pl.col("offense_team_id"))
            & (pl.col("person_id") == pl.col("team_id"))
        )
        .then(codes["off_team_rebound"])
        .when((et == "rebound") & (pl.col("team_id") == pl.col("offense_team_id")))
        .then(codes["off_rebound"])
        .when(et == "rebound")
        .then(codes["def_rebound"])
        .when((et == "foul") & st.str.starts_with("Shooting"))
        .then(codes["shooting_foul"])
        .when((et == "foul") & st.str.starts_with("Offensive"))
        .then(codes["offensive_foul"])
        .when(et == "foul")
        .then(codes["other_foul"])
        .when(et == "free_throw")
        .then(codes["free_throw"])
        .when(et == "turnover")
        .then(codes["turnover"])
        .when(et == "timeout")
        .then(codes["timeout"])
        .when(et == "violation")
        .then(codes["violation"])
        .when(et == "jump_ball")
        .then(codes["jump_ball"])
        .otherwise(None)
        .cast(pl.Int32)
    )


def _carried(expr: pl.Expr, start: int = 0) -> pl.Expr:
    """``expr`` carried forward inside the possession, ``start`` before its first value."""
    return expr.forward_fill().over(_KEY).fill_null(start)


def _event_states(ev: pl.DataFrame, poss: pl.DataFrame) -> pl.DataFrame:
    """One state row per event inside a possession: the state after the event, its points and label inputs."""
    et = pl.col("event_type")
    zone = (
        pl.when(et.is_in(_SHOTS) & (pl.col("shot_value") == 3))
        .then(4)
        .when(et.is_in(_SHOTS) & (pl.col("shot_distance").fill_null(0) < _RIM_FEET))
        .then(1)
        .when(et.is_in(_SHOTS) & (pl.col("shot_distance").fill_null(0) < _SHORT_FEET))
        .then(2)
        .when(et.is_in(_SHOTS))
        .then(3)
    )
    of = pl.col("sub_type").str.extract_groups(r"(\d) of (\d)")
    # a technical (its foul, or its lone free throw) can fall inside an ordinary trip: it leaves
    # the trip's count as it was
    technical = et.is_in(["foul", "free_throw"]) & pl.col("sub_type").fill_null("").str.contains("Technical")
    pending = (
        pl.when(technical)
        .then(None)
        .when(et == "free_throw")
        .then((of.struct.field("2").cast(pl.Int32) - of.struct.field("1").cast(pl.Int32)).fill_null(0))
        .when(et.is_in(_LIVE))
        .then(0)
    )
    rows = (
        ev.join_asof(
            poss.sort("game_id", "start_order_index"),
            left_on="order_index",
            right_on="start_order_index",
            by="game_id",
            strategy="backward",
            check_sortedness=False,
            suffix="_poss",
        )
        .filter(pl.col("order_index") <= pl.col("end_order_index"))
        .sort("game_id", "order_index")
    )
    first_shot = rows.filter(et.is_in(_SHOTS)).group_by(_KEY).agg(pl.col("order_index").min().alias("_first_shot"))
    seen = (pl.col("order_index") >= pl.col("_first_shot")).fill_null(False)
    team_rebound = (et == "rebound") & (pl.col("person_id") == pl.col("team_id"))
    stopped = (et.is_in(_DEAD_BALL) | team_rebound).cast(pl.Int32).cum_max().over(_KEY) > 0
    # a rebound with free throws still to come is the dead-ball one between them: it changes nothing
    between_fts = (et == "rebound") & (pl.col("ft_pending") > 0)
    rows = rows.join(first_shot, on=_KEY, how="left").with_columns(_carried(pending.cast(pl.Int32)).alias("ft_pending"))
    return rows.with_columns(
        _carried(pl.when(between_fts).then(None).otherwise(_last_event_code()), _START_EVENT).alias("last_event"),
        _carried(zone.cast(pl.Int32)).alias("shot_zone"),
        pl.when(seen & pl.col("is_transition"))
        .then(2)
        .when(
            pl.col("_live_start")
            & ~seen
            & ~stopped
            & (pl.col("_ball_clock") - pl.col("seconds_remaining") <= TRANSITION_SECONDS)
        )
        .then(1)
        .otherwise(0)
        .cast(pl.Int32)
        .alias("transition"),
        pl.when(pl.col("offense_team_id") == pl.col("home_team_id"))
        .then(pl.col("_dh"))
        .otherwise(pl.col("_da"))
        .alias("points"),
    )


def _start_states(ev: pl.DataFrame, poss: pl.DataFrame) -> pl.DataFrame:
    """One state row per possession: its start, at the clock of the change of possession."""
    pre = ev.select(
        "game_id", pl.col("order_index").alias("start_order_index"), "_pre_home_margin", "home_team_id", "season"
    )
    return poss.join(pre, on=["game_id", "start_order_index"], how="inner").with_columns(
        pl.col("_ball_clock").alias("seconds_remaining"),
        pl.when(pl.col("_live_start")).then(1).otherwise(0).cast(pl.Int32).alias("transition"),
        pl.lit(_START_EVENT, dtype=pl.Int32).alias("last_event"),
        *[pl.lit(0, dtype=pl.Int32).alias(c) for c in ("ft_pending", "shot_zone")],
        pl.lit(0, dtype=pl.Int64).alias("points"),
    )


def epv_states(possessions: pl.DataFrame, pbp: pl.DataFrame) -> pl.DataFrame:
    """Every EPV state of the possessions: each one's start, then the state after each of its events.

    Args:
        possessions: Every possession of the games (``nba_stats_possessions`` /
            ``wnba_stats_possessions``): ``game_id``, ``period``, ``possession_number``,
            ``number_in_period``, ``start_order_index``, ``end_order_index``,
            ``start_seconds_remaining``, ``end_seconds_remaining``, ``possession_start_type``,
            ``offense_team_id``, ``defense_team_id`` and ``off_player_1..5`` / ``def_player_1..5``.
        pbp: Their play-by-play (``nba_stats_pbp`` / ``wnba_stats_pbp``): ``season``, ``game_id``,
            ``order_index``, ``period``, ``seconds_remaining``, ``event_type``, ``sub_type``,
            ``description``, ``shot_distance``, ``shot_value``, ``team_id``, ``person_id``,
            ``location``, ``score_home``, ``score_away`` and ``action_number``. Each event carries its
            possession's starting ten (``off_player_*`` / ``def_player_*``).

    Returns:
        One row per state, in game order: ``step`` 0 is the possession's start (no event columns),
        then one row per event. Carries :data:`EPV_FEATURES`, ``points`` (the offense's points on
        the event), ``remaining_points`` (the offense's points after the state, to the end of the
        possession: the model's label), ``is_last`` (the possession's last event),
        ``next_in_period`` and the event's ``team_id`` / ``person_id`` / ``event_type`` / ``sub_type`` /
        ``description`` / player slots.

    Raises:
        ValueError: If a needed column is missing, a ``possession_start_type`` is not one of
            :data:`START_TYPES`, or ``pbp`` has no event for a possession (the start of one, or
            any: the BB-D7 flags' check).
    """
    missing = [c for c in _PBP_COLS if c not in pbp.columns] + [c for c in _POSS_COLS if c not in possessions.columns]
    if missing:
        raise ValueError(f"missing columns {missing}")
    ev = _scored_events(pbp)
    poss = _possession_frame(possessions, pbp)
    events = _event_states(ev, poss).with_columns(pl.int_range(1, pl.len() + 1).over(_KEY).alias("step"))
    starts = _start_states(ev, poss).with_columns(pl.lit(0, dtype=pl.Int64).alias("step"))
    if starts.height != possessions.height:
        raise ValueError(f"pbp has no first event for {possessions.height - starts.height} possessions")
    margin = (
        pl.when(pl.col("offense_team_id") == pl.col("home_team_id"))
        .then(pl.col("_pre_home_margin"))
        .otherwise(-pl.col("_pre_home_margin"))
    )
    start_margin = starts.select(*_KEY, margin.alias("start_margin"))
    out = pl.concat(
        [starts.join(start_margin, on=_KEY), events.join(start_margin, on=_KEY, how="inner")],
        how="diagonal_relaxed",
    )
    return (
        out.with_columns(
            pl.col("period").clip(upper_bound=5).cast(pl.Int32),
            pl.col("start_margin").cast(pl.Float64),
            pl.col("seconds_remaining").cast(pl.Float64),
        )
        .sort("game_id", "possession_number", "step")
        .with_columns(
            (pl.col("points").sum().over(_KEY) - pl.col("points").cum_sum().over(_KEY)).alias("remaining_points"),
            ((pl.col("step") == pl.col("step").max().over(_KEY)) & (pl.col("step") > 0)).alias("is_last"),
            pl.col("_next_in_period").alias("next_in_period"),
        )
        .select(
            "season",
            *_KEY,
            "step",
            "order_index",
            "action_number",
            "offense_team_id",
            "defense_team_id",
            "home_team_id",
            "team_id",
            "person_id",
            "event_type",
            "sub_type",
            "description",
            *_SLOTS,
            *EPV_FEATURES,
            "points",
            "remaining_points",
            "is_last",
            "next_in_period",
        )
    )


@lru_cache(maxsize=2)
def load_epv_model(league_id: str = "00") -> xgb.Booster:
    """The bundled EPV booster for ``league_id`` (``"00"`` NBA; the state builder takes WNBA frames too).

    Its attributes carry ``start_epv_mean`` (the average start value the possession-ending term
    is measured against), the train / holdout / confirmation seasons and the fitting script.
    """
    import xgboost as xgb

    if league_id not in _LEAGUES:
        raise ValueError(f"league_id must be one of {sorted(_LEAGUES)}, got {league_id!r}")
    booster = xgb.Booster()
    booster.load_model(bytearray(files("sportsdataverse.nba.models").joinpath(_LEAGUES[league_id]).read_bytes()))
    _check_features(booster)
    return booster


def _check_features(booster: xgb.Booster) -> None:
    """The booster must read :data:`EPV_FEATURES` in order (``inplace_predict`` on an array never checks)."""
    if list(booster.feature_names or []) != EPV_FEATURES:
        raise ValueError(f"the booster reads {booster.feature_names}, not {EPV_FEATURES}")


def add_epv(states: pl.DataFrame, *, league_id: str = "00", booster: xgb.Booster | None = None) -> pl.DataFrame:
    """Score :func:`epv_states` and return its event rows with ``epv_before``, ``epv_after`` and ``epv_added``.

    Args:
        states: :func:`epv_states` output (whole possessions: the start row and every event).
        league_id: The bundled booster to use: ``"00"`` NBA (the only one shipped).
        booster: A booster to use instead (the fitting script scores candidates with it); it must
            carry the ``start_epv_mean`` attribute.

    Returns:
        The event rows (``step`` >= 1) with ``epv`` (the state after the event), ``epv_start``
        (the possession's start value), ``epv_before``, ``epv_after`` and ``epv_added``, defined in
        the module docstring.

    Raises:
        ValueError: If the booster has no ``start_epv_mean`` attribute.
    """
    import numpy as np

    if booster is None:
        _check_seasons(states["season"].drop_nulls().unique().to_list())
        booster = load_epv_model(league_id)
    if booster.attr("start_epv_mean") is not None:
        _check_features(booster)
    mean = booster.attr("start_epv_mean")
    if mean is None:
        raise ValueError("the booster has no start_epv_mean attribute; fit it with tools/models/nba_epv/train_epv.py")
    if states.is_empty():
        return states.with_columns(
            pl.lit(None, dtype=pl.Float64).alias(c)
            for c in ("epv", "epv_start", "epv_before", "epv_after", "epv_added")
        )
    x = states.select(pl.col(EPV_FEATURES).cast(pl.Float64)).to_numpy()
    scored = states.with_columns(pl.Series("epv", np.asarray(booster.inplace_predict(x), dtype=np.float64)))
    start = scored.filter(pl.col("step") == 0).select(*_KEY, pl.col("epv").alias("epv_start"))
    nxt = start.select("game_id", (pl.col("possession_number") - 1).alias("possession_number"), "epv_start")
    return (
        scored.join(start, on=_KEY, how="left")
        .join(nxt.rename({"epv_start": "_next_start"}), on=_KEY, how="left")
        .sort("game_id", "possession_number", "step")
        .with_columns(pl.col("epv").shift(1).over(_KEY).alias("epv_before"))
        .with_columns(
            pl.when(~pl.col("is_last"))
            .then(pl.col("epv"))
            .when(pl.col("next_in_period") & pl.col("_next_start").is_not_null())
            .then(float(mean) - pl.col("_next_start"))
            .otherwise(0.0)
            .alias("epv_after")
        )
        .with_columns((pl.col("points") + pl.col("epv_after") - pl.col("epv_before")).alias("epv_added"))
        .filter(pl.col("step") > 0)
        .drop("_next_start")
    )


def _is_player() -> pl.Expr:
    """``person_id`` names a player (the feed puts the team id, or 0, on team events)."""
    p = pl.col("person_id")
    return p.is_not_null() & (p > 0) & (p != pl.col("offense_team_id")) & (p != pl.col("defense_team_id"))


def _own_credit(plays: pl.DataFrame) -> pl.DataFrame:
    """Each event's actor and the EPV credited to them (see the module docstring), players only."""
    d = pl.col("description").fill_null("")
    prev = pl.col("epv_added").shift(1).over("game_id")
    return (
        plays.sort("game_id", "order_index")
        .with_columns(
            pl.when((pl.col("event_type") == "other") & d.str.contains("STEAL|BLOCK"))
            .then(-prev)
            .when(pl.col("event_type").is_in(["substitution", "timeout", "period", "replay", "other"]))
            .then(None)
            .when(pl.col("team_id") == pl.col("offense_team_id"))
            .then(pl.col("epv_added"))
            .when(pl.col("team_id") == pl.col("defense_team_id"))
            .then(-pl.col("epv_added"))
            .alias("_credit")
        )
        .filter(_is_player() & pl.col("_credit").is_not_null())
        .select("season", "game_id", "team_id", pl.col("person_id").alias("player_id"), "_credit")
    )


def _action_tens(game_lineups: pl.DataFrame) -> pl.DataFrame:
    """``(game_id, action_number) -> _home_1..5 / _away_1..5`` from the game lineups release (BB-D6's read)."""
    return game_lineups.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("action_number").cast(pl.Int64),
        *[pl.col(f"{s}_player_{i}").cast(pl.Int64).alias(f"_{s}_{i}") for s in ("home", "away") for i in range(1, 6)],
    ).unique(["game_id", "action_number"], keep="first")


def _on_court(plays: pl.DataFrame, game_lineups: pl.DataFrame | None) -> pl.DataFrame:
    """Each event's EPV added on the ten players then on the floor, free throws on the foul's lineup."""
    home = pl.col("offense_team_id") == pl.col("home_team_id")
    slot = {
        f"{side}_{i}": pl.when(home == (side == "home"))
        .then(pl.col(f"off_player_{i}"))
        .otherwise(pl.col(f"def_player_{i}"))
        for side in ("home", "away")
        for i in range(1, 6)
    }
    if game_lineups is not None:
        plays = plays.join(_action_tens(game_lineups), on=["game_id", "action_number"], how="left")
        known = pl.all_horizontal(pl.col(f"_{k}").is_not_null() for k in slot)
        # the action's ten when the release has all of them, else the possession's starting ten
        slot = {k: pl.when(known).then(pl.col(f"_{k}")).otherwise(v) for k, v in slot.items()}
    rows = plays.sort("game_id", "order_index").with_columns(
        **slot,
        seconds=pl.lit(0.0),  # no clock moves, so the BB-D6 helper splits no row and only moves the slots
        home_pts=pl.when(home).then(pl.col("epv_added")).otherwise(0.0),
        away_pts=pl.when(home).then(0.0).otherwise(pl.col("epv_added")),
    )
    technical = pl.col("sub_type").fill_null("").str.contains("Technical")
    rows = credit_free_throws_to_foul_lineup(
        rows,
        is_free_throw=(pl.col("event_type") == "free_throw") & ~technical,
        is_foul=(pl.col("event_type") == "foul") & ~technical,
    )
    away_team = pl.when(home).then(pl.col("defense_team_id")).otherwise(pl.col("offense_team_id"))
    sides = []
    for side, team, mine, theirs in (
        ("home", pl.col("home_team_id"), "home_pts", "away_pts"),
        ("away", away_team, "away_pts", "home_pts"),
    ):
        sides.append(
            rows.select(
                "season",
                "game_id",
                team.alias("team_id"),
                pl.concat_list(f"{side}_{i}" for i in range(1, 6)).alias("_five"),
                pl.col(mine).alias("off_epv"),
                pl.col(theirs).alias("def_epv"),
            )
        )
    return pl.concat(sides)


def epv_player_games(plays: pl.DataFrame, game_lineups: pl.DataFrame | None = None) -> pl.DataFrame:
    """Per player and game: EPV added on the player's own actions and on-court EPV +/-.

    Args:
        plays: :func:`add_epv` output (or :func:`nba_epv_plays`).
        game_lineups: The ``nba_stats_game_lineups`` release (``game_id``,
            ``action_number``, ``home_player_1..5``, ``away_player_1..5``): the ten on the floor
            at each action. Without it each play takes its possession's starting ten.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``epv_own`` and ``own_actions``
        (the EPV credited to the player's own events, and how many), ``off_epv_on`` /
        ``def_epv_on`` (the offense's EPV added while the player's team / the opponent had the
        ball with the player on the floor) and ``epv_on`` (their difference: on-court EPV +/-).
        Sorted by game, team and player. These are accounting sums, like a box score's +/-, not
        estimates of a player's value: one game carries no interval and is not a ranking.
    """
    on = (
        _on_court(plays, game_lineups)
        .explode("_five")
        .rename({"_five": "player_id"})
        .drop_nulls("player_id")
        .group_by("season", "game_id", "team_id", "player_id")
        .agg(pl.col("off_epv").sum().alias("off_epv_on"), pl.col("def_epv").sum().alias("def_epv_on"))
    )
    own = (
        _own_credit(plays)
        .group_by("season", "game_id", "team_id", "player_id")
        .agg(pl.col("_credit").sum().alias("epv_own"), pl.len().cast(pl.Int64).alias("own_actions"))
    )
    keys = ["season", "game_id", "team_id", "player_id"]
    return (
        on.join(own, on=keys, how="full", coalesce=True)
        .with_columns(
            pl.col("epv_own", "off_epv_on", "def_epv_on").fill_null(0.0),
            pl.col("own_actions").fill_null(0),
        )
        .with_columns((pl.col("off_epv_on") - pl.col("def_epv_on")).alias("epv_on"))
        .select(*keys, "epv_own", "own_actions", "off_epv_on", "def_epv_on", "epv_on")
        .sort("game_id", "team_id", "player_id")
    )


def epv_lineup_games(plays: pl.DataFrame, game_lineups: pl.DataFrame | None = None) -> pl.DataFrame:
    """Per five-man unit and game: the offense's EPV added with the unit on, both ways.

    Args:
        plays: :func:`add_epv` output (or :func:`nba_epv_plays`).
        game_lineups: The game lineups release, as for :func:`epv_player_games`.

    Returns:
        One row per ``(season, game_id, team_id, lineup_id)``: ``lineup_id`` (the five player ids
        sorted and joined with ``-``), ``off_epv`` / ``def_epv`` (EPV added by the unit's team / by
        the opponent while it was on), ``net_epv`` and ``plays``. Events with an unknown slot are
        left out. Sorted by game, team and unit; accounting sums, not estimates (as
        :func:`epv_player_games`).
    """
    return (
        _on_court(plays, game_lineups)
        .filter(pl.col("_five").list.drop_nulls().list.n_unique() == 5)  # a repeated id is a source glitch
        .with_columns(pl.col("_five").list.sort().cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
        .group_by("season", "game_id", "team_id", "lineup_id")
        .agg(pl.col("off_epv").sum(), pl.col("def_epv").sum(), pl.len().cast(pl.Int64).alias("plays"))
        .with_columns((pl.col("off_epv") - pl.col("def_epv")).alias("net_epv"))
        .select("season", "game_id", "team_id", "lineup_id", "off_epv", "def_epv", "net_epv", "plays")
        .sort("game_id", "team_id", "lineup_id")
    )


def epv_team_games(plays: pl.DataFrame) -> pl.DataFrame:
    """Per team and game: EPV added on offense and allowed on defense.

    Args:
        plays: :func:`add_epv` output (or :func:`nba_epv_plays`).

    Returns:
        One row per ``(season, game_id, team_id)``: ``opp_team_id``, ``off_epv`` (the team's EPV
        added with the ball), ``def_epv`` (the opponent's), ``net_epv``, ``poss`` / ``opp_poss``
        (possessions with a play) and ``off_epv_per_poss`` / ``def_epv_per_poss``.
    """
    off = plays.group_by("season", "game_id", pl.col("offense_team_id").alias("team_id")).agg(
        pl.col("defense_team_id").first().alias("opp_team_id"),
        pl.col("epv_added").sum().alias("off_epv"),
        pl.col("possession_number").n_unique().cast(pl.Int64).alias("poss"),
    )
    opp = off.select(
        "season", "game_id",
        pl.col("opp_team_id").alias("team_id"), pl.col("off_epv").alias("def_epv"), pl.col("poss").alias("opp_poss"),
    )  # fmt: skip
    return (
        off.join(opp, on=["season", "game_id", "team_id"], how="left")
        .with_columns(
            (pl.col("off_epv") - pl.col("def_epv")).alias("net_epv"),
            (pl.col("off_epv") / pl.col("poss")).alias("off_epv_per_poss"),
            (pl.col("def_epv") / pl.col("opp_poss")).alias("def_epv_per_poss"),
        )
        .sort("game_id", "team_id")
    )


def _check_seasons(seasons: int | list[int]) -> list[int]:
    """The seasons as a list; refuses one before the booster's first training season."""
    out = [seasons] if isinstance(seasons, int) else list(seasons)
    first = min(int(x) for x in load_epv_model().attr("train_seasons").split(","))
    if any(s < first for s in out):
        raise ValueError(f"EPV is served from the {first} season on, got {out}")
    return out


def _load(seasons: int | list[int], *, lineups: bool = False) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """The scored event rows of ``seasons`` (``nba_stats_pbp`` / ``_possessions``) and, asked, the game lineups."""
    from sportsdataverse.nba import nba_loaders as N

    start = [x - 1 for x in _check_seasons(seasons)]  # the NBA loaders take the start year
    pbp, poss = N.load_nba_stats_pbp(start), N.load_nba_stats_possessions(start)
    tens = N.load_nba_stats_game_lineups(start) if lineups else None
    return add_epv(epv_states(poss, pbp)), tens


def _out(frame: pl.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    return frame.to_pandas() if return_as_pandas else frame


def nba_epv_plays(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """EPV and EPV added on every play-by-play event inside a possession.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per event, in game order: ``season``, ``game_id``, ``possession_number``,
        ``step`` (the event's place in the possession), ``order_index``, ``action_number``, the
        offense, defense and home team ids, the event's ``team_id`` / ``person_id`` /
        ``event_type`` / ``sub_type`` / ``description``, the possession's starting ten, the state
        (:data:`EPV_FEATURES`), ``points``, ``remaining_points``, ``is_last``,
        ``next_in_period``, ``epv``, ``epv_start``, ``epv_before``, ``epv_after`` and
        ``epv_added``. Every game of every season type is scored.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        The biggest plays of a game::

            from sportsdataverse.nba.nba_epv import nba_epv_plays
            plays = nba_epv_plays(2026)
            plays.filter(game_id="0022500101").sort("epv_added", descending=True).head(5)
    """
    return _out(_load(seasons)[0], return_as_pandas)


def nba_epv_player_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per player and game: EPV added on the player's own actions and on-court EPV +/-.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`epv_player_games` of the seasons' plays, on-court from the
        ``nba_stats_game_lineups`` release: one row per ``(season, game_id, team_id,
        player_id)`` with ``epv_own``, ``own_actions``, ``off_epv_on``, ``def_epv_on`` and
        ``epv_on``. Ids are the stats.nba.com ids (Int64; ``game_id`` a String).

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A game's EPV box::

            from sportsdataverse.nba.nba_epv import nba_epv_player_games
            box = nba_epv_player_games(2026)
            box.filter(game_id="0022500101")
    """
    return _out(epv_player_games(*_load(seasons, lineups=True)), return_as_pandas)


def nba_epv_lineup_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per five-man unit and game: the EPV added with the unit on the floor.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`epv_lineup_games` of the seasons' plays, on-court from the
        ``nba_stats_game_lineups`` release: one row per ``(season, game_id, team_id,
        lineup_id)`` with ``off_epv``, ``def_epv``, ``net_epv`` and ``plays``.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A game's units::

            from sportsdataverse.nba.nba_epv import nba_epv_lineup_games
            units = nba_epv_lineup_games(2026)
            units.filter(game_id="0022500101")
    """
    return _out(epv_lineup_games(*_load(seasons, lineups=True)), return_as_pandas)


def nba_epv_team_games(seasons: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Per team and game: EPV added on offense and allowed on defense.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`epv_team_games` of the seasons' plays: one row per ``(season, game_id, team_id)``
        with ``opp_team_id``, ``off_epv``, ``def_epv``, ``net_epv``, ``poss``, ``opp_poss``,
        ``off_epv_per_poss`` and ``def_epv_per_poss``.

    Raises:
        ValueError: If a season is before the first one served.

    Example:
        A season's offenses by EPV added per possession::

            import polars as pl
            from sportsdataverse.nba.nba_epv import nba_epv_team_games
            teams = nba_epv_team_games(2026)
            teams.group_by("team_id").agg(pl.col("off_epv_per_poss").mean()).sort("off_epv_per_poss")
    """
    return _out(epv_team_games(_load(seasons)[0]), return_as_pandas)
