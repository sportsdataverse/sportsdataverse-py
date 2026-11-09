"""Situational splits (BB-D21): the shared core behind ``{lg}_situational_{team,player,lineup}``.

Every number comes from possessions: one **situation row** per possession
(:func:`situation_row_schema`), BB-D3's :data:`~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`
plus the regulation period count, BB-D7's flags and the ten players on the floor when it starts.
Each possession is seen twice, from the offense (an offensive possession, its points scored) and from
the defense (a defensive possession, its points allowed), and tagged once per split family:

==============  ==========================================  ==========================================
family          splits                                      the possession is in the split when
==============  ==========================================  ==========================================
``all``         ``all``                                     always
``clutch``      ``clutch``, ``not_clutch``                  BB-D3's clutch window: it is in play with
                                                            5:00 or less left (it starts there, or is
                                                            the one live at 5:00) in the last
                                                            regulation period or an overtime, the
                                                            score within 5 as it starts
                                                            (stats.nba.com's ``nba_clutch`` window)
``period``      ``p1`` .. ``p4``, ``ot``                    its period; every overtime is ``ot``; a
                                                            two-halves game has ``p1`` and ``p2`` only
``half``        ``h1``, ``h2``, ``ot``                      its half (quarters 1-2 and 3-4)
``garbage``     ``garbage_time``, ``not_garbage_time``      the possession's ``is_garbage_time`` flag
                                                            (BB-D7 for the pros, bigballR's for college)
``score``       ``leading``, ``tied``, ``trailing``         the team's margin as the possession starts
``venue``       ``home``, ``away``, ``neutral``             the team's side of the game
``opponent``    ``vs_top``, ``vs_rest``                     the opponent is in the season's top ``top_n``
                                                            by ``adj_net_rtg`` (an unrated opponent is
                                                            ``vs_rest``)
``play_type``   ``transition``, ``half_court``              the possession's ``is_transition`` flag
``chance``      ``second_chance``, ``first_chance``         the possession had an offensive rebound
                                                            (``is_second_chance``); no rows when the
                                                            source has no flag (college)
==============  ==========================================  ==========================================

The margin as a possession starts is the running total of the points of every earlier possession of
the game (BB-D3's clutch margin). From the defense's side a transition or second-chance possession is
the opponent's: a team's ``transition`` row holds its transition offense and its transition defense.

So within a family the splits **add up to** the ``all`` row: ``poss``, ``opp_poss``, ``pts``,
``opp_pts`` and ``seconds`` of ``leading + tied + trailing`` equal ``all``'s, and so on for every
family (``games`` does not add: a game has several splits).

Per row: ``games`` (games with a possession in the split), ``seconds`` / ``minutes`` (the game clock
its possessions used), ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``, ``plus_minus``, ``off_rtg`` /
``def_rtg`` / ``net_rtg`` per 100 possessions and their ``_n`` (BB-D2 grammar: ``poss``,
``opp_poss`` and both). A **player** is credited with the possessions he is on the floor for when
they start and all their points (the RAPM stint convention), so a player's ``all`` row can differ
from BB-D6's impact box, which credits each point to the lineup at the scoring action. A **lineup**
is the five on the floor at the start; a possession with fewer than five known players has no
lineup row. There are no ranks: a split's cohort is too thin to rank most of them.

The ``opponent`` ranking is hindsight: the season's ratings, the games being split included. It
describes who a team played, and is not a feature to predict with. A possession whose flag is null
is in no split of that family (so the family no longer adds up); the four leagues' sources have no
null flag, and college has no second-chance flag at all (no ``chance`` rows).

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

from typing import Final

import polars as pl

from sportsdataverse._common.game_impact import _TOTALS, _rates
from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA, _with_clutch

__all__ = [
    "DEF_SLOTS",
    "FAMILIES",
    "OFF_SLOTS",
    "situation_row_schema",
    "situational_lineup",
    "situational_lineup_schema",
    "situational_player",
    "situational_player_schema",
    "situational_team",
    "situational_team_schema",
]

#: Split family -> its splits, in output order.
FAMILIES: Final[dict[str, tuple[str, ...]]] = {
    "all": ("all",),
    "clutch": ("clutch", "not_clutch"),
    "period": ("p1", "p2", "p3", "p4", "ot"),
    "half": ("h1", "h2", "ot"),
    "garbage": ("garbage_time", "not_garbage_time"),
    "score": ("leading", "tied", "trailing"),
    "venue": ("home", "away", "neutral"),
    "opponent": ("vs_top", "vs_rest"),
    "play_type": ("transition", "half_court"),
    "chance": ("second_chance", "first_chance"),
}
OFF_SLOTS: Final[tuple[str, ...]] = tuple(f"off_{i}" for i in range(1, 6))
DEF_SLOTS: Final[tuple[str, ...]] = tuple(f"def_{i}" for i in range(1, 6))
_SUMS = ("seconds", "poss", "opp_poss", "pts", "opp_pts")
_STATS: dict[str, pl.DataType] = {"games": pl.Int64(), **_TOTALS}
_SPLIT_KEYS: dict[str, pl.DataType] = {"family": pl.Utf8(), "split": pl.Utf8()}
_ORDER = pl.DataFrame(
    [(f, s, i) for i, (f, s) in enumerate((f, s) for f, ss in FAMILIES.items() for s in ss)],
    schema={"family": pl.Utf8, "split": pl.Utf8, "_order": pl.Int64},
    orient="row",
)


def situation_row_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """The possession frame the builders read; ``player_dtype`` is the league's player id dtype.

    BB-D3's ``RATE_ROWS_SCHEMA`` plus ``reg_periods`` (2 for a game in halves, 4 in quarters), the
    flags ``is_transition``, ``is_garbage_time`` and ``is_second_chance`` (null when the source has
    none) and the offense's and defense's players on the floor as it starts (``off_1..5`` /
    ``def_1..5``).
    """
    return {
        **RATE_ROWS_SCHEMA,
        "reg_periods": pl.Int64(),
        "is_transition": pl.Boolean(),
        "is_garbage_time": pl.Boolean(),
        "is_second_chance": pl.Boolean(),
        **dict.fromkeys(OFF_SLOTS + DEF_SLOTS, player_dtype),
    }


def situational_team_schema() -> dict[str, pl.DataType]:
    """Columns of :func:`situational_team`."""
    return {"season": pl.Int64(), "team_id": pl.Int64(), **_SPLIT_KEYS, **_STATS}


def situational_player_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`situational_player` for a league whose player ids are ``player_dtype``."""
    return {"season": pl.Int64(), "team_id": pl.Int64(), "player_id": player_dtype, **_SPLIT_KEYS, **_STATS}


def situational_lineup_schema(player_dtype: pl.DataType) -> dict[str, pl.DataType]:
    """Columns of :func:`situational_lineup` for a league whose player ids are ``player_dtype``."""
    players = {f"player_{i}": player_dtype for i in range(1, 6)}
    return {"season": pl.Int64(), "team_id": pl.Int64(), "lineup_id": pl.Utf8(), **players, **_SPLIT_KEYS, **_STATS}


def _flag(col: str, yes: str, no: str) -> pl.Expr:
    """``yes`` / ``no`` from a Boolean flag; null stays null (the family then has no row)."""
    return pl.when(pl.col(col)).then(pl.lit(yes)).when(~pl.col(col)).then(pl.lit(no))


def _labels(own_home: pl.Expr) -> list[pl.Expr]:
    """One label column per family, for the side whose team is home when ``own_home``."""
    period, reg = pl.col("period"), pl.col("reg_periods")
    ot = period > reg
    margin = pl.when(own_home).then(pl.col("start_margin")).otherwise(-pl.col("start_margin"))
    return [
        pl.lit("all").alias("all"),
        _flag("clutch", "clutch", "not_clutch").alias("clutch"),
        pl.when(ot).then(pl.lit("ot")).otherwise(pl.format("p{}", period)).alias("period"),
        pl.when(ot)
        .then(pl.lit("ot"))
        .when((reg == 2) | (period <= 2))
        .then(pl.format("h{}", pl.when(reg == 2).then(period).otherwise(1)))
        .otherwise(pl.lit("h2"))
        .alias("half"),
        _flag("is_garbage_time", "garbage_time", "not_garbage_time").alias("garbage"),
        pl.when(margin > 0)
        .then(pl.lit("leading"))
        .when(margin < 0)
        .then(pl.lit("trailing"))
        .otherwise(pl.lit("tied"))
        .alias("score"),
        pl.when(pl.col("neutral_site").fill_null(False))
        .then(pl.lit("neutral"))
        .when(own_home)
        .then(pl.lit("home"))
        .otherwise(pl.lit("away"))
        .alias("venue"),
        pl.when(pl.col("_opp_top").fill_null(False))
        .then(pl.lit("vs_top"))
        .otherwise(pl.lit("vs_rest"))
        .alias("opponent"),
        _flag("is_transition", "transition", "half_court").alias("play_type"),
        _flag("is_second_chance", "second_chance", "first_chance").alias("chance"),
    ]


def _sides(rows: pl.DataFrame, ratings: pl.DataFrame, top_n: int, player_dtype: pl.DataType) -> pl.DataFrame:
    """Each possession twice, once from each team's side, with its players and a label per family."""
    schema = situation_row_schema(player_dtype)
    missing = sorted(set(schema) - set(rows.columns))
    if missing:
        raise KeyError(f"rows missing columns: {missing}")
    if top_n < 1:
        raise ValueError(f"top_n must be at least 1, got {top_n}")
    if rows.height == 0:  # the callers return their empty schema; ratings may be empty too
        return pl.DataFrame()
    top = (
        ratings.select(pl.col("season").cast(pl.Int64), pl.col("team_id").cast(pl.Int64), "adj_net_rtg")
        .drop_nulls("adj_net_rtg")
        .filter(pl.col("adj_net_rtg").rank("ordinal", descending=True).over("season") <= top_n)
        .select("season", "team_id", pl.lit(True).alias("_top"))
    )
    flagged = _with_clutch(rows.select(list(schema)).cast(schema)).with_columns(
        (pl.col("start_clock") - pl.col("end_clock")).alias("seconds")
    )
    frames = []
    for us, them, slots, is_off in (("offense", "defense", OFF_SLOTS, True), ("defense", "offense", DEF_SLOTS, False)):
        own_home = pl.col("offense_is_home") if is_off else ~pl.col("offense_is_home")
        pts = pl.col("points").fill_null(0)
        side = flagged.with_columns(
            pl.col(f"{us}_team_id").alias("team_id"), pl.col(f"{them}_team_id").alias("opp_team_id")
        ).join(top.rename({"team_id": "opp_team_id", "_top": "_opp_top"}), on=["season", "opp_team_id"], how="left")
        frames.append(
            side.select(
                "season",
                "game_id",
                "team_id",
                pl.concat_list(slots).list.drop_nulls().list.unique().list.sort().alias("players"),
                "seconds",
                pl.lit(int(is_off), dtype=pl.Int64).alias("poss"),
                pl.lit(int(not is_off), dtype=pl.Int64).alias("opp_poss"),
                (pts if is_off else pts * 0).alias("pts"),
                (pts * 0 if is_off else pts).alias("opp_pts"),
                *_labels(own_home),
            )
        )
    return pl.concat(frames).filter(pl.col("team_id").is_not_null())


def _totals(sides: pl.DataFrame, by: list[str], *, per_player: bool = False) -> pl.DataFrame:
    """Sum each family's splits over ``by``; ``per_player`` explodes each side's players first."""
    sums = [pl.col(c).sum() for c in _SUMS]
    parts = []
    for fam in FAMILIES:
        s = sides.filter(pl.col(fam).is_not_null())
        if per_player:
            # one row per stint and split before the explode: a few players, many possessions
            s = (
                s.group_by(["season", "game_id", "team_id", "players", fam])
                .agg(*sums)
                .explode("players")
                .rename({"players": "player_id"})
                .drop_nulls("player_id")
            )
        parts.append(
            s.group_by([*by, fam])
            .agg(
                pl.col("game_id").n_unique().alias("games"),
                *sums,
                *([pl.col("players").first()] if "lineup_id" in by else []),
            )
            .rename({fam: "split"})
            .with_columns(pl.lit(fam).alias("family"))
        )
    out = _rates(pl.concat(parts, how="diagonal_relaxed"))
    return out.join(_ORDER, on=["family", "split"], how="left", validate="m:1")


def situational_team(rows: pl.DataFrame, ratings: pl.DataFrame, *, top_n: int) -> pl.DataFrame:
    """Per team, season and split: possessions, points, minutes and ratings.

    Args:
        rows: Situation rows (:func:`situation_row_schema`; any player dtype, the slots are unused).
        ratings: ``season``, ``team_id`` and ``adj_net_rtg`` (BB-D19), the ``opponent`` family's
            ranking.
        top_n: How many of the season's best teams by ``adj_net_rtg`` are ``vs_top``.

    Returns:
        One row per ``(season, team_id, family, split)`` in :func:`situational_team_schema`, sorted
        by season, team and split (the :data:`FAMILIES` order). A split the team never played (no
        overtime, no neutral site) has no row. A side with no team id (a college non-D1 opponent)
        gets no row; its points still count in the margin. An empty input returns that schema with
        zero rows.

    Raises:
        KeyError: If ``rows`` is missing a schema column.
        ValueError: If ``top_n`` is below 1.
    """
    schema = situational_team_schema()
    dtype = rows.schema.get("off_1", pl.Int64())
    sides = _sides(rows, ratings, top_n, dtype)
    if sides.height == 0:
        return pl.DataFrame(schema=schema)
    out = _totals(sides.drop("players"), ["season", "team_id"])
    return out.sort("season", "team_id", "_order").select(list(schema)).cast(schema)


def situational_player(
    rows: pl.DataFrame, ratings: pl.DataFrame, *, top_n: int, player_dtype: pl.DataType
) -> pl.DataFrame:
    """Per player, team, season and split: on-floor possessions, points, minutes and ratings.

    Args:
        rows: Situation rows (:func:`situation_row_schema`).
        ratings: As :func:`situational_team`.
        top_n: As :func:`situational_team`.
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, team_id, player_id, family, split)`` in
        :func:`situational_player_schema`, sorted by season, team, player and split. A player who
        played for two teams has a row set for each. Every sum over a team's players is five times
        the team's (possessions with an unknown slot aside). An empty input returns that schema
        with zero rows.

    Raises:
        KeyError: If ``rows`` is missing a schema column.
        ValueError: If ``top_n`` is below 1.
    """
    schema = situational_player_schema(player_dtype)
    sides = _sides(rows, ratings, top_n, player_dtype)
    if sides.height == 0:
        return pl.DataFrame(schema=schema)
    out = _totals(sides, ["season", "team_id", "player_id"], per_player=True)
    return out.sort("season", "team_id", "player_id", "_order").select(list(schema)).cast(schema)


def situational_lineup(
    rows: pl.DataFrame, ratings: pl.DataFrame, *, top_n: int, player_dtype: pl.DataType
) -> pl.DataFrame:
    """Per five-man unit, team, season and split: possessions, points, minutes and ratings.

    Args:
        rows: Situation rows (:func:`situation_row_schema`).
        ratings: As :func:`situational_team`.
        top_n: As :func:`situational_team`.
        player_dtype: The league's player id dtype.

    Returns:
        One row per ``(season, team_id, lineup_id, family, split)`` in
        :func:`situational_lineup_schema`: ``lineup_id`` is the five player ids sorted and joined
        with ``-`` (BB-D4's id), ``player_1..5`` the same ids. Possessions with fewer than five known
        players are left out. Sorted by season, team, lineup and split. An empty input returns that
        schema with zero rows.

    Raises:
        KeyError: If ``rows`` is missing a schema column.
        ValueError: If ``top_n`` is below 1.
    """
    schema = situational_lineup_schema(player_dtype)
    sides = _sides(rows, ratings, top_n, player_dtype)
    if sides.height:
        sides = sides.filter(pl.col("players").list.len() == 5)
    if sides.height == 0:
        return pl.DataFrame(schema=schema)
    sides = sides.with_columns(pl.col("players").cast(pl.List(pl.Utf8)).list.join("-").alias("lineup_id"))
    out = _totals(sides, ["season", "team_id", "lineup_id"]).with_columns(
        pl.col("players").list.get(i, null_on_oob=True).alias(f"player_{i + 1}") for i in range(5)
    )
    return out.sort("season", "team_id", "lineup_id", "_order").select(list(schema)).cast(schema)
