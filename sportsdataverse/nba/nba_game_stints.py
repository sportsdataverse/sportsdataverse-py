"""NBA / WNBA game stints, in-game units and on/off (BB-D32): ``nba.game_stints`` and ``nba.game_units``.

Builds the :mod:`sportsdataverse._common.game_stints` tables from the four stats.nba.com releases
the game impact box reads (:mod:`sportsdataverse.nba.nba_game_impact`):

* the **stints** walk the ``nba_stats_pbp`` actions in ``order_index`` order on the
  ``nba_stats_game_lineups`` lineup at each action; the clock in and out is the action's
  ``seconds_remaining``, and each ``nba_stats_possessions`` row is counted on the action at its
  ``start_order_index`` (on the 2025-26 fixture games that action's lineup is the possession's on
  99.5% of possessions, on every 2026 WNBA one);
* the **units** and **on/off** roll up the game impact rows, with the four-factor counts of each
  possession (:func:`sportsdataverse.nba.nba_lineup_seasons.nba_lineup_count_rows`) and its rim and
  mid-range twos from the pbp shots (a rim attempt is a two under 4 feet);
* the **player box** (:func:`nba_box_events`, on the stints and in :func:`nba_game_player_box_splits`)
  reads each action's own player (``person_id``): shots, free throws, rebounds (the change in the
  ``(Off:n Def:n)`` running counts), steals and blocks (their own ``STEAL`` / ``BLOCK`` rows),
  turnovers and personal fouls (technicals and defensive three seconds are not); the assist is
  the passer named in a made shot's ``(Name n AST)``, matched to a teammate by the pbp's
  ``player_name`` / ``player_name_i`` (accents folded). Team rows (team rebounds and turnovers)
  are no player's. An action's ``is_garbage_time`` is its possession's BB-D7 flag
  (:func:`~sportsdataverse.nba.nba_possession_flags.add_possession_flags`, the one the
  situational splits read): once a game's possession triggers it, every action from that
  possession's first on is garbage time.

``league_id="10"`` reads the WNBA twins; :mod:`sportsdataverse.wnba.wnba_game_stints` binds it.
Every game of every season type is included.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from sportsdataverse._common.game_stints import (
    BOX_COUNTS,
    box_event_schema,
    credit_free_throws_to_foul_row,
    game_on_off,
    game_player_box_splits,
    game_stints,
    game_units,
    stint_row_schema,
)
from sportsdataverse.nba.nba_game_impact import (
    _QUARTER,
    _actions,
    _check_league,
    _load_releases,
    _possession_rows,
    nba_impact_rows,
)
from sportsdataverse.nba.nba_lineup_seasons import nba_lineup_count_rows
from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "nba_box_events",
    "nba_game_on_off",
    "nba_game_player_box_splits",
    "nba_game_stints",
    "nba_game_units",
    "nba_stint_rows",
]

_PLAYER = pl.Int64()
_ROWS = stint_row_schema(_PLAYER)
_EVENTS = box_event_schema(_PLAYER)
_COUNTS = [c for c in BOX_COUNTS if c != "reb"]
#: The pbp columns BB-D7's garbage-time flag reads.
_FLAG_PBP = ("game_id", "order_index", "event_type", "seconds_remaining", "score_home", "score_away")
#: The pbp columns :func:`nba_box_events` reads.
_BOX_PBP = (
    "season", "game_id", "order_index", "team_id", "person_id", "player_name", "player_name_i",
    "action_type", "sub_type", "description", "shot_value",
)  # fmt: skip


def _fold(name: pl.Expr) -> pl.Expr:
    """A player name with its accents dropped (the pbp text writes ``Jokic`` for ``Jokić``)."""
    return name.str.normalize("NFKD").str.replace_all(r"\p{Mn}", "")


def _games(schedule: pl.DataFrame) -> pl.DataFrame:
    return schedule.select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), pl.col("away_team_id").cast(pl.Int64)
    ).unique("game_id")


def nba_box_events(pbp: pl.DataFrame, schedule: pl.DataFrame) -> pl.DataFrame:
    """Each player's box counts per stats.nba.com action, the input of the stint and split box.

    Args:
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows (``season``, ``game_id``,
            ``order_index``, ``team_id``, ``person_id``, ``player_name``, ``player_name_i``,
            ``action_type``, ``sub_type``, ``description``, ``shot_value``).
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows (``game_id``,
            ``home_team_id``, ``away_team_id``).

    Returns:
        Rows in :func:`sportsdataverse._common.game_stints.box_event_schema` (Int64 player ids),
        ``order`` the action's ``order_index``: one per player action with a count, and one per
        assist on the made shot's action. An assist whose passer matches no single teammate's name
        in the game has no row. Games not in ``schedule`` are dropped.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_stints import nba_box_events
            nba_box_events(L.load_nba_stats_pbp(2025), L.load_nba_stats_schedules(2025)).head()
    """
    if pbp.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_EVENTS)
    team = pl.col("team_id")
    e = (
        pbp.select(_BOX_PBP)
        .with_columns(pl.col("game_id").cast(pl.Utf8), team.cast(pl.Int64), pl.col("person_id").cast(pl.Int64))
        .join(_games(schedule), on="game_id", how="inner", validate="m:1")
        .filter((team == pl.col("home_team_id")) | (team == pl.col("away_team_id")))  # team rows have team_id 0
        .sort("game_id", "order_index")
    )
    act, text = pl.col("action_type").fill_null(""), pl.col("description").fill_null("")
    fg, made, three = act.is_in(["Made Shot", "Missed Shot"]), act == "Made Shot", pl.col("shot_value") == 3
    ft = act == "Free Throw"
    rebound = act == "Rebound"
    # a rebound row carries the player's running (Off:n Def:n); his rebound is the step up
    run = {s: pl.when(rebound).then(text.str.extract(r"\(Off:(\d+) Def:(\d+)\)", i).cast(pl.Int64)) for s, i in (("oreb", 1), ("dreb", 2))}  # fmt: skip
    per = ["game_id", "person_id"]
    flags = {
        "fga": fg,
        "fgm": made,
        "fg3a": fg & three,
        "fg3m": made & three,
        "fta": ft,
        "ftm": ft & ~text.str.starts_with("MISS"),
        "stl": (act == "") & text.str.contains(" STEAL (", literal=True),
        "blk": (act == "") & text.str.contains(" BLOCK (", literal=True),
        "tov": act == "Turnover",
        "pf": (act == "Foul") & ~pl.col("sub_type").fill_null("").str.contains(r"(?i)technical|3 second"),
    }
    own = (
        e.with_columns(*[x.fill_null(False).cast(pl.Int64).alias(c) for c, x in flags.items()], **run)
        .with_columns(
            (pl.col(s) - pl.col(s).forward_fill().shift(1).over(per).fill_null(0)).fill_null(0).alias(s) for s in run
        )
        .with_columns(
            (pl.col("fgm") * pl.col("shot_value").fill_null(0) + pl.col("ftm")).alias("points"), pl.lit(0).alias("ast")
        )
    )
    counted = own.filter(pl.sum_horizontal(_COUNTS) > 0)
    # a player's names: the pbp's two forms and the one his own rows' text opens with ("Jal. Williams
    # REBOUND", "MISS Hansen Free Throw"), which is how a pass to him is written when the others clash
    said = text.str.extract(r"^(?:MISS )?(.+?) (?:REBOUND \(Off|STEAL \(|BLOCK \(|Free Throw )")
    names = (
        pl.concat(
            [
                own.select("game_id", "team_id", "person_id", _fold(x).alias("_name"))
                for x in (pl.col("player_name"), pl.col("player_name_i"), said)
            ]
        )
        .filter(pl.col("_name").fill_null("") != "")
        .unique()
    )
    passes = own.filter(made).select(
        "season", "game_id", "team_id", "order_index", _fold(text.str.extract(r"\(([^()]+?) \d+ AST\)")).alias("_name")
    )
    # ponytail: a passer name two teammates share is unresolved (the feed disambiguates them itself)
    assists = (
        passes.drop_nulls("_name")
        .join(names, on=["game_id", "team_id", "_name"], how="inner")
        .filter(pl.len().over("game_id", "order_index") == 1)
        .with_columns(pl.lit(1).alias("ast"))
    )
    out = pl.concat([counted, assists], how="diagonal_relaxed").with_columns(pl.col(_COUNTS).fill_null(0))
    return (
        out.rename({"person_id": "player_id", "order_index": "order"})
        .select(list(_EVENTS))
        .cast(_EVENTS)
        .sort("game_id", "order", "player_id")
    )


def _garbage_from(pbp: pl.DataFrame, possessions: pl.DataFrame, games: pl.DataFrame) -> pl.DataFrame:
    """Per game with possessions: the ``order_index`` garbage time starts at (null when it never does)."""
    poss = possessions.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi")
    events = pbp.select(_FLAG_PBP).with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi")
    # a release that already carries BB-D7's flags is re-flagged, as the situational splits do
    flagged = add_possession_flags(poss.drop(*POSSESSION_FLAGS_SCHEMA, strict=False), events)
    start = pl.col("start_order_index").cast(pl.Int64)
    return flagged.group_by("game_id").agg(start.filter(pl.col("is_garbage_time")).min().alias("_garbage_from"))


def nba_stint_rows(
    pbp: pl.DataFrame,
    game_lineups: pl.DataFrame,
    possessions: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    league_id: str = "00",
) -> pl.DataFrame:
    """Map the stats.nba.com releases onto the stint-row frame.

    One row per play-by-play action in game order, on the lineup at that action: its clock, its
    points (a free throw's on the row of the game's last foul) and the possessions that start at
    it. Games not in ``schedule`` are dropped, and so are possessions whose offense is neither team.

    Args:
        pbp: ``load_nba_stats_pbp`` / ``load_wnba_stats_pbp`` rows (``game_id``, ``order_index``,
            ``action_number``, ``period``, ``seconds_remaining``, ``score_home``, ``score_away``,
            ``is_free_throw``, ``is_foul``, ``event_type``, ``season``).
        game_lineups: ``load_nba_stats_game_lineups`` / ``load_wnba_stats_game_lineups`` rows.
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows, every
            possession of each game (the columns BB-D7's
            :func:`~sportsdataverse.nba.nba_possession_flags.add_possession_flags` reads).
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows
            (``game_id``, ``home_team_id``, ``away_team_id``).
        league_id: ``"00"`` NBA (12-minute quarters) or ``"10"`` WNBA (10-minute quarters).

    Returns:
        Rows in :func:`sportsdataverse._common.game_stints.stint_row_schema` with Int64 player
        ids: ``period``, ``clock`` (the action's ``seconds_remaining``), ``order`` (its
        ``order_index``) and ``is_garbage_time`` (null in a game with no possessions).

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``pbp`` has no action for a
            possession (BB-D7's check).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_loaders as L
            from sportsdataverse.nba.nba_game_stints import nba_stint_rows
            rows = nba_stint_rows(L.load_nba_stats_pbp(2025), L.load_nba_stats_game_lineups(2025),
                                  L.load_nba_stats_possessions(2025), L.load_nba_stats_schedules(2025))
    """
    _check_league(league_id)
    if pbp.height == 0 or schedule.height == 0:
        return pl.DataFrame(schema=_ROWS)
    games = _games(schedule)
    actions = credit_free_throws_to_foul_row(
        _actions(pbp, game_lineups, games, _QUARTER[league_id]),
        is_free_throw=pl.col("is_free_throw").eq(True),
        is_foul=pl.col("is_foul").eq(True),
    )
    starts = pl.DataFrame(
        schema={"game_id": pl.Utf8(), "order_index": pl.Int64(), "home_poss": pl.Int64(), "away_poss": pl.Int64()}
    )
    garbage = pl.DataFrame(schema={"game_id": pl.Utf8(), "_garbage_from": pl.Int64()})
    if possessions.height:
        starts = (
            _possession_rows(possessions, games, keep=("start_order_index",))
            .group_by("game_id", pl.col("start_order_index").cast(pl.Int64).alias("order_index"))
            .agg(pl.col("home_poss", "away_poss").sum())
        )
        garbage = _garbage_from(pbp, possessions, games.select("game_id"))
    out = actions.join(starts, on=["game_id", "order_index"], how="left", validate="1:1", maintain_order="left")
    out = out.join(garbage.with_columns(_flagged=pl.lit(True)), on="game_id", how="left", maintain_order="left")
    return (
        out.with_columns(
            pl.col("home_poss", "away_poss").fill_null(0),
            pl.col("seconds_remaining").alias("clock"),
            pl.col("order_index").alias("order"),
            pl.when(pl.col("_flagged"))
            .then((pl.col("order_index") >= pl.col("_garbage_from")).fill_null(False))
            .alias("is_garbage_time"),
        )
        .select(list(_ROWS))
        .cast(_ROWS)
    )


def nba_game_stints(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Game stints: per player and game, every stretch on the floor with its clock, points and possessions.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id, stint_number)``: ``opp_team_id``,
        ``stint_number`` (1 = his first in the game), ``period``, ``clock_in`` / ``clock_out`` (the
        period clock, seconds left, when the stint began and ended), ``seconds`` / ``minutes`` on
        the floor, ``poss`` / ``opp_poss`` (offensive / defensive possessions started), ``pts`` /
        ``opp_pts`` (team points scored / allowed), ``plus_minus`` and the player's own box in the
        stint (:func:`nba_box_events`): ``points``, ``fgm`` / ``fga``, ``fg3m`` / ``fg3a``,
        ``ftm`` / ``fta``, ``oreb`` / ``dreb`` / ``reb``, ``ast``, ``stl``, ``blk``, ``tov`` and
        ``pf``. Ids are the stats.nba.com ids (Int64; ``game_id`` a String). Empty input returns
        that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_stints
            stints = nba_game_stints(2026)
            stints.filter(game_id="0022500101")
    """
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    rows = nba_stint_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)
    out = game_stints(rows, player_dtype=_PLAYER, events=nba_box_events(pbp, schedule))
    return out.to_pandas() if return_as_pandas else out


def nba_game_player_box_splits(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Player box by game, period and garbage time: minutes and counts from the play-by-play.

    The definitions are in :mod:`sportsdataverse._common.game_stints`
    (:func:`~sportsdataverse._common.game_stints.game_player_box_splits`).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        Per ``(season, game_id, team_id, player_id)``: the ``family="all"`` row (the game), one
        ``family="period"`` row per period played (``split`` ``p1`` .. ``p4``, ``p5`` on for the
        overtimes; ``period``; ``on_floor_at_end``) and the two ``family="garbage"`` rows
        (``garbage_time``, ``not_garbage_time``: the box with garbage time out is the latter).
        Each has ``seconds`` / ``minutes`` on the floor and ``points``, ``fgm`` / ``fga``,
        ``fg3m`` / ``fg3a``, ``ftm`` / ``fta``, ``oreb`` / ``dreb`` / ``reb``, ``ast``, ``stl``,
        ``blk``, ``tov`` and ``pf``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_player_box_splits
            splits = nba_game_player_box_splits(2026)
            splits.filter(family="garbage", split="not_garbage_time").head()
    """
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    rows = nba_stint_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)
    out = game_player_box_splits(rows, nba_box_events(pbp, schedule), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_game_units(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """In-game units: per 1- to 5-man set of teammates and game, minutes, ratings, four factors and shot zones.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, n_players, lineup_id)``: ``opp_team_id``,
        ``n_players`` (1 to 5), ``lineup_id`` (the player ids sorted and joined with ``-``),
        ``player_1..5``, ``minutes`` / ``seconds``, ``poss`` / ``opp_poss``, ``pts`` / ``opp_pts``,
        ``plus_minus``, ``off_rtg`` / ``def_rtg`` / ``net_rtg`` (per 100 possessions), the four
        factors for (``efg_pct``, ``tov_pct``, ``orb_pct``, ``ft_rate``) and against (``opp_*``),
        the shot zones for and against (``rim_rate``, ``mid_rate``, ``fg3_rate`` shares of
        field-goal attempts, ``rim_pct``, ``mid_pct``, ``fg3_pct`` make rates, ``opp_*``) as
        fractions, and every rate's ``<m>_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or the possessions carry a null
            shot, free-throw, rebound or turnover count.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_units
            units = nba_game_units(2026)
            units.filter(game_id="0022500101", n_players=2).head()
    """
    pbp, game_lineups, possessions, schedule = _load_releases(seasons, league_id)
    rows = nba_impact_rows(pbp, game_lineups, possessions, schedule, league_id=league_id)
    out = game_units(rows, nba_lineup_count_rows(possessions, schedule, pbp=pbp), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out


def nba_game_on_off(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """In-game on/off: per player and game, the team with him on the floor and the rest of its game.

    The definitions are in :mod:`sportsdataverse._common.game_stints`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, game_id, team_id, player_id)``: ``opp_team_id``, for each side
        (``_on``, ``_off``) ``minutes``, ``poss``, ``opp_poss``, ``pts``, ``opp_pts``,
        ``plus_minus``, ``off_rtg``, ``def_rtg`` and ``net_rtg``; ``<rating>_diff`` (on minus off)
        and every rating's ``_n``. Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_game_stints import nba_game_on_off
            nba_game_on_off(2026).filter(game_id="0022500101")
    """
    out = game_on_off(nba_impact_rows(*_load_releases(seasons, league_id), league_id=league_id), player_dtype=_PLAYER)
    return out.to_pandas() if return_as_pandas else out
