"""Expected possession value (EPV) and EPV added for college play-by-play, men's and women's (BB-D24).

The states, labels, EPV added and credit are :mod:`sportsdataverse.nba.nba_epv`'s. This module
turns the ``ncaa_{mbb,wbb}_pbp`` release (stats.ncaa.org play-by-play with the possession stamped
on every row and the ten players on the floor) into the NBA engine's possessions and play-by-play,
then scores them with the college booster (``league_id`` ``"mbb"`` / ``"wbb"``).

**What the college feed carries, and how it is read:**

* Possessions are the release's ``poss_num``, split where the other side shoots (a shot or free throw
  by the team without the ball starts its possession). The rows trailing a possession's last live event
  (a shot, free throw, rebound, turnover, foul or jump ball) -- the steal after a turnover, the
  substitutions and timeouts at the change -- move to the next possession of the period, as the
  NBA engine files them.
* ``possession_start_type`` is the NBA engine's coarse vocabulary, read off the change:
  ``OffTimeout`` when a timeout comes before the possession's first live event,
  ``OffLiveBallTurnover`` when a steal does, ``OffMadeShot`` after a made shot or free throw,
  ``OffMissedShot`` after a player's defensive rebound, else ``OffDeadball`` (a period's first
  possession, a team or dead-ball rebound, a turnover without a steal, a foul or a jump ball).
* Shot zones: the feed has no shot distance, only ``is_paint``, so a two in the paint is the rim
  zone (1), any other two the long two zone (3); zone 2 (short) is never used.
* Free throws pending come from ``ft_number`` of ``ft_attempts``. The free throws after a
  technical carry no marker, so they count as an ordinary trip.
* Ids: ``game_id`` is the stats.ncaa.org ``contest_id``, ``team_id`` the ESPN team id (Int64)
  and ``player_id`` the stats.ncaa.org player id (a String), as in
  :mod:`sportsdataverse.mbb.mbb_game_impact`. A non-D1 opponent has no ESPN team id: its
  players, units and team get no row, but its possessions are scored and its points count.

Women's college played 20-minute halves before 2015-16; both boosters are fitted and served from
2021-22 on, in quarters (women) and halves (men), with the period read from the release.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

import polars as pl

from sportsdataverse.nba import nba_epv as E

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "mbb_epv_lineup_games",
    "mbb_epv_player_games",
    "mbb_epv_plays",
    "mbb_epv_team_games",
]

_LEAGUES: Final[dict[str, str]] = {"mens": "mbb", "womens": "wbb"}
_SHOTS: Final[tuple[str, ...]] = ("Three Point Jumper", "Layup", "Two Point Jumper", "Dunk", "Hook")
_REBOUNDS: Final[tuple[str, ...]] = ("Offensive Rebound", "Defensive Rebound", "Deadball Rebound")
#: The events a possession can end on (the NBA vocabulary); the rest trail it.
_LIVE_EVENTS: Final[tuple[str, ...]] = (
    "made_shot", "missed_shot", "free_throw", "rebound", "turnover", "foul", "jump_ball",
)  # fmt: skip
#: ``shot_distance`` stand-ins for the NBA zones (:data:`sportsdataverse.nba.nba_epv.SHOT_ZONES`).
_PAINT_FEET: Final[int] = 0
_LONG_TWO_FEET: Final[int] = 15
_PBP_COLS: Final[list[str]] = [
    "season", "contest_id", "game_date", "home", "away", "period", "clock", "event_team", "event_type",
    "event_result", "event_description", "shot_value", "poss_num", "poss_team", "home_score", "away_score",
    "is_paint", "ft_number", "ft_attempts", "is_shooting_foul", "foul_class", "player_1_id",
    "home_espn_team_id", "away_espn_team_id",
    *[f"{s}_{i}_player_id" for s in ("home", "away") for i in range(1, 6)],
]  # fmt: skip
_KEY: Final[list[str]] = ["game_id", "possession_number"]
_HOME: Final[int] = 1
_AWAY: Final[int] = 2


def _check_league(league: str) -> str:
    if league not in _LEAGUES:
        raise ValueError(f"league must be one of {list(_LEAGUES)}, got {league!r}")
    return _LEAGUES[league]


def _clock_seconds() -> pl.Expr:
    """``"MM:SS"`` (or ``"M:SS.s"``) left in the period, in seconds."""
    parts = pl.col("clock").cast(pl.Utf8).str.split(":")
    return parts.list.get(0, null_on_oob=True).cast(pl.Float64, strict=False) * 60 + parts.list.get(
        1, null_on_oob=True
    ).cast(pl.Float64, strict=False)


def _side(name: str) -> pl.Expr:
    """Home (1) or away (2) by the team name the row carries."""
    return pl.when(pl.col(name) == pl.col("home")).then(_HOME).when(pl.col(name) == pl.col("away")).then(_AWAY)


def _running_score(side: int) -> pl.Expr:
    """A side's score after each row, summed from its made baskets and free throws in release order.

    The release's ``home_score`` / ``away_score`` cannot be read as they are: in about one game in
    ten (612 of 6,293 men's games in 2024-25) ``home_score`` is the ``away`` team's, and now and
    then a basket's points land a row early. Summed from the events, a game's score matches the
    release's final in 99.8% of men's and 99.9% of women's games (the rest miss an event).
    A wrong-basket two counts for the other side.
    """
    et = pl.col("event_type")
    made = (et.is_in(_SHOTS) | (et == "Free Throw")) & (pl.col("event_result") == "made")
    wrong = et.str.starts_with("Wrong Basket")
    pts = pl.when(made & (_side("event_team") == side)).then(pl.col("shot_value"))
    pts = pts.when(wrong & (_side("event_team") == 3 - side)).then(2).otherwise(0)
    return pts.cast(pl.Int64).cum_sum().over("contest_id")


def _events(pbp: pl.DataFrame) -> pl.DataFrame:
    """The release rows in the NBA engine's play-by-play vocabulary, ``team_id`` the side (1 home, 2 away)."""
    et, made = pl.col("event_type"), pl.col("event_result") == "made"
    nba_type = (
        pl.when(et.is_in(_SHOTS) & made)
        .then(pl.lit("made_shot"))
        .when(et.is_in(_SHOTS))
        .then(pl.lit("missed_shot"))
        .when(et == "Free Throw")
        .then(pl.lit("free_throw"))
        .when(et.is_in(_REBOUNDS))
        .then(pl.lit("rebound"))
        .when(et == "Commits Foul")
        .then(pl.lit("foul"))
        .when(et == "Turnover")
        .then(pl.lit("turnover"))
        .when(et.str.contains("(?i)timeout"))
        .then(pl.lit("timeout"))
        .when(et.str.contains("(?i)jumpball"))
        .then(pl.lit("jump_ball"))
        .when(et.is_in(["Enters Game", "Leaves Game"]))
        .then(pl.lit("substitution"))
        .otherwise(pl.lit("other"))
    )
    foul = pl.col("foul_class").fill_null("")
    sub_type = (
        pl.when(et == "Free Throw")
        .then(pl.format("Free Throw {} of {}", pl.col("ft_number"), pl.col("ft_attempts")))
        .when((et == "Commits Foul") & foul.str.contains("(?i)technical"))
        .then(pl.lit("Technical"))
        .when((et == "Commits Foul") & (pl.col("is_shooting_foul") == True))
        .then(pl.lit("Shooting"))
        .when((et == "Commits Foul") & (foul == "offensive"))
        .then(pl.lit("Offensive"))
        .when(et == "Commits Foul")
        .then(pl.lit("Personal"))
    )
    player = pl.col("player_1_id").cast(pl.Int64, strict=False)
    shot_feet = (
        pl.when(~et.is_in(_SHOTS))
        .then(None)
        .when(pl.col("is_paint") == True)
        .then(_PAINT_FEET)
        .otherwise(_LONG_TWO_FEET)
    )
    return pbp.select(_PBP_COLS).with_columns(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.int_range(pl.len(), dtype=pl.Int64).over("contest_id").alias("order_index"),
        _side("event_team").cast(pl.Int64).alias("team_id"),
        _side("poss_team").cast(pl.Int64).alias("_poss_side"),
        nba_type.alias("_nba_type"),
        sub_type.alias("sub_type"),
        _clock_seconds().alias("seconds_remaining"),
        shot_feet.cast(pl.Int64).alias("shot_distance"),
        pl.col("shot_value").cast(pl.Int64),
        *[_running_score(side).alias(name) for side, name in ((_HOME, "score_home"), (_AWAY, "score_away"))],
        pl.when(et == "Steal")
        .then(pl.concat_str(pl.lit("STEAL "), pl.col("event_description").fill_null("")))
        .when(et == "Blocked Shot")
        .then(pl.concat_str(pl.lit("BLOCK "), pl.col("event_description").fill_null("")))
        .otherwise(pl.col("event_description"))
        .alias("description"),
        # a team rebound (no player, or a dead-ball one) carries the team in person_id, as the NBA feed does
        pl.when(et.is_in(_REBOUNDS) & (player.is_null() | (et == "Deadball Rebound")))
        .then(_side("event_team").cast(pl.Int64))
        .otherwise(player)
        .alias("person_id"),
    )


def _segments(ev: pl.DataFrame) -> pl.DataFrame:
    """``_seg`` (the game's possessions, numbered from 1) and ``_off_side`` (each one's offense).

    The release now and then stamps the other team's shots or free throws on a possession (the
    free throws after a foul on the turnover that ended it): a shot or free throw by the side
    without the ball starts that side's possession there.
    """
    shooter = pl.when(pl.col("_nba_type").is_in(["made_shot", "missed_shot", "free_throw"])).then(pl.col("team_id"))
    ev = ev.sort("game_id", "order_index").with_columns(
        shooter.forward_fill().over("game_id", "poss_num").fill_null(pl.col("_poss_side")).alias("_off_side")
    )
    new = (pl.col("_off_side") != pl.col("_off_side").shift(1).over("game_id")) | (
        pl.col("poss_num") != pl.col("poss_num").shift(1).over("game_id")
    )
    return ev.with_columns(new.fill_null(True).cast(pl.Int64).cum_sum().over("game_id").alias("_seg"))


def _regroup(ev: pl.DataFrame) -> pl.DataFrame:
    """Possessions as :func:`_segments`, the rows trailing each one's last live event moved to the next of the period."""
    ev = _segments(ev)
    live = pl.col("_nba_type").is_in(_LIVE_EVENTS)
    nxt = ev.group_by("game_id", "_seg").agg(pl.col("period").first().alias("_next_period"))
    return (
        ev.with_columns(pl.col("order_index").filter(live).max().over("game_id", "_seg").alias("_last_live"))
        .join(nxt.with_columns(pl.col("_seg") - 1), on=["game_id", "_seg"], how="left")
        .with_columns(
            pl.when((pl.col("order_index") > pl.col("_last_live")) & (pl.col("_next_period") == pl.col("period")))
            .then(pl.col("_seg") + 1)
            .otherwise(pl.col("_seg"))
            .cast(pl.Int64)
            .alias("possession_number")
        )
        .sort("game_id", "order_index")
    )


def _start_types(ev: pl.DataFrame, poss: pl.DataFrame) -> pl.DataFrame:
    """``poss`` with the coarse NBA ``possession_start_type`` of each possession (see the module docstring)."""
    live = pl.col("_nba_type").is_in(_LIVE_EVENTS)
    first_live = pl.col("order_index").filter(live).min().over(_KEY)
    lead = ev.with_columns(first_live.fill_null(pl.col("order_index").max().over(_KEY) + 1).alias("_first_live"))
    lead = (
        lead.filter(pl.col("order_index") < pl.col("_first_live"))
        .group_by(_KEY)
        .agg(
            (pl.col("_nba_type") == "timeout").any().alias("_lead_timeout"),
            (pl.col("event_type") == "Steal").any().alias("_lead_steal"),
        )
    )
    last = ev.join(
        poss.select("game_id", pl.col("end_order_index").alias("order_index"), "possession_number"),
        on=["game_id", "order_index", "possession_number"],
        how="semi",
    ).select(
        "game_id",
        (pl.col("possession_number") + 1).alias("possession_number"),
        pl.col("_nba_type").alias("_prev_type"),
        (pl.col("event_result") == "made").alias("_prev_made"),
        (pl.col("event_type") == "Defensive Rebound").alias("_prev_dreb"),
        (pl.col("person_id") != pl.col("team_id")).fill_null(False).alias("_prev_player"),
    )
    return (
        poss.join(lead, on=_KEY, how="left")
        .join(last, on=_KEY, how="left")
        .sort(_KEY)
        .with_columns(
            pl.when(pl.col("number_in_period") == 1)
            .then(pl.lit("OffDeadball"))
            .when(pl.col("_lead_timeout") == True)
            .then(pl.lit("OffTimeout"))
            .when(pl.col("_lead_steal") == True)
            .then(pl.lit("OffLiveBallTurnover"))
            .when(pl.col("_prev_type").is_in(["made_shot", "free_throw"]) & (pl.col("_prev_made") == True))
            .then(pl.lit("OffMadeShot"))
            .when((pl.col("_prev_dreb") == True) & (pl.col("_prev_player") == True))
            .then(pl.lit("OffMissedShot"))
            .otherwise(pl.lit("OffDeadball"))
            .alias("possession_start_type")
        )
        .drop("_lead_timeout", "_lead_steal", "_prev_type", "_prev_made", "_prev_dreb", "_prev_player")
    )


def _possessions(ev: pl.DataFrame) -> pl.DataFrame:
    """One row per possession in the NBA engine's possessions schema."""
    first = pl.col("order_index").min()
    home = [
        pl.col(f"home_{i}_player_id").cast(pl.Int64, strict=False).sort_by("order_index").first() for i in range(1, 6)
    ]
    away = [
        pl.col(f"away_{i}_player_id").cast(pl.Int64, strict=False).sort_by("order_index").first() for i in range(1, 6)
    ]
    poss = (
        ev.group_by(_KEY)
        .agg(
            pl.col("period").sort_by("order_index").first(),
            first.alias("start_order_index"),
            pl.col("order_index").max().alias("end_order_index"),
            pl.col("seconds_remaining").sort_by("order_index").first().alias("start_seconds_remaining"),
            pl.col("seconds_remaining").sort_by("order_index").last().alias("end_seconds_remaining"),
            # the offense the release stamped on the possession's own rows (not the trailing ones moved in)
            pl.col("_off_side")
            .filter(pl.col("_seg") == pl.col("possession_number"))
            .drop_nulls()
            .first()
            .alias("_poss_side"),
            *[h.alias(f"_home_{i}") for i, h in enumerate(home, 1)],
            *[a.alias(f"_away_{i}") for i, a in enumerate(away, 1)],
        )
        .sort(_KEY)
        .with_columns(pl.int_range(1, pl.len() + 1, dtype=pl.Int64).over("game_id", "period").alias("number_in_period"))
    )
    off_home = pl.col("_poss_side") == _HOME
    poss = poss.with_columns(
        pl.col("_poss_side").alias("offense_team_id"),
        (3 - pl.col("_poss_side")).alias("defense_team_id"),
        *[
            pl.when(off_home).then(pl.col(f"_home_{i}")).otherwise(pl.col(f"_away_{i}")).alias(f"off_player_{i}")
            for i in range(1, 6)
        ],
        *[
            pl.when(off_home).then(pl.col(f"_away_{i}")).otherwise(pl.col(f"_home_{i}")).alias(f"def_player_{i}")
            for i in range(1, 6)
        ],
    )
    return _start_types(ev, poss).drop("_poss_side", *[f"_{s}_{i}" for s in ("home", "away") for i in range(1, 6)])


def ncaa_epv_inputs(pbp: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The NBA engine's inputs from the ``ncaa_{mbb,wbb}_pbp`` release.

    Args:
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows, in release (game) order.

    Returns:
        ``(possessions, pbp, game_lineups, games)``: the possessions and play-by-play
        :func:`sportsdataverse.nba.nba_epv.epv_states` reads (team ids are the side, 1 home and
        2 away; player ids the stats.ncaa.org ids as Int64), the ten on the floor at each row
        (``game_id``, ``action_number``, ``home_player_1..5``, ``away_player_1..5``), and one row
        per game with ``game_order`` (its date) and the sides' ESPN team ids (``home_espn_team_id``,
        ``away_espn_team_id``).

    Raises:
        ValueError: If a needed column is missing.
    """
    missing = [c for c in _PBP_COLS if c not in pbp.columns]
    if missing:
        raise ValueError(f"missing columns {missing}")
    ev = _regroup(_events(pbp.filter(pl.col("poss_num").is_not_null())))
    poss = _possessions(ev).filter(pl.col("offense_team_id").is_not_null())
    ev = ev.join(poss.select(_KEY), on=_KEY, how="semi")
    out = ev.with_columns(
        pl.col("order_index").alias("action_number"),
        pl.col("_nba_type").alias("event_type"),
        pl.when(pl.col("team_id") == _HOME)
        .then(pl.lit("h"))
        .when(pl.col("team_id") == _AWAY)
        .then(pl.lit("v"))
        .alias("location"),
    )
    tens = out.select(
        "game_id",
        "action_number",
        *[
            pl.col(f"{s}_{i}_player_id").cast(pl.Int64, strict=False).alias(f"{s}_player_{i}")
            for s in ("home", "away")
            for i in range(1, 6)
        ],
    )
    games = out.group_by("game_id").agg(
        pl.col("season").first(),
        pl.col("game_date").drop_nulls().first().str.to_date("%m/%d/%Y", strict=False).alias("game_order"),
        pl.col("home_espn_team_id").drop_nulls().first().cast(pl.Int64),
        pl.col("away_espn_team_id").drop_nulls().first().cast(pl.Int64),
    )
    return poss, out, tens, games


def _espn(frame: pl.DataFrame, games: pl.DataFrame, cols: list[str]) -> pl.DataFrame:
    """Each side column (1 home, 2 away) as the side's ESPN team id."""
    g = games.select("game_id", "home_espn_team_id", "away_espn_team_id")
    return (
        frame.join(g, on="game_id", how="left")
        .with_columns(
            pl.when(pl.col(c) == _HOME)
            .then(pl.col("home_espn_team_id"))
            .when(pl.col(c) == _AWAY)
            .then(pl.col("away_espn_team_id"))
            .alias(c)
            for c in cols
        )
        .drop("home_espn_team_id", "away_espn_team_id")
    )


def _load(seasons: int | list[int], league: str) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """The scored event rows of ``seasons`` (sides as team ids), the game lineups and the games."""
    lg = _check_league(league)
    seasons = E._check_seasons(seasons, lg)
    if lg == "wbb":
        from sportsdataverse.wbb.wbb_loaders import load_ncaa_wbb_pbp as load_pbp
    else:
        from sportsdataverse.mbb.mbb_loaders import load_ncaa_mbb_pbp as load_pbp
    poss, pbp, tens, games = ncaa_epv_inputs(load_pbp(seasons))
    plays = E.add_epv(E.epv_states(poss, pbp), league_id=lg, game_order=games.select("game_id", "game_order"))
    return plays, tens, games


def _out(frame: pl.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    return frame.to_pandas() if return_as_pandas else frame


def mbb_epv_plays(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """EPV and EPV added on every college play-by-play event inside a possession.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_epv.wbb_epv_plays`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        The columns of :func:`sportsdataverse.nba.nba_epv.nba_epv_plays`, one row per event in game
        order. ``game_id`` is the NCAA contest id; the team ids are ESPN ids (Int64, null for a
        non-D1 team); ``person_id`` and the player slots are stats.ncaa.org player ids (String,
        null on a team event); ``event_type`` is the NBA engine's vocabulary.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or a season is before the
            first one served.

    Example:
        The biggest plays of a game::

            from sportsdataverse.mbb.mbb_epv import mbb_epv_plays
            plays = mbb_epv_plays(2026)
            plays.sort("epv_added", descending=True).head(5)
    """
    plays, _, games = _load(seasons, league)
    team_cols = ["offense_team_id", "defense_team_id", "home_team_id", "team_id"]
    is_team = pl.col("person_id").is_in([_HOME, _AWAY])
    out = _espn(
        plays.with_columns(
            pl.when(is_team).then(None).otherwise(pl.col("person_id")).cast(pl.Utf8).alias("person_id"),
            *[pl.col(c).cast(pl.Utf8) for c in E._SLOTS],
        ),
        games,
        team_cols,
    )
    return _out(out, return_as_pandas)


def _ids(frame: pl.DataFrame, games: pl.DataFrame, team_cols: list[str]) -> pl.DataFrame:
    """ESPN team ids, String player ids, and no rows for a team without an ESPN id."""
    out = _espn(frame, games, team_cols)
    if "player_id" in out.columns:
        out = out.with_columns(pl.col("player_id").cast(pl.Utf8))
    return out.filter(pl.col("team_id").is_not_null())


def mbb_epv_player_games(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Per college player and game: EPV added on the player's own actions and on-court EPV +/-.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_epv.wbb_epv_player_games`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_player_games` of the seasons' plays, on-court from
        the ten the release puts on each row: one row per ``(season, game_id, team_id,
        player_id)`` with ``epv_own``, ``own_actions``, ``off_epv_on``, ``def_epv_on`` and
        ``epv_on``. ``team_id`` is the ESPN id (Int64), ``player_id`` the stats.ncaa.org id
        (String); a non-D1 team's players get no row.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or a season is before the
            first one served.

    Example:
        A game's EPV box::

            from sportsdataverse.mbb.mbb_epv import mbb_epv_player_games
            box = mbb_epv_player_games(2026)
            box.sort("epv_on", descending=True).head()
    """
    plays, tens, games = _load(seasons, league)
    return _out(_ids(E.epv_player_games(plays, tens), games, ["team_id"]), return_as_pandas)


def mbb_epv_lineup_games(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Per college five-man unit and game: the EPV added with the unit on the floor.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_epv.wbb_epv_lineup_games`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_lineup_games` of the seasons' plays: one row per
        ``(season, game_id, team_id, lineup_id)`` with ``off_epv``, ``def_epv``, ``net_epv`` and
        ``plays``. ``lineup_id`` joins the five stats.ncaa.org player ids with ``-``; a non-D1
        team's units get no row.

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or a season is before the
            first one served.

    Example:
        A season's units::

            from sportsdataverse.mbb.mbb_epv import mbb_epv_lineup_games
            units = mbb_epv_lineup_games(2026)
            units.sort("net_epv", descending=True).head()
    """
    plays, tens, games = _load(seasons, league)
    return _out(_ids(E.epv_lineup_games(plays, tens), games, ["team_id"]), return_as_pandas)


def mbb_epv_team_games(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Per college team and game: EPV added on offense and allowed on defense.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26),
            from the booster's first training season (2022) on.
        league: ``"mens"`` or ``"womens"`` (:func:`sportsdataverse.wbb.wbb_epv.wbb_epv_team_games`).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        :func:`sportsdataverse.nba.nba_epv.epv_team_games` of the seasons' plays: one row per
        ``(season, game_id, team_id)`` with ``opp_team_id``, ``off_epv``, ``def_epv``, ``net_epv``,
        ``poss``, ``opp_poss``, ``off_epv_per_poss`` and ``def_epv_per_poss``. ESPN team ids; a
        non-D1 team gets no row (``opp_team_id`` is null against one).

    Raises:
        ValueError: If ``league`` is not ``"mens"`` or ``"womens"``, or a season is before the
            first one served.

    Example:
        A season's offenses by EPV added per possession::

            import polars as pl
            from sportsdataverse.mbb.mbb_epv import mbb_epv_team_games
            teams = mbb_epv_team_games(2026)
            teams.group_by("team_id").agg(pl.col("off_epv_per_poss").mean()).sort("off_epv_per_poss")
    """
    plays, _, games = _load(seasons, league)
    return _out(_ids(E.epv_team_games(plays), games, ["team_id", "opp_team_id"]), return_as_pandas)
