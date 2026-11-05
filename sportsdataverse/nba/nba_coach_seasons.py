"""NBA / WNBA coach seasons and careers (BB-D8): ``nba.coach_seasons`` / ``nba.coach_careers``.

One row per head coach and team-season, with the W-L, points, ratings and four factors of the
games he coached; careers sum them. The crediting rule is in
:mod:`sportsdataverse._common.coach_seasons`. ``league_id="10"`` reads the WNBA releases;
:mod:`sportsdataverse.wnba.wnba_coach_seasons` binds it.

**Who coached.** ``nba_stats_coaches`` / ``wnba_stats_coaches`` (stats.nba.com's
``commonteamroster`` staff) name the head coaches, and their ``coach_id`` is the id here. A
release file is the staff as it stood when the season was last captured, which is after the
season, so it already shows the next season's hires: the file stamped 2021 (the 2020-21 season)
lists Ime Udoka at Boston, hired in June 2021. Against Basketball-Reference's per-coach game
counts for 2016-2026, crediting each file's coach with its own season gets 60-97% of games right,
season by season. So each team-season takes two candidates: the head coach of the file before it
(the staff after the previous season, i.e. this season's opener) and of its own file (the staff
after this season: the closer, or next season's hire).

* The same coach in both: he coached the whole season.
* Different coaches: ESPN's per-coach regular-season record (the core API's
  ``seasons/{season}/coaches`` records) gives the game count of whichever candidate it lists, and
  the stints are placed as runs of the team's games in date order whose wins match, the opener's
  at the start and the closer's at the end. ESPN lists one coach per team-season at most. When it
  counts the closer, the opener fills the games before him; when it counts the opener, the games
  after him go to nobody, because the closer without a count of his own is too often next
  season's hire (every such gap in the 2016-2026 fixtures was: John Beilein would have had Larry
  Drew's 76 games of 2018-19). A team-season ESPN counts neither candidate for stays uncredited:
  the change could have come in the season or after it.

ESPN's coach records are used only for a coach stats.nba.com names for that team and season:
ESPN also lists, under a team, coaches who were its assistants then (Darko Rajakovic under the
2019 Thunder). ``espn=False`` skips ESPN and credits only the one-coach seasons.

Against Basketball-Reference for 2016-2026 this credits 77% of the NBA regular-season games
(19,967 of 25,926 scored) and 99.2% of those to the right coach; per coach-season, 250 of 260
have BBRef's games and W-L exactly and 4 more are a game apart. The misses: a coach on leave
whom the staff still lists (Chauncey Billups, 2025-26 Portland), ESPN counting an assistant
under his later team (Tyronn Lue's 2 games of the 2019-20 Clippers), and Dave / David Joerger,
the same man. The WNBA's staff files are sparse (none for 2019, five head coaches for 2024):
2016-2025 credits 55% of games (2,246 of 4,108), all to the right coach.

Playoff and play-in games go to the coach of the team's last regular-season game.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._codegen_runtime import _get
from sportsdataverse._common.coach_seasons import (
    COACH_GAMES_SCHEMA,
    STINTS_SCHEMA,
    TEAM_GAMES_SCHEMA,
    coach_careers,
    coach_of_record,
    coach_seasons,
)
from sportsdataverse.errors import NoDataError
from sportsdataverse.nba.nba_team_season_rates import _check_args, _league_loaders, nba_possession_rate_rows

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_coach_careers", "nba_coach_seasons"]

logger = logging.getLogger(__name__)

_ESPN_LEAGUE = {"00": "nba", "10": "wnba"}
_MINUTES = {"00": 48, "10": 40}
_REGULAR = "regular-season"

#: ESPN coach records: one row per (season, ESPN team, coach) with the regular-season W-L.
ESPN_RECORDS_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "espn_team_id": pl.Int64(),
    "coach": pl.Utf8(),
    "wins": pl.Int64(),
    "losses": pl.Int64(),
}

#: The staff rows a stats.nba.com release names, one head coach per (file season, team).
_HEAD_COACHES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Int64(),
    "coach_id": pl.Int64(),
    "coach": pl.Utf8(),
}


def _name_key(col: str) -> pl.Expr:
    """A name compared letters-only and case-folded (``J.B. Bickerstaff`` = ``JB Bickerstaff``)."""
    return pl.col(col).str.to_lowercase().str.replace_all(r"[^a-z]", "")


def nba_team_games(schedule: pl.DataFrame, *, season_type: str = _REGULAR) -> pl.DataFrame:
    """Two rows per completed game of ``season_type``, with its date and the team abbreviation.

    Args:
        schedule: ``load_nba_stats_schedules`` / ``load_wnba_stats_schedules`` rows.
        season_type: The schedule ``season_type`` to keep.

    Returns:
        Completed games in :data:`~sportsdataverse._common.coach_seasons.TEAM_GAMES_SCHEMA`.
    """
    if schedule.height == 0:
        return pl.DataFrame(schema=TEAM_GAMES_SCHEMA)
    done = schedule.filter(
        (pl.col("season_type") == season_type) & pl.col("home_pts").is_not_null() & pl.col("away_pts").is_not_null()
    )
    day = pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d")
    return pl.concat(
        [
            done.select(
                "season",
                pl.col("game_id").cast(pl.Utf8),
                day.alias("game_date"),
                pl.col(f"{us}_team_id").alias("team_id"),
                pl.col(f"{us}_team_abbreviation").alias("team"),
                pl.col(f"{them}_team_id").alias("opp_team_id"),
                pl.col(f"{us}_pts").alias("pts"),
                pl.col(f"{them}_pts").alias("opp_pts"),
            ).cast(TEAM_GAMES_SCHEMA)
            for us, them in (("home", "away"), ("away", "home"))
        ]
    )


def head_coaches(staff: pl.DataFrame) -> pl.DataFrame:
    """The head coach of each (file season, team) of a ``*_stats_coaches`` release.

    Args:
        staff: ``load_nba_stats_coaches`` / ``load_wnba_stats_coaches`` rows.

    Returns:
        One row per (season, team_id) with ``coach_id`` and ``coach``; the first by
        ``sort_sequence`` when a file lists two head coaches.
    """
    if staff.height == 0:
        return pl.DataFrame(schema=_HEAD_COACHES_SCHEMA)
    return (
        staff.filter(pl.col("coach_type") == "Head Coach")
        .sort("season", "team_id", "sort_sequence", "coach_id", nulls_last=True)
        .unique(["season", "team_id"], keep="first", maintain_order=True)
        .select("season", "team_id", "coach_id", pl.col("coach_name").alias("coach"))
        .cast(_HEAD_COACHES_SCHEMA)
    )


def nba_coach_stints(heads: pl.DataFrame, espn: pl.DataFrame, crosswalk: pl.DataFrame) -> pl.DataFrame:
    """Each team-season's candidate head coaches, with ESPN's counts where ESPN lists one of them.

    Args:
        heads: :func:`head_coaches` rows, covering the season before each wanted season too.
        espn: ESPN coach records in :data:`ESPN_RECORDS_SCHEMA` (may be empty).
        crosswalk: ``(season, espn_team_id, team_id)`` rows mapping ESPN teams to stats.nba.com.

    Returns:
        Stints in :data:`~sportsdataverse._common.coach_seasons.STINTS_SCHEMA`. The opener (the
        head coach of the file before; ``position="first"``) without counts; the closer (of the
        season's own file; ``position="last"``) only with ESPN's count for him; one coach who is
        both, ``position="any"``. A candidate ESPN lists gets his games and wins. ESPN records that
        name no candidate are dropped, and counted in a log line.
    """
    heads = heads.select(list(_HEAD_COACHES_SCHEMA)).cast(_HEAD_COACHES_SCHEMA)
    cands = pl.concat(
        [
            heads.with_columns(pl.col("season") + 1, pl.lit("first").alias("position")),
            heads.with_columns(pl.lit("last").alias("position")),
        ]
    ).with_columns(_name_key("coach").alias("_name"))
    xw = crosswalk.select(
        pl.col("season").cast(pl.Int64), pl.col("espn_team_id").cast(pl.Int64), pl.col("team_id").cast(pl.Int64)
    )
    rec = (
        espn.select(list(ESPN_RECORDS_SCHEMA))
        .cast(ESPN_RECORDS_SCHEMA)
        .join(xw.unique(["season", "espn_team_id"]), on=["season", "espn_team_id"], how="inner", validate="m:1")
        .with_columns(_name_key("coach").alias("_name"))
    )
    # ESPN's count, only for a coach stats.nba.com names for that team-season, at his position there
    named = cands.group_by("season", "team_id", "_name").agg(
        pl.col("coach_id").first(),
        pl.col("coach").first(),
        pl.when(pl.col("position").n_unique() > 1)
        .then(pl.lit("any"))
        .otherwise(pl.col("position").first())
        .alias("position"),
    )
    unmatched = rec.join(named, on=["season", "team_id", "_name"], how="anti").height
    if unmatched:
        logger.info("nba_coach_stints: %d ESPN coach records name no stats.nba.com candidate (dropped)", unmatched)
    counted = (
        named.join(
            rec.select("season", "team_id", "_name", "wins", "losses"), on=["season", "team_id", "_name"], how="inner"
        )
        .unique(["season", "team_id", "_name"], keep="first")
        .select(
            "season",
            "team_id",
            "coach_id",
            "coach",
            (pl.col("wins") + pl.col("losses")).alias("games"),
            "wins",
            "position",
            "_name",
        )
    )
    # a closer with no count of his own never fills a gap: the staff after the season is often
    # next season's hire (John Beilein for Larry Drew's 76 games, 2018-19 Cleveland)
    rest = named.join(
        counted.select("season", "team_id", "_name"), on=["season", "team_id", "_name"], how="anti"
    ).filter(pl.col("position") != "last")
    out = pl.concat(
        [
            counted.drop("_name"),
            rest.drop("_name").with_columns(
                pl.lit(None, dtype=pl.Int64).alias("games"), pl.lit(None, dtype=pl.Int64).alias("wins")
            ),
        ],
        how="diagonal",
    )
    return out.select(list(STINTS_SCHEMA)).cast(STINTS_SCHEMA).sort("season", "team_id", "position")


def espn_coach_records(season: int, *, league_id: str = "00") -> pl.DataFrame:
    """ESPN's regular-season record of each coach it lists for ``season`` (core API).

    One request for the season's coach list, then one per coach and one per record; about 90 for
    an NBA season.

    Args:
        season: The season, named by the year it ends (the WNBA's calendar year).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.

    Returns:
        Rows in :data:`ESPN_RECORDS_SCHEMA`; empty when ESPN has no coach list for the season.
    """
    base = f"https://sports.core.api.espn.com/v2/sports/basketball/leagues/{_ESPN_LEAGUE[league_id]}"
    try:
        listing = _get(f"{base}/seasons/{season}/coaches", params={"limit": 1000})
    except NoDataError:
        return pl.DataFrame(schema=ESPN_RECORDS_SCHEMA)
    rows = []
    for item in listing.get("items") or []:
        coach = _get(item["$ref"])
        name = " ".join(p for p in (coach.get("firstName"), coach.get("lastName")) if p)
        for rec in coach.get("records") or []:
            team_ref = (rec.get("team") or {}).get("$ref") or ""
            rec_ref = (rec.get("record") or {}).get("$ref") or ""
            if "/teams/" not in team_ref or "/types/2/" not in rec_ref:  # the regular season only
                continue
            stats = {s.get("name"): s.get("value") for s in (_get(rec_ref).get("stats") or [])}
            team = team_ref.split("/teams/")[1].split("?")[0]
            rows.append(
                {
                    "season": season,
                    "espn_team_id": int(team),
                    "coach": name,
                    "wins": stats.get("wins"),
                    "losses": stats.get("losses"),
                }
            )
    return (
        pl.DataFrame(rows, schema=ESPN_RECORDS_SCHEMA, orient="row")
        if rows
        else pl.DataFrame(schema=ESPN_RECORDS_SCHEMA)
    )


def carry_last_coach(games: pl.DataFrame, regular: pl.DataFrame, credit: pl.DataFrame) -> pl.DataFrame:
    """Credit every game of ``games`` to the coach of the team's last regular-season game.

    Args:
        games: Postseason games in :data:`~sportsdataverse._common.coach_seasons.TEAM_GAMES_SCHEMA`.
        regular: The same seasons' regular-season games (same schema).
        credit: :func:`~sportsdataverse._common.coach_seasons.coach_of_record` output for ``regular``.

    Returns:
        ``games`` credited, in :data:`~sportsdataverse._common.coach_seasons.COACH_GAMES_SCHEMA`. A
        team whose last regular-season game is uncredited gets none: an earlier credited game's
        coach may have left by then.
    """
    final = regular.sort("game_date", "game_id").group_by("season", "team_id").agg(pl.col("game_id").last())
    last = final.join(
        credit.select("game_id", "team_id", "coach_id", "coach"), on=["game_id", "team_id"], how="inner", validate="1:1"
    ).drop("game_id")
    return (
        games.join(last, on=["season", "team_id"], how="inner", validate="m:1")
        .select(list(COACH_GAMES_SCHEMA))
        .cast(COACH_GAMES_SCHEMA)
    )


def _crosswalk(seasons: list[int], league_id: str) -> pl.DataFrame:
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_team_crosswalk

        xw = load_wnba_team_crosswalk(seasons)
        return xw.select("season", "espn_team_id", pl.col("wnba_team_id").alias("team_id")) if xw.height else xw
    from sportsdataverse.nba.nba_loaders import load_nba_team_crosswalk

    xw = load_nba_team_crosswalk(seasons)
    return xw.select("season", "espn_team_id", pl.col("nba_team_id").alias("team_id")) if xw.height else xw


def _staff_loader(league_id: str):  # type: ignore[no-untyped-def]
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_stats_coaches

        return load_wnba_stats_coaches
    from sportsdataverse.nba.nba_loaders import load_nba_stats_coaches

    return load_nba_stats_coaches


@overload
def nba_coach_seasons(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = _REGULAR,
    espn: bool = True,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_coach_seasons(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = _REGULAR,
    espn: bool = True,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_coach_seasons(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = _REGULAR,
    espn: bool = True,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Head-coach seasons: W-L, ratings and four factors over the games each coach was on the bench for.

    One row per season, team and head coach. A coach who took over mid-season gets his games
    only. Who coached each game: see :mod:`sportsdataverse.nba.nba_coach_seasons`.

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: ``"regular-season"`` (default), ``"playoffs"`` or ``"play-in"``; the
            postseason games go to the coach of the team's last regular-season game.
        espn: Read ESPN's per-coach records to split a team-season that changed coach (about 90
            requests a season). ``False`` credits only the seasons one coach had throughout.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, team_id, coach_id)``: ``coach_id`` is the stats.nba.com coach id,
        ``team`` the team abbreviation, ``role`` ``"HC"``. ``team_games`` is the team's completed
        games of ``season_type``, ``games`` / ``wins`` / ``losses`` / ``win_pct`` / ``pts`` /
        ``opp_pts`` the coach's (final scores), ``first_game_date`` / ``last_game_date`` his
        span. ``pbp_games``, ``poss``, ``opp_poss``, ``poss_pts``, ``opp_poss_pts``, ``seconds``
        and the ``o_*`` / ``d_*`` four-factor counts come from his games' possessions, and give
        ``off_rtg``, ``def_rtg``, ``net_rtg``, ``pace`` and ``efg_pct`` .. ``opp_ft_rate``.
        ``win_pct`` and the twelve rates have ``<m>_rank`` (1 = best), ``<m>_pct`` and ``<m>_n``,
        within the season among coaches with half the most games; null under five coaches.
        Empty input returns that schema with zero rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"``, or ``season_type`` is not one of
            the three above (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_coach_seasons import nba_coach_seasons
            coaches = nba_coach_seasons(2026)
            coaches.sort("net_rtg", descending=True).select("coach", "team", "games", "wins", "net_rtg").head()

        Without ESPN (only the coaches who had the whole season)::

            nba_coach_seasons(2026, espn=False)
    """
    _check_args(league_id, season_type)
    if season_type == "preseason":
        raise ValueError("season_type: preseason has no coach of record; use regular-season, playoffs or play-in")
    wanted = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    schedule = load_schedule([s - offset for s in wanted])
    staff = _staff_loader(league_id)(sorted({s - offset for s in wanted} | {s - 1 - offset for s in wanted}))
    records = (
        pl.concat([espn_coach_records(s, league_id=league_id) for s in wanted])
        if espn
        else pl.DataFrame(schema=ESPN_RECORDS_SCHEMA)
    )
    xw = (
        _crosswalk(wanted, league_id)
        if espn
        else pl.DataFrame(schema={"season": pl.Int64, "espn_team_id": pl.Int64, "team_id": pl.Int64})
    )
    stints = nba_coach_stints(head_coaches(staff), records, xw).filter(pl.col("season").is_in(wanted))
    regular = nba_team_games(schedule)
    credit = coach_of_record(regular, stints)
    games = regular
    if season_type != _REGULAR:
        games = nba_team_games(schedule, season_type=season_type)
        credit = carry_last_coach(games, regular, credit)
    rows = nba_possession_rate_rows(
        load_possessions([s - offset for s in wanted]), schedule, league_id=league_id, season_type=season_type
    )
    out = coach_seasons(games, credit, rows, minutes_per_game=_MINUTES[league_id])
    return out.to_pandas() if return_as_pandas else out


def nba_coach_careers(
    seasons: int | list[int],
    *,
    league_id: str = "00",
    season_type: str = _REGULAR,
    espn: bool = True,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Head-coach careers over ``seasons``: :func:`nba_coach_seasons` summed per coach, rates recomputed.

    Args:
        seasons: The seasons the careers cover (a career counts only these).
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        season_type: As :func:`nba_coach_seasons`.
        espn: As :func:`nba_coach_seasons`.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``coach_id``, most games first: ``teams`` (abbreviations, first to last),
        ``seasons``, ``first_season``, ``last_season``, the summed record and counts and the
        recomputed rates, with ranks among coaches with at least a full season of games. To
        build careers from coach seasons already in hand, call
        :func:`~sportsdataverse._common.coach_seasons.coach_careers` on them.

    Raises:
        ValueError: As :func:`nba_coach_seasons`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_coach_seasons import nba_coach_careers
            nba_coach_careers(list(range(2016, 2027))).select("coach", "teams", "games", "win_pct").head()
    """
    rows = nba_coach_seasons(seasons, league_id=league_id, season_type=season_type, espn=espn)
    out = coach_careers(rows, minutes_per_game=_MINUTES[league_id])
    return out.to_pandas() if return_as_pandas else out
