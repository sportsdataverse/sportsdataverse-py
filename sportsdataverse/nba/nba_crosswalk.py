"""NBA cross-source identity crosswalks (ESPN / NBA Stats API / Fox Sports).

Port of ``hoopR/R/nba_crosswalk.R``. ESPN is the anchor; the NBA Stats API is
joined on ``espn_team_id`` (the Stats team directory already carries the ESPN
linkage) and Fox on the normalized team name. Yahoo columns are null
placeholders.

Public surface:

* :func:`nba_team_crosswalk` -- ESPN x Stats x Fox team-id crosswalk.
* :func:`nba_schedule_crosswalk` -- ESPN x Stats game-id crosswalk.
* :func:`nba_player_crosswalk` -- ESPN x Stats x Fox player-id crosswalk.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._common_crosswalk_basketball import (
    assemble_player_espn_stats_fox,
    normalize_team,
    str_id,
)
from sportsdataverse._crosswalk_basketball_sources import (
    espn_scoreboard_games,
    espn_team_directory,
    require_source,
)

__all__ = [
    "nba_team_crosswalk",
    "nba_schedule_crosswalk",
    "nba_player_crosswalk",
]

TEAM_COLUMNS = [
    "season",
    "espn_team_id",
    "espn_abbreviation",
    "espn_display_name",
    "espn_short_name",
    "espn_location",
    "espn_mascot",
    "nba_team_id",
    "nba_team_abbreviation",
    "nba_team_name",
    "nba_team_city",
    "nba_team_slug",
    "nba_conference",
    "nba_division",
    "fox_team_id",
    "fox_team_name",
    "yahoo_team_id",
    "yahoo_team_abbreviation",
    "yahoo_team_name",
    "match_method",
    "match_confidence",
]

SCHEDULE_COLUMNS = [
    "season",
    "season_type",
    "game_date",
    "home_espn_team_id",
    "away_espn_team_id",
    "espn_game_id",
    "nba_game_id",
    "nba_game_code",
    "nba_home_team_id",
    "nba_away_team_id",
    "fox_game_id",
    "fox_home_team_id",
    "fox_away_team_id",
    "yahoo_game_id",
    "match_method",
    "match_confidence",
]

_STATS_SCHEMA = {
    "espn_team_id": pl.Int32,
    "nba_team_id": pl.Utf8,
    "nba_team_abbreviation": pl.Utf8,
    "nba_team_name": pl.Utf8,
    "nba_team_city": pl.Utf8,
    "nba_team_slug": pl.Utf8,
    "nba_conference": pl.Utf8,
    "nba_division": pl.Utf8,
}


def _assemble_team_crosswalk(
    espn: pl.DataFrame,
    stats: Optional[pl.DataFrame],
    fox: Optional[pl.DataFrame],
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`nba_team_crosswalk` (no network).

    Port of ``.bb_assemble_team_crosswalk_nba``. Stats joins on
    ``espn_team_id``; Fox joins on the normalized team name.

    Args:
        espn: ESPN team directory (``team_id``, ``abbreviation``,
            ``display_name``, ``short_name``, ``team``, ``mascot``).
        stats: NBA Stats team directory with :data:`_STATS_SCHEMA` columns;
            may be ``None``/empty.
        fox: Fox directory with ``fox_team_id`` / ``fox_team_name``; may be
            ``None``/empty.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`TEAM_COLUMNS`.
    """
    espn2 = espn.select(
        pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
        pl.col("abbreviation").cast(pl.Utf8).alias("espn_abbreviation"),
        pl.col("display_name").cast(pl.Utf8).alias("espn_display_name"),
        pl.col("short_name").cast(pl.Utf8).alias("espn_short_name"),
        pl.col("team").cast(pl.Utf8).alias("espn_location"),
        pl.col("mascot").cast(pl.Utf8).alias("espn_mascot"),
    ).with_columns(pl.Series("team_key", [normalize_team(v) for v in espn["display_name"].to_list()], dtype=pl.Utf8))

    if stats is None or stats.height == 0:
        stats2 = pl.DataFrame(schema=_STATS_SCHEMA)
    else:
        stats2 = stats.select(
            pl.col("espn_team_id").cast(pl.Int32),
            str_id(stats, "nba_team_id"),
            pl.col("nba_team_abbreviation").cast(pl.Utf8),
            pl.col("nba_team_name").cast(pl.Utf8),
            pl.col("nba_team_city").cast(pl.Utf8),
            pl.col("nba_team_slug").cast(pl.Utf8),
            pl.col("nba_conference").cast(pl.Utf8),
            pl.col("nba_division").cast(pl.Utf8),
        )

    if fox is None or fox.height == 0:
        fox2 = pl.DataFrame(schema={"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "team_key": pl.Utf8})
    else:
        fox2 = fox.select(
            str_id(fox, "fox_team_id"),
            pl.col("fox_team_name").cast(pl.Utf8),
        ).with_columns(
            pl.Series("team_key", [normalize_team(v) for v in fox["fox_team_name"].to_list()], dtype=pl.Utf8)
        )

    matched = pl.col("nba_team_id").is_not_null()
    return (
        espn2.join(stats2, on="espn_team_id", how="left", maintain_order="left")
        .join(fox2, on="team_key", how="left", maintain_order="left")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_id"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_abbreviation"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_name"),
            pl.when(matched).then(pl.lit("exact_name")).otherwise(pl.lit("unmatched")).alias("match_method"),
            pl.when(matched).then(pl.lit(1.0)).otherwise(pl.lit(None, dtype=pl.Float64)).alias("match_confidence"),
        )
        .select(TEAM_COLUMNS)
    )


def _assemble_schedule_crosswalk(
    espn_games: pl.DataFrame,
    stats_games: pl.DataFrame,
    team_xwalk: pl.DataFrame,
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`nba_schedule_crosswalk` (no network).

    Port of ``.bb_assemble_schedule_crosswalk_nba``: both sides key on
    ``(game_date, home_espn_team_id, away_espn_team_id)`` after the Stats team
    ids are resolved to ESPN ids through ``team_xwalk``.

    Args:
        espn_games: ``espn_game_id``, ``game_date``, ``home_espn_team_id``,
            ``away_espn_team_id``.
        stats_games: ``nba_game_id``, ``nba_game_code``, ``game_date``,
            ``nba_home_team_id``, ``nba_away_team_id``, ``season_type``.
        team_xwalk: Output of :func:`_assemble_team_crosswalk`.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`SCHEDULE_COLUMNS`.
    """
    resolve: Dict[str, int] = {}
    for stats_id, espn_id in zip(
        team_xwalk.select(str_id(team_xwalk, "nba_team_id")).to_series().to_list(),
        team_xwalk["espn_team_id"].to_list(),
    ):
        if stats_id is not None:
            resolve.setdefault(stats_id, espn_id)

    espn2 = espn_games.select(
        pl.col("game_date"),
        pl.col("home_espn_team_id").cast(pl.Int32),
        pl.col("away_espn_team_id").cast(pl.Int32),
        str_id(espn_games, "espn_game_id"),
    )

    home_ids = stats_games.select(str_id(stats_games, "nba_home_team_id")).to_series().to_list()
    away_ids = stats_games.select(str_id(stats_games, "nba_away_team_id")).to_series().to_list()
    stats2 = stats_games.select(
        pl.col("game_date"),
        pl.col("season_type").cast(pl.Utf8),
        str_id(stats_games, "nba_game_id"),
        str_id(stats_games, "nba_game_code"),
        str_id(stats_games, "nba_home_team_id"),
        str_id(stats_games, "nba_away_team_id"),
    ).with_columns(
        pl.Series("home_espn_team_id", [resolve.get(v) for v in home_ids], dtype=pl.Int32),
        pl.Series("away_espn_team_id", [resolve.get(v) for v in away_ids], dtype=pl.Int32),
    )

    keys = ["game_date", "home_espn_team_id", "away_espn_team_id"]
    return (
        espn2.join(stats2, on=keys, how="full", coalesce=True, maintain_order="left_right")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_game_id"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_home_team_id"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_away_team_id"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_game_id"),
            pl.when(pl.col("espn_game_id").is_not_null() & pl.col("nba_game_id").is_not_null())
            .then(pl.lit("both"))
            .when(pl.col("espn_game_id").is_not_null())
            .then(pl.lit("espn_only"))
            .otherwise(pl.lit("stats_only"))
            .alias("match_method"),
        )
        .with_columns(
            pl.when(pl.col("match_method") == "both")
            .then(pl.lit(1.0))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("match_confidence")
        )
        .select(SCHEDULE_COLUMNS)
    )


def _stats_team_tricodes(season: int, **kwargs: Any) -> Dict[str, str]:
    """``{stats_team_id: tricode}`` from ``leaguegamelog`` (hoopR's join source).

    ``leaguestandingsv3`` has no abbreviation column, so hoopR's ``nba_teams()``
    picks the tricode up from the game log. An empty log means the season has
    not tipped off yet; hoopR falls back one season for exactly that case and so
    does this.

    Raises:
        CrosswalkSourceError: The game log could not be produced.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_leaguegamelog

    out: Dict[str, str] = {}
    for year in (season, season - 1):
        stats_season = f"{year - 1}-{str(year)[-2:]}"

        # Default-bound so the closure captures this iteration's season, not the
        # loop variable's final value.
        def _fetch(s: str = stats_season) -> Any:
            raw = nba_stats_leaguegamelog(league_id="00", season=s, **kwargs)
            return raw.get("LeagueGameLog") if isinstance(raw, dict) else raw

        log = require_source(f"nba_stats_leaguegamelog(season={stats_season!r})", _fetch)
        if log.height == 0 or "team_abbreviation" not in log.columns:
            continue
        ids = log.select(str_id(log, "team_id")).to_series().to_list()
        for team_id, abbr in zip(ids, log["team_abbreviation"].cast(pl.Utf8).to_list()):
            if team_id is not None and abbr is not None:
                out.setdefault(team_id, abbr)
        break
    return out


def _stats_team_directory(season: int, **kwargs: Any) -> pl.DataFrame:
    """NBA Stats team directory joined to ESPN ids (hoopR ``nba_teams()`` recipe).

    Follows ``hoopR::nba_teams()``: ``leaguestandingsv3`` carries the city /
    nickname / slug / conference / division, and ``leaguegamelog`` is joined on
    ``team_id`` for the tricode -- ``leaguestandingsv3`` publishes no
    abbreviation column at all (92 columns, none of them a tricode), which is
    why the abbreviation needs the second call rather than being a projection
    of the standings payload.

    Raises:
        CrosswalkSourceError: ``leaguestandingsv3`` or ``leaguegamelog`` could
            not be produced. A standings payload that renders to zero rows is
            provably empty (a season whose standings have not opened) and
            returns a typed empty frame instead.
    """
    stats_season = f"{season - 1}-{str(season)[-2:]}"
    label = f"nba_stats_leaguestandingsv3(season={stats_season!r})"

    def _fetch() -> Any:
        from sportsdataverse.nba.nba_stats import nba_stats_leaguestandingsv3

        raw = nba_stats_leaguestandingsv3(season=stats_season, **kwargs)
        return raw.get("Standings") if isinstance(raw, dict) else raw

    standings = require_source(label, _fetch)
    if standings.height == 0:
        return pl.DataFrame(schema=_STATS_SCHEMA)
    tricode = _stats_team_tricodes(season, **kwargs)
    espn = espn_team_directory("nba", **kwargs)
    espn_by_short = {
        normalize_team(name): tid
        for name, tid in zip(espn["short_name"].to_list(), espn["team_id"].to_list())
        if name is not None
    }
    names = standings.select(pl.col("team_name")).to_series().to_list()
    stats_ids = standings.select(str_id(standings, "team_id")).to_series().to_list()
    return standings.select(
        str_id(standings, "team_id").alias("nba_team_id"),
        pl.Series("nba_team_abbreviation", [tricode.get(v) for v in stats_ids], dtype=pl.Utf8),
        (pl.col("team_city") + pl.lit(" ") + pl.col("team_name")).cast(pl.Utf8).alias("nba_team_name"),
        pl.col("team_city").cast(pl.Utf8).alias("nba_team_city"),
        pl.col("team_slug").cast(pl.Utf8).alias("nba_team_slug"),
        pl.col("conference").cast(pl.Utf8).alias("nba_conference"),
        pl.col("division").cast(pl.Utf8).alias("nba_division"),
    ).with_columns(
        # espn_team_directory ships team_id as Utf8 for every league, so build the
        # series at that dtype and let polars cast -- a direct dtype=pl.Int32
        # construction from those strings raises TypeError, which is what blocked
        # every NBA team/player crosswalk. Matches the wnba_crosswalk cast.
        pl.Series(
            "espn_team_id",
            [espn_by_short.get(normalize_team(n)) for n in names],
            dtype=pl.Utf8,
        ).cast(pl.Int32)
    )


def nba_team_crosswalk(
    season: Optional[int] = None,
    *,
    stats: Optional[pl.DataFrame] = None,
    fox: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the NBA cross-source team crosswalk (ESPN / NBA Stats / Fox).

    One row per ESPN team, keyed on ``espn_team_id``. ESPN and Stats team
    endpoints are current-season snapshots, so ``season`` is a stamp;
    historical relocations are not back-modelled.

    Args:
        season: Season year per hoopR convention (``2026`` = 2025-26).
            Defaults to the most recent NBA season.
        stats: Pre-fetched Stats team directory (``espn_team_id`` +
            ``nba_team_*``). ``None`` derives it from
            ``nba_stats_leaguestandingsv3`` joined to ESPN on the normalized
            team nickname.
        fox: Pre-fetched Fox directory. ``None`` fetches live.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN team, with
        :data:`TEAM_COLUMNS`.

    Raises:
        CrosswalkSourceError: A source that was not passed in pre-fetched could
            not be produced (Fox or the Stats endpoints). Building on a missing
            source would emit a well-formed crosswalk whose ``fox_*`` /
            ``nba_*`` columns are silently all-null, so it fails here instead.

    Note:
        ``stats.nba.com`` TLS-fingerprint-blocks plain ``requests`` and hangs
        on datacenter IPs; the live path needs ``curl_cffi`` and a residential
        connection. Pass ``stats=`` to build fully offline.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_team_crosswalk
            df = nba_team_crosswalk(season=2026)
            print(df.shape)

        Offline with a pre-fetched Stats frame::

            df = nba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

        Pipeline next step (one line)::

            df.select("espn_team_id", "nba_team_id", "match_method").head()

        See Also:
            * `hoopR`_ -- R sister package this ports
            * `nba_api`_ -- Python client for the same Stats API

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    from sportsdataverse.nba.nba_schedule import most_recent_nba_season

    season = int(season) if season is not None else most_recent_nba_season()
    espn = espn_team_directory("nba", season=season, **kwargs)
    if stats is None:
        stats = _stats_team_directory(season, **kwargs)
    if fox is None:

        def _fox() -> Any:
            from sportsdataverse.nba.nba_fox_ext import fox_nba_teams

            return fox_nba_teams(**kwargs)

        fox = require_source("fox_nba_teams()", _fox)
    out = _assemble_team_crosswalk(espn, stats, fox, season)
    return out.to_pandas() if return_as_pandas else out


def nba_schedule_crosswalk(
    season: Optional[int] = None,
    *,
    stats_games: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the NBA cross-source schedule crosswalk (ESPN / NBA Stats).

    One row per game. Both sides reduce to the Eastern-Time game date before
    joining on ``(game_date, home_espn_team_id, away_espn_team_id)``. The
    Stats CDN serves the current season only, so the live builder is
    effectively current-season.

    Args:
        season: Season year per hoopR convention. Defaults to the most recent
            NBA season.
        stats_games: Pre-fetched Stats schedule frame; ``None`` fetches live.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas) with :data:`SCHEDULE_COLUMNS`.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_schedule_crosswalk
            df = nba_schedule_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "both").select("espn_game_id", "nba_game_id").head()

        See Also:
            * `hoopR`_ -- R sister package this ports

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse._crosswalk_basketball_sources import stats_schedule_games
    from sportsdataverse.nba.nba_schedule import most_recent_nba_season

    season = int(season) if season is not None else most_recent_nba_season()
    team_xwalk = nba_team_crosswalk(season=season, **kwargs)
    if stats_games is None:
        stats_games = stats_schedule_games("nba", season, **kwargs)
    dates = sorted({d for d in stats_games["game_date"].to_list() if d is not None})
    espn_games = espn_scoreboard_games("nba", dates, **kwargs)
    out = _assemble_schedule_crosswalk(espn_games, stats_games, team_xwalk, season)
    return out.to_pandas() if return_as_pandas else out


def nba_player_crosswalk(
    season: Optional[int] = None,
    min_confidence: float = 0.92,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the NBA cross-source player crosswalk (ESPN / NBA Stats / Fox).

    One row per ESPN athlete per team. ``match_method`` / ``match_confidence``
    describe the **Stats API** match (normalized exact name, then
    Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
    ``fox_athlete_id`` only.

    Args:
        season: Season year per hoopR convention. Defaults to the most recent
            NBA season.
        min_confidence: Jaro-Winkler floor for fuzzy matches (R default 0.92).
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN athlete, 21 columns.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_player_crosswalk
            df = nba_player_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Tighten the fuzzy floor::

            strict = nba_player_crosswalk(season=2026, min_confidence=0.97)

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "fuzzy_jw").head()

        See Also:
            * `hoopR`_ -- R sister package this ports
            * `nba_api`_ -- Python client for the same Stats API

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    from sportsdataverse._crosswalk_basketball_sources import espn_rosters, fox_rosters, stats_rosters
    from sportsdataverse.nba.nba_schedule import most_recent_nba_season

    season = int(season) if season is not None else most_recent_nba_season()
    stats_season = f"{season - 1}-{str(season)[-2:]}"
    team_xwalk = nba_team_crosswalk(season=season, **kwargs)
    frames: List[pl.DataFrame] = []
    for row in team_xwalk.iter_rows(named=True):
        espn = espn_rosters("nba", row["espn_team_id"], row["espn_abbreviation"], season, **kwargs)
        if espn.height == 0:
            continue
        stats = stats_rosters("nba", row["espn_team_id"], row["nba_team_id"], stats_season, **kwargs)
        fox = fox_rosters("nba", row["espn_team_id"], row["fox_team_id"], **kwargs)
        frames.append(
            assemble_player_espn_stats_fox(espn, stats, fox, season, "nba", min_confidence, exact_tiebreak=True)
        )
    out = (
        pl.concat(frames, how="diagonal_relaxed")
        if frames
        else assemble_player_espn_stats_fox(
            pl.DataFrame(
                schema={
                    "espn_team_id": pl.Int32,
                    "team_abbreviation": pl.Utf8,
                    "espn_athlete_id": pl.Utf8,
                    "espn_full_name": pl.Utf8,
                    "espn_jersey": pl.Utf8,
                    "espn_position": pl.Utf8,
                    "espn_birth_date": pl.Utf8,
                }
            ),
            pl.DataFrame(),
            pl.DataFrame(),
            season,
            "nba",
            exact_tiebreak=True,
        )
    )
    return out.to_pandas() if return_as_pandas else out
