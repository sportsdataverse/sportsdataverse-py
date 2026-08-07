"""WNBA cross-source identity crosswalks (ESPN / WNBA Stats API / Fox Sports).

Port of ``wehoop/R/wnba_crosswalk.R``. ESPN is the anchor; the WNBA Stats API
and Fox are both joined on the normalized full team name. Yahoo columns are
null placeholders.

Public surface:

* :func:`wnba_team_crosswalk` -- ESPN x Stats x Fox team-id crosswalk.
* :func:`wnba_schedule_crosswalk` -- ESPN x Stats game-id crosswalk.
* :func:`wnba_player_crosswalk` -- ESPN x Stats x Fox player-id crosswalk.
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
from sportsdataverse._crosswalk_basketball_sources import espn_scoreboard_games, espn_team_directory

__all__ = [
    "wnba_team_crosswalk",
    "wnba_schedule_crosswalk",
    "wnba_player_crosswalk",
]

TEAM_COLUMNS = [
    "season",
    "espn_team_id",
    "espn_abbreviation",
    "espn_display_name",
    "espn_short_name",
    "espn_location",
    "espn_mascot",
    "wnba_team_id",
    "wnba_team_tricode",
    "wnba_team_name",
    "wnba_team_city",
    "wnba_team_slug",
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
    "wnba_game_id",
    "wnba_game_code",
    "wnba_home_team_id",
    "wnba_away_team_id",
    "fox_game_id",
    "fox_home_team_id",
    "fox_away_team_id",
    "yahoo_game_id",
    "match_method",
    "match_confidence",
]

_STATS_SCHEMA = {
    "wnba_team_id": pl.Utf8,
    "wnba_team_tricode": pl.Utf8,
    "wnba_team_name": pl.Utf8,
    "wnba_team_city": pl.Utf8,
    "wnba_team_slug": pl.Utf8,
    "team_key": pl.Utf8,
}


def _assemble_team_crosswalk(
    espn: pl.DataFrame,
    stats: Optional[pl.DataFrame],
    fox: Optional[pl.DataFrame],
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`wnba_team_crosswalk` (no network).

    Port of ``.bb_assemble_team_crosswalk_wnba``. The Stats side keys on
    ``normalize_team(city + " " + name)``, matching ESPN's display name.

    Args:
        espn: ESPN team directory (``team_id``, ``abbreviation``,
            ``display_name``, ``short_name``, ``team``, ``mascot``).
        stats: Stats team directory with ``wnba_team_id``,
            ``wnba_team_tricode``, ``wnba_team_name``, ``wnba_team_city``,
            ``wnba_team_slug``; may be ``None``/empty.
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
        keys = [
            normalize_team(f"{'' if c is None else c} {'' if n is None else n}")
            for c, n in zip(stats["wnba_team_city"].to_list(), stats["wnba_team_name"].to_list())
        ]
        stats2 = stats.select(
            str_id(stats, "wnba_team_id"),
            pl.col("wnba_team_tricode").cast(pl.Utf8),
            pl.col("wnba_team_name").cast(pl.Utf8),
            pl.col("wnba_team_city").cast(pl.Utf8),
            pl.col("wnba_team_slug").cast(pl.Utf8),
        ).with_columns(pl.Series("team_key", keys, dtype=pl.Utf8))

    if fox is None or fox.height == 0:
        fox2 = pl.DataFrame(schema={"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "team_key": pl.Utf8})
    else:
        fox2 = fox.select(
            str_id(fox, "fox_team_id"),
            pl.col("fox_team_name").cast(pl.Utf8),
        ).with_columns(
            pl.Series("team_key", [normalize_team(v) for v in fox["fox_team_name"].to_list()], dtype=pl.Utf8)
        )

    matched = pl.col("wnba_team_id").is_not_null()
    return (
        espn2.join(stats2, on="team_key", how="left", maintain_order="left")
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
    """Pure assembler behind :func:`wnba_schedule_crosswalk` (no network).

    Port of ``.bb_assemble_schedule_crosswalk_wnba``.

    Args:
        espn_games: ``espn_game_id``, ``game_date``, ``home_espn_team_id``,
            ``away_espn_team_id``.
        stats_games: ``wnba_game_id``, ``wnba_game_code``, ``game_date``,
            ``wnba_home_team_id``, ``wnba_away_team_id``, ``season_type``.
        team_xwalk: Output of :func:`_assemble_team_crosswalk`.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`SCHEDULE_COLUMNS`.
    """
    resolve: Dict[str, int] = {}
    for stats_id, espn_id in zip(
        team_xwalk.select(str_id(team_xwalk, "wnba_team_id")).to_series().to_list(),
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

    home_ids = stats_games.select(str_id(stats_games, "wnba_home_team_id")).to_series().to_list()
    away_ids = stats_games.select(str_id(stats_games, "wnba_away_team_id")).to_series().to_list()
    stats2 = stats_games.select(
        pl.col("game_date"),
        pl.col("season_type").cast(pl.Utf8),
        str_id(stats_games, "wnba_game_id"),
        str_id(stats_games, "wnba_game_code"),
        str_id(stats_games, "wnba_home_team_id"),
        str_id(stats_games, "wnba_away_team_id"),
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
            pl.when(pl.col("espn_game_id").is_not_null() & pl.col("wnba_game_id").is_not_null())
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


def wnba_team_crosswalk(
    season: Optional[int] = None,
    *,
    stats: Optional[pl.DataFrame] = None,
    fox: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox).

    One row per ESPN team, keyed on ``espn_team_id``. The Stats side is
    derived from the season schedule's home/away team fields (as in wehoop)
    and joined on the normalized ``city + name``.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WNBA
            season.
        stats: Pre-fetched Stats team directory. ``None`` derives it from the
            Stats schedule.
        fox: Pre-fetched ``fox_wnba_teams()`` frame. ``None`` fetches live.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN team, with
        :data:`TEAM_COLUMNS`.

    Note:
        ``stats.wnba.com`` TLS-fingerprint-blocks plain ``requests`` and hangs
        on datacenter IPs; the live path needs ``curl_cffi`` and a residential
        connection. Pass ``stats=`` to build fully offline.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_team_crosswalk
            df = wnba_team_crosswalk(season=2026)
            print(df.shape)

        Offline with pre-fetched provider frames::

            df = wnba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

        Pipeline next step (one line)::

            df.select("espn_team_id", "wnba_team_id", "match_method").head()

        See Also:
            * `wehoop`_ -- R sister package this ports
            * `nba_api`_ -- Python client for the sibling Stats API

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    from sportsdataverse._crosswalk_basketball_sources import stats_schedule_games
    from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

    season = int(season) if season is not None else most_recent_wnba_season()
    espn = espn_team_directory("wnba", season=season, **kwargs)
    if stats is None:
        stats = stats_schedule_games("wnba", season, teams=True, **kwargs)
    if fox is None:
        try:
            from sportsdataverse.wnba.wnba_fox_ext import fox_wnba_teams

            fox = fox_wnba_teams(**kwargs)
        except Exception:
            fox = None
    out = _assemble_team_crosswalk(espn, stats, fox, season)
    return out.to_pandas() if return_as_pandas else out


def wnba_schedule_crosswalk(
    season: Optional[int] = None,
    *,
    stats_games: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats).

    One row per game, joined on ``(game_date, home_espn_team_id,
    away_espn_team_id)`` after both sides reduce to the Eastern-Time date. The
    Stats CDN serves the current season only, so the live builder is
    effectively current-season.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WNBA
            season.
        stats_games: Pre-fetched Stats schedule frame; ``None`` fetches live.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas) with :data:`SCHEDULE_COLUMNS`.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_schedule_crosswalk
            df = wnba_schedule_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "both").select("espn_game_id", "wnba_game_id").head()

        See Also:
            * `wehoop`_ -- R sister package this ports

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse._crosswalk_basketball_sources import stats_schedule_games
    from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

    season = int(season) if season is not None else most_recent_wnba_season()
    if stats_games is None:
        stats_games = stats_schedule_games("wnba", season, **kwargs)
    team_xwalk = wnba_team_crosswalk(season=season, **kwargs)
    dates = sorted({d for d in stats_games["game_date"].to_list() if d is not None})
    espn_games = espn_scoreboard_games("wnba", dates, **kwargs)
    out = _assemble_schedule_crosswalk(espn_games, stats_games, team_xwalk, season)
    return out.to_pandas() if return_as_pandas else out


def wnba_player_crosswalk(
    season: Optional[int] = None,
    min_confidence: float = 0.92,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox).

    One row per ESPN athlete per team. ``match_method`` / ``match_confidence``
    describe the **Stats API** match (normalized exact name, then
    Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
    ``fox_athlete_id`` only.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WNBA
            season.
        min_confidence: Jaro-Winkler floor for fuzzy matches (R default 0.92).
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN athlete, 21 columns.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_player_crosswalk
            df = wnba_player_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Tighten the fuzzy floor::

            strict = wnba_player_crosswalk(season=2026, min_confidence=0.97)

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "fuzzy_jw").head()

        See Also:
            * `wehoop`_ -- R sister package this ports

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse._crosswalk_basketball_sources import espn_rosters, fox_rosters, stats_rosters
    from sportsdataverse.wnba.wnba_schedule import most_recent_wnba_season

    season = int(season) if season is not None else most_recent_wnba_season()
    team_xwalk = wnba_team_crosswalk(season=season, **kwargs)
    frames: List[pl.DataFrame] = []
    for row in team_xwalk.iter_rows(named=True):
        espn = espn_rosters("wnba", row["espn_team_id"], row["espn_abbreviation"], season, **kwargs)
        if espn.height == 0:
            continue
        stats = stats_rosters("wnba", row["espn_team_id"], row["wnba_team_id"], str(season), **kwargs)
        fox = fox_rosters("wnba", row["espn_team_id"], row["fox_team_id"], **kwargs)
        frames.append(assemble_player_espn_stats_fox(espn, stats, fox, season, "wnba", min_confidence))
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
            "wnba",
        )
    )
    return out.to_pandas() if return_as_pandas else out
