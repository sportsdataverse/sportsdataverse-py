"""WBB cross-source identity crosswalks (ESPN / Fox Sports / Bart Torvik).

Port of ``wehoop/R/wbb_crosswalk.R``. ESPN is the anchor: one row per ESPN
team per season, joined to Fox Sports (by normalized full mascot name, via a
curated alias bridge) and Bart Torvik (by normalized school/location name,
via a second curated alias pass). Yahoo columns are null placeholders.

The two alias tables below are **transcribed verbatim from the R source** —
they are data, not logic. Do not re-derive or "fix" entries; a divergence here
silently changes which teams match.

Public surface:

* :func:`wbb_team_crosswalk` -- ESPN x Fox x Torvik team-id crosswalk.
* :func:`wbb_schedule_crosswalk` -- ESPN x Torvik game-id crosswalk.
* :func:`wbb_player_crosswalk` -- ESPN x Fox player-id crosswalk.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse._common_crosswalk_basketball import (
    assemble_player_espn_fox,
    normalize_college_team,
    normalize_team,
    pair_key,
    str_id,
)
from sportsdataverse._crosswalk_basketball_sources import (
    bart_super_sked,
    espn_scoreboard_games,
    espn_team_directory,
    require_source,
)

__all__ = [
    "wbb_team_crosswalk",
    "wbb_schedule_crosswalk",
    "wbb_player_crosswalk",
]

# ---------------------------------------------------------------------------
# Torvik -> ESPN location alias table (VERBATIM from wehoop .wbb_bart_alias).
# Maps a Torvik `team` name to the matching ESPN `team` (location/school).
# Applied BEFORE normalizing so the resulting keys align.
# ---------------------------------------------------------------------------
BART_ALIAS: Dict[str, str] = {
    "Connecticut": "UConn",
    "Mississippi": "Ole Miss",
    "N.C. State": "NC State",
    "Appalachian St.": "App State",
    "Southeastern Louisiana": "SE Louisiana",
    "Albany": "UAlbany",
    "Illinois Chicago": "UIC",
    "Hawaii": "Hawai'i",
    "Penn": "Pennsylvania",
    "St. Thomas": "St. Thomas-Minnesota",
    "Tennessee Martin": "UT Martin",
    "Louisiana Monroe": "UL Monroe",
    "Nebraska Omaha": "Omaha",
    "Sam Houston St.": "Sam Houston",
    "Nicholls St.": "Nicholls",
    "Cal Baptist": "California Baptist",
    "Texas A&M Corpus Chris": "Texas A&M-Corpus Christi",
    "IU Indy": "IU Indianapolis",
    "Grambling St.": "Grambling",
    "Loyola MD": "Loyola Maryland",
    "McNeese St.": "McNeese",
    "Seattle": "Seattle U",
    "Miami FL": "Miami",
    "UMKC": "Kansas City",
    "FIU": "Florida International",
    "LIU": "Long Island University",
    "USC Upstate": "South Carolina Upstate",
}

# ---------------------------------------------------------------------------
# Fox display_name -> ESPN display_name alias table (VERBATIM from wehoop
# .wbb_fox_display_alias). Both sides are already normalized keys.
# ---------------------------------------------------------------------------
FOX_DISPLAY_ALIAS: Dict[str, str] = {
    # Fox keeps "State" / drops university suffixes differently
    "american eagles": "american university eagles",
    "appalachian state mountaineers": "app state mountaineers",
    "central connecticut state blue devils": "central connecticut blue devils",
    "delaware fightin blue hens": "delaware blue hens",
    "east tennessee state buccaneers": "east tennessee state bucs",
    "fdu knights": "fairleigh dickinson knights",
    "grambling state lady tigers": "grambling lady tigers",
    "iu indy jaguars": "iu indianapolis jaguars",
    "jackson state tigers": "jackson state lady tigers",
    "liu sharks": "long island university sharks",
    "louisiana monroe warhawks": "ul monroe warhawks",
    "mcneese cowgirls": "mcneese cowgirls",
    "nicholls state colonels": "nicholls colonels",
    # Fox "Southeastern Louisiana" vs ESPN "SE Louisiana"
    "southeastern louisiana lady lions": "se louisiana lady lions",
    "seattle redhawks": "seattle u redhawks",
    "siu edwardsville cougars": "siu edwardsville cougars",
    "southern university jaguars": "southern jaguars",
    "st thomas tommies": "st thomas minnesota tommies",
    "tennessee martin skyhawks": "ut martin skyhawks",
    "tennessee state tigers": "tennessee state lady tigers",
    "uconn huskies": "uconn huskies",
    "uic flames": "uic flames",
    "university at albany great danes": "ualbany great danes",
    "umass minutewomen": "massachusetts minutewomen",
    "usc upstate spartans": "south carolina upstate spartans",
    # Fox "Miami (Fl)" vs ESPN "Miami"
    "miami fl hurricanes": "miami hurricanes",
    # Fox "Penn" vs ESPN "Pennsylvania"
    "penn quakers": "pennsylvania quakers",
    # Fox "St. Francis (Pa)" vs ESPN "Saint Francis"
    "st francis pa red flash": "saint francis red flash",
}

TEAM_COLUMNS = [
    "season",
    "espn_team_id",
    "espn_abbreviation",
    "espn_display_name",
    "espn_short_name",
    "espn_location",
    "espn_mascot",
    "espn_conference",
    "fox_team_id",
    "fox_team_name",
    "fox_section",
    "bart_team",
    "bart_conf",
    "yahoo_team_id",
    "yahoo_team_name",
    "fox_match_confidence",
    "bart_match_confidence",
    "match_method",
]

SCHEDULE_COLUMNS = [
    "season",
    "game_date",
    "home_espn_team_id",
    "away_espn_team_id",
    "espn_game_id",
    "bart_muid",
    "bart_team1",
    "bart_team2",
    "bart_winner",
    "fox_game_id",
    "yahoo_game_id",
    "match_method",
    "match_confidence",
]


def _apply_bart_alias(values: List[Optional[str]]) -> List[Optional[str]]:
    """Apply :data:`BART_ALIAS` to Torvik team names (pass-through on a miss)."""
    return [BART_ALIAS.get(v, v) if v is not None else v for v in values]


def _apply_fox_alias(values: List[str]) -> List[str]:
    """Apply :data:`FOX_DISPLAY_ALIAS` to normalized Fox keys (pass-through)."""
    return [FOX_DISPLAY_ALIAS.get(v, v) for v in values]


def _bart_keys(values: List[Optional[str]]) -> List[str]:
    return [normalize_college_team(v) for v in _apply_bart_alias(values)]


def _assemble_team_crosswalk(
    espn: pl.DataFrame,
    fox: Optional[pl.DataFrame],
    bart: Optional[pl.DataFrame],
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`wbb_team_crosswalk` (no network).

    Port of ``.bb_assemble_team_crosswalk_wbb``.

    Args:
        espn: ESPN team directory with ``team_id``, ``abbreviation``,
            ``display_name``, ``short_name``, ``team``, ``mascot`` and
            (optionally) ``conference_name``.
        fox: Fox directory with ``fox_team_id``, ``fox_team_name``,
            ``fox_section``; may be ``None`` or empty.
        bart: Torvik ratings with ``team`` and ``conf``; may be ``None``/empty.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`TEAM_COLUMNS`.
    """
    espn = espn.unique(subset=["team_id"], keep="first", maintain_order=True)
    espn2 = espn.select(
        pl.col("team_id").cast(pl.Int32).alias("espn_team_id"),
        pl.col("abbreviation").cast(pl.Utf8).alias("espn_abbreviation"),
        pl.col("display_name").cast(pl.Utf8).alias("espn_display_name"),
        pl.col("short_name").cast(pl.Utf8).alias("espn_short_name"),
        pl.col("team").cast(pl.Utf8).alias("espn_location"),
        pl.col("mascot").cast(pl.Utf8).alias("espn_mascot"),
        (
            pl.col("conference_name").cast(pl.Utf8)
            if "conference_name" in espn.columns
            else pl.lit(None, dtype=pl.Utf8)
        ).alias("espn_conference"),
    ).with_columns(
        pl.Series("fox_key", [normalize_team(v) for v in espn["display_name"].to_list()], dtype=pl.Utf8),
        pl.Series("bart_key", [normalize_college_team(v) for v in espn["team"].to_list()], dtype=pl.Utf8),
    )

    if fox is None or fox.height == 0:
        fox2 = pl.DataFrame(
            schema={"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "fox_section": pl.Utf8, "fox_key": pl.Utf8}
        )
    else:
        keys = _apply_fox_alias([normalize_team(v) for v in fox["fox_team_name"].to_list()])
        fox2 = fox.select(
            str_id(fox, "fox_team_id"),
            pl.col("fox_team_name").cast(pl.Utf8),
            pl.col("fox_section").cast(pl.Utf8),
        ).with_columns(pl.Series("fox_key", keys, dtype=pl.Utf8))
        fox2 = fox2.unique(subset=["fox_key"], keep="first", maintain_order=True)

    if bart is None or bart.height == 0:
        bart2 = pl.DataFrame(schema={"bart_team": pl.Utf8, "bart_conf": pl.Utf8, "bart_key": pl.Utf8})
    else:
        bart2 = bart.select(
            pl.col("team").cast(pl.Utf8).alias("bart_team"),
            pl.col("conf").cast(pl.Utf8).alias("bart_conf"),
        ).with_columns(pl.Series("bart_key", _bart_keys(bart["team"].to_list()), dtype=pl.Utf8))
        bart2 = bart2.unique(subset=["bart_key"], keep="first", maintain_order=True)

    return (
        espn2.join(fox2, on="fox_key", how="left", maintain_order="left")
        .join(bart2, on="bart_key", how="left", maintain_order="left")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_id"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_name"),
            pl.when(pl.col("fox_team_id").is_not_null())
            .then(pl.lit(1.0))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("fox_match_confidence"),
            pl.when(pl.col("bart_team").is_not_null())
            .then(pl.lit(1.0))
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias("bart_match_confidence"),
            pl.when(pl.col("fox_team_id").is_not_null() & pl.col("bart_team").is_not_null())
            .then(pl.lit("fox+bart"))
            .when(pl.col("fox_team_id").is_not_null())
            .then(pl.lit("fox_only"))
            .when(pl.col("bart_team").is_not_null())
            .then(pl.lit("bart_only"))
            .otherwise(pl.lit("espn_only"))
            .alias("match_method"),
        )
        .select(TEAM_COLUMNS)
    )


def _assemble_schedule_crosswalk(
    espn_games: pl.DataFrame,
    bart_games: pl.DataFrame,
    team_xwalk: pl.DataFrame,
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`wbb_schedule_crosswalk` (no network).

    Port of ``.bb_assemble_schedule_crosswalk_wbb``. Torvik rows whose team
    names cannot be resolved to ESPN ids are KEPT (as ``bart_only``) rather
    than dropped -- the WBB variant differs from MBB here.

    Args:
        espn_games: ``espn_game_id``, ``game_date``, ``home_espn_team_id``,
            ``away_espn_team_id``.
        bart_games: ``muid``, ``game_date``, ``team1``, ``team2``, ``winner``.
        team_xwalk: Output of :func:`_assemble_team_crosswalk`.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`SCHEDULE_COLUMNS`.
    """
    lookup = team_xwalk.filter(pl.col("bart_team").is_not_null())
    resolve: Dict[str, int] = {}
    for key, tid in zip(_bart_keys(lookup["bart_team"].to_list()), lookup["espn_team_id"].to_list()):
        resolve.setdefault(key, tid)

    espn2 = espn_games.select(
        pl.col("game_date"),
        pl.col("home_espn_team_id").cast(pl.Int32),
        pl.col("away_espn_team_id").cast(pl.Int32),
        str_id(espn_games, "espn_game_id"),
    ).with_columns(
        pl.Series(
            "pair_key",
            [
                pair_key(h, a)
                for h, a in zip(espn_games["home_espn_team_id"].to_list(), espn_games["away_espn_team_id"].to_list())
            ],
            dtype=pl.Utf8,
        )
    )
    espn2 = espn2.unique(subset=["espn_game_id"], keep="first", maintain_order=True)

    t1 = [resolve.get(k) for k in _bart_keys(bart_games["team1"].to_list())]
    t2 = [resolve.get(k) for k in _bart_keys(bart_games["team2"].to_list())]
    bart2 = bart_games.select(
        pl.col("game_date"),
        str_id(bart_games, "muid").alias("bart_muid"),
        pl.col("team1").cast(pl.Utf8).alias("bart_team1"),
        pl.col("team2").cast(pl.Utf8).alias("bart_team2"),
        pl.col("winner").cast(pl.Utf8).alias("bart_winner"),
    ).with_columns(pl.Series("pair_key", [pair_key(a, b) for a, b in zip(t1, t2)], dtype=pl.Utf8))
    bart2 = bart2.unique(subset=["bart_muid"], keep="first", maintain_order=True)

    return (
        espn2.join(bart2, on=["game_date", "pair_key"], how="full", coalesce=True, maintain_order="left_right")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("fox_game_id"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_game_id"),
            pl.when(pl.col("espn_game_id").is_not_null() & pl.col("bart_muid").is_not_null())
            .then(pl.lit("both"))
            .when(pl.col("espn_game_id").is_not_null())
            .then(pl.lit("espn_only"))
            .otherwise(pl.lit("bart_only"))
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


def wbb_team_crosswalk(
    season: Optional[int] = None,
    *,
    fox: Optional[pl.DataFrame] = None,
    bart: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WBB cross-source team crosswalk (ESPN / Fox / Torvik).

    One row per ESPN team, keyed on ``espn_team_id``. Fox is joined on the
    normalized full mascot name (with the curated :data:`FOX_DISPLAY_ALIAS`
    bridge); Torvik on the normalized school name after the
    :data:`BART_ALIAS` pass. Yahoo columns are null placeholders.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WBB
            season.
        fox: Pre-fetched ``fox_wbb_teams_all()`` frame. ``None`` fetches live
            (~60 s); pass an empty frame to skip Fox entirely.
        bart: Pre-fetched ``bart_wbb_ratings()`` frame. ``None`` fetches live.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN team, with
        :data:`TEAM_COLUMNS`.

    Note:
        sdv-py's ``espn_wbb_teams()`` ships no conference labels, so
        ``espn_conference`` is reconstructed from ESPN's Core v2 season group
        tree (see
        :func:`~sportsdataverse._crosswalk_basketball_sources.espn_conference_map`),
        the same walk the R producer does. A pre-fetched ``espn`` frame that
        already carries ``conference_name`` is used as-is.

    Raises:
        CrosswalkSourceError: A source that was not passed in pre-fetched could
            not be produced (Fox or Torvik). Building on a missing source would
            emit a well-formed crosswalk whose ``fox_*`` / ``bart_*`` columns
            are silently all-null, so it fails here instead. Pass an explicit
            empty frame (``fox=pl.DataFrame()``) to opt a source out.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_team_crosswalk
            df = wbb_team_crosswalk(season=2026)
            print(df.shape)

        Skip the slow Fox enumeration::

            import polars as pl
            df = wbb_team_crosswalk(season=2026, fox=pl.DataFrame())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "fox+bart").head()

        See Also:
            * `wehoop`_ -- R sister package this ports
            * `Bart Torvik`_ -- women's T-Rank source

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com/ncaaw
    """
    from sportsdataverse.wbb.wbb_schedule import most_recent_wbb_season

    season = int(season) if season is not None else most_recent_wbb_season()
    espn = espn_team_directory("wbb", season=season, **kwargs)
    if fox is None:

        def _fox() -> Any:
            from sportsdataverse.wbb.wbb_fox_ext import fox_wbb_teams_all

            return fox_wbb_teams_all(**kwargs)

        fox = require_source("fox_wbb_teams_all()", _fox)
    if bart is None:
        # The provider import lives inside the callable so a missing/broken
        # bart_wbb module surfaces as CrosswalkSourceError, and so a caller who
        # supplied `bart` never pays for (or trips over) the import at all.
        def _bart() -> Any:
            from sportsdataverse.wbb.bart_wbb import bart_wbb_ratings

            return bart_wbb_ratings(year=season, **kwargs)

        bart = require_source(f"bart_wbb_ratings(year={season})", _bart)
    out = _assemble_team_crosswalk(espn, fox, bart, season)
    return out.to_pandas() if return_as_pandas else out


def wbb_schedule_crosswalk(
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WBB cross-source schedule crosswalk (ESPN / Torvik).

    One row per game. Dates are reduced to the Eastern-Time game date before
    joining and Torvik's unordered ``team1``/``team2`` join through a sorted
    ESPN team-pair key, so home/away is taken from the ESPN side only. Torvik
    games whose teams cannot be resolved to ESPN ids survive as ``bart_only``.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WBB
            season.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas) with :data:`SCHEDULE_COLUMNS`.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_schedule_crosswalk
            df = wbb_schedule_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "both").select("espn_game_id", "bart_muid").head()

        See Also:
            * `wehoop`_ -- R sister package this ports

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse.wbb.wbb_schedule import most_recent_wbb_season

    season = int(season) if season is not None else most_recent_wbb_season()
    # An empty Fox frame skips the ~60 s enumeration: only espn_team_id and
    # bart_team are needed here (mirrors the R builder's `.empty_fox`).
    team_xwalk = wbb_team_crosswalk(season=season, fox=pl.DataFrame(), **kwargs)
    bart_games = bart_super_sked("wbb", season, **kwargs)
    dates = sorted({d for d in bart_games["game_date"].to_list() if d is not None})
    espn_games = espn_scoreboard_games("wbb", dates, **kwargs)
    out = _assemble_schedule_crosswalk(espn_games, bart_games, team_xwalk, season)
    return out.to_pandas() if return_as_pandas else out


def wbb_player_crosswalk(
    season: Optional[int] = None,
    min_confidence: float = 0.92,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the WBB cross-source player crosswalk (ESPN / Fox).

    One row per ESPN athlete per team. Fox is matched by normalized name
    within each team block -- exact first, then Jaro-Winkler at or above
    ``min_confidence`` with a jersey tiebreak. Torvik has no per-player table
    for WBB, so it is not joined; Yahoo columns are null placeholders.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent WBB
            season.
        min_confidence: Jaro-Winkler floor for fuzzy matches (R default 0.92).
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN athlete, 17 columns
        ending in ``match_method`` / ``match_confidence`` / ``match_keys``.

    Example:
        Quick start::

            from sportsdataverse.wbb import wbb_player_crosswalk
            df = wbb_player_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Tighten the fuzzy floor::

            strict = wbb_player_crosswalk(season=2026, min_confidence=0.97)

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "fuzzy_jw").head()

        See Also:
            * `wehoop`_ -- R sister package this ports

        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    from sportsdataverse._crosswalk_basketball_sources import espn_rosters, fox_rosters
    from sportsdataverse.wbb.wbb_schedule import most_recent_wbb_season

    season = int(season) if season is not None else most_recent_wbb_season()
    team_xwalk = wbb_team_crosswalk(season=season, **kwargs)
    frames: List[pl.DataFrame] = []
    for row in team_xwalk.iter_rows(named=True):
        espn = espn_rosters("wbb", row["espn_team_id"], row["espn_abbreviation"], season, **kwargs)
        if espn.height == 0:
            continue
        fox = fox_rosters("wbb", row["espn_team_id"], row["fox_team_id"], **kwargs)
        frames.append(assemble_player_espn_fox(espn, fox, season, min_confidence))
    out = (
        pl.concat(frames, how="diagonal_relaxed")
        if frames
        else assemble_player_espn_fox(
            pl.DataFrame(
                schema={
                    "espn_team_id": pl.Int32,
                    "team_abbreviation": pl.Utf8,
                    "espn_athlete_id": pl.Utf8,
                    "espn_full_name": pl.Utf8,
                    "espn_jersey": pl.Utf8,
                    "espn_position": pl.Utf8,
                }
            ),
            pl.DataFrame(),
            season,
        )
    )
    return out.to_pandas() if return_as_pandas else out
