"""MBB cross-source identity crosswalks (ESPN / Fox / Bart Torvik / KenPom).

Port of ``hoopR/R/mbb_crosswalk.R``. ESPN is the anchor: one row per ESPN team
per season, joined to Fox Sports (normalized full mascot name via a curated
alias bridge), Bart Torvik and KenPom (each on the normalized school/location
name after its own curated alias pass). Yahoo columns are null placeholders.

The three alias tables below are **transcribed verbatim from the R source** --
they are data, not logic. Do not re-derive or "fix" entries.

Public surface:

* :func:`mbb_team_crosswalk` -- ESPN x Fox x Torvik x KenPom team crosswalk.
* :func:`mbb_schedule_crosswalk` -- ESPN x Torvik game-id crosswalk.
* :func:`mbb_player_crosswalk` -- ESPN x Fox player-id crosswalk.
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
    "mbb_team_crosswalk",
    "mbb_schedule_crosswalk",
    "mbb_player_crosswalk",
]

# ---------------------------------------------------------------------------
# Torvik -> ESPN location alias table (VERBATIM from hoopR .mbb_bart_alias).
# ---------------------------------------------------------------------------
BART_ALIAS: Dict[str, str] = {
    "Mississippi": "Ole Miss",
    "Connecticut": "UConn",
    "McNeese St.": "McNeese",
    "St. Thomas": "St. Thomas-Minnesota",
    "N.C. State": "NC State",
    "Nebraska Omaha": "Omaha",
    "Texas A&M Corpus Chris": "Texas A&M-Corpus Christi",
    "Seattle": "Seattle U",
    "Nicholls St.": "Nicholls",
    "Sam Houston St.": "Sam Houston",
    "Cal Baptist": "California Baptist",
    "Appalachian St.": "App State",
    "Miami FL": "Miami",
    "Illinois Chicago": "UIC",
    "Southeastern Louisiana": "SE Louisiana",
    "UMKC": "Kansas City",
    "Hawaii": "Hawai'i",
    "FIU": "Florida International",
    "Albany": "UAlbany",
    "LIU": "Long Island University",
    "Tennessee Martin": "UT Martin",
    "Penn": "Pennsylvania",
    "IU Indy": "IU Indianapolis",
    "Loyola MD": "Loyola Maryland",
    "Grambling St.": "Grambling",
    "USC Upstate": "South Carolina Upstate",
    "Louisiana Monroe": "UL Monroe",
}

# ---------------------------------------------------------------------------
# KenPom -> ESPN location alias table (VERBATIM from hoopR .mbb_kp_alias).
# Most Torvik aliases also apply to KenPom (same terse style); the last three
# handle KenPom-specific divergences.
# ---------------------------------------------------------------------------
KP_ALIAS: Dict[str, str] = {
    # shared with Torvik
    "Mississippi": "Ole Miss",
    "Connecticut": "UConn",
    "McNeese St.": "McNeese",
    "St. Thomas": "St. Thomas-Minnesota",
    "N.C. State": "NC State",
    "Nebraska Omaha": "Omaha",
    "Texas A&M Corpus Chris": "Texas A&M-Corpus Christi",
    "Seattle": "Seattle U",
    "Nicholls St.": "Nicholls",
    "Sam Houston St.": "Sam Houston",
    "Cal Baptist": "California Baptist",
    "Appalachian St.": "App State",
    "Miami FL": "Miami",
    "Illinois Chicago": "UIC",
    "Southeastern Louisiana": "SE Louisiana",
    "UMKC": "Kansas City",
    "Hawaii": "Hawai'i",
    "FIU": "Florida International",
    "Albany": "UAlbany",
    "LIU": "Long Island University",
    "Tennessee Martin": "UT Martin",
    "Penn": "Pennsylvania",
    "IU Indy": "IU Indianapolis",
    "Loyola MD": "Loyola Maryland",
    "Grambling St.": "Grambling",
    "USC Upstate": "South Carolina Upstate",
    "Louisiana Monroe": "UL Monroe",
    # KenPom-specific
    "CSUN": "Cal State Northridge",
    "SIUE": "SIU Edwardsville",
    "Southeast Missouri": "Southeast Missouri State",
}

# ---------------------------------------------------------------------------
# Fox display_name -> ESPN display_name alias table (VERBATIM from hoopR
# .mbb_fox_display_alias). Both sides are already normalized keys.
# ---------------------------------------------------------------------------
FOX_DISPLAY_ALIAS: Dict[str, str] = {
    # Fox drops "University", uses shortened or alternative forms
    "american eagles": "american university eagles",
    "appalachian state mountaineers": "app state mountaineers",
    "central connecticut state blue devils": "central connecticut blue devils",
    "delaware fightin blue hens": "delaware blue hens",
    "fdu knights": "fairleigh dickinson knights",
    "grambling state tigers": "grambling tigers",
    "iu indy jaguars": "iu indianapolis jaguars",
    "liu sharks": "long island university sharks",
    "louisiana monroe warhawks": "ul monroe warhawks",
    "miami fl hurricanes": "miami hurricanes",
    "nicholls state colonels": "nicholls colonels",
    "penn quakers": "pennsylvania quakers",
    "queens university royals": "queens university royals",
    "seattle redhawks": "seattle u redhawks",
    "southeastern louisiana lions": "se louisiana lions",
    "southern indiana screaming eagles": "southern indiana screaming eagles",
    "southern university jaguars": "southern jaguars",
    "st francis pa red flash": "saint francis red flash",
    "st thomas tommies": "st thomas minnesota tommies",
    "tennessee martin skyhawks": "ut martin skyhawks",
    "uconn huskies": "uconn huskies",
    "uic flames": "uic flames",
    "umass minutemen": "massachusetts minutemen",
    "unlv runnin rebels": "unlv rebels",
    "university at albany great danes": "ualbany great danes",
    "usc upstate spartans": "south carolina upstate spartans",
    "utah runnin utes": "utah utes",
    "lindenwood lions": "lindenwood lions",
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
    "kp_team",
    "kp_conf",
    "yahoo_team_id",
    "yahoo_team_name",
    "fox_match_confidence",
    "bart_match_confidence",
    "kp_match_confidence",
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
    "kp_game_id",
    "fox_game_id",
    "yahoo_game_id",
    "match_method",
    "match_confidence",
]


def _apply_alias(values: List[Optional[str]], table: Dict[str, str]) -> List[Optional[str]]:
    return [table.get(v, v) if v is not None else v for v in values]


def _bart_keys(values: List[Optional[str]]) -> List[str]:
    return [normalize_college_team(v) for v in _apply_alias(values, BART_ALIAS)]


def _kp_keys(values: List[Optional[str]]) -> List[str]:
    return [normalize_college_team(v) for v in _apply_alias(values, KP_ALIAS)]


def _assemble_team_crosswalk(
    espn: pl.DataFrame,
    fox: Optional[pl.DataFrame],
    bart: Optional[pl.DataFrame],
    kp: Optional[pl.DataFrame],
    season: int,
) -> pl.DataFrame:
    """Pure assembler behind :func:`mbb_team_crosswalk` (no network).

    Port of ``.bb_assemble_team_crosswalk_mbb``. Torvik and KenPom share the
    same ESPN-side normalized key but each apply their own alias table, so the
    key is duplicated into ``ct_key_bart`` / ``ct_key_kp`` before joining.

    Args:
        espn: ESPN team directory (see the WBB assembler for the columns).
        fox: Fox directory; may be ``None``/empty.
        bart: Torvik ratings with ``team`` / ``conf``; may be ``None``/empty.
        kp: KenPom teams with ``Team`` / ``Conf``; may be ``None``/empty.
        season: Season stamp.

    Returns:
        ``pl.DataFrame`` with :data:`TEAM_COLUMNS`.
    """
    espn = espn.unique(subset=["team_id"], keep="first", maintain_order=True)
    ct_keys = [normalize_college_team(v) for v in espn["team"].to_list()]
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
        pl.Series("ct_key_bart", ct_keys, dtype=pl.Utf8),
        pl.Series("ct_key_kp", ct_keys, dtype=pl.Utf8),
    )

    if fox is None or fox.height == 0:
        fox2 = pl.DataFrame(
            schema={"fox_team_id": pl.Utf8, "fox_team_name": pl.Utf8, "fox_section": pl.Utf8, "fox_key": pl.Utf8}
        )
    else:
        keys = [FOX_DISPLAY_ALIAS.get(k, k) for k in (normalize_team(v) for v in fox["fox_team_name"].to_list())]
        fox2 = (
            fox.select(
                str_id(fox, "fox_team_id"),
                pl.col("fox_team_name").cast(pl.Utf8),
                pl.col("fox_section").cast(pl.Utf8),
            )
            .with_columns(pl.Series("fox_key", keys, dtype=pl.Utf8))
            .unique(subset=["fox_key"], keep="first", maintain_order=True)
        )

    if bart is None or bart.height == 0:
        bart2 = pl.DataFrame(schema={"bart_team": pl.Utf8, "bart_conf": pl.Utf8, "ct_key_bart": pl.Utf8})
    else:
        bart2 = (
            bart.select(
                pl.col("team").cast(pl.Utf8).alias("bart_team"),
                pl.col("conf").cast(pl.Utf8).alias("bart_conf"),
            )
            .with_columns(pl.Series("ct_key_bart", _bart_keys(bart["team"].to_list()), dtype=pl.Utf8))
            .unique(subset=["ct_key_bart"], keep="first", maintain_order=True)
        )

    if kp is None or kp.height == 0:
        kp2 = pl.DataFrame(schema={"kp_team": pl.Utf8, "kp_conf": pl.Utf8, "ct_key_kp": pl.Utf8})
    else:
        team_col = "Team" if "Team" in kp.columns else "team"
        conf_col = "Conf" if "Conf" in kp.columns else "conf"
        kp2 = (
            kp.select(
                pl.col(team_col).cast(pl.Utf8).alias("kp_team"),
                pl.col(conf_col).cast(pl.Utf8).alias("kp_conf"),
            )
            .with_columns(pl.Series("ct_key_kp", _kp_keys(kp[team_col].to_list()), dtype=pl.Utf8))
            .unique(subset=["ct_key_kp"], keep="first", maintain_order=True)
        )

    fox_ok = pl.col("fox_team_id").is_not_null()
    bart_ok = pl.col("bart_team").is_not_null()
    kp_ok = pl.col("kp_team").is_not_null()
    return (
        espn2.join(fox2, on="fox_key", how="left", maintain_order="left")
        .join(bart2, on="ct_key_bart", how="left", maintain_order="left")
        .join(kp2, on="ct_key_kp", how="left", maintain_order="left")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_id"),
            pl.lit(None, dtype=pl.Utf8).alias("yahoo_team_name"),
            pl.when(fox_ok).then(pl.lit(1.0)).otherwise(pl.lit(None, dtype=pl.Float64)).alias("fox_match_confidence"),
            pl.when(bart_ok).then(pl.lit(1.0)).otherwise(pl.lit(None, dtype=pl.Float64)).alias("bart_match_confidence"),
            pl.when(kp_ok).then(pl.lit(1.0)).otherwise(pl.lit(None, dtype=pl.Float64)).alias("kp_match_confidence"),
            pl.when(fox_ok & bart_ok & kp_ok)
            .then(pl.lit("fox+bart+kp"))
            .when(fox_ok & bart_ok)
            .then(pl.lit("fox+bart"))
            .when(fox_ok & kp_ok)
            .then(pl.lit("fox+kp"))
            .when(bart_ok & kp_ok)
            .then(pl.lit("bart+kp"))
            .when(fox_ok)
            .then(pl.lit("fox_only"))
            .when(bart_ok)
            .then(pl.lit("bart_only"))
            .when(kp_ok)
            .then(pl.lit("kp_only"))
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
    """Pure assembler behind :func:`mbb_schedule_crosswalk` (no network).

    Port of ``.bb_assemble_schedule_crosswalk_mbb``. Unlike the WBB variant,
    Torvik rows are DROPPED when either team fails to resolve to an ESPN id.

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

    espn2 = (
        espn_games.select(
            pl.col("game_date"),
            pl.col("home_espn_team_id").cast(pl.Int32),
            pl.col("away_espn_team_id").cast(pl.Int32),
            str_id(espn_games, "espn_game_id"),
        )
        .with_columns(
            pl.Series(
                "pair_key",
                [
                    pair_key(h, a)
                    for h, a in zip(
                        espn_games["home_espn_team_id"].to_list(), espn_games["away_espn_team_id"].to_list()
                    )
                ],
                dtype=pl.Utf8,
            )
        )
        .unique(subset=["espn_game_id"], keep="first", maintain_order=True)
    )

    t1 = [resolve.get(k) for k in _bart_keys(bart_games["team1"].to_list())]
    t2 = [resolve.get(k) for k in _bart_keys(bart_games["team2"].to_list())]
    keep = [a is not None and b is not None for a, b in zip(t1, t2)]
    bart2 = (
        bart_games.select(
            pl.col("game_date"),
            str_id(bart_games, "muid").alias("bart_muid"),
            pl.col("team1").cast(pl.Utf8).alias("bart_team1"),
            pl.col("team2").cast(pl.Utf8).alias("bart_team2"),
            pl.col("winner").cast(pl.Utf8).alias("bart_winner"),
        )
        .with_columns(pl.Series("pair_key", [pair_key(a, b) for a, b in zip(t1, t2)], dtype=pl.Utf8))
        .filter(pl.Series(keep, dtype=pl.Boolean))
        .unique(subset=["bart_muid"], keep="first", maintain_order=True)
    )

    return (
        espn2.join(bart2, on=["game_date", "pair_key"], how="full", coalesce=True, maintain_order="left_right")
        .with_columns(
            pl.lit(season, dtype=pl.Int32).alias("season"),
            pl.lit(None, dtype=pl.Utf8).alias("kp_game_id"),
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


def mbb_team_crosswalk(
    season: Optional[int] = None,
    *,
    fox: Optional[pl.DataFrame] = None,
    bart: Optional[pl.DataFrame] = None,
    kenpom: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the MBB cross-source team crosswalk (ESPN / Fox / Torvik / KenPom).

    One row per ESPN team, keyed on ``espn_team_id``. Fox joins on the
    normalized mascot name via :data:`FOX_DISPLAY_ALIAS`; Torvik and KenPom
    each join on the normalized school name after :data:`BART_ALIAS` /
    :data:`KP_ALIAS`.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent MBB
            season.
        fox: Pre-fetched ``fox_mbb_teams_all()``-shaped frame. ``None`` fetches
            live; pass an empty frame to skip Fox.
        bart: Pre-fetched ``torvik_ratings()`` frame. ``None`` fetches live.
        kenpom: KenPom teams frame with ``Team`` / ``Conf``. KenPom is a paid
            subscription and sdv-py bundles no KenPom data, so this is
            ``None`` (KenPom columns null) unless you supply a frame.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN team, with
        :data:`TEAM_COLUMNS`.

    Note:
        ``espn_conference`` is null unless the ESPN frame carries a
        ``conference_name`` column -- sdv-py's ``espn_mbb_teams()`` does not
        ship conference labels.

    Raises:
        CrosswalkSourceError: A source that was not passed in pre-fetched could
            not be produced (Fox or Torvik). Building on a missing source would
            emit a well-formed crosswalk whose ``fox_*`` / ``bart_*`` columns
            are silently all-null, so it fails here instead. Pass an explicit
            empty frame (``fox=pl.DataFrame()``) to opt a source out.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_team_crosswalk
            df = mbb_team_crosswalk(season=2026)
            print(df.shape)

        Skip the slow Fox enumeration::

            import polars as pl
            df = mbb_team_crosswalk(season=2026, fox=pl.DataFrame())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "espn_only").select("espn_display_name").head()

        See Also:
            * `hoopR`_ -- R sister package this ports
            * `Bart Torvik`_ -- T-Rank source

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _Bart Torvik: https://barttorvik.com
    """
    from sportsdataverse.mbb.mbb_schedule import most_recent_mbb_season

    season = int(season) if season is not None else most_recent_mbb_season()
    espn = espn_team_directory("mbb", season=season, **kwargs)
    if fox is None:

        def _fox() -> Any:
            from sportsdataverse.mbb.mbb_fox_ext import fox_mbb_teams_all

            return fox_mbb_teams_all(**kwargs)

        fox = require_source("fox_mbb_teams_all()", _fox)
    if bart is None:
        # The provider import lives inside the callable so a missing/broken
        # torvik module surfaces as CrosswalkSourceError, and so a caller who
        # supplied `bart` never pays for (or trips over) the import at all.
        def _bart() -> Any:
            from sportsdataverse.mbb.torvik import torvik_ratings

            return torvik_ratings(year=season, **kwargs)

        bart = require_source(f"torvik_ratings(year={season})", _bart)
    out = _assemble_team_crosswalk(espn, fox, bart, kenpom, season)
    return out.to_pandas() if return_as_pandas else out


def mbb_schedule_crosswalk(
    season: Optional[int] = None,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the MBB cross-source schedule crosswalk (ESPN / Torvik).

    One row per game. Dates reduce to the Eastern-Time game date before
    joining and Torvik's unordered ``team1``/``team2`` join through a sorted
    ESPN team-pair key. Torvik games whose teams cannot be resolved to ESPN
    ids are dropped (the MBB variant differs from WBB here). ``kp_game_id`` is
    a null placeholder -- the R builder's optional KenPom enrichment needs a
    paid subscription and is not ported.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent MBB
            season.
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas) with :data:`SCHEDULE_COLUMNS`.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_schedule_crosswalk
            df = mbb_schedule_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "both").select("espn_game_id", "bart_muid").head()

        See Also:
            * `hoopR`_ -- R sister package this ports

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse.mbb.mbb_schedule import most_recent_mbb_season

    season = int(season) if season is not None else most_recent_mbb_season()
    team_xwalk = mbb_team_crosswalk(season=season, fox=pl.DataFrame(), **kwargs)
    bart_games = bart_super_sked("mbb", season, **kwargs)
    dates = sorted({d for d in bart_games["game_date"].to_list() if d is not None})
    espn_games = espn_scoreboard_games("mbb", dates, **kwargs)
    out = _assemble_schedule_crosswalk(espn_games, bart_games, team_xwalk, season)
    return out.to_pandas() if return_as_pandas else out


def mbb_player_crosswalk(
    season: Optional[int] = None,
    min_confidence: float = 0.92,
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build the MBB cross-source player crosswalk (ESPN / Fox).

    One row per ESPN athlete per team. Fox is matched by normalized name
    within each team block -- exact first (jersey-tiebroken, per hoopR), then
    Jaro-Winkler at or above ``min_confidence``. KenPom and Torvik publish no
    per-player tables, so neither is joined.

    Args:
        season: Season year (e.g. ``2026``). Defaults to the most recent MBB
            season.
        min_confidence: Jaro-Winkler floor for fuzzy matches (R default 0.92).
        return_as_pandas: Return pandas instead of polars.
        **kwargs: Forwarded to the underlying HTTP calls.

    Returns:
        ``pl.DataFrame`` (or pandas), one row per ESPN athlete, 17 columns.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_player_crosswalk
            df = mbb_player_crosswalk(season=2026)
            print(df["match_method"].value_counts())

        Tighten the fuzzy floor::

            strict = mbb_player_crosswalk(season=2026, min_confidence=0.97)

        Pipeline next step (one line)::

            df.filter(pl.col("match_method") == "fuzzy_jw").head()

        See Also:
            * `hoopR`_ -- R sister package this ports

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    from sportsdataverse._crosswalk_basketball_sources import espn_rosters, fox_rosters
    from sportsdataverse.mbb.mbb_schedule import most_recent_mbb_season

    season = int(season) if season is not None else most_recent_mbb_season()
    team_xwalk = mbb_team_crosswalk(season=season, **kwargs)
    frames: List[pl.DataFrame] = []
    for row in team_xwalk.iter_rows(named=True):
        espn = espn_rosters("mbb", row["espn_team_id"], row["espn_abbreviation"], season, **kwargs)
        if espn.height == 0:
            continue
        fox = fox_rosters("mbb", row["espn_team_id"], row["fox_team_id"], **kwargs)
        frames.append(assemble_player_espn_fox(espn, fox, season, min_confidence, exact_tiebreak=True))
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
            exact_tiebreak=True,
        )
    )
    return out.to_pandas() if return_as_pandas else out
