"""Hand-written NBA loaders preserved alongside the generated
``sportsdataverse.nba.nba_loaders``: the ``nba_stats_leaguedash`` release.

Every generated release template in ``tools/codegen/endpoints/releases.yaml``
addresses an asset by season alone (``{tag}/{name}_{season}.parquet``).
``nba_stats_leaguedash`` fans a SECOND axis out across asset families --
``{family}_{season}.parquet``, 833 assets over 36 families as of 2026-09-02 --
so it cannot be expressed as a ``releases.yaml`` row and needs a loader taking
a ``family`` argument. See ``tools/codegen/generate.py``'s
``_GENERATED_LOADER_LEAGUES`` comment for the ``{league}_loaders_extra.py``
convention (cfb / nhl / pwhl / wnba use it too).

The shared :func:`_load_leaguedash` body is imported by
``sportsdataverse.wnba.wnba_loaders_extra`` for the WNBA twin tag, which ships
the same 24 non-tracking families off ``stats.wnba.com``.

The same reason puts the multi-asset ``nba_stats_synergy``, ``nba_stats_hustle``,
``nba_stats_matchups`` and ``nba_stats_draft_combine`` loaders here: each of those
tags publishes several files per season whose discriminators (season type, per
mode, play type, entity) live in the FILE NAME, so one loader unions the files
of a season and stamps any discriminator a file's rows do not already carry.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import warnings
from typing import Dict, Iterable, List, Literal, Optional, Sequence, Tuple, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import (
    SeasonNotFoundError,
    _as_season_list,
    _documented_empty,
    _read_release_parquet,
    cli_warn,
)
from sportsdataverse.config import NBA_STATS_LEAGUEDASH_URL, SDVRELEASES

__all__ = [
    "NBA_STATS_LEAGUEDASH_FAMILIES",
    "NBA_STATS_SYNERGY_PLAY_TYPES",
    "load_nba_stats_draft_combine_anthro",
    "load_nba_stats_draft_combine_drills",
    "load_nba_stats_draft_combine_nonstationary_shooting",
    "load_nba_stats_draft_combine_spot_shooting",
    "load_nba_stats_draft_combine_stats",
    "load_nba_stats_hustle_player",
    "load_nba_stats_hustle_team",
    "load_nba_stats_leaguedash",
    "load_nba_stats_matchups",
    "load_nba_stats_matchups_rollup",
    "load_nba_stats_synergy",
]

#: Every asset family published on the ``nba_stats_leaguedash`` release tag,
#: verified against the live asset listing on 2026-09-02 (833 assets / 36
#: families). Each family is one ``stats.nba.com`` league-dashboard parameter
#: combination, so its column set is family-specific -- ``lineups_*`` key on
#: ``group_id``, ``player_*`` on ``player_id``, ``team_*`` / ``standings`` on
#: ``team_id`` (all ``Int64``).
NBA_STATS_LEAGUEDASH_FAMILIES: Tuple[str, ...] = (
    "lineups_advanced",
    "lineups_base",
    "lineups_fourfactors",
    "lineups_master",
    "lineups_misc",
    "lineups_opponent",
    "lineups_scoring",
    "player_bio",
    "player_master",
    "player_stats_advanced",
    "player_stats_base",
    "player_stats_defense",
    "player_stats_misc",
    "player_stats_scoring",
    "player_stats_usage",
    "player_tracking_catchshoot",
    "player_tracking_defense",
    "player_tracking_drives",
    "player_tracking_efficiency",
    "player_tracking_elbowtouch",
    "player_tracking_painttouch",
    "player_tracking_passing",
    "player_tracking_possessions",
    "player_tracking_posttouch",
    "player_tracking_pullupshot",
    "player_tracking_rebounding",
    "player_tracking_speeddistance",
    "standings",
    "team_master",
    "team_stats_advanced",
    "team_stats_base",
    "team_stats_defense",
    "team_stats_fourfactors",
    "team_stats_misc",
    "team_stats_opponent",
    "team_stats_scoring",
)

# Earliest season published on the tag (``standings_1996.parquet``). Per-family
# floors differ a lot -- lineups start 2008, most tracking families 2014 -- but
# they are NOT enforced per family on purpose: the producer backfills, and a
# hard-coded per-family floor would start rejecting seasons that exist. A
# season the tag does not carry is warned about and skipped, the same contract
# the generated loaders use. That is not hypothetical: on 2026-09-02
# ``player_tracking_catchshoot`` covers 1997-2026 with a real hole at 2002.
_NBA_MIN_SEASON = 1996


def _load_leaguedash(
    fn_name: str,
    url_template: str,
    families: Sequence[str],
    family: str,
    seasons: int | Iterable[int],
    min_season: int,
    return_as_pandas: bool,
) -> pl.DataFrame | pd.DataFrame:
    """Season-loop a single ``{family}_{season}.parquet`` asset family.

    Shared by the NBA and WNBA ``*_stats_leaguedash`` loaders -- the two tags
    have identical asset naming and differ only in host league and family list.

    Args:
        fn_name: public caller's name, used in the missing-season warning.
        url_template: release URL with ``{family}`` and ``{season}`` fields.
        families: the tag's valid families, for validation + the error message.
        family: the requested family.
        seasons: an int or an iterable of seasons.
        min_season: earliest season published on the tag.
        return_as_pandas: return pandas instead of polars.

    Returns:
        One frame per requested season concatenated ``diagonal_relaxed``;
        an empty frame when no requested season is published.

    Raises:
        ValueError: if ``family`` is not one of ``families``.
        SeasonNotFoundError: if any requested season is below ``min_season``.
    """
    if family not in families:
        raise ValueError(f"{fn_name}: unknown family {family!r}; expected one of {', '.join(families)}")
    frames, missing = [], []
    for season in _as_season_list(seasons):
        if int(season) < min_season:
            raise SeasonNotFoundError(f"season cannot be less than {min_season}")
        df = _read_release_parquet(url_template.format(family=family, season=season))
        if df is None:
            missing.append(season)
            continue
        frames.append(df)
    if missing:
        cli_warn(f"{fn_name}: no {family} data for season(s) {missing} (skipped)")
    # diagonal_relaxed: a family's column set drifts across seasons (stats.nba.com
    # added measures over time) -- union columns, null-fill gaps.
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


@overload
def load_nba_stats_leaguedash(
    family: str,
    seasons: int | Iterable[int],
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def load_nba_stats_leaguedash(
    family: str,
    seasons: int | Iterable[int],
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def load_nba_stats_leaguedash(
    family: str,
    seasons: int | Iterable[int],
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load one asset family of the ``nba_stats_leaguedash`` release.

    ``nba_stats_leaguedash`` is a parameter cube: one asset per
    (family, season) pair rather than one per season, so a family must be named.
    The valid families are exported as
    :data:`NBA_STATS_LEAGUEDASH_FAMILIES` -- import that tuple to discover them
    rather than passing a bare string; an unknown family raises ``ValueError``
    listing every valid value.

    Column sets are family-specific (a ``lineups_*`` frame keys on ``group_id``,
    a ``player_*`` frame on ``player_id``), so this loader documents no fixed
    returns table. ``player_id`` / ``team_id`` are ``Int64`` in every family and
    season, so cross-family joins need no dtype reconciliation.

    Args:
        family (str): Asset family, e.g. ``"player_stats_advanced"``. Must be one
            of :data:`NBA_STATS_LEAGUEDASH_FAMILIES`.
        seasons (int | Iterable[int]): Season, or iterable of seasons, to load.
            Seasons are END years (``2024`` = the 2023-24 NBA season). 1996 is the
            earliest season on the tag; per-family coverage starts later
            (``lineups_*`` 2008, most ``player_tracking_*`` 2014). A requested
            season the family does not publish is warned about and skipped, not
            an error.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe with one row per player / team / lineup
        per requested season for the requested family; an empty frame when no
        requested season is published.

    Raises:
        ValueError: If `family` is not one of :data:`NBA_STATS_LEAGUEDASH_FAMILIES`.
        SeasonNotFoundError: If any requested season is less than 1996.
        AssetFetchError: If a release asset request fails (a 403 or an exhausted
            retry budget) -- distinct from an absent season, which is skipped.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_leaguedash
            adv = load_nba_stats_leaguedash("player_stats_advanced", seasons=2024)
            print(adv.shape)

        Discover the valid families::

            from sportsdataverse.nba import NBA_STATS_LEAGUEDASH_FAMILIES
            print([f for f in NBA_STATS_LEAGUEDASH_FAMILIES if f.startswith("player_tracking_")])

        Multi-season, pandas round-trip::

            drives_pd = load_nba_stats_leaguedash(
                "player_tracking_drives", seasons=range(2020, 2025), return_as_pandas=True
            )

        Pipeline next step (top usage rates in 2024)::

            import polars as pl
            usage = load_nba_stats_leaguedash("player_stats_usage", seasons=2024)
            usage.sort("usg_pct", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com surface
    """
    return _load_leaguedash(
        "load_nba_stats_leaguedash",
        NBA_STATS_LEAGUEDASH_URL,
        NBA_STATS_LEAGUEDASH_FAMILIES,
        family,
        seasons,
        _NBA_MIN_SEASON,
        return_as_pandas,
    )


# --------------------------------------------------------------------------
# Multi-asset nba_stats tags: several files per season, discriminated by name.
# --------------------------------------------------------------------------

_SEASON_TYPES: Tuple[str, ...] = ("regular-season", "playoffs")
_PER_MODES: Tuple[str, ...] = ("pergame", "totals")

#: The Synergy play types published on ``nba_stats_synergy``, by type grouping
#: (verified against the live asset listing on 2026-10-11: 72 assets per season =
#: 2 season types x (11 offensive + 7 defensive) x 2 per modes). Cut, misc,
#: offensive rebound and transition have no defensive split on stats.nba.com.
NBA_STATS_SYNERGY_PLAY_TYPES: Dict[str, Tuple[str, ...]] = {
    "offensive": (
        "cut",
        "handoff",
        "isolation",
        "misc",
        "offrebound",
        "offscreen",
        "postup",
        "prballhandler",
        "prrollman",
        "spotup",
        "transition",
    ),
    "defensive": ("handoff", "isolation", "offscreen", "postup", "prballhandler", "prrollman", "spotup"),
}

_Choice = Union[str, Iterable[str], None]
_Asset = Tuple[str, Dict[str, str]]


def _pick(name: str, value: _Choice, valid: Sequence[str]) -> Tuple[str, ...]:
    """``None`` -> every valid value; a str or iterable -> those values, validated."""
    if value is None:
        return tuple(valid)
    # dict.fromkeys de-duplicates in order, so per_mode=["totals", "totals"] reads once
    picked = tuple(dict.fromkeys((value,) if isinstance(value, str) else value))
    bad = [v for v in picked if v not in valid]
    if bad or not picked:
        raise ValueError(f"unknown {name} {bad or picked!r}; expected one of {', '.join(valid)}")
    return picked


def _load_season_assets(
    fn_name: str,
    tag: str,
    assets: Sequence[_Asset],
    seasons: int | Iterable[int],
    min_season: int,
    return_as_pandas: bool,
) -> pl.DataFrame | pd.DataFrame:
    """Read every ``{tag}/{stem}_{END year}.parquet`` asset of each season and union them.

    ``seasons`` are START years (``2024`` = 2024-25), like the generated
    ``load_nba_stats_*`` loaders; the asset is keyed by the END year, so the
    translation is ``season + 1``. Each asset's ``stamps`` (the discriminators its
    file name carries) are added as columns only when the file's rows lack them,
    and ``season`` is stamped with the END year on the same rule -- so the frame
    always says which file a row came from, in the publisher's own vocabulary.

    A season with no asset at all is warned about and skipped (404-safe, the
    generated-loader contract). A single absent asset inside a published season
    (e.g. no playoffs file yet) is skipped silently. A failed fetch still raises.
    """
    season_list = _as_season_list(seasons)
    for season in season_list:
        if int(season) < min_season:
            raise SeasonNotFoundError(f"season cannot be less than {min_season}")
    jobs = [(int(s), stem, stamps) for s in season_list for stem, stamps in assets]

    def read(job: Tuple[int, str, Dict[str, str]]) -> Optional[pl.DataFrame]:
        season, stem, stamps = job
        df = _read_release_parquet(f"{SDVRELEASES}{tag}/{stem}_{season + 1}.parquet")
        if df is None:
            return None
        add = [pl.lit(v, dtype=pl.String).alias(k) for k, v in stamps.items() if k not in df.columns]
        if "season" not in df.columns:
            add.append(pl.lit(season + 1, dtype=pl.Int64).alias("season"))
        return df.with_columns(add) if add else df

    # ponytail: fixed 8-thread fan-out (I/O bound, ~0.3 s per asset; a synergy
    # season is 72 assets). Expose a knob if GitHub ever throttles release reads.
    with ThreadPoolExecutor(max_workers=max(1, min(8, len(jobs)))) as pool:
        results = list(pool.map(read, jobs))
    found = {job[0] for job, df in zip(jobs, results) if df is not None}
    missing = [s for s in season_list if int(s) not in found]
    if missing:
        cli_warn(f"{fn_name}: no data for season(s) {missing} (skipped)")
    frames: List[pl.DataFrame] = [df for df in results if df is not None]
    # diagonal_relaxed: per-season (and per-mode) column sets and dtypes drift --
    # union columns, null-fill gaps, widen to the common supertype.
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out


@_documented_empty("autodoc/nba/load_nba_stats_synergy")
def load_nba_stats_synergy(
    seasons: int | Iterable[int],
    season_type: _Choice = None,
    per_mode: _Choice = None,
    type_grouping: _Choice = None,
    play_type: _Choice = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load Synergy play-type stats (the ``nba_stats_synergy`` release).

    ``nba_stats_synergy`` publishes 72 files per season, one per
    (season type, play type, offensive/defensive, per mode) combination, named
    ``{season_type}_{play_type}_{type_grouping}_{per_mode}_{END year}.parquet``.
    This loader reads the requested combinations (all of them by default) and
    unions them into one frame, one row per player per play type per split.
    ``season_type`` and ``per_mode`` are stamped from the file name when a file's
    rows do not carry them, so every row says which file it came from.

    Args:
        seasons (int | Iterable[int]): Season START year(s), e.g. ``2024`` for
            2024-25 (the asset is keyed by the END year; the loader translates).
            2015 (2015-16) is the earliest season on the tag.
        season_type (str | Iterable[str] | None): ``"regular-season"`` and/or
            ``"playoffs"``; ``None`` reads both.
        per_mode (str | Iterable[str] | None): ``"pergame"`` and/or ``"totals"``;
            ``None`` reads both. Counting columns (``poss``, ``pts``, ``fgm``,
            ``fga``, ``fgmx``) are Int64 in ``totals`` files and Float64 in
            ``pergame`` files, so reading both widens them to Float64.
        type_grouping (str | Iterable[str] | None): ``"offensive"`` and/or
            ``"defensive"``; ``None`` reads both.
        play_type (str | Iterable[str] | None): Play type(s) from
            :data:`NBA_STATS_SYNERGY_PLAY_TYPES` (e.g. ``"isolation"``);
            ``None`` reads every play type of each requested type grouping.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per player per play type per type grouping per
        season type per per mode. ``season`` is the END year (``2025`` for 2024-25), the published
        assets' own stamp; ``season_id`` is stats.nba.com's code (``22024``).
        An empty frame when no requested season is published.

    Raises:
        ValueError: If a filter value is unknown, or if ``play_type`` and
            ``type_grouping`` select no published file (e.g. a defensive ``cut``).
        SeasonNotFoundError: If any requested season is below 2015.
        AssetFetchError: If a release asset request fails (a 403 or an exhausted
            retry budget) -- distinct from an absent season, which is skipped.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_synergy
            iso = load_nba_stats_synergy(2024, play_type="isolation", per_mode="totals")
            print(iso.shape)

        Every play type for one split::

            off = load_nba_stats_synergy(2024, season_type="regular-season", type_grouping="offensive")

        Pipeline next step (most efficient isolation scorers)::

            import polars as pl
            iso.filter(pl.col("poss") >= 100).sort("ppp", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com synergyplaytypes endpoint
    """
    season_types = _pick("season_type", season_type, _SEASON_TYPES)
    per_modes = _pick("per_mode", per_mode, _PER_MODES)
    groupings = _pick("type_grouping", type_grouping, tuple(NBA_STATS_SYNERGY_PLAY_TYPES))
    every_play_type = sorted({p for ps in NBA_STATS_SYNERGY_PLAY_TYPES.values() for p in ps})
    play_types = _pick("play_type", play_type, every_play_type)
    assets: List[_Asset] = [
        (f"{st}_{pt}_{tg}_{pm}", {"season_type": st, "per_mode": pm})
        for st in season_types
        for tg in groupings
        for pt in play_types
        if pt in NBA_STATS_SYNERGY_PLAY_TYPES[tg]
        for pm in per_modes
    ]
    if not assets:
        raise ValueError(f"load_nba_stats_synergy: no {groupings} split is published for play_type {play_types}")
    # Only when the caller named BOTH: play_type alone means "wherever it is published".
    if play_type is not None and type_grouping is not None:
        unpublished = [
            f"{pt}/{tg}" for tg in groupings for pt in play_types if pt not in NBA_STATS_SYNERGY_PLAY_TYPES[tg]
        ]
        if unpublished:
            warnings.warn(
                f"load_nba_stats_synergy: no published split for {', '.join(unpublished)}; skipped",
                stacklevel=2,
            )
    return _load_season_assets("load_nba_stats_synergy", "nba_stats_synergy", assets, seasons, 2015, return_as_pandas)


def _hustle(
    entity: str, seasons: int | Iterable[int], season_type: _Choice, per_mode: _Choice, return_as_pandas: bool
) -> pl.DataFrame | pd.DataFrame:
    assets: List[_Asset] = [
        (f"leaguehustlestats{entity}_{st}_{pm}", {"season_type": st, "per_mode": pm})
        for st in _pick("season_type", season_type, _SEASON_TYPES)
        for pm in _pick("per_mode", per_mode, _PER_MODES)
    ]
    return _load_season_assets(
        f"load_nba_stats_hustle_{entity}", "nba_stats_hustle", assets, seasons, 2015, return_as_pandas
    )


@_documented_empty("autodoc/nba/load_nba_stats_hustle_player")
def load_nba_stats_hustle_player(
    seasons: int | Iterable[int],
    season_type: _Choice = None,
    per_mode: _Choice = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load player hustle stats (the ``nba_stats_hustle`` release, player files).

    ``nba_stats_hustle`` publishes eight files per season,
    ``leaguehustlestats{player|team}_{season_type}_{per_mode}_{END year}.parquet``.
    Player and team files have different columns, so each entity has its own
    loader; this one unions the four player files (or the requested subset).

    Args:
        seasons (int | Iterable[int]): Season START year(s), e.g. ``2024`` for
            2024-25 (the asset is keyed by the END year; the loader translates).
            2015 (2015-16) is the earliest season on the tag.
        season_type (str | Iterable[str] | None): ``"regular-season"`` and/or
            ``"playoffs"``; ``None`` reads both.
        per_mode (str | Iterable[str] | None): ``"pergame"`` and/or ``"totals"``;
            ``None`` reads both. Counting columns are Int64 in ``totals`` files and
            Float64 in ``pergame`` files, so reading both widens them to Float64.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per player per season type per per mode, with
        ``season`` as the END year. An empty frame when no requested season is
        published.

    Raises:
        ValueError: If ``season_type`` or ``per_mode`` is unknown.
        SeasonNotFoundError: If any requested season is below 2015.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_hustle_player
            hustle = load_nba_stats_hustle_player(2024, season_type="regular-season", per_mode="totals")
            print(hustle.shape)

        Pipeline next step (league leaders in deflections)::

            hustle.sort("deflections", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com leaguehustlestatsplayer endpoint
    """
    return _hustle("player", seasons, season_type, per_mode, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_hustle_team")
def load_nba_stats_hustle_team(
    seasons: int | Iterable[int],
    season_type: _Choice = None,
    per_mode: _Choice = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load team hustle stats (the ``nba_stats_hustle`` release, team files).

    The team half of ``nba_stats_hustle``: unions the four
    ``leaguehustlestatsteam_{season_type}_{per_mode}_{END year}.parquet`` files
    (or the requested subset). See :func:`load_nba_stats_hustle_player` for the
    player files.

    Args:
        seasons (int | Iterable[int]): Season START year(s), e.g. ``2024`` for
            2024-25 (the asset is keyed by the END year; the loader translates).
            2015 (2015-16) is the earliest season on the tag.
        season_type (str | Iterable[str] | None): ``"regular-season"`` and/or
            ``"playoffs"``; ``None`` reads both.
        per_mode (str | Iterable[str] | None): ``"pergame"`` and/or ``"totals"``;
            ``None`` reads both. Counting columns are Int64 in ``totals`` files and
            Float64 in ``pergame`` files, so reading both widens them to Float64.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per team per season type per per mode, with
        ``season`` as the END year. An empty frame when no requested season is
        published.

    Raises:
        ValueError: If ``season_type`` or ``per_mode`` is unknown.
        SeasonNotFoundError: If any requested season is below 2015.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_hustle_team
            teams = load_nba_stats_hustle_team(2024, season_type="regular-season", per_mode="pergame")
            print(teams.shape)

        Pipeline next step (most screen assists per game)::

            teams.sort("screen_assists", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com leaguehustlestatsteam endpoint
    """
    return _hustle("team", seasons, season_type, per_mode, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_matchups")
def load_nba_stats_matchups(
    seasons: int | Iterable[int],
    per_mode: _Choice = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load offensive-vs-defensive player matchups (``nba_stats_matchups``, season files).

    Reads ``leagueseasonmatchups_regular-season_{per_mode}_{END year}.parquet``
    from ``nba_stats_matchups``: one row per (offensive player, defensive player)
    pair. The tag publishes this table for the regular season only; the
    position roll-up lives in :func:`load_nba_stats_matchups_rollup`.

    Args:
        seasons (int | Iterable[int]): Season START year(s), e.g. ``2024`` for
            2024-25 (the asset is keyed by the END year; the loader translates).
            2017 (2017-18) is the earliest season on the tag.
        per_mode (str | Iterable[str] | None): ``"pergame"`` and/or ``"totals"``;
            ``None`` reads both. ``matchup_min`` is ``"MM:SS"`` text in
            ``totals`` files and decimal minutes in ``pergame`` files, so reading
            both makes it String; counting columns widen to Float64. Pass one
            ``per_mode`` to keep the native dtypes.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per matchup pair per per mode (about 140,000 per
        season and per mode). ``season`` is the END year and ``season_id``
        stats.nba.com's code (``22024``). An empty frame when no requested season
        is published.

    Raises:
        ValueError: If ``per_mode`` is unknown.
        SeasonNotFoundError: If any requested season is below 2017.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_matchups
            pairs = load_nba_stats_matchups(2024, per_mode="totals")
            print(pairs.shape)

        Pipeline next step (who guarded a scorer the most)::

            import polars as pl
            pairs.filter(pl.col("off_player_id") == 1628983).sort("partial_poss", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com leagueseasonmatchups endpoint
    """
    assets: List[_Asset] = [
        (f"leagueseasonmatchups_regular-season_{pm}", {"season_type": "regular-season", "per_mode": pm})
        for pm in _pick("per_mode", per_mode, _PER_MODES)
    ]
    return _load_season_assets("load_nba_stats_matchups", "nba_stats_matchups", assets, seasons, 2017, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_matchups_rollup")
def load_nba_stats_matchups_rollup(
    seasons: int | Iterable[int],
    season_type: _Choice = None,
    per_mode: _Choice = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load defensive matchup roll-ups by position (``nba_stats_matchups``, rollup files).

    Reads ``matchupsrollup_{season_type}_{per_mode}_{END year}.parquet`` from
    ``nba_stats_matchups``: one row per defender per offensive position
    (``G``/``F``/``C``) plus a ``TOTAL`` row. Pair-level rows are in
    :func:`load_nba_stats_matchups`.

    Args:
        seasons (int | Iterable[int]): Season START year(s), e.g. ``2024`` for
            2024-25 (the asset is keyed by the END year; the loader translates).
            2017 (2017-18) is the earliest season on the tag.
        season_type (str | Iterable[str] | None): ``"regular-season"`` and/or
            ``"playoffs"``; ``None`` reads both.
        per_mode (str | Iterable[str] | None): ``"pergame"`` and/or ``"totals"``;
            ``None`` reads both. ``matchup_min`` is ``"MM:SS"`` text in
            ``totals`` files and decimal minutes in ``pergame`` files, so reading
            both makes it String; counting columns widen to Float64.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per defender per position per season type per per
        mode, with ``season`` as the END year. An empty frame when no requested
        season is published.

    Raises:
        ValueError: If ``season_type`` or ``per_mode`` is unknown.
        SeasonNotFoundError: If any requested season is below 2017.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_matchups_rollup
            rollup = load_nba_stats_matchups_rollup(2024, season_type="playoffs", per_mode="totals")
            print(rollup.shape)

        Pipeline next step (share of time each defender spent on guards)::

            import polars as pl
            rollup.filter(pl.col("position") == "G").sort("percent_of_time", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com matchupsrollup endpoint
    """
    assets: List[_Asset] = [
        (f"matchupsrollup_{st}_{pm}", {"season_type": st, "per_mode": pm})
        for st in _pick("season_type", season_type, _SEASON_TYPES)
        for pm in _pick("per_mode", per_mode, _PER_MODES)
    ]
    return _load_season_assets(
        "load_nba_stats_matchups_rollup", "nba_stats_matchups", assets, seasons, 2017, return_as_pandas
    )


# Draft-combine asset Y holds the combine held in May of Y-1 (``..._2025`` = the
# 2024 combine), the same START -> END translation as every nba_stats loader.
_COMBINE_MIN_SEASON = 2000


def _combine(
    stem: str, fn_name: str, seasons: int | Iterable[int], return_as_pandas: bool
) -> pl.DataFrame | pd.DataFrame:
    return _load_season_assets(
        fn_name, "nba_stats_draft_combine", [(stem, {})], seasons, _COMBINE_MIN_SEASON, return_as_pandas
    )


@_documented_empty("autodoc/nba/load_nba_stats_draft_combine_stats")
def load_nba_stats_draft_combine_stats(
    seasons: int | Iterable[int], return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load the draft-combine summary table (``nba_stats_draft_combine``, ``draftcombinestats``).

    One row per combine participant: measurements, athletic testing and the
    shooting drills summarised as ``"made-attempted"`` text.

    Args:
        seasons (int | Iterable[int]): Draft year(s) of the combine, e.g. ``2024``
            for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's
            START year; the asset is keyed by the END year and the loader
            translates). 2000 is the earliest combine on the tag.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per participant; ``season`` is the asset's END year
        (``2025`` for the 2024 combine). An empty frame when no requested season
        is published.

    Raises:
        SeasonNotFoundError: If any requested season is below 2000.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_draft_combine_stats
            combine = load_nba_stats_draft_combine_stats(2024)
            print(combine.shape)

        Pipeline next step (longest wingspans)::

            combine.sort("wingspan", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com draftcombinestats endpoint
    """
    return _combine("draftcombinestats", "load_nba_stats_draft_combine_stats", seasons, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_draft_combine_anthro")
def load_nba_stats_draft_combine_anthro(
    seasons: int | Iterable[int], return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load draft-combine anthropometric measurements (``draftcombineplayeranthro``).

    One row per combine participant: height with and without shoes, weight,
    wingspan, standing reach, body fat and hand size.

    Args:
        seasons (int | Iterable[int]): Draft year(s) of the combine, e.g. ``2024``
            for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's
            START year; the asset is keyed by the END year and the loader
            translates). 2000 is the earliest combine on the tag.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per participant; ``season`` is the asset's END year.
        An empty frame when no requested season is published.

    Raises:
        SeasonNotFoundError: If any requested season is below 2000.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_draft_combine_anthro
            anthro = load_nba_stats_draft_combine_anthro(2024)
            print(anthro.shape)

        Pipeline next step (largest hands)::

            anthro.sort("hand_length", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com draftcombineplayeranthro endpoint
    """
    return _combine("draftcombineplayeranthro", "load_nba_stats_draft_combine_anthro", seasons, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_draft_combine_drills")
def load_nba_stats_draft_combine_drills(
    seasons: int | Iterable[int], return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load draft-combine athletic drill results (``draftcombinedrillresults``).

    One row per combine participant: vertical leaps, lane agility, the
    three-quarter-court sprint and the bench press.

    Args:
        seasons (int | Iterable[int]): Draft year(s) of the combine, e.g. ``2024``
            for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's
            START year; the asset is keyed by the END year and the loader
            translates). 2000 is the earliest combine on the tag.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per participant; ``season`` is the asset's END year.
        An empty frame when no requested season is published.

    Raises:
        SeasonNotFoundError: If any requested season is below 2000.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_draft_combine_drills
            drills = load_nba_stats_draft_combine_drills(2024)
            print(drills.shape)

        Pipeline next step (highest max vertical)::

            drills.sort("max_vertical_leap", descending=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com draftcombinedrillresults endpoint
    """
    return _combine("draftcombinedrillresults", "load_nba_stats_draft_combine_drills", seasons, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_draft_combine_spot_shooting")
def load_nba_stats_draft_combine_spot_shooting(
    seasons: int | Iterable[int], return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load draft-combine spot-up shooting drills (``draftcombinespotshooting``).

    One row per combine participant: made, attempted and percentage from five
    floor spots at 15 feet, the college three and the NBA three. Spots a year's
    combine did not run are null.

    Args:
        seasons (int | Iterable[int]): Draft year(s) of the combine, e.g. ``2024``
            for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's
            START year; the asset is keyed by the END year and the loader
            translates). 2000 is the earliest combine on the tag.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per participant; ``season`` is the asset's END year.
        An empty frame when no requested season is published.

    Raises:
        SeasonNotFoundError: If any requested season is below 2000.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_draft_combine_spot_shooting
            spot = load_nba_stats_draft_combine_spot_shooting(2024)
            print(spot.shape)

        Pipeline next step (best corner-three shooters)::

            spot.sort("college_corner_left_pct", descending=True, nulls_last=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com draftcombinespotshooting endpoint
    """
    return _combine("draftcombinespotshooting", "load_nba_stats_draft_combine_spot_shooting", seasons, return_as_pandas)


@_documented_empty("autodoc/nba/load_nba_stats_draft_combine_nonstationary_shooting")
def load_nba_stats_draft_combine_nonstationary_shooting(
    seasons: int | Iterable[int], return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Load draft-combine off-the-dribble and on-the-move shooting (``draftcombinenonstationaryshooting``).

    One row per combine participant: made, attempted and percentage for the
    off-the-dribble (15 feet and college three, three spots) and on-the-move
    drills. Drills a year's combine did not run are null.

    Args:
        seasons (int | Iterable[int]): Draft year(s) of the combine, e.g. ``2024``
            for the May 2024 combine ahead of the 2024 draft (the 2024-25 season's
            START year; the asset is keyed by the END year and the loader
            translates). 2000 is the earliest combine on the tag.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per participant; ``season`` is the asset's END year.
        An empty frame when no requested season is published.

    Raises:
        SeasonNotFoundError: If any requested season is below 2000.
        AssetFetchError: If a release asset request fails.

    Example:
        Quick start::

            from sportsdataverse.nba import load_nba_stats_draft_combine_nonstationary_shooting
            moving = load_nba_stats_draft_combine_nonstationary_shooting(2024)
            print(moving.shape)

        Pipeline next step (best on-the-move college-three shooters)::

            moving.sort("on_move_college_pct", descending=True, nulls_last=True).head()

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- R sister package for the same release
            * `nba_api <https://github.com/swar/nba_api>`_ -- the underlying stats.nba.com draftcombinenonstationaryshooting endpoint
    """
    return _combine(
        "draftcombinenonstationaryshooting",
        "load_nba_stats_draft_combine_nonstationary_shooting",
        seasons,
        return_as_pandas,
    )
