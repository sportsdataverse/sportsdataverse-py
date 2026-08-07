from __future__ import annotations

from typing import List

import polars as pl
from tqdm import tqdm

from sportsdataverse._deprecation import warn_deprecated as _warn_deprecated
from sportsdataverse.config import (
    NFL_BASE_URL,
    NFL_COMBINE_URL,
    NFL_CONTRACTS_URL,
    NFL_DEPTH_CHARTS_URL,
    NFL_DRAFT_PICKS_URL,
    NFL_ESPN_QBR_SEASON_URL,
    NFL_ESPN_QBR_WEEK_URL,
    NFL_FF_OPPORTUNITY_URL,
    NFL_FF_PLAYERIDS_URL,
    NFL_FF_RANKINGS_ALL_URL,
    NFL_FF_RANKINGS_DRAFT_URL,
    NFL_FF_RANKINGS_WEEK_URL,
    NFL_FTN_CHARTING_URL,
    NFL_INJURIES_URL,
    NFL_MODEL_PBP_URL,
    NFL_NGS_PASSING_URL,
    NFL_NGS_RECEIVING_URL,
    NFL_NGS_RUSHING_URL,
    NFL_OFFICIALS_URL,
    NFL_PBP_PARTICIPATION_URL,
    NFL_PFR_SEASON_DEF_URL,
    NFL_PFR_SEASON_PASS_URL,
    NFL_PFR_SEASON_REC_URL,
    NFL_PFR_SEASON_RUSH_URL,
    NFL_PFR_WEEK_DEF_URL,
    NFL_PFR_WEEK_PASS_URL,
    NFL_PFR_WEEK_REC_URL,
    NFL_PFR_WEEK_RUSH_URL,
    NFL_PLAYER_KICKING_STATS_URL,
    NFL_PLAYER_STATS_URL,
    NFL_PLAYER_URL,
    NFL_RATINGS_WEEKLY_URL,
    NFL_ROSTER_URL,
    NFL_SDV_ESPN_QBR_SEASON_URL,
    NFL_SDV_ESPN_QBR_WEEK_URL,
    NFL_SDV_PLAYER_STATS_URL,
    NFL_SDV_PLAYER_URL,
    NFL_SDV_ROSTER_URL,
    NFL_SDV_TEAM_STATS_URL,
    NFL_SNAP_COUNTS_URL,
    NFL_TEAM_LOGO_URL,
    NFL_TEAM_SCHEDULE_URL,
    NFL_TEAM_STATS_URL,
    NFL_TRADES_URL,
    NFL_WEEKLY_ROSTER_URL,
)
from sportsdataverse.errors import season_not_found_error
from sportsdataverse.nfl.cache import cached_loader


@cached_loader
def load_nfl_pbp(seasons: List[int], return_as_pandas=False, *, source: str = "nflverse") -> pl.DataFrame:
    """Load NFL play by play data going back to 1999

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.
        source (str): Which enriched play-by-play release to read. ``"nflverse"`` (the
            default, also accepts ``None``) returns the nflverse/nflfastR
            ``play_by_play_{season}.parquet`` releases — full history from 1999,
            unchanged behavior. ``"sportsdataverse"`` / ``"sdv"`` returns the
            SDV-native ``nfl_model_pbp`` release: a Python-built, nflfastR-faithful
            enriched frame (ep/epa, wp/wpa/vegas_wp, cp/cpoe, xyac_*/air_epa) that
            covers the published seasons (2023+) and drops administrative / timeout
            rows for a clean modeling subset. Any other value raises ``ValueError``.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the play-by-plays available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999, or `source` is not one of
            ``"nflverse"``, ``None``, ``"sportsdataverse"``, or ``"sdv"``.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            pbp = load_nfl_pbp(seasons=[2024])
            print(pbp.shape)

        Multi-season range::

            pbp = load_nfl_pbp(seasons=range(2020, 2025))

        SDV-native enriched play-by-play (Python-built, nflfastR-faithful
        EP/WP/CP/xYAC; published seasons only; admin/timeout rows dropped)::

            pbp_sdv = load_nfl_pbp(seasons=[2024], source="sdv")
            pbp_sdv.select(["ep", "epa", "wp", "wpa", "cp", "cpoe", "xyac_epa"]).head()

        With cache off (development workflow)::

            from sportsdataverse.nfl import load_nfl_pbp, update_config
            update_config(cache_mode="off")
            pbp = load_nfl_pbp(seasons=[2024])

        Pandas round-trip::

            pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
            pbp_pd.head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings
            * `nflfastR`_ -- R sister package for NFL PBP

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
        .. _nflfastR: https://www.nflfastr.com
    """
    if source in ("nflverse", None):
        base_url = NFL_BASE_URL
    elif source in ("sportsdataverse", "sdv"):
        base_url = NFL_MODEL_PBP_URL
    else:
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1999)
        i_data = pl.read_parquet(base_url.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL schedule data

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing the schedule for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_schedule
            schedule = load_nfl_schedule(seasons=[2024])
            schedule.shape

        Multi-season range::

            schedule = load_nfl_schedule(seasons=range(2020, 2025))

        Filter to a single week::

            import polars as pl
            week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

        Pandas round-trip::

            schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
            schedule_pd[["game_id", "home_team", "away_team", "week"]].head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if type(seasons) is int:
        seasons = [seasons]
    for i in seasons:
        season_not_found_error(int(i), 1999)
    # Upstream (nflverse-data `schedules/games`) is a single combined parquet
    # covering all seasons (1999-present). Read once and post-filter by season.
    data = pl.read_parquet(NFL_TEAM_SCHEDULE_URL, use_pyarrow=True, columns=None)
    if "season" in data.columns and seasons:
        season_ints = [int(s) for s in seasons]
        data = data.filter(pl.col("season").is_in(season_ints))
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_player_stats(kicking=False, return_as_pandas=False, *, source: str = "nflverse") -> pl.DataFrame:
    """Load NFL player stats data

    One combined week-level parquet (all seasons, offense) mirroring nflverse's
    ``player_stats``.

    Args:
        kicking (bool): If True, load kicking stats. If False, load all other stats.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.
        source (str): Which player-stats release to read.
            ``"nflverse"`` (the default, also accepts ``None``) returns the
            nflverse published ``player_stats.parquet``. ``"sportsdataverse"`` /
            ``"sdv"`` returns the SDV-native ``nfl_player_stats`` release built by
            :func:`sportsdataverse.nfl.build_nfl_player_stats` from SDV-native
            play-by-play (1999-present, week-level, REG+POST). Any other value
            raises ``ValueError``.

    Returns:
        pl.DataFrame: Polars dataframe containing player stats.

    Raises:
        ValueError: If ``source`` is not one of ``"nflverse"``, ``None``,
            ``"sportsdataverse"``, or ``"sdv"``; or if ``kicking=True`` is
            combined with the SDV source (the SDV play-by-play surface lacks a
            ``kicker_player_id``, so kicking stats are nflverse-only).

    Note:
        ``kicking=True`` is **not available** for the SDV source: the SDV-native
        play-by-play does not carry ``kicker_player_id``, so kicking aggregates
        cannot be rebuilt. Request kicking stats from the default nflverse source
        instead.

    Example:
        Quick start (offense / defense / special teams)::

            from sportsdataverse.nfl import load_nfl_player_stats
            stats = load_nfl_player_stats()
            stats.shape

        SDV-native player stats (week-level, built from SDV play-by-play)::

            stats_sdv = load_nfl_player_stats(source="sdv")
            stats_sdv.select(["season", "week", "player_id", "attempts"]).head()

        Kicking-only stats (nflverse source only)::

            kicking = load_nfl_player_stats(kicking=True)

        Filter to a single season after load::

            import polars as pl
            stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if source in ("nflverse", None):
        if kicking is False:
            url = NFL_PLAYER_STATS_URL
        else:
            url = NFL_PLAYER_KICKING_STATS_URL
    elif source in ("sportsdataverse", "sdv"):
        if kicking is True:
            raise ValueError(
                "kicking=True is not available for source='sdv': the SDV-native "
                "play-by-play lacks kicker_player_id, so kicking stats cannot be "
                "rebuilt. Use the default source='nflverse' for kicking stats."
            )
        url = NFL_SDV_PLAYER_STATS_URL
    else:
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")

    data = pl.read_parquet(url, use_pyarrow=True, columns=None)
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


# NextGen Stats URL dispatch table. Used by load_nfl_nextgen_stats and the
# 3 deprecated per-stat-type aliases.
_NFL_NGS_URLS = {
    "passing": NFL_NGS_PASSING_URL,
    "rushing": NFL_NGS_RUSHING_URL,
    "receiving": NFL_NGS_RECEIVING_URL,
}


@cached_loader
def load_nfl_nextgen_stats(
    seasons: List[int],
    stat_type: str = "passing",
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Load NFL NextGen Stats data going back to 2016.

    Unified loader that consolidates the per-stat-type NextGen Stats
    accessors. Mirrors the API surface of nflreadpy's
    ``load_nextgen_stats`` so downstream code can swap engines without
    changing call sites.

    Args:
        seasons (list[int]): Seasons to filter to. The upstream parquet
            covers a single combined file per stat type — ``seasons`` is
            applied as a post-filter on the ``season`` column.
        stat_type (str): One of ``"passing"``, ``"rushing"``,
            ``"receiving"``. Defaults to ``"passing"``.
        return_as_pandas (bool): If True, returns a pandas dataframe.
            If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing NextGen Stats data
            for the requested ``stat_type`` and ``seasons``.

    Raises:
        ValueError: If ``stat_type`` is not one of the allowed values.

    Example:
        Passing NextGen stats (default)::

            from sportsdataverse.nfl import load_nfl_nextgen_stats
            ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

        Rushing NextGen stats::

            ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

        Receiving NextGen stats with a follow-up filter::

            import polars as pl
            ngs_rec = (
                load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
                .filter(pl.col("week") > 0)
            )

        Pandas round-trip::

            ngs_pd = load_nfl_nextgen_stats(
                seasons=[2024], stat_type="passing", return_as_pandas=True
            )

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if stat_type not in _NFL_NGS_URLS:
        raise ValueError(f"stat_type must be one of {sorted(_NFL_NGS_URLS)}; got {stat_type!r}")

    if isinstance(seasons, int):
        seasons = [seasons]

    url = _NFL_NGS_URLS[stat_type]
    # The upstream NGS parquet is a single combined file per stat type,
    # so the read happens once and we filter by season afterwards.
    data = pl.read_parquet(url, use_pyarrow=True, columns=None)

    if "season" in data.columns and seasons:
        season_ints = [int(s) for s in seasons]
        data = data.filter(pl.col("season").is_in(season_ints))

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_ngs_passing(seasons: List[int] = None, return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_nextgen_stats(stat_type='passing')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_nextgen_stats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_nextgen_stats
            ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
    """
    _warn_deprecated(
        "load_nfl_ngs_passing",
        replacement="load_nfl_nextgen_stats(stat_type='passing')",
        removed_in="0.1.0",
    )
    if seasons is None:
        # Preserve the legacy "load every season" behavior of the original alias.
        data = pl.read_parquet(NFL_NGS_PASSING_URL, use_pyarrow=True, columns=None)
        return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
    return load_nfl_nextgen_stats(seasons, stat_type="passing", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_ngs_rushing(seasons: List[int] = None, return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_nextgen_stats(stat_type='rushing')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_nextgen_stats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_nextgen_stats
            ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
    """
    _warn_deprecated(
        "load_nfl_ngs_rushing",
        replacement="load_nfl_nextgen_stats(stat_type='rushing')",
        removed_in="0.1.0",
    )
    if seasons is None:
        data = pl.read_parquet(NFL_NGS_RUSHING_URL, use_pyarrow=True, columns=None)
        return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
    return load_nfl_nextgen_stats(seasons, stat_type="rushing", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_ngs_receiving(seasons: List[int] = None, return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_nextgen_stats(stat_type='receiving')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_nextgen_stats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_nextgen_stats
            ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    """
    _warn_deprecated(
        "load_nfl_ngs_receiving",
        replacement="load_nfl_nextgen_stats(stat_type='receiving')",
        removed_in="0.1.0",
    )
    if seasons is None:
        data = pl.read_parquet(NFL_NGS_RECEIVING_URL, use_pyarrow=True, columns=None)
        return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
    return load_nfl_nextgen_stats(seasons, stat_type="receiving", return_as_pandas=return_as_pandas)


# PFR advstats URL dispatch table. Keyed by (stat_type, summary_level)
# matching nflreadpy's ``load_pfr_advstats`` semantics. ``season``-level
# URLs are single combined files; ``week``-level URLs are per-season
# templates with a ``{season}`` placeholder.
_NFL_PFR_URLS = {
    ("pass", "season"): NFL_PFR_SEASON_PASS_URL,
    ("pass", "week"): NFL_PFR_WEEK_PASS_URL,
    ("rush", "season"): NFL_PFR_SEASON_RUSH_URL,
    ("rush", "week"): NFL_PFR_WEEK_RUSH_URL,
    ("rec", "season"): NFL_PFR_SEASON_REC_URL,
    ("rec", "week"): NFL_PFR_WEEK_REC_URL,
    ("def", "season"): NFL_PFR_SEASON_DEF_URL,
    ("def", "week"): NFL_PFR_WEEK_DEF_URL,
}

_NFL_PFR_STAT_TYPES = ("pass", "rush", "rec", "def")
_NFL_PFR_SUMMARY_LEVELS = ("week", "season")


@cached_loader
def load_nfl_pfr_advstats(
    seasons: List[int],
    stat_type: str = "pass",
    summary_level: str = "week",
    return_as_pandas: bool = False,
) -> pl.DataFrame:
    """Load Pro-Football Reference advanced statistics going back to 2018.

    Unified loader that consolidates the per-stat-type / per-summary-level
    PFR advstats accessors. Mirrors the API surface of nflreadpy's
    ``load_pfr_advstats`` so downstream code can swap engines without
    changing call sites.

    Args:
        seasons (list[int]): Seasons to load. For ``summary_level='week'``
            this drives the per-season parquet fan-out; for
            ``summary_level='season'`` it post-filters the combined
            parquet by the ``season`` column.
        stat_type (str): One of ``"pass"``, ``"rush"``, ``"rec"``,
            ``"def"``. Defaults to ``"pass"``.
        summary_level (str): One of ``"week"`` or ``"season"``. Defaults
            to ``"week"``.
        return_as_pandas (bool): If True, returns a pandas dataframe.
            If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing PFR advanced stats
            data for the requested ``stat_type``, ``summary_level``,
            and ``seasons``.

    Raises:
        ValueError: If ``stat_type`` or ``summary_level`` are not allowed
            values, or if any season is less than 2018.

    Example:
        Weekly passing advanced stats (per-game splits)::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            pass_week = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="pass", summary_level="week"
            )

        Season-level rushing summaries (one row per player per season)::

            rush_season = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="rush", summary_level="season"
            )

        Defensive stats with a follow-up filter::

            import polars as pl
            def_week = (
                load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
                .filter(pl.col("week") <= 8)
            )

        Pandas round-trip::

            rec_pd = load_nfl_pfr_advstats(
                seasons=[2024],
                stat_type="rec",
                summary_level="season",
                return_as_pandas=True,
            )

        See Also:
            * `Pro Football Reference`_ -- upstream source for advanced stats
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _Pro Football Reference: https://www.pro-football-reference.com
        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if stat_type not in _NFL_PFR_STAT_TYPES:
        raise ValueError(f"stat_type must be one of {list(_NFL_PFR_STAT_TYPES)}; got {stat_type!r}")
    if summary_level not in _NFL_PFR_SUMMARY_LEVELS:
        raise ValueError(f"summary_level must be one of {list(_NFL_PFR_SUMMARY_LEVELS)}; got {summary_level!r}")

    if isinstance(seasons, int):
        seasons = [seasons]

    url = _NFL_PFR_URLS[(stat_type, summary_level)]

    if summary_level == "season":
        # Single combined parquet per stat type — read once and filter
        # by season afterwards.
        data = pl.read_parquet(url, use_pyarrow=True, columns=None)
        if "season" in data.columns and seasons:
            season_ints = [int(s) for s in seasons]
            for s in season_ints:
                season_not_found_error(s, 2018)
            data = data.filter(pl.col("season").is_in(season_ints))
        return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data

    # summary_level == "week": per-season parquet fan-out. Adopt the
    # cleaner concat pattern: collect frames in a list, then concat once
    # at the end. Avoids the empty-seed-DataFrame schema-conflict gotcha
    # that ``how="vertical"`` triggers when the seed has no columns.
    frames: list[pl.DataFrame] = []
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pl.read_parquet(url.format(season=i), use_pyarrow=True, columns=None)
        frames.append(i_data)

    if not frames:
        data = pl.DataFrame()
    elif len(frames) == 1:
        data = frames[0]
    else:
        data = pl.concat(frames, how="vertical")

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pfr_pass(return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='season')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="pass", summary_level="season"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_pass",
        replacement="load_nfl_pfr_advstats(stat_type='pass', summary_level='season')",
        removed_in="0.1.0",
    )
    # Preserve the legacy "no seasons filter" behavior — read the full combined parquet.
    data = pl.read_parquet(NFL_PFR_SEASON_PASS_URL, use_pyarrow=True, columns=None)
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pfr_weekly_pass(seasons: List[int], return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='week')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="pass", summary_level="week"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_weekly_pass",
        replacement="load_nfl_pfr_advstats(stat_type='pass', summary_level='week')",
        removed_in="0.1.0",
    )
    return load_nfl_pfr_advstats(seasons, stat_type="pass", summary_level="week", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_pfr_rush(return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='season')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="rush", summary_level="season"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_rush",
        replacement="load_nfl_pfr_advstats(stat_type='rush', summary_level='season')",
        removed_in="0.1.0",
    )
    data = pl.read_parquet(NFL_PFR_SEASON_RUSH_URL, use_pyarrow=True, columns=None)
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pfr_weekly_rush(seasons: List[int], return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='week')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="rush", summary_level="week"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_weekly_rush",
        replacement="load_nfl_pfr_advstats(stat_type='rush', summary_level='week')",
        removed_in="0.1.0",
    )
    return load_nfl_pfr_advstats(seasons, stat_type="rush", summary_level="week", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_pfr_rec(return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='season')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="rec", summary_level="season"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_rec",
        replacement="load_nfl_pfr_advstats(stat_type='rec', summary_level='season')",
        removed_in="0.1.0",
    )
    data = pl.read_parquet(NFL_PFR_SEASON_REC_URL, use_pyarrow=True, columns=None)
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pfr_weekly_rec(seasons: List[int], return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='week')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="rec", summary_level="week"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_weekly_rec",
        replacement="load_nfl_pfr_advstats(stat_type='rec', summary_level='week')",
        removed_in="0.1.0",
    )
    return load_nfl_pfr_advstats(seasons, stat_type="rec", summary_level="week", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_pfr_def(return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='season')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="def", summary_level="season"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_def",
        replacement="load_nfl_pfr_advstats(stat_type='def', summary_level='season')",
        removed_in="0.1.0",
    )
    data = pl.read_parquet(NFL_PFR_SEASON_DEF_URL, use_pyarrow=True, columns=None)
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pfr_weekly_def(seasons: List[int], return_as_pandas: bool = False) -> pl.DataFrame:
    """Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='week')``.

    Will be removed in a future release. Migrate callers to the unified
    ``load_nfl_pfr_advstats`` function.

    Example:
        Migrate to the unified entry point::

            from sportsdataverse.nfl import load_nfl_pfr_advstats
            df = load_nfl_pfr_advstats(
                seasons=[2024], stat_type="def", summary_level="week"
            )
    """
    _warn_deprecated(
        "load_nfl_pfr_weekly_def",
        replacement="load_nfl_pfr_advstats(stat_type='def', summary_level='week')",
        removed_in="0.1.0",
    )
    return load_nfl_pfr_advstats(seasons, stat_type="def", summary_level="week", return_as_pandas=return_as_pandas)


@cached_loader
def load_nfl_rosters(seasons: List[int], return_as_pandas=False, *, source: str = "nflverse") -> pl.DataFrame:
    """Load NFL season roster data for the requested seasons.

    Reads nflverse's published season-roster parquet (one row per player per
    season). nflverse's roster product is the union of three upstream tiers --
    NFL Next Gen Stats (2016+), the credentialed NFL Data Exchange (2002-2015),
    and the public NFL Shield endpoint (all seasons) -- so it carries densely
    populated cross-system identifier columns (``espn_id``, ``sportradar_id``,
    ``yahoo_id``, ``pff_id``, ``pfr_id``, ...) alongside biographical and
    depth-chart fields. This is the richest roster surface; prefer it whenever a
    network round trip to nflverse is acceptable.

    Args:
        seasons (list): Seasons to load (e.g. ``[2024]`` or ``range(2020, 2025)``).
            A single ``int`` is accepted and wrapped. 1920 is the earliest
            available season.
        source (str): Which roster release to read.
            ``"nflverse"`` (the default, also accepts ``None``) returns the
            nflverse season-roster releases described above -- the full
            multi-source product (1920+, densely populated cross-system IDs).
            ``"sportsdataverse"`` / ``"sdv"`` returns the SDV-native
            ``nfl_rosters`` release built by
            :func:`sportsdataverse.nfl.build_nfl_rosters` from the **public NFL
            Shield / ESPN** surface only. The SDV tier is a partial build: its
            30 columns are a subset of nflverse's 36, and cross-system IDs are
            sparser pre-2016. It covers only the published seasons (rosters
            2022+). The default stays ``"nflverse"``. Any other value raises
            ``ValueError``.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe (default).

    Returns:
        pl.DataFrame: Polars dataframe of season rosters for the requested
        seasons (``pandas.DataFrame`` when ``return_as_pandas=True``).

    Raises:
        SeasonNotFoundError: If a requested season precedes the earliest
            available season (1920).
        ValueError: If ``source`` is not one of ``"nflverse"``, ``None``,
            ``"sportsdataverse"``, or ``"sdv"``.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_rosters
            rosters = load_nfl_rosters(seasons=[2024])

        Multi-season range::

            rosters = load_nfl_rosters(seasons=range(2020, 2025))

        Filter to a single team::

            import polars as pl
            kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")

        SDV-native rosters (public Shield/ESPN build; published seasons 2022+;
        30-column subset of nflverse, sparser cross-IDs pre-2016)::

            rosters_sdv = load_nfl_rosters(seasons=[2023], source="sdv")
            rosters_sdv.select(["season", "team", "full_name", "gsis_id"]).head()

        See Also:
            * :func:`sportsdataverse.nfl.build_nfl_rosters` -- SDV-native rosters
              built from the public NFL Shield API only (no nflverse dependency;
              partial cross-system IDs)
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if source in ("nflverse", None):
        base_url = NFL_ROSTER_URL
    elif source in ("sportsdataverse", "sdv"):
        base_url = NFL_SDV_ROSTER_URL
    else:
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1920)
        i_data = pl.read_parquet(base_url.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_weekly_rosters(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL weekly roster data for the requested seasons.

    Reads nflverse's published weekly-roster parquet (one row per player per
    team per week), so the roster snapshot reflects mid-season transactions
    (signings, releases, IR moves) rather than a single season-end view. Like
    :func:`load_nfl_rosters` it is sourced from nflverse's full multi-tier
    roster product and carries densely populated cross-system identifier columns
    plus a ``week`` / ``game_type`` pair identifying each snapshot.

    Unlike :func:`load_nfl_rosters` and :func:`load_nfl_players`, this loader has
    **no SDV-native (``source="sdv"``) tier**: the SDV roster build
    (:func:`build_nfl_rosters`) is season-only, and weekly snapshots require the
    credential-gated NFL Data Exchange that the public build cannot reach.

    Args:
        seasons (list): Seasons to load (e.g. ``[2024]`` or ``range(2022, 2025)``).
            A single ``int`` is accepted and wrapped. 2002 is the earliest
            available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe (default).

    Returns:
        pl.DataFrame: Polars dataframe of weekly rosters for the requested
        seasons (``pandas.DataFrame`` when ``return_as_pandas=True``).

    Raises:
        SeasonNotFoundError: If a requested season precedes the earliest
            available season (2002).

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_weekly_rosters
            weekly = load_nfl_weekly_rosters(seasons=[2024])

        Multi-season range with a follow-up week filter::

            import polars as pl
            wk1 = (
                load_nfl_weekly_rosters(seasons=range(2022, 2025))
                .filter(pl.col("week") == 1)
            )

        See Also:
            * :func:`sportsdataverse.nfl.load_nfl_rosters` -- season-level rosters
            * :func:`sportsdataverse.nfl.build_nfl_rosters` -- SDV-native rosters
              built from the public NFL Shield API only (season-level)
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2002)
        i_data = pl.read_parquet(NFL_WEEKLY_ROSTER_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_teams(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL team ID information and logos

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing teams available.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_teams
            teams = load_nfl_teams()
            teams.shape

        Pandas round-trip::

            teams_pd = load_nfl_teams(return_as_pandas=True)
            teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    return (
        pl.read_csv(NFL_TEAM_LOGO_URL).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_csv(NFL_TEAM_LOGO_URL)
    )


@cached_loader
def load_nfl_players(return_as_pandas=False, *, source: str = "nflverse") -> pl.DataFrame:
    """Load the nflverse NFL player-identity master.

    Reads nflverse's published ``players.parquet`` — a one-row-per-player
    identity master that is the union of **seven** upstream systems (GSIS, ESPN,
    NGS roster, Pro-Football-Reference, OverTheCap, PFF, and the Sleeper / Yahoo
    cross-walk). It is the canonical source for cross-system identifier
    columns (``gsis_id``, ``espn_id``, ``pfr_id``, ``pff_id``, ``otc_id``,
    ``smart_id``, ``esb_id``, ``nfl_id``) plus name, position, physical, draft,
    and status fields.

    This is the **full identity master**. For an SDV-native, public-source-only
    alternative that does not depend on the nflverse release, see
    :func:`sportsdataverse.nfl.build_nfl_players` (ESPN-athletes tier only) and
    :func:`sportsdataverse.nfl.nfl_players_crosswalk` (a thin ID-only slice of
    this same parquet).

    Args:
        source (str): Which player-master release to read.
            ``"nflverse"`` (the default, also accepts ``None``) returns the
            nflverse seven-system ``players.parquet`` identity master described
            above. ``"sportsdataverse"`` / ``"sdv"`` returns the SDV-native
            ``nfl_players`` release built by
            :func:`sportsdataverse.nfl.build_nfl_players` from the **public NFL
            Shield / ESPN-athletes** surface only. The SDV tier is a partial
            build: its columns are a subset of nflverse's and cross-system IDs
            are sparser (notably pre-2016), though ``espn_id`` is populated. The
            default stays ``"nflverse"``. Any other value raises ``ValueError``.
        return_as_pandas (bool): If ``True``, return a ``pandas.DataFrame``;
            otherwise a ``polars.DataFrame`` (default).

    Returns:
        pl.DataFrame: One-row-per-player identity master. ``return_as_pandas``
        narrows the return to a ``pandas.DataFrame``.

    Raises:
        ValueError: If ``source`` is not one of ``"nflverse"``, ``None``,
            ``"sportsdataverse"``, or ``"sdv"``.
        Exception: Propagates any network / parquet-read error from the
            underlying ``pl.read_parquet`` against the release URL.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_players
            players = load_nfl_players()
            print(players.shape)

        Pandas round-trip::

            players_pd = load_nfl_players(return_as_pandas=True)
            players_pd.head()

        SDV-native player master (public Shield/ESPN-athletes build; subset of
        nflverse columns, sparser cross-IDs)::

            players_sdv = load_nfl_players(source="sdv")
            players_sdv.select(["display_name", "position", "espn_id"]).head()

        Pipeline next step (one line)::

            import polars as pl
            load_nfl_players().select(["gsis_id", "display_name", "position"]).head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings (load_players)

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if source in ("nflverse", None):
        url = NFL_PLAYER_URL
    elif source in ("sportsdataverse", "sdv"):
        url = NFL_SDV_PLAYER_URL
    else:
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")
    return (
        pl.read_parquet(url, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(url, use_pyarrow=True, columns=None)
    )


@cached_loader
def load_nfl_snap_counts(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL snap counts data for selected seasons

    Args:
        seasons (list): Used to define different seasons. 2012 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing snap counts available for the requested seasons.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_snap_counts
            snaps = load_nfl_snap_counts(seasons=[2024])

        Multi-season range with offense-only filter::

            import polars as pl
            offense = (
                load_nfl_snap_counts(seasons=range(2022, 2025))
                .filter(pl.col("offense_snaps") > 0)
            )

        See Also:
            * `Pro Football Reference`_ -- upstream snap-count source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _Pro Football Reference: https://www.pro-football-reference.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2012)
        i_data = pl.read_parquet(NFL_SNAP_COUNTS_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_pbp_participation(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL play-by-play participation data for selected seasons

    Args:
        seasons (list): Used to define different seasons. 2016 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing play-by-play participation data available for the requested seasons.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_pbp_participation
            participation = load_nfl_pbp_participation(seasons=[2022])

        Multi-season range::

            participation = load_nfl_pbp_participation(seasons=range(2018, 2023))

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2016)
        i_data = pl.read_parquet(NFL_PBP_PARTICIPATION_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_injuries(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL injuries data for selected seasons

    Args:
        seasons (list): Used to define different seasons. 2009 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing injuries data available for the requested seasons.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_injuries
            injuries = load_nfl_injuries(seasons=[2024])

        Multi-season range with team filter::

            import polars as pl
            sf_injuries = (
                load_nfl_injuries(seasons=range(2020, 2025))
                .filter(pl.col("team") == "SF")
            )

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2009)
        i_data = pl.read_parquet(NFL_INJURIES_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_depth_charts(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL Depth Chart data for selected seasons

    Args:
        seasons (list): Used to define different seasons. 2001 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing depth chart data available for the requested seasons.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_depth_charts
            depth = load_nfl_depth_charts(seasons=[2024])

        Multi-season range::

            depth = load_nfl_depth_charts(seasons=range(2020, 2025))

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2001)
        i_data = pl.read_parquet(NFL_DEPTH_CHARTS_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_contracts(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL Historical contracts information

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing historical contracts available.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_contracts
            contracts = load_nfl_contracts()
            contracts.shape

        Pandas round-trip with sort by APY::

            contracts_pd = load_nfl_contracts(return_as_pandas=True)
            contracts_pd.sort_values("apy", ascending=False).head()

        See Also:
            * `Over The Cap`_ -- upstream contracts source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _Over The Cap: https://overthecap.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    return (
        pl.read_parquet(NFL_CONTRACTS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_CONTRACTS_URL, use_pyarrow=True, columns=None)
    )


@cached_loader
def load_nfl_combine(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL Combine information

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing NFL combine data available.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_combine
            combine = load_nfl_combine()
            combine.shape

        Filter by draft year and position::

            import polars as pl
            qbs_2024 = (
                load_nfl_combine()
                .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
            )

        See Also:
            * `Pro Football Reference`_ -- upstream combine source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _Pro Football Reference: https://www.pro-football-reference.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    return (
        pl.read_parquet(NFL_COMBINE_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_COMBINE_URL, use_pyarrow=True, columns=None)
    )


@cached_loader
def load_nfl_draft_picks(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL Draft picks information

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing NFL Draft picks data available.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_draft_picks
            picks = load_nfl_draft_picks()
            picks.shape

        Filter to a single year and round::

            import polars as pl
            r1_2024 = (
                load_nfl_draft_picks()
                .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
            )

        See Also:
            * `Pro Football Reference`_ -- upstream draft source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _Pro Football Reference: https://www.pro-football-reference.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    return (
        pl.read_parquet(NFL_DRAFT_PICKS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_DRAFT_PICKS_URL, use_pyarrow=True, columns=None)
    )


@cached_loader
def load_nfl_officials(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL Officials information

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing officials available.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_officials
            officials = load_nfl_officials()
            officials.shape

        Pandas round-trip::

            officials_pd = load_nfl_officials(return_as_pandas=True)
            officials_pd.head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    return (
        pl.read_parquet(NFL_OFFICIALS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_OFFICIALS_URL, use_pyarrow=True, columns=None)
    )


@cached_loader
def load_nfl_team_stats(
    seasons: List[int], summary_level: str = "week", return_as_pandas=False, *, source: str = "nflverse"
) -> pl.DataFrame:
    """Load NFL team stats data going back to 1999

    Args:
        seasons (list): Used to define different seasons. 1999 is the earliest available season.
        summary_level (str): Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week".
            Ignored when ``source`` is the SDV-native release (a single week-level
            parquet covering all seasons; filter post-load).
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.
        source (str): Which team-stats release to read. ``"nflverse"`` (the
            default) reads the per-season nflverse ``stats_team`` releases.
            ``"sportsdataverse"`` / ``"sdv"`` reads the SDV-native
            ``nfl_team_stats`` release (a single combined week-level parquet,
            built by :func:`sportsdataverse.nfl.build_nfl_team_stats` from the
            SDV play-by-play and filtered to the requested seasons post-load).

    Returns:
        pl.DataFrame: Polars dataframe containing team stats available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 1999, if `summary_level` is not one of the
            allowed values, or if ``source`` is not one of ``"nflverse"``, ``None``,
            ``"sportsdataverse"``, or ``"sdv"``.

    Example:
        Weekly team stats (default)::

            from sportsdataverse.nfl import load_nfl_team_stats
            weekly = load_nfl_team_stats(seasons=[2024])

        Regular-season-only team stats::

            reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

        SDV-native team stats (built from SDV play-by-play)::

            sdv = load_nfl_team_stats(seasons=[2024], source="sdv")

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if type(seasons) is int:
        seasons = [seasons]

    if source in ("sportsdataverse", "sdv"):
        for i in seasons:
            season_not_found_error(int(i), 1999)
        data = pl.read_parquet(NFL_SDV_TEAM_STATS_URL, use_pyarrow=True)
        data = data.filter(pl.col("season").is_in([int(i) for i in seasons]))
        return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data

    if source not in ("nflverse", None):
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")

    if summary_level not in ("week", "reg", "post", "reg+post"):
        raise ValueError("summary_level must be one of 'week', 'reg', 'post', 'reg+post'")

    level_str = summary_level.replace("+", "")  # "reg+post" -> "regpost"

    data = pl.DataFrame()
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1999)
        i_data = pl.read_parquet(
            NFL_TEAM_STATS_URL.format(level=level_str, season=i),
            use_pyarrow=True,
            columns=None,
        )
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_ftn_charting(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """Load NFL FTN charting data going back to 2022

    Args:
        seasons (list): Used to define different seasons. 2022 is the earliest available season.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing FTN charting data available for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2022.

    Example:
        Single season::

            from sportsdataverse.nfl import load_nfl_ftn_charting
            charting = load_nfl_ftn_charting(seasons=[2024])

        Multi-season range::

            charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

        Filter to plays with motion::

            import polars as pl
            motion_plays = (
                load_nfl_ftn_charting(seasons=[2024])
                .filter(pl.col("is_motion") == 1)
            )

        See Also:
            * `FTN Network`_ -- upstream charting source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _FTN Network: https://ftnfantasy.com
        .. _nflverse: https://nflverse.nflverse.com
    """
    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2022)
        i_data = pl.read_parquet(NFL_FTN_CHARTING_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_trades(return_as_pandas=False) -> pl.DataFrame:
    """Load NFL trades data

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing NFL trade information.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_trades
            trades = load_nfl_trades()
            trades.shape

        Filter to a single season::

            import polars as pl
            trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    return (
        pl.read_parquet(NFL_TRADES_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_TRADES_URL, use_pyarrow=True, columns=None)
    )


def _read_csv_retry(url: str, *, attempts: int = 4, **kwargs) -> pl.DataFrame:
    """``pl.read_csv`` with exponential backoff on transient upstream errors.

    The DynastyProcess raw-GitHub CSVs rate-limit CI runners (the parallel
    test matrix triggers HTTP 429); a short backoff clears it. Non-transient
    errors and the final attempt re-raise unchanged.
    """
    import time
    import urllib.error

    for attempt in range(attempts):
        try:
            return pl.read_csv(url, **kwargs)
        except urllib.error.HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == attempts - 1:
                raise
            time.sleep(2.0 * 2**attempt)
    raise AssertionError("unreachable")


@cached_loader
def load_nfl_ff_playerids(return_as_pandas=False) -> pl.DataFrame:
    """Load fantasy football player IDs from DynastyProcess.com

    Args:
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing fantasy football player ID mappings across platforms.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_ff_playerids
            ids = load_nfl_ff_playerids()
            ids.shape

        Filter to active QBs::

            import polars as pl
            qbs = (
                load_nfl_ff_playerids()
                .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
            )

        See Also:
            * `DynastyProcess`_ -- upstream ID-mapping project
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _DynastyProcess: https://github.com/dynastyprocess
        .. _nflverse: https://nflverse.nflverse.com
    """
    return (
        _read_csv_retry(NFL_FF_PLAYERIDS_URL, null_values=["NA", "NULL", ""]).to_pandas(
            use_pyarrow_extension_array=True
        )
        if return_as_pandas
        else _read_csv_retry(NFL_FF_PLAYERIDS_URL, null_values=["NA", "NULL", ""])
    )


@cached_loader
def load_nfl_ff_rankings(
    type: str = "draft",
    kind: str = None,
    return_as_pandas=False,
) -> pl.DataFrame:
    """Load fantasy football rankings and projections

    Args:
        type (str): Type of rankings to load. One of ``"draft"`` (current draft
            rankings), ``"week"`` (weekly rankings), or ``"all"`` (full historical
            rankings). Defaults to ``"draft"``. Kept for nflreadpy parity since
            its parameter is also called ``type``; the forward-going preferred
            name is ``kind``.
        kind (str): Preferred parameter name. Same semantics and allowed values
            as ``type``. If both are supplied, ``kind`` wins. If neither is
            supplied, defaults to ``"draft"`` via ``type``.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing fantasy football rankings data.

    Raises:
        ValueError: If the resolved value is not one of the allowed values.

    Note:
        Available as the alias ``sportsdataverse.nfl.load_ff_rankings`` for
        nflreadpy parity.

    Example:
        Preferred ``kind=`` parameter::

            from sportsdataverse.nfl import load_nfl_ff_rankings
            draft = load_nfl_ff_rankings(kind="draft")

        Weekly rankings::

            weekly = load_nfl_ff_rankings(kind="week")

        Full historical rankings (parquet)::

            history = load_nfl_ff_rankings(kind="all")

        nflreadpy-parity ``type=`` parameter (still supported)::

            draft = load_nfl_ff_rankings(type="draft")

        See Also:
            * `DynastyProcess`_ -- upstream rankings source
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _DynastyProcess: https://github.com/dynastyprocess
        .. _nflverse: https://nflverse.nflverse.com
    """
    effective = kind if kind is not None else type
    if effective not in ("draft", "week", "all"):
        raise ValueError("type/kind must be one of 'draft', 'week', 'all'")

    if effective == "draft":
        data = _read_csv_retry(NFL_FF_RANKINGS_DRAFT_URL, null_values=["NA", "NULL", ""])
    elif effective == "week":
        data = _read_csv_retry(NFL_FF_RANKINGS_WEEK_URL, null_values=["NA", "NULL", ""])
    else:  # all
        data = pl.read_parquet(NFL_FF_RANKINGS_ALL_URL, use_pyarrow=True, columns=None)

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_ff_opportunity(
    seasons: List[int],
    stat_type: str = "weekly",
    model_version: str = "latest",
    return_as_pandas=False,
) -> pl.DataFrame:
    """Load NFL fantasy football opportunity data from ffverse/ffopportunity

    Args:
        seasons (list): Used to define different seasons. 2006 is the earliest available season.
        stat_type (str): One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly".
        model_version (str): One of "latest", "v1.0.0". Defaults to "latest".
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: Polars dataframe containing fantasy football opportunity data
            for the requested seasons.

    Raises:
        ValueError: If `season` is less than 2006, or if `stat_type` / `model_version`
            are not allowed values.

    Example:
        Weekly opportunity stats (default)::

            from sportsdataverse.nfl import load_nfl_ff_opportunity
            weekly = load_nfl_ff_opportunity(seasons=[2024])

        Pass play-by-play opportunity stats::

            pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

        Rush play-by-play opportunity stats with pinned model version::

            pbp_rush = load_nfl_ff_opportunity(
                seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
            )

        See Also:
            * `ffopportunity`_ -- upstream opportunity model
            * `nflverse`_ -- full data ecosystem (R + Python)

        .. _ffopportunity: https://github.com/ffverse/ffopportunity
        .. _nflverse: https://nflverse.nflverse.com
    """
    if stat_type not in ("weekly", "pbp_pass", "pbp_rush"):
        raise ValueError("stat_type must be one of 'weekly', 'pbp_pass', 'pbp_rush'")
    if model_version not in ("latest", "v1.0.0"):
        raise ValueError("model_version must be one of 'latest', 'v1.0.0'")

    data = pl.DataFrame()
    if type(seasons) is int:
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2006)
        i_data = pl.read_parquet(
            NFL_FF_OPPORTUNITY_URL.format(model_version=model_version, stat_type=stat_type, season=i),
            use_pyarrow=True,
            columns=None,
        )
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_espn_qbr(
    seasons: List[int],
    summary_type: str = "season",
    return_as_pandas: bool = False,
    *,
    source: str = "nflverse",
) -> pl.DataFrame:
    """Load ESPN Total QBR (Quarterback Rating) data going back to 2006.

    Mirrors nflreadpy / nflreadr ``load_espn_qbr`` -- the lone nflreadpy dataset
    that previously had no sdv-py loader. ESPN publishes Total QBR only from 2006
    onward, so 2006 is the earliest available season (unlike the 1999 floor on
    play-by-play). nflverse republishes ESPN's QBR through the ``espn_data``
    release as two combined files (one per ``summary_type``), each covering all
    seasons; this loader reads the requested file once and post-filters by
    ``season`` (the same access pattern as :func:`load_nfl_schedule`).

    Args:
        seasons (list): Seasons to return. 2006 is the earliest available season.
        summary_type (str): Aggregation level. ``"season"`` (default) returns one
            row per quarterback-season; ``"week"`` returns one row per
            quarterback-game. Any other value raises ``ValueError``.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False,
            returns a polars dataframe.
        source (str): Which QBR release to read. ``"nflverse"`` (the default,
            also accepts ``None``) returns the nflverse ``espn_data`` release.
            ``"sportsdataverse"`` / ``"sdv"`` returns the SDV-native
            ``nfl_espn_qbr`` release (built by ``nfl-data`` from ESPN's QBR web
            endpoint -- the same source nflverse's espnscrapeR uses). Any other
            value raises ``ValueError``.

    Returns:
        pl.DataFrame: Polars dataframe containing ESPN Total QBR for the requested
            seasons, summarized per ``summary_type``.

    Raises:
        ValueError: If ``summary_type`` is not ``"season"`` / ``"week"``, or if
            ``source`` is not one of ``"nflverse"``, ``None``,
            ``"sportsdataverse"``, or ``"sdv"``.
        SeasonNotFoundError: If any requested season is before 2006.

    Note:
        ``source="sdv"`` reads the ``nfl_espn_qbr`` release, which ``nfl-data``
        publishes as part of the CFB<->NFL dataset-parity backlog. Until that
        release exists the default ``source="nflverse"`` is the working path.

    Example:
        Season-level QBR (default)::

            from sportsdataverse.nfl import load_nfl_espn_qbr
            qbr = load_nfl_espn_qbr(seasons=[2024])
            qbr.shape

        Week-level QBR::

            qbr_week = load_nfl_espn_qbr(seasons=[2024], summary_type="week")

        Multi-season range::

            qbr = load_nfl_espn_qbr(seasons=range(2020, 2025))

        Pandas round-trip::

            qbr_pd = load_nfl_espn_qbr(seasons=[2024], return_as_pandas=True)
            qbr_pd[["season", "team_abb", "qbr_total"]].head()

        See Also:
            * `nflverse`_ -- full data ecosystem (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if summary_type not in ("season", "week"):
        raise ValueError(f"Invalid summary_type {summary_type!r}; expected 'season' or 'week'.")
    if source in ("nflverse", None):
        url = NFL_ESPN_QBR_SEASON_URL if summary_type == "season" else NFL_ESPN_QBR_WEEK_URL
    elif source in ("sportsdataverse", "sdv"):
        url = NFL_SDV_ESPN_QBR_SEASON_URL if summary_type == "season" else NFL_SDV_ESPN_QBR_WEEK_URL
    else:
        raise ValueError(f"Invalid source {source!r}; expected one of 'nflverse', None, 'sportsdataverse', or 'sdv'.")

    if type(seasons) is int:
        seasons = [seasons]
    for i in seasons:
        season_not_found_error(int(i), 2006)
    # The espn_data release ships ONE combined parquet per summary_type (all
    # seasons, 2006-present). Read once and post-filter by season.
    data = pl.read_parquet(url, use_pyarrow=True, columns=None)
    if "season" in data.columns and seasons:
        season_ints = [int(s) for s in seasons]
        data = data.filter(pl.col("season").is_in(season_ints))
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


@cached_loader
def load_nfl_ratings_weekly(seasons: List[int], return_as_pandas: bool = False) -> pl.DataFrame:
    """Load per-week as-of vintages of the SDV NFL ratings spine (1999+).

    SDV-native dataset (no nflreadpy equivalent), built by nfl-data's
    ``nfl_ratings_weekly`` job: for each week ``W`` the ratings spine
    (:func:`sportsdataverse.nfl.nfl_ratings`) is refit as of week ``W``'s
    FIRST kickoff. A row with ``as_of_week = W`` therefore contains ONLY
    information from games strictly before week ``W`` — STRICTLY EXCLUSIVE
    semantics, safe to join onto week-``W`` games with no leakage (contrast
    the CFB ``through_week`` convention, which is inclusive of its week).

    Args:
        seasons (List[int]): Seasons to load (1999 through the current season).
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pl.DataFrame: One row per ``(season, as_of_week, team_id)`` — the
        ``nfl_ratings`` columns (``adj_off_epa`` / ``adj_def_epa`` /
        ``adj_st_epa`` / ``adj_net``, ranks, ``net_z``, ``games``) plus
        ``as_of_week`` (Int32). Weeks run 2-21 (2-22 from the 2021
        18-game era); week 1 has no prior games and emits no vintage.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_ratings_weekly
            vintages = load_nfl_ratings_weekly(seasons=[2024])
            print(vintages.shape)

        Leak-free join for week-10 games::

            import polars as pl
            wk10 = load_nfl_ratings_weekly(seasons=[2024]).filter(pl.col("as_of_week") == 10)

        See Also:
            * `nflverse`_ -- companion data ecosystem for the NFL
            * `nflfastR`_ -- R sister package for NFL play-by-play

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflfastR: https://www.nflfastr.com
    """
    if type(seasons) is int:
        seasons = [seasons]
    frames = []
    for i in seasons:
        season_not_found_error(int(i), 1999)
        frames.append(pl.read_parquet(NFL_RATINGS_WEEKLY_URL.format(season=i), use_pyarrow=True, columns=None))
    data = pl.concat(frames, how="vertical_relaxed")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data
