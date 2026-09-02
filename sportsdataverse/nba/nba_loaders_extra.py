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
"""

from __future__ import annotations

from typing import Iterable, Literal, Sequence, Tuple, overload

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import (
    SeasonNotFoundError,
    _as_season_list,
    _read_release_parquet,
    cli_warn,
)
from sportsdataverse.config import NBA_STATS_LEAGUEDASH_URL

__all__ = ["NBA_STATS_LEAGUEDASH_FAMILIES", "load_nba_stats_leaguedash"]

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
