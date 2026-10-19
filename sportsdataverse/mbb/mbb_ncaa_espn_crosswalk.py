"""stats.ncaa.org <-> ESPN college-basketball team-id crosswalk.

stats.ncaa.org mints a NEW numeric team id every season and writes school
names in AP style (``Central Conn. St.``, ``Ark.-Pine Bluff``); ESPN keeps one
stable id per school and spells the names out. Joining the two therefore needs
a **season-keyed** crosswalk.

It is built and published by sdv-reference-data (``sdv_reference/crosswalks/``)
to the ``{mbb,wbb}_crosswalk`` sportsdataverse-data releases, keyed on the NCAA
school (org id), with each season's conference from the ``{mbb,wbb}_groups``
tables. The loader reads that release, so a new season needs no sdv-py release,
and falls back to the copy sdv-py bundles when the release cannot be read:

- ``sportsdataverse/mbb/data/ncaa_espn_team_crosswalk_mbb.csv``
- ``sportsdataverse/wbb/data/ncaa_espn_team_crosswalk_wbb.csv``

The build is deterministic: there is no fuzzy matching at runtime OR at build
time, and a school that cannot be resolved is emitted with a null
``espn_team_id`` rather than dropped.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Dict, Optional, Union

import polars as pl

from sportsdataverse._codegen_runtime import _read_release_parquet, cli_warn
from sportsdataverse.config import SDVRELEASES
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.mbb.mbb_ncaa_team_ids import _LEAGUE_PKG, _league_data_bytes

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_CROSSWALK_CACHE: Dict[str, pl.DataFrame] = {}

#: ESPN ids stay Utf8 (the convention everywhere else in sdv-py); NCAA ids stay
#: Int64 to match ``ncaa_{mbb,wbb}_team_ids().id``.
_SCHEMA: Dict[str, pl.DataType] = {
    "season": pl.Utf8,
    "ncaa_team_id": pl.Int64,
    "ncaa_team": pl.Utf8,
    "ncaa_conference": pl.Utf8,
    "conference_id": pl.Utf8,
    "espn_team_id": pl.Utf8,
    "espn_display_name": pl.Utf8,
    "espn_location": pl.Utf8,
    "espn_mascot": pl.Utf8,
    "espn_abbreviation": pl.Utf8,
    "espn_conference_name": pl.Utf8,
    "espn_conference_id": pl.Utf8,
    "match_method": pl.Utf8,
}

_RELEASE_URL = SDVRELEASES + "{league}_crosswalk/ncaa_espn_team_crosswalk.parquet"


def _packaged_crosswalk(league: str) -> pl.DataFrame:
    """The crosswalk bundled with sdv-py (the fallback)."""
    raw = _league_data_bytes(league, f"ncaa_espn_team_crosswalk_{league}.csv")
    return pl.read_csv(io.BytesIO(raw), schema_overrides=_SCHEMA).select(list(_SCHEMA))


def _release_crosswalk(league: str) -> Optional[pl.DataFrame]:
    """The ``{league}_crosswalk`` release asset in the loader's schema; ``None`` (with a warning) when it is absent
    or cannot be read or conformed, so the caller falls back to the bundled copy."""
    url = _RELEASE_URL.format(league=league)
    try:
        df = _read_release_parquet(url)
        if df is not None:
            return df.select([pl.col(c).cast(t) for c, t in _SCHEMA.items()])
        reason = "no published asset"
    # A failed fetch (offline, rate-limited) or a drifted asset (a column gone or of another type) is a stale
    # fallback, not an empty one: the bundled copy still answers, with a warning (MD-4). Anything else is a bug.
    except (AssetFetchError, NoDataError, pl.exceptions.PolarsError) as e:
        reason = f"{type(e).__name__}: {e}"
    cli_warn(f"ncaa_espn_team_crosswalk: {url} unavailable ({reason}); using the bundled copy")
    return None


def ncaa_espn_team_crosswalk(
    league: str = "mbb", *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Season-keyed stats.ncaa.org -> ESPN team-id crosswalk.

    One row per ``(season, ncaa_team_id)``. Reads the ``{league}_crosswalk``
    sportsdataverse-data release, which gains a season as soon as it is
    published; when that cannot be read it warns and returns the copy bundled
    with sdv-py. Teams that could not be resolved to an ESPN team are kept with
    a null ``espn_team_id`` and ``match_method="unmatched"`` -- never dropped.

    Args:
        league: ``"mbb"`` (men's, 2009-10 onward) or ``"wbb"`` (women's).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with columns ``season`` (str, ``"YYYY-YY"``),
        ``ncaa_team_id`` (Int64 -- the season-specific stats.ncaa.org id),
        ``ncaa_team`` / ``ncaa_conference`` (str), ``conference_id`` (str,
        nullable -- the SDV group id, e.g. ``"mbb:big-ten"``), ``espn_team_id``
        (str, nullable -- ESPN ids are strings throughout sdv-py),
        ``espn_display_name`` / ``espn_location`` / ``espn_mascot`` /
        ``espn_abbreviation`` / ``espn_conference_name`` /
        ``espn_conference_id`` (str, nullable), and ``match_method`` (str --
        ``"exact"``, ``"dict"``, ``"alias"``, ``"org"`` (the school's ESPN id
        from its other seasons) or ``"unmatched"``). The three
        conference columns and ``ncaa_conference`` are per season: Maryland is
        ACC in 2013-14 and Big Ten in 2014-15.

    Raises:
        ValueError: If *league* is not ``"mbb"`` or ``"wbb"``.

    Example:
        Quick start::

            from sportsdataverse.mbb import ncaa_espn_team_crosswalk
            df = ncaa_espn_team_crosswalk()
            print(df.shape)

        Women's crosswalk as pandas::

            wdf = ncaa_espn_team_crosswalk(league="wbb", return_as_pandas=True)

        Pipeline next step (one line)::

            df.filter(pl.col("season") == "2025-26").select("ncaa_team_id", "espn_team_id")

    See Also:
        * `hoopR`_ -- men's college basketball in R, source of the name dictionary
        * `wehoop`_ -- women's college basketball in R

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if league not in _LEAGUE_PKG:
        raise ValueError(f"league must be one of {sorted(_LEAGUE_PKG)}, got {league!r}")
    df = _CROSSWALK_CACHE.get(league)
    if df is None:
        release = _release_crosswalk(league)
        if release is not None:
            _CROSSWALK_CACHE[league] = df = release
        else:
            # Not cached: a later call retries the release (a transient failure must not pin the
            # bundled copy for the life of the process).
            df = _packaged_crosswalk(league)
    return df.to_pandas() if return_as_pandas else df


__all__ = ["ncaa_espn_team_crosswalk"]
