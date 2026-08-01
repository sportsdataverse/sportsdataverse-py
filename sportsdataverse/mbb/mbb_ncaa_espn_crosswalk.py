"""stats.ncaa.org <-> ESPN college-basketball team-id crosswalk.

stats.ncaa.org mints a NEW numeric team id every season and writes school
names in AP style (``Central Conn. St.``, ``Ark.-Pine Bluff``); ESPN keeps one
stable id per school and spells the names out. Joining the two therefore needs
a **season-keyed** crosswalk, which sdv-py bundles as CSV:

- ``sportsdataverse/mbb/data/ncaa_espn_team_crosswalk_mbb.csv``
- ``sportsdataverse/wbb/data/ncaa_espn_team_crosswalk_wbb.csv``

Both are generated offline by ``tools/crosswalk/build_ncaa_espn_crosswalk.py``
from three committed inputs -- the ESPN team reference tables, hoopR's
cross-provider name dictionary, and a hand-curated alias table. The build is
deterministic: there is no fuzzy matching at runtime OR at build time, and a
school that cannot be resolved is emitted with a null ``espn_team_id`` rather
than dropped.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Dict, Union

import polars as pl

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
    "espn_team_id": pl.Utf8,
    "espn_display_name": pl.Utf8,
    "espn_location": pl.Utf8,
    "espn_mascot": pl.Utf8,
    "espn_abbreviation": pl.Utf8,
    "espn_conference_name": pl.Utf8,
    "espn_conference_id": pl.Utf8,
    "match_method": pl.Utf8,
}


def ncaa_espn_team_crosswalk(
    league: str = "mbb", *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Season-keyed stats.ncaa.org -> ESPN team-id crosswalk.

    One row per ``(season, ncaa_team_id)``. Teams that could not be resolved to
    an ESPN team are kept with a null ``espn_team_id`` and
    ``match_method="unmatched"`` -- never dropped -- so the row count always
    equals ``ncaa_{league}_team_ids()``.

    Args:
        league: ``"mbb"`` (men's, 2009-10 onward) or ``"wbb"`` (women's).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        DataFrame with columns ``season`` (str, ``"YYYY-YY"``),
        ``ncaa_team_id`` (Int64 -- the season-specific stats.ncaa.org id),
        ``ncaa_team`` / ``ncaa_conference`` (str), ``espn_team_id`` (str,
        nullable -- ESPN ids are strings throughout sdv-py),
        ``espn_display_name`` / ``espn_location`` / ``espn_mascot`` /
        ``espn_abbreviation`` / ``espn_conference_name`` /
        ``espn_conference_id`` (str, nullable), and ``match_method`` (str --
        ``"exact"``, ``"dict"``, ``"alias"`` or ``"unmatched"``).

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
    if league not in _CROSSWALK_CACHE:
        raw = _league_data_bytes(league, f"ncaa_espn_team_crosswalk_{league}.csv")
        _CROSSWALK_CACHE[league] = pl.read_csv(io.BytesIO(raw), schema_overrides=_SCHEMA).select(list(_SCHEMA))
    df = _CROSSWALK_CACHE[league]
    return df.to_pandas() if return_as_pandas else df


__all__ = ["ncaa_espn_team_crosswalk"]
