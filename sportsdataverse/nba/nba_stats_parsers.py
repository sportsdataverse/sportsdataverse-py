"""Parsers for the stats.nba.com / stats.wnba.com resultSets envelope.

One generic parser handles every endpoint because the response shape is uniform:
``{resultSets: [{name, headers, rowSet}]}`` (a few endpoints use singular ``resultSet``).
Honors the universal parser contract: polars by default, pandas via flag, empty/malformed
returns a zero-row frame, columns snake_cased via dl_utils.underscore.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import polars as pl

from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "parse_nba_stats_result_sets",
    "parse_nba_stats_leaguedashplayerstats",
    "parse_nba_stats_playercareerstats",
    "parse_nba_stats_commonallplayers",
]


def _result_sets(raw: dict) -> list:
    """Extract the list of result-set dicts from a stats.nba.com response envelope.

    Handles both the plural ``resultSets`` and the singular ``resultSet`` variants.

    Args:
        raw: The raw JSON response dictionary from stats.nba.com.

    Returns:
        A list of result-set dicts, each containing ``name``, ``headers``, and ``rowSet``.
    """
    if isinstance(raw.get("resultSets"), list):
        return raw["resultSets"]
    rs = raw.get("resultSet")
    if isinstance(rs, dict):
        return [rs]
    if isinstance(rs, list):
        return rs
    return []


def _to_frame(rs: dict) -> pl.DataFrame:
    """Convert a single result-set dict to a polars DataFrame.

    Args:
        rs: A dict with ``headers`` (list of str) and ``rowSet`` (list of lists).

    Returns:
        A polars DataFrame with snake_cased column names. Returns an empty DataFrame
        when ``headers`` is absent or ``rowSet`` is empty.
    """
    headers = [underscore(h) for h in rs.get("headers", [])]
    rows = rs.get("rowSet", []) or []
    if not headers:
        return pl.DataFrame()
    # Stringify any list-valued cells so polars accepts a uniform schema
    norm = [[("|".join(map(str, c)) if isinstance(c, list) else c) for c in row] for row in rows]
    if norm:
        return pl.DataFrame(norm, schema=headers, orient="row")
    return pl.DataFrame(schema={h: pl.Utf8 for h in headers})


def parse_nba_stats_result_sets(
    raw: dict,
    result_set: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame", dict]:
    """Parse a stats.nba.com / stats.wnba.com ``resultSets`` response.

    The stats.nba.com / stats.wnba.com API wraps every endpoint response in the
    same envelope: ``{resultSets: [{name, headers, rowSet}]}``. This function
    converts that envelope into tidy polars DataFrames (or pandas if requested).

    Args:
        raw: Raw JSON response dict from stats.nba.com or stats.wnba.com.
            Malformed or empty payloads return a zero-row frame rather than raising.
        result_set: If given, return only the named result-set as a DataFrame.
            If the name is not found, returns a zero-row polars DataFrame.
            If ``None`` and the response has exactly one result-set, returns that
            frame directly. If ``None`` and there are multiple result-sets, returns
            a ``dict`` mapping result-set names to DataFrames.
        return_as_pandas: When ``True``, convert the output to a pandas DataFrame
            (or dict of pandas DataFrames for multi-set responses). Defaults to
            ``False`` (polars output).

    Returns:
        * ``pl.DataFrame`` (or ``pd.DataFrame``) when ``result_set`` is given or
          the payload has exactly one result-set.
        * ``dict[str, pl.DataFrame]`` (or ``dict[str, pd.DataFrame]``) when
          ``result_set`` is ``None`` and the payload has multiple result-sets.
        * Zero-row ``pl.DataFrame`` (or ``pd.DataFrame``) on empty/malformed input.

    Example:
        Quick start — named result-set::

            import json
            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

            with open("leaguedashplayerstats.json", encoding="utf-8") as f:
                raw = json.load(f)
            df = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats")
            print(df.shape)

        All result-sets as a dict::

            out = parse_nba_stats_result_sets(raw)  # dict[str, pl.DataFrame]
            print(list(out.keys()))

        Pandas output::

            df_pd = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats",
                                                 return_as_pandas=True)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)
            * `wehoop`_ — women's basketball / WNBA (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    sets = _result_sets(raw)
    frames: dict[str, pl.DataFrame] = {rs.get("name", f"set_{i}"): _to_frame(rs) for i, rs in enumerate(sets)}

    def _maybe_pandas(df: pl.DataFrame) -> Union[pl.DataFrame, "pd.DataFrame"]:
        return df.to_pandas() if return_as_pandas else df

    if result_set is not None:
        return _maybe_pandas(frames.get(result_set, pl.DataFrame()))
    if len(frames) == 1:
        return _maybe_pandas(next(iter(frames.values())))
    return {name: _maybe_pandas(df) for name, df in frames.items()}


def parse_nba_stats_leaguedashplayerstats(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse the ``LeagueDashPlayerStats`` result-set from a stats.nba.com response.

    Convenience wrapper around :func:`parse_nba_stats_result_sets` that selects
    the ``LeagueDashPlayerStats`` result-set, which is present on the
    ``leaguedashplayerstats`` endpoint for NBA, WNBA, G-League, and Summer League.

    Args:
        raw: Raw JSON response dict from stats.nba.com or stats.wnba.com.
        return_as_pandas: When ``True``, return a pandas DataFrame. Defaults to
            ``False`` (polars output).

    Returns:
        A polars DataFrame (or pandas if ``return_as_pandas=True``) with one row per
        player. Zero-row frame when the result-set is absent or payload is malformed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_leaguedashplayerstats

            df = parse_nba_stats_leaguedashplayerstats(raw)
            print(df.select(["player_name", "pts"]).head())

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    result: Union[pl.DataFrame, "pd.DataFrame", dict] = parse_nba_stats_result_sets(
        raw, "LeagueDashPlayerStats", return_as_pandas=return_as_pandas
    )
    return result


def parse_nba_stats_playercareerstats(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[dict, "pd.DataFrame", pl.DataFrame]:
    """Parse all result-sets from a stats.nba.com ``playercareerstats`` response.

    The ``playercareerstats`` endpoint returns up to 14 named result-sets
    (regular season, post-season, all-star, college, etc.). This wrapper returns
    the full dict so callers can access any result-set by name.

    Args:
        raw: Raw JSON response dict from stats.nba.com.
        return_as_pandas: When ``True``, each dict value is a pandas DataFrame.
            Defaults to ``False`` (polars output).

    Returns:
        A ``dict`` mapping result-set names to DataFrames (polars or pandas).
        Falls back to a single DataFrame when the response has exactly one result-set.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_playercareerstats

            out = parse_nba_stats_playercareerstats(raw)
            df_reg = out["SeasonTotalsRegularSeason"]
            print(df_reg.shape)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return parse_nba_stats_result_sets(raw, return_as_pandas=return_as_pandas)


def parse_nba_stats_commonallplayers(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse the ``CommonAllPlayers`` result-set from a stats.nba.com response.

    Convenience wrapper around :func:`parse_nba_stats_result_sets` targeting the
    ``CommonAllPlayers`` result-set returned by the ``commonallplayers`` endpoint.

    Args:
        raw: Raw JSON response dict from stats.nba.com.
        return_as_pandas: When ``True``, return a pandas DataFrame. Defaults to
            ``False`` (polars output).

    Returns:
        A polars DataFrame (or pandas if ``return_as_pandas=True``) with one row
        per player. Zero-row frame when the result-set is absent or payload is malformed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_commonallplayers

            df = parse_nba_stats_commonallplayers(raw)
            print(df.columns)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    result: Union[pl.DataFrame, "pd.DataFrame", dict] = parse_nba_stats_result_sets(
        raw, "CommonAllPlayers", return_as_pandas=return_as_pandas
    )
    return result
