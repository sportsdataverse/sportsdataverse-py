"""Parsers for the generated ``on3`` wrappers (on3.com Next.js data routes).

Both rankings payloads share one envelope::

    {"pageProps": {"playerData"|"teamData": {"relatedModel": {...},
                                             "pagination": {...},
                                             "list": [...]}}}

The parsers follow the package-wide parser contract: polars by default,
pandas via ``return_as_pandas=True``, zero-row frame on empty / malformed
payloads, snake-cased columns via :func:`sportsdataverse.dl_utils.underscore`,
``pandas.json_normalize`` for nested flattening, and list/dict-valued cells
stringified so polars accepts the frame.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = [
    "parse_on3_rankings",
    "parse_on3_rdb",
    "parse_on3_team_rankings",
]


def _rows_to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    """Flatten a list of On3 ranking entries into a tidy polars frame.

    Handles the two On3-specific wrinkles: snake-cased names can collide
    (``person.highSchoolName`` vs ``person.highSchool.name`` both map to
    ``person_high_school_name`` — later duplicates gain a ``_2`` suffix), and
    several cells are list/dict-valued (``ratings``, ``person.predictions``)
    or mixed-type (``highSchoolRating``) — those are JSON/str-encoded so the
    pandas→polars handoff never rejects the frame.
    """
    if not rows:
        return pl.DataFrame()
    # a bare scalar array (e.g. filters/status -> list of strings) has no keys to
    # flatten: surface it as a single `value` column; mixed lists wrap scalars the
    # same way so json_normalize only ever sees dicts
    if not any(isinstance(r, dict) for r in rows):
        return pl.DataFrame({"value": [str(r) for r in rows]})
    rows = [r if isinstance(r, dict) else {"value": r} for r in rows]
    df = pd.json_normalize(rows, sep="_")
    seen: Dict[str, int] = {}
    cols: List[str] = []
    for c in df.columns:
        name = underscore(str(c))
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 1
        cols.append(name)
    df.columns = cols
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].map(
                lambda v: (
                    json.dumps(v) if isinstance(v, (list, dict)) else (v if v is None or isinstance(v, str) else str(v))
                ),
            )
    return pl.from_pandas(df)


def _list_at(raw: Optional[Dict[str, Any]], data_key: str) -> List[Dict[str, Any]]:
    """Extract ``pageProps.{data_key}.list`` defensively (``[]`` on any miss)."""
    page_props = (raw or {}).get("pageProps") or {}
    data = page_props.get(data_key) or {}
    rows = data.get("list") if isinstance(data, dict) else None
    return rows if isinstance(rows, list) else []


def parse_on3_rankings(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an On3 player-rankings payload (``player`` / ``industry-player``).

    Args:
        raw: data-route payload (``{"pageProps": {"playerData": ...}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per ranked recruit: On3 + consensus ranks/ratings/stars,
        position/state ranks, NIL valuation, commitment / transfer status and
        the flattened ``person_*`` identity columns. Zero-row frame when the
        payload is empty or malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_player_rankings
            df = on3_player_rankings(sport="football", year=2026)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("person_rating_stars") == 5).head()

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    df = _rows_to_frame(_list_at(raw, "playerData"))
    return df.to_pandas() if return_as_pandas else df


def parse_on3_team_rankings(
    raw: Optional[Dict[str, Any]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an On3 team-rankings payload (``team`` / ``industry-team``).

    Args:
        raw: data-route payload (``{"pageProps": {"teamData": ...}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per team class: commit counts, star counts, applied/average
        ratings (On3 + consensus), NIL average and the flattened
        ``organization_*`` columns. Zero-row frame when the payload is empty
        or malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_team_rankings
            df = on3_team_rankings(sport="football", year=2026)
            print(df.shape)

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    df = _rows_to_frame(_list_at(raw, "teamData"))
    return df.to_pandas() if return_as_pandas else df


def parse_on3_rdb(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an On3 Recruit Database (RDB) payload into a tidy frame.

    The RDB serves every endpoint in one of three envelope shapes, all handled
    here:

    * **paged** -- ``{"relatedModel": ..., "pagination": {...}, "list": [...]}``:
      rows are ``raw["list"]``.
    * **single object** -- a bare ``dict`` without a ``list`` key (e.g. a player
      profile / latest valuation): the one object becomes a single row.
    * **bare array** -- a top-level ``list`` (e.g. ``all-rankings``,
      ``filters/status``): rows are the list itself.

    Args:
        raw: an RDB JSON body (dict or list) as returned by
            :func:`sportsdataverse.cfb.on3_runtime._get`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record with snake-cased, ``json_normalize``-flattened
        columns (list/dict-valued cells stringified). A zero-row frame when the
        payload is ``None`` / empty / malformed — callers can chain without a
        null-check.

    Example:
        Quick start::

            from sportsdataverse.cfb import on3_commits_latest
            df = on3_commits_latest(sport_key=1)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("commit_status_committed") == True).head()

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    if isinstance(raw, list):
        rows: List[Any] = raw
    elif isinstance(raw, dict):
        listed = raw.get("list")
        rows = listed if isinstance(listed, list) else ([raw] if raw else [])
    else:
        rows = []
    df = _rows_to_frame(rows)
    return df.to_pandas() if return_as_pandas else df
