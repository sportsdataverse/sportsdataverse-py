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
