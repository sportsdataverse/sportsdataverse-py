"""Parser for the generated ``euroleague`` wrappers (EuroLeague Competition Engine API).

``api-live.euroleague.net/v2`` is keyless (``Accept: application/json`` is sent by
:mod:`sportsdataverse.euroleague.euroleague_runtime`; without it the API answers
XML) and serves two body shapes:

* **List routes** (``competitions``, ``seasons``, ``rounds``, ``clubs``, ``people``,
  ``games``) answer an envelope ``{"total": n, "data": [...]}``. Rows are ``data``;
  ``total`` is paging metadata and is dropped.
* **The box score** (``games/{gameCode}/stats``) is a page object
  ``{"local": {...}, "road": {...}}`` (home / away). It becomes **one row**: the
  nested ``coach`` / ``team`` / ``total`` blocks flatten to prefixed columns
  (``local_total_points``, ``road_coach_code``) and each side's ``players`` list is
  kept as a JSON-encoded cell.

Row rule: a list is the rows; a dict with exactly one list-valued top-level key is
that list; any other non-empty dict is a single row; anything else is zero rows.
The "id-keyed map" rule used by sibling families is deliberately **not** applied
here -- the box score's ``{"local", "road"}`` object would otherwise split into
two rows.

Ids and codes (``id``, ``group_id``, ``code``, ``competition_code``, ``season_code``,
``game_code``, ``tv_code``, ``venue_code``, ...) are opaque join keys and are pinned
to ``Utf8``; an integer one (``gameCode``, ``externalId``) is cast through ``Int64``
first so it can never stringify as ``"406.0"``.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, rows_to_frame, to_utf8_ids

__all__ = ["parse_euroleague"]

# ``id`` / ``*_id`` / ``code`` / ``*_code`` -- every EuroLeague join key.
_ID = re.compile(r"(^|_)(id|code)$")


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Normalize a EuroLeague body to a row list (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict) and raw:
        lists = [v for v in raw.values() if isinstance(v, list)]
        if len(lists) == 1:
            return lists[0]
        return [raw]
    return []


def parse_euroleague(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a EuroLeague Competition Engine API body into a tidy frame.

    Covers every ``euroleague`` route: the ``{"total", "data": [...]}`` list
    envelopes (``competitions``, ``seasons``, ``rounds``, ``clubs``, ``people``,
    ``games``) and the ``{"local", "road"}`` box-score page object (``game_stats``).

    Args:
        raw: a EuroLeague JSON body -- an envelope whose ``data`` list becomes the
            rows, a bare list, or a page object that becomes a single row.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased, nested objects flattened to prefixed
        columns, list cells JSON-encoded, and every ``id`` / ``*_id`` / ``code`` /
        ``*_code`` column pinned to ``Utf8``. A zero-row frame when the payload is
        ``None``, empty or malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.euroleague.euroleague import euroleague_games

            df = euroleague_games(competition_code="E", season_code="E2025")
            print(df.shape)

        Pipeline next step (one line)::

            import polars as pl

            df.filter(pl.col("played") == True).select("game_code", "local_club_code", "road_club_code")

    See Also:
        * `EuroLeague Basketball`_ -- the site the API serves.

    .. _EuroLeague Basketball: https://www.euroleaguebasketball.net/
    """
    df = rows_to_frame(_as_rows(raw))
    df = to_utf8_ids(df, [c for c in df.columns if _ID.search(c)])
    return as_output(df, return_as_pandas=return_as_pandas)
