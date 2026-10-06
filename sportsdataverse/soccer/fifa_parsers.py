"""Parser for the generated ``fifa`` wrappers (FIFA public API v3, ``api.fifa.com/api/v3``).

The keyless front-end API behind fifa.com answers in two shapes:

* **list routes** (``/competitions``, ``/seasons``, ``/calendar/matches``,
  ``/live/football``, ``/stadiums``, ``/teams/search``, ``/players/search``) --
  the envelope ``{"ContinuationToken", "ContinuationHash", "Results": [...]}``;
  the rows are ``Results`` and the paging tokens are dropped (the API's paging
  *request* parameter is undocumented, so a wrapper call is one first page);
* **page objects** (``/competitions/{idCompetition}``, ``/teams/{idTeam}``) --
  one wide object, which becomes a **one-row** frame.

Row rule, in order: a list is the rows; a dict with a list-valued ``Results`` is
that list; a dict of two or more dicts is an id-keyed map (rows carry the key in
``id``; no FIFA route serves one today); a dict with exactly one list-valued
top-level key is that list; any other non-empty dict is one row; anything else
is zero rows.

FIFA ids are opaque **strings** named ``IdCompetition`` / ``IdSeason`` / ``IdTeam``
/ ``IdMatch`` / ``IdPlayer`` / ``IdStadium`` (snake_cased to ``id_*``, and
``home_id_team`` / ``away_team_id_team`` once a nested side is flattened); every
such column is pinned to ``Utf8``. Localised name fields (``Name``, ``ShortName``,
``TeamName``, ``CityName``, ``StageName``, ...) are lists of
``{"Locale", "Description"}`` and stay **one JSON-encoded string cell** under the
shared list rule -- pick a locale with ``json.loads`` downstream rather than
exploding the frame. Nested event lists (line-ups, goals, bookings,
substitutions, officials) are JSON-encoded the same way.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, is_id_name, rows_to_frame, to_utf8_ids

__all__ = ["parse_fifa"]

# Column that carries the key of an id-keyed map (the shared wave-1 row rule).
ID_KEY = "id"

# ``IdCompetition`` -> ``id_competition``; ``Home.IdTeam`` -> ``home_id_team``;
# ``OfficialId`` -> ``official_id`` (already covered by ``is_id_name``).
_FIFA_ID = re.compile(r"(^|_)id(_|$)")


def _is_fifa_id(name: str) -> bool:
    """True for a FIFA id column (``id_*`` / ``*_id_*`` / the shared ``*_id`` names)."""
    return is_id_name(name) or _FIFA_ID.search(name) is not None


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Normalize a FIFA body to a row list (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict) or not raw:
        return []
    results = raw.get("Results")
    if isinstance(results, list):
        return results
    if len(raw) >= 2 and all(isinstance(v, dict) for v in raw.values()):
        return [{ID_KEY: k, **v} for k, v in raw.items()]
    lists = [v for v in raw.values() if isinstance(v, list)]
    if len(lists) == 1:
        return lists[0]
    return [raw]


def parse_fifa(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a FIFA public API v3 body into a tidy frame.

    Args:
        raw: a FIFA JSON body -- the ``{"Results": [...]}`` list envelope or a
            single page object (see the module docstring for the row rule).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record (one row for a page object), snake_cased, nested
        objects flattened to ``parent_child`` columns, list cells (localised
        names, event lists) JSON-encoded, and every ``id_*`` / ``*_id`` column
        pinned to ``Utf8``. A zero-row frame when the payload is ``None``, empty
        or malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.fifa import fifa_seasons

            df = fifa_seasons(id_competition="17", language="en")
            print(df.select("id_season", "name").head())

        Pipeline next step (one line)::

            df.filter(pl.col("id_competition") == "17")

    See Also:
        * `FIFA`_ -- data origin (the keyless front-end API behind fifa.com; not official).

    .. _FIFA: https://www.fifa.com/
    """
    df = rows_to_frame(_as_rows(raw))
    # ponytail: ``json_normalize`` gap-fills a nested object missing from some rows
    # (live ``BallPossession``) with NaN and the shared ``_frames._encode`` writes it
    # as the string "nan"; null it here. Upgrade path: one NaN guard in ``_encode``.
    df = df.with_columns(pl.col(pl.String).replace("nan", None))
    df = to_utf8_ids(df, [c for c in df.columns if _is_fifa_id(c)])
    return as_output(df, return_as_pandas=return_as_pandas)
