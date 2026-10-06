"""Parser for the generated ``fotmob`` wrappers (FotMob's unofficial site JSON).

``www.fotmob.com/api/data/*`` (plus ``/api/trendingnews`` at the host root) is the
JSON the FotMob site renders from -- keyless as of the 2026-10-05 capture, soccer
only, not official or supported. Fourteen routes answer with four body shapes, and
one parser covers them all because the **shape decides the rows**:

* **bare list** (``audio-matches``, ``search/suggest``, ``trendingnews``): one row
  per element.
* **id-keyed map** (``tvlistings``: ``{"<matchId>": [listing, ...], ...}`` at the
  top level): one row per inner record with the map key in :data:`ID_KEY`
  (``id``). A dict is a map when it has two or more keys and either every value is
  a dict or every key is a numeric string with a list value. A record's own ``id``
  wins over the map key when both exist.
* **envelope** -- exactly one list-valued key whose items are dicts, e.g.
  ``matches`` ``{"leagues": [...], "date"}``, ``tlnews`` ``{"data": [...],
  "totalItems"}``, ``top-transfers`` ``{"transfers": [...], "hits", "maxFee"}`` and
  ``team-of-the-week/rounds`` ``{"last": {...}, "rounds": [...]}``: rows are that
  list; the sibling metadata is dropped. Envelope and list records flatten fully
  (``station_name``, ``program_root_id``).
* **page object** -- everything else (``leagues``, ``teams``, ``playerData``,
  ``matchDetails``, ``table``, ``allLeagues``): ONE wide row, flattened **one level
  deep**. A top-level object becomes prefixed columns (``details_name``,
  ``general_match_id``, ``next_match_home_name``); anything nested deeper, and every
  list, is a JSON-encoded cell (``content_lineup``, ``tabs``). Flattening further
  would turn FotMob's name-keyed maps (goal scorers keyed by surname, JSON-LD
  ``@context`` keys) into data-dependent, non-snake column names. ``matchDetails``
  carries one list (``nav`` -- tab names), whose items are strings rather than
  records: that is why the envelope rule insists on dict items.

Ids (``id``, ``league_id``, ``match_id``, ``team_id``, ``player_id`` ...) are
integers on the wire and pinned to ``Utf8`` by :mod:`sportsdataverse.soccer._frames`
(never ``"47.0"``). Package parser contract: polars by default, pandas via
``return_as_pandas=True``, and a zero-row frame -- never an exception -- for an empty,
``null`` or malformed payload (FotMob answers ``{}`` or ``null`` for unknown ids).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, rows_to_frame

__all__ = ["parse_fotmob"]

# Column the key of an id-keyed map lands in (``tvlistings`` keys are match ids).
ID_KEY = "id"


def _is_id_map(raw: Dict[str, Any]) -> bool:
    """True for ``{"<id>": record-or-records, ...}``: 2+ keys, all dict values or numeric keys over lists."""
    if len(raw) < 2:
        return False
    values = list(raw.values())
    if all(isinstance(v, dict) for v in values):
        return True
    return all(k.isdigit() for k in raw) and all(isinstance(v, list) for v in values)


def _one_level(page: Dict[str, Any]) -> Dict[str, Any]:
    """JSON-encode every container below the first level so the page flattens one level deep."""
    return {
        key: {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in value.items()}
        if isinstance(value, dict)
        else value
        for key, value in page.items()
    }


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Apply the shape rule from the module docstring (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict) or not raw:
        return []
    if _is_id_map(raw):
        rows: List[Dict[str, Any]] = []
        for key, value in raw.items():
            items = value if isinstance(value, list) else [value]
            rows.extend({ID_KEY: key, **item} for item in items if isinstance(item, dict))
        return rows
    lists = [v for v in raw.values() if isinstance(v, list)]
    if len(lists) == 1 and all(isinstance(item, dict) for item in lists[0]):
        return lists[0]
    return [_one_level(raw)]


def parse_fotmob(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any FotMob body into a tidy frame by its shape (see the module docstring).

    Args:
        raw: a decoded FotMob JSON body -- a bare list, an id-keyed map, an envelope
            around one record list, or a wide page object.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record (one row for a page object, flattened one level deep),
        snake_cased, nested cells JSON-encoded and every id column pinned to
        ``Utf8``. A zero-row frame when the payload is ``None``, empty or
        malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.fotmob import fotmob_matches

            df = fotmob_matches(date="20260301", timezone="UTC")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id", "name", "ccode").sort("name")

    See Also:
        * `FotMob`_ -- the site these routes render.

    .. _FotMob: https://www.fotmob.com/
    """
    return as_output(rows_to_frame(_as_rows(raw)), return_as_pandas=return_as_pandas)
