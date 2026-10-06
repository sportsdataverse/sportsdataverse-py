"""Parser for the generated ``uefa`` wrappers (UEFA front-end APIs).

The keyless JSON APIs behind uefa.com are spread over four hosts --
``comp.uefa.com/v2`` (competitions, teams, players), ``match.uefa.com/v5``
(matches, livescore), ``standings.uefa.com/v1`` (standings) and
``matchstats.uefa.com/v1`` (team-statistics/{matchId}) -- and every committed
capture answers with a **bare top-level JSON array**: one element per
competition / team / player / match / livescore entry / standings group, and for
``team-statistics`` one element per team (two per match). No route answers an
envelope or a page object.

Row rule (shared with the other wave-1 families, so a route that grows a new
shape still parses):

* a list -> its elements are the rows;
* a dict of two or more dict values (an id-keyed map) -> one row per value with
  the key stored in ``id`` (:data:`ID_KEY`);
* a dict with exactly one list-valued key and only scalar siblings (an envelope
  such as ``{"total": n, "data": [...]}``) -> that list;
* any other non-empty dict (a wide page object) -> ONE row. A single match object
  is the reason the envelope branch demands scalar siblings: it carries one list
  (``referees``) beside nested ``homeTeam`` / ``awayTeam`` objects and must stay one
  row, not become its referee list;
* anything else -> no rows.

Ids (``id``, ``teamId``, ``nationalTeamId``, ``associationId``, ``organizationId``,
``clubId``) are numeric-looking **strings** on the wire and are pinned to ``Utf8``
by :mod:`sportsdataverse.soccer._frames`, which also flattens nested objects to
``snake_case`` columns (``translations.name.EN`` -> ``translations_name_en``) and
JSON-encodes list cells (``statistics``, ``items``, ``referees``).

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload.
"""

from __future__ import annotations

from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.soccer._frames import as_output, rows_to_frame

__all__ = ["ID_KEY", "parse_uefa"]

#: Column that receives the key of an id-keyed map (``{"<id>": {...}, ...}``).
ID_KEY = "id"


def _as_rows(raw: Union[Dict[str, Any], List[Any], None]) -> List[Any]:
    """Normalize a UEFA body to a row list (``[]`` for anything unusable)."""
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict) or not raw:
        return []
    if len(raw) >= 2 and all(isinstance(v, dict) for v in raw.values()):
        return [{ID_KEY: key, **value} for key, value in raw.items()]
    lists = [v for v in raw.values() if isinstance(v, list)]
    if len(lists) == 1 and not any(isinstance(v, dict) for v in raw.values()):
        return lists[0]
    return [raw]


def parse_uefa(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a UEFA front-end API body into a tidy frame.

    Covers every ``uefa`` route: ``competitions``, ``teams``, ``players``,
    ``matches``, ``livescore``, ``standings`` and ``team_statistics``. All of them
    answer with a bare JSON array; the module docstring lists the row rule that
    also handles an id-keyed map, a one-list envelope and a wide page object.

    Args:
        raw: a UEFA JSON body -- normally a top-level list of row objects.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record, snake_cased, nested objects flattened to prefixed
        columns, list cells JSON-encoded and every ``id`` / ``*_id`` column pinned
        to ``Utf8``. A zero-row frame when the payload is ``None``, empty or
        malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer.uefa import uefa_teams

            df = uefa_teams(competition_id="1", season_year="2026")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id", "international_name", "country_code").head()

    See Also:
        * `UEFA.com`_ -- the public site these keyless hosts render.

    .. _UEFA.com: https://www.uefa.com/
    """
    return as_output(rows_to_frame(_as_rows(raw)), return_as_pandas=return_as_pandas)
