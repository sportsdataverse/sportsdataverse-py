"""Parser for the generated ``sleeper`` wrappers (Sleeper fantasy API v1).

``api.sleeper.app/v1`` is keyless and read-only, and every route answers with one
of three JSON shapes. One parser covers all fifteen routes with this row rule:

* **list** (``rosters``, ``users``, ``matchups``, ``transactions``, ``traded_picks``,
  ``drafts``, ``draft_picks``, ``winners_bracket``, ``user_leagues``,
  ``trending_adds``) -> one row per element;
* **id-keyed map** (``/players/nfl`` is ``{player_id: {...}}``, ~5 MB live) -> one
  row per value, with the key written to ``player_id``;
* **page object** (``user``, ``league``, ``state``, ``draft``) -> exactly one row,
  nested fixed-key objects (``settings``, ``scoring_settings``, ``metadata``)
  flattened to ``settings_num_teams``-style columns;
* anything else (``None``, ``{}``, ``[]``, a scalar) -> a zero-row frame.

Sleeper ids (``player_id``, ``league_id``, ``user_id``, ``draft_id``,
``transaction_id``, ...) are 18-digit **strings**; a numeric cast overflows a
float and a float cast writes ``"123.0"``. Every column named ``id`` / ``*_id`` /
``*_code`` is therefore pinned to ``Utf8``, routed through ``Int64`` first when
the source serialized it as a number (``roster_id``, ``matchup_id``, the
``owner_id`` of a traded pick).

Nested dicts keyed by a player or user id (``players_points``, ``draft_order``,
``slot_to_roster_id``, ``adds``, ``drops``, ``player_map``, and a roster
``metadata`` carrying ``p_nick_<player_id>`` nicknames) are kept as one
JSON-encoded cell rather than flattened: flattening them would make the column
set depend on who was rostered that week. List cells are JSON-encoded too.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, and snake_cased columns.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_sleeper"]

ID_KEY = "player_id"

_ID = re.compile(r"(^|_)(id|code)$")

# Nested objects keyed by a player / user / slot id. Kept as one JSON cell so the
# column set does not vary with the league's roster.
# ponytail: a name list; add a key here when a new id-keyed map shows up.
_ID_KEYED_MAPS = frozenset({"players_points", "draft_order", "slot_to_roster_id", "adds", "drops", "player_map"})


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    """Apply the module row rule; ``[]`` for anything unusable."""
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if not isinstance(raw, dict) or not raw:
        return []
    if len(raw) >= 2 and all(isinstance(v, dict) for v in raw.values()):
        return [{ID_KEY: k, **v} for k, v in raw.items()]
    return [raw]


def _is_id_keyed(key: str, value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    # A roster's ``metadata`` mixes fixed flags with one ``p_nick_<player_id>`` per player.
    return key in _ID_KEYED_MAPS or (key == "metadata" and any(k.startswith("p_nick_") for k in value))


def _freeze(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: json.dumps(v) if _is_id_keyed(k, v) else v for k, v in row.items()}


def _encode(value: Any) -> Any:
    return json.dumps(value) if isinstance(value, (list, dict)) else value


def _to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    pdf = pd.json_normalize([_freeze(r) for r in rows], sep="_")
    pdf.columns = [underscore(str(c)) for c in pdf.columns]
    for name in pdf.columns:
        if pdf[name].dtype == object:
            pdf[name] = pdf[name].map(_encode)
    df = pl.from_pandas(pdf)
    ids = []
    for name, dtype in df.schema.items():
        if not _ID.search(name) or dtype == pl.String:
            continue
        expr = pl.col(name)
        if dtype.is_float():
            expr = expr.cast(pl.Int64, strict=False)
        ids.append(expr.cast(pl.String).alias(name))
    return df.with_columns(ids) if ids else df


def parse_sleeper(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any Sleeper API v1 body into a tidy frame.

    Args:
        raw: a decoded Sleeper JSON body -- a list of records, the ``/players/nfl``
            id-keyed map, or a single page object (see the module docstring for
            the row rule).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per record (one row for a page object), snake_cased, with every
        ``id`` / ``*_id`` column pinned to ``Utf8`` and nested lists / id-keyed
        maps JSON-encoded. A zero-row frame when the payload is ``None``, empty
        or malformed -- callers can chain without a null-check.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.nfl.sleeper import sleeper_rosters

            df = sleeper_rosters(league_id="289646328504385536")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("roster_id", "owner_id", "settings_wins", "settings_fpts")

    See Also:
        * `Sleeper API docs`_ -- the route reference this family wraps.

    .. _Sleeper API docs: https://docs.sleeper.com/
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
