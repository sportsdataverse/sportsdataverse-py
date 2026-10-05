"""Parsers for the generated ``fox_api_*`` wrappers (``api.foxsports.com``).

Fox's Bifrost API is a layout API (sections -> tables -> rows -> cells), so a
handful of payload shapes cover the 38 endpoints. The dedicated parsers here are
thin adapters over the proven row builders in :mod:`sportsdataverse._fox_layout`
(the hand-written ``fox_<league>_*`` wrappers use the same ones); everything else
falls through to :func:`parse_fox_api`, a generic "largest record list" flattener.

Parser contract: polars by default, pandas via ``return_as_pandas=True``, a
zero-row frame (never an exception) on empty / malformed input, snake_case
column names via :func:`sportsdataverse.dl_utils.underscore`. Bifrost ids stay
strings (``"11195"``); nothing here ever routes an id through a float.
"""

from __future__ import annotations

import json
from collections import deque
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Union

import polars as pl

from sportsdataverse import _fox_layout as _fx
from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

__all__ = [
    "parse_fox_api",
    "parse_fox_api_events",
    "parse_fox_api_header",
    "parse_fox_api_nav",
    "parse_fox_api_polls",
    "parse_fox_api_roster",
    "parse_fox_api_scorechip",
    "parse_fox_api_search",
    "parse_fox_api_standings",
    "parse_fox_api_trending",
]

_Frame = Union[pl.DataFrame, "pd.DataFrame"]


def _flatten(rec: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in rec.items():
        key = f"{prefix}{underscore(str(k))}"
        if isinstance(v, dict):
            out.update(_flatten(v, f"{key}_"))
        elif isinstance(v, list):
            out[key] = json.dumps(v, default=str)
        else:
            out[key] = v
    return out


def _largest_record_list(payload: Any) -> List[Dict[str, Any]]:
    """Breadth-first search for the longest list of dicts anywhere in ``payload``."""
    best: List[Dict[str, Any]] = []
    queue: deque = deque([payload])
    while queue:
        node = queue.popleft()
        if isinstance(node, dict):
            queue.extend(node.values())
        elif isinstance(node, list):
            recs = [x for x in node if isinstance(x, dict)]
            if len(recs) > len(best):
                best = recs
            queue.extend(recs)
    return best


def _homogenize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Make each column single-typed (polars refuses mixed ones): an int/float mix is
    promoted to float, any other mix is stringified."""
    kinds: Dict[str, set] = {}
    for r in rows:
        for k, v in r.items():
            if v is not None:
                kinds.setdefault(k, set()).add(bool if isinstance(v, bool) else type(v))
    to_float = {k for k, t in kinds.items() if t == {int, float}}
    to_str = {k for k, t in kinds.items() if len(t) > 1} - to_float
    if not (to_float or to_str):
        return rows

    def fix(k: str, v: Any) -> Any:
        if v is None:
            return v
        return float(v) if k in to_float else (str(v) if k in to_str else v)

    return [{k: fix(k, v) for k, v in r.items()} for r in rows]


def _out(rows: List[Dict[str, Any]], return_as_pandas: bool) -> _Frame:
    if not rows:
        if return_as_pandas:
            import pandas as pd

            return pd.DataFrame()
        return pl.DataFrame()
    return _fx.frame(_homogenize(rows), return_as_pandas)


def _safe(fn: Callable[[Dict], List[Dict]], raw: Any) -> List[Dict[str, Any]]:
    """Run a layout row-builder; a malformed payload yields ``[]`` rather than raising."""
    if not isinstance(raw, dict):
        return []
    try:
        return fn(raw)
    except (AttributeError, KeyError, IndexError, TypeError, ValueError, RecursionError):
        return []


def parse_fox_api(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Generic Bifrost / Fox payload -> one row per record of the largest record list.

    Args:
        raw: Decoded JSON body (any Fox endpoint).
        return_as_pandas: Return pandas instead of polars.

    Returns:
        Flattened rows (nested dicts joined with ``_``, lists JSON-encoded); an
        empty frame when the body holds no record list.

    Raises:
        None: empty or malformed input returns an empty frame.

    Example:
        Flatten any Fox body::

            from sportsdataverse.fox.fox_api_parsers import parse_fox_api
            df = parse_fox_api({"items": [{"id": 1, "meta": {"a": "x"}}]})
    """
    try:
        rows = [_flatten(r) for r in _largest_record_list(raw)]
    except (AttributeError, TypeError, ValueError, RecursionError):
        rows = []
    return _out(rows, return_as_pandas)


def _dedicated(builder: Callable[[Dict], List[Dict]]) -> Callable[..., _Frame]:
    def parse(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
        rows = _safe(builder, raw)
        return _out(rows, return_as_pandas) if rows else parse_fox_api(raw, return_as_pandas=return_as_pandas)

    return parse


def _roster_rows(raw: Dict) -> List[Dict]:
    rows = _fx.parse_roster(raw, "")
    return [{k: v for k, v in r.items() if k != "team_id"} for r in rows]


_events = _dedicated(_fx.parse_segment_events)
_standings = _dedicated(_fx.parse_standings)
_polls = _dedicated(_fx.parse_polls)
_nav = _dedicated(_fx.parse_nav_items)
_header = _dedicated(_fx.parse_header)
_search = _dedicated(_fx.parse_search_results)
_trending = _dedicated(_fx.parse_trending)
_roster = _dedicated(_roster_rows)


def parse_fox_api_events(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Scoreboard / schedule / segment payloads -> one row per game (generic fallback for the nav shell)."""
    return _events(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_standings(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """``standingsSections`` tables -> wide rows (one per team), generic fallback otherwise."""
    return _standings(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_polls(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """League poll tables -> one row per ranked team, generic fallback otherwise."""
    return _polls(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_nav(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """teamnav / conferences / explore-browse -> one row per navigable entity."""
    return _nav(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_header(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Entity header (league or team) -> a single descriptive row."""
    return _header(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_search(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Search content / entities / popular -> one row per hit."""
    return _search(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_trending(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Trending articles / videos -> one row per item."""
    return _trending(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_roster(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Team roster groups -> one row per athlete (``athlete_id`` as a string)."""
    return _roster(raw, return_as_pandas=return_as_pandas)


def parse_fox_api_scorechip(raw: Any, *, return_as_pandas: bool = False) -> _Frame:
    """Single score chip (one game) -> a one-row frame; empty unless the body carries an ``id``."""
    if not isinstance(raw, dict) or not raw.get("id"):
        return _out([], return_as_pandas)
    return _out([_flatten(raw)], return_as_pandas)
