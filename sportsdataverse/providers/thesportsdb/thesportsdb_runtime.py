"""Runtime for the generated TheSportsDB wrappers.

thesportsdb.com puts the API key in the **path**, not a query parameter:
``/api/v1/json/<key>/all_sports.php``. The endpoint YAML pins the documented free
test key ``123`` in its host -- so the reference page prints a base URL and "Valid URL"
examples a reader can actually open -- and this getter swaps that segment for
``$THESPORTSDB_API_KEY`` when one is set. A literal ``{key}`` placeholder is substituted
too, so a hand-built URL works either way, and a URL already carrying some other key is
left alone. The old free key ``3`` answers HTTP 403 ("permanently disabled", measured
2026-10-20).

A rejected key is distinguishable from "no data", which matters because an unknown **id**
answers HTTP 200 with a null member and parses to a zero-row frame. Measured 2026-10-06: a
bad key answers **HTTP 400** with ``{"Message": "Invalid Premium API key: ..."}``, which the
shared runtime raises as ``ValueError`` -- so an auth failure surfaces rather than looking
like an empty result.

The free key also caps every list route at a fixed row count (gap-B-038): a list that
reaches the cap is warned about, because it is most likely a truncated sample.
"""

from __future__ import annotations

import os
import warnings
from typing import Any, Dict, Optional

from sportsdataverse import _codegen_runtime as _rt

FREE_TEST_KEY = "123"

# thesportsdb.com sits behind Cloudflare; a bare ``python-requests`` UA is a routine 403
# trigger, and a 403 is retried 15x before it surfaces. Same constant shape as the
# torvik / kenpom / football_data runtimes.
_UA = "Mozilla/5.0 (sportsdataverse-py; +https://py.sportsdataverse.org)"

#: route -> (envelope key, the most rows the free key returns), measured 2026-10-20 with key
#: ``123`` (EPL 4328 / Arsenal 133604 / season 2025-2026; the full lists are 20 teams, 20 table
#: rows, 380 events). A list that long under the free key is probably cut off.
_FREE_KEY_CAPS = {
    "all_sports.php": ("sports", 2),
    "all_leagues.php": ("leagues", 10),
    "search_all_teams.php": ("teams", 10),
    "lookup_all_players.php": ("player", 10),
    "searchplayers.php": ("player", 1),
    "eventsseason.php": ("events", 15),
    "eventsnextleague.php": ("events", 1),
    "lookuptable.php": ("table", 5),
}


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Dict:
    """GET ``url`` with ``{key}`` replaced by the caller's TheSportsDB key.

    Args:
        url: Endpoint URL, usually still carrying the literal ``{key}`` segment.
        params: Query parameters; ``None`` values are dropped downstream.
        **kwargs: Forwarded to :func:`sportsdataverse._codegen_runtime._get`.

    Returns:
        The decoded JSON body.

    Raises:
        NoDataError: TheSportsDB answered 404.
        ValueError: TheSportsDB answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx or a connection failure after retries.

    Warns:
        UserWarning: the free test key answered a list route with as many rows as it ever
            returns there, so the list is probably truncated.
    """
    # ``.strip()`` first: a key pasted with a trailing newline is truthy and would build
    # ``/json/%20/``, then burn the whole retry budget on a URL that can never work.
    key = (os.environ.get("THESPORTSDB_API_KEY") or "").strip() or FREE_TEST_KEY
    kwargs.setdefault("headers", {"User-Agent": _UA})
    keyed = url.replace("{key}", key).replace(f"/json/{FREE_TEST_KEY}/", f"/json/{key}/")
    body = _rt._get(keyed, params, **kwargs)
    route = keyed.split("?", 1)[0].rsplit("/", 1)[-1]
    cap = _FREE_KEY_CAPS.get(route)
    rows = body.get(cap[0]) if cap is not None and isinstance(body, dict) else None
    if cap is not None and key == FREE_TEST_KEY and isinstance(rows, list) and len(rows) >= cap[1]:
        warnings.warn(
            f"TheSportsDB's free test key returns at most {cap[1]} rows from {route} and this answer has "
            f"{len(rows)}: the list is probably truncated. Set THESPORTSDB_API_KEY to a premium key for the full list.",
            UserWarning,
            stacklevel=3,
        )
    return body
