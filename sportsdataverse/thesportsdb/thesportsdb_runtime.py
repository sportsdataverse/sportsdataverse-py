"""Runtime for the generated TheSportsDB wrappers.

thesportsdb.com puts the API key in the **path**, not a query parameter:
``/api/v1/json/<key>/all_sports.php``. The endpoint YAML pins the documented free
test key ``3`` (30 req/min, read-only) in its host -- so the reference page prints
a base URL and "Valid URL" examples a reader can actually open -- and this getter
swaps that segment for ``$THESPORTSDB_API_KEY`` when one is set. A literal
``{key}`` placeholder is substituted too, so a hand-built URL works either way,
and a URL already carrying some other key is left alone.

A rejected key is distinguishable from "no data", which matters because an unknown **id**
answers HTTP 200 with a null member and parses to a zero-row frame. Measured 2026-10-06: a
bad key answers **HTTP 400** with ``{"Message": "Invalid Premium API key: ..."}``, which the
shared runtime raises as ``ValueError`` -- so an auth failure surfaces rather than looking
like an empty result.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from sportsdataverse import _codegen_runtime as _rt

FREE_TEST_KEY = "3"

# thesportsdb.com sits behind Cloudflare; a bare ``python-requests`` UA is a routine 403
# trigger, and a 403 is retried 15x before it surfaces. Same constant shape as the
# torvik / kenpom / football_data runtimes.
_UA = "Mozilla/5.0 (sportsdataverse-py; +https://py.sportsdataverse.org)"


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
    """
    # ``.strip()`` first: a key pasted with a trailing newline is truthy and would build
    # ``/json/%20/``, then burn the whole retry budget on a URL that can never work.
    key = (os.environ.get("THESPORTSDB_API_KEY") or "").strip() or FREE_TEST_KEY
    kwargs.setdefault("headers", {"User-Agent": _UA})
    keyed = url.replace("{key}", key).replace(f"/json/{FREE_TEST_KEY}/", f"/json/{key}/")
    return _rt._get(keyed, params, **kwargs)
