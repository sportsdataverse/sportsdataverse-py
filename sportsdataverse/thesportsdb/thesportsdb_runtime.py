"""Runtime for the generated TheSportsDB wrappers.

thesportsdb.com puts the API key in the **path**, not a query parameter:
``/api/v1/json/<key>/all_sports.php``. The endpoint YAML therefore carries the
literal ``{key}`` placeholder in its host and this getter substitutes it at call
time -- ``$THESPORTSDB_API_KEY`` when set, else the documented free test key
``3`` (30 req/min, read-only). A URL with no placeholder passes through
unchanged, so a caller may hand-build a keyed URL.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from sportsdataverse import _codegen_runtime as _rt

FREE_TEST_KEY = "3"


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
    key = os.environ.get("THESPORTSDB_API_KEY") or FREE_TEST_KEY
    return _rt._get(url.replace("{key}", key), params, **kwargs)
