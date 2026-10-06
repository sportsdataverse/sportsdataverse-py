"""Runtime for the generated EuroLeague wrappers: two hosts, two body contracts.

* ``api-live.euroleague.net`` (v2 and v3) answers XML unless asked for JSON, so
  ``Accept: application/json`` is merged into every request's headers.
* ``live.euroleague.net/api`` answers JSON by default, and an **empty 200 body** is
  its "no such game" sentinel (``/Points?gamecode=9999&seasoncode=E2025``). That one
  case returns ``{}`` -- the family parsers turn it into a zero-row frame -- instead
  of the :class:`~sportsdataverse.errors.AssetFetchError` the shared getter raises
  for an empty 2xx body (which on every other host means a block or a throttle).
  A non-2xx, a dead connection and a non-JSON body keep the package error vocabulary.
"""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse import _codegen_runtime as _rt

ACCEPT_JSON = {"Accept": "application/json"}
LIVE_HOST = "https://live.euroleague.net/api"


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET ``url`` as JSON under the contract of its host (see the module docstring)."""
    if url.startswith(LIVE_HOST):
        return _get_live(url, params, **kwargs)
    headers = {**ACCEPT_JSON, **(kwargs.pop("headers", None) or {})}
    return _rt._get(url, params, headers=headers, **kwargs)


def _get_live(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a ``live.euroleague.net/api`` route: ``{}`` for the empty-body "no such game" answer."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    with _rt._transport_errors(url):
        resp = _rt.download(url=url, params=clean, **kwargs)
    _rt._check_response(resp, url)
    if not (getattr(resp, "text", "") or "").strip():
        return {}
    return _rt._json_body(resp, url)
