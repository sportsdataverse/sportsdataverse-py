"""Runtime getter for the generated PFF Developer API wrappers (:mod:`sportsdataverse.nfl.pff_api`).

The PFF Developer API (``https://api.pff.com``, documented at developer.pff.com) is PFF's
**official** programmatic surface. It replaces the reverse-engineered ``premium.pff.com``
cookie flow in :mod:`sportsdataverse.nfl.pff_runtime` (LEGACY): one bearer API key, no
browser session, no 60-second JWT, no TLS impersonation.

**Auth precedence** (a PFF Pro subscription is required either way):

1. an explicit ``api_key=`` on the call,
2. an ``Authorization`` header already present in ``headers=``,
3. environment: ``SDV_PY_PFF_API_KEY``, then ``PFF_API_KEY`` (the name PFF's own guide uses),
4. otherwise a clear :class:`RuntimeError`.

Keys are created at https://www.pff.com/account/api-keys (``ak_live_...``) and are never
logged. The account has a per-minute read budget shared by every client holding it; a
``429`` is retried with the server's ``Retry-After``, and the transport is injectable
(``transport=``) so wrappers and tests run fully offline.

**Error vocabulary** (the error body is always ``{"error": {code, message, request_id,
details}}``): 404 -> :class:`~sportsdataverse.errors.NoDataError`; 400/422 ->
:class:`ValueError` (the request itself was rejected); 401/403/429/5xx that outlive the
retries -> :class:`~sportsdataverse.errors.AssetFetchError` -- a refused or failed fetch is
an unknown answer, never an empty frame.

**Withheld columns.** A view-only entitlement answers ``200`` with some columns REMOVED and a
``restricted`` list naming them. By default the partial body is returned with a
:class:`UserWarning` naming the columns; ``strict=True`` on any wrapper (or
``SDV_PY_PFF_STRICT=1``) raises :class:`~sportsdataverse.errors.AssetFetchError` instead --
use it in pipelines, where a missing column must never read as a missing stat.
"""

from __future__ import annotations

import json
import os
import warnings
from datetime import timedelta
from typing import Any, Callable, Dict, Optional

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError

__all__ = ["_get"]

Transport = Callable[[str, dict, dict], tuple[int, str]]

_KEY_ENV = ("SDV_PY_PFF_API_KEY", "PFF_API_KEY")
# A 403 from PFF is an entitlement answer (subscription / league), not load: never retry it.
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_RETRIES = 4
# The package response cache keys on URL + params only -- NOT the Authorization header -- so a
# cached body could be served under a different (or revoked) key. PFF bodies are per-caller
# (entitlement shapes them) and PFF marks them no-store: always bypass it.
_NO_CACHE = timedelta(0)


def _resolve_api_key(api_key: Optional[str] = None) -> Optional[str]:
    """Return the explicit key, else the first non-empty ``_KEY_ENV`` variable, else ``None``."""
    if api_key and api_key.strip():
        return api_key.strip()
    for name in _KEY_ENV:
        val = os.environ.get(name, "").strip()
        if val:
            return val
    return None


def _strict(strict: Optional[bool]) -> bool:
    """The explicit ``strict=`` flag, else ``SDV_PY_PFF_STRICT`` (1/true/yes), else ``False``."""
    if strict is not None:
        return bool(strict)
    return os.environ.get("SDV_PY_PFF_STRICT", "").strip().lower() in {"1", "true", "yes"}


def _default_transport(url: str, params: dict, headers: dict) -> tuple[int, str]:
    """Issue the GET through :func:`sportsdataverse.dl_utils.download` (retry + pooling, no cache)."""
    resp = download(
        url=url,
        params=params,
        headers=headers,
        num_retries=_RETRIES,
        retry_statuses=_RETRY_STATUSES,
        cache_ttl=_NO_CACHE,
    )
    return resp.status_code, resp.text


def _error_detail(text: str) -> str:
    """``code: message (details) [request_id]`` from PFF's error envelope, or the raw text."""
    try:
        err = json.loads(text).get("error") or {}
    except (ValueError, AttributeError):
        return (text or "").strip()[:200]
    if not isinstance(err, dict):  # a gateway's {"error": "invalid_token"}
        return (text or "").strip()[:200]
    msg = f"{err.get('code') or 'error'}: {err.get('message') or ''}".rstrip(": ")
    if err.get("details"):
        msg += " " + json.dumps(err["details"], sort_keys=True)
    if err.get("request_id"):
        msg += f" [request_id {err['request_id']}]"
    return msg


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    *,
    api_key: Optional[str] = None,
    transport: Optional[Transport] = None,
    strict: Optional[bool] = None,
    **kwargs: Any,
) -> Dict:
    """GET an ``api.pff.com`` endpoint with a bearer API key and return its JSON body.

    Args:
        url: Fully-qualified ``https://api.pff.com/...`` URL built by the generated wrapper.
        params: Query-string parameters; ``None`` values are dropped.
        headers: Extra request headers. An ``Authorization`` header here wins over every
            other key source (reuse one dict across many calls).
        api_key: PFF API key (``ak_live_...``). Falls back to ``SDV_PY_PFF_API_KEY`` /
            ``PFF_API_KEY``.
        transport: Callable ``(url, params, headers) -> (status_code, text)``; defaults to
            :func:`sportsdataverse.dl_utils.download`. Inject a fake to run offline.
        strict: What to do when PFF withheld columns (a view-only entitlement answers 200 with
            a ``restricted`` list and those columns removed): ``False`` returns the partial
            body with a :class:`UserWarning`; ``True`` raises :class:`AssetFetchError`.
            ``None`` (default) reads ``SDV_PY_PFF_STRICT`` (``1``/``true``/``yes`` = strict).
            Every generated wrapper forwards it (``pff_api_team_stats(..., strict=True)``).
        **kwargs: Accepted for forward-compatibility with generated callers; unused.

    Returns:
        The decoded JSON ``dict``.

    Raises:
        RuntimeError: No API key could be resolved.
        ValueError: PFF rejected the request (400 ``invalid_parameter`` / 422).
        NoDataError: PFF answered 404 (raised by the transport).
        AssetFetchError: 401/403/429/5xx, a 200 whose body is not a JSON object, or (strict
            mode) a body with columns withheld -- the answer is unknown or partial.

    Example:
        Offline, with an injected transport::

            import json
            from sportsdataverse.nfl.pff_api_runtime import _get

            def fake(url, params, headers):
                return 200, json.dumps({"leagues": []})

            _get("https://api.pff.com/v1/leagues", api_key="ak_test", transport=fake)

        Live (a PFF Pro API key)::

            import os
            os.environ["PFF_API_KEY"] = "ak_live_..."
            whoami = _get("https://api.pff.com/v1/auth/whoami")

        See Also:
            * `PFF Developer API`_ -- reference, CLI guide, key management

        .. _PFF Developer API: https://developer.pff.com
    """
    hdrs = {"Accept": "application/json", "User-Agent": "sportsdataverse-py"}
    hdrs.update(headers or {})
    if not any(k.lower() == "authorization" for k in hdrs):
        key = _resolve_api_key(api_key)
        if not key:
            raise RuntimeError(
                "No PFF API key: pass api_key=, or set PFF_API_KEY (or SDV_PY_PFF_API_KEY). "
                "Create one at https://www.pff.com/account/api-keys (PFF Pro)."
            )
        hdrs["Authorization"] = f"Bearer {key}"
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    status, text = (transport or _default_transport)(url, clean, hdrs)
    if status == 200:
        # every PFF operation answers a JSON object: anything else (a CDN interstitial, a
        # truncated body) is an UNKNOWN answer, never an empty one
        try:
            body = json.loads(text or "")
        except json.JSONDecodeError:
            body = None
        if not isinstance(body, dict):
            raise AssetFetchError(f"PFF {url} -> HTTP 200 with a non-object body: {(text or '').strip()[:120]!r}")
        restricted = body.get("restricted")
        if restricted:
            # A view-only entitlement answers 200 with these columns REMOVED (PFF spec,
            # RestrictedColumns: "never assume a missing column means the stat does not exist").
            # Interactive callers keep the partial answer with a warning; pipelines opt into strict.
            cols = ", ".join(map(str, restricted)) if isinstance(restricted, list) else str(restricted)
            msg = f"PFF withheld columns by entitlement on {url}: {cols} (missing != absent stat)"
            if _strict(strict):
                raise AssetFetchError(msg)
            warnings.warn(msg, UserWarning, stacklevel=3)
        return body
    if status in (400, 422):
        raise ValueError(f"PFF rejected {url}: {_error_detail(text)}")
    raise AssetFetchError(f"PFF {url} -> HTTP {status}: {_error_detail(text)}")
