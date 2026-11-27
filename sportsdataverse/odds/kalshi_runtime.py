"""HTTP getter for the ``kalshi_*`` wrappers: the shared JSON getter plus a second Kalshi host.

Kalshi serves the keyless Trade API v2 market data from two hosts that front one backend:
``api.elections.kalshi.com`` (behind a CDN) and ``external-api.kalshi.com`` (a load balancer, no
CDN). Both returned byte-identical bodies on 2026-11-26. The wrappers call the first; this getter
retries a request on the second only when the first **failed** -- a connection error that
outlived the retries, or a 5xx. A 4xx is an answer about the request (404 unknown ticker, 400 bad
parameter, 429 rate limit), and the second host would give the same answer, so it never fails over.

Everything else -- retries, ``None``-param stripping, the error vocabulary -- is
:func:`sportsdataverse._codegen_runtime._get`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from sportsdataverse import _codegen_runtime as _rt

logger = logging.getLogger(__name__)

PRIMARY_HOST = "https://api.elections.kalshi.com/trade-api/v2"
FALLBACK_HOST = "https://external-api.kalshi.com/trade-api/v2"


def _failed(resp: Any) -> bool:
    """A response the fallback host should get a chance at: none at all, or a 5xx."""
    status = getattr(resp, "status_code", None) if resp is not None else 500
    return isinstance(status, int) and status >= 500


def _failover(transport: Callable[..., Any] | None = None) -> Callable[..., Any]:
    """A transport: ``transport(**kwargs)`` (default ``download``), repeated on :data:`FALLBACK_HOST` after a
    connection error or a 5xx. ``download`` is looked up at call time, so a test that patches it still intercepts."""

    def call(**kwargs: Any) -> Any:
        fetch = transport or _rt.download
        url = kwargs.get("url", "")
        if not url.startswith(PRIMARY_HOST):
            return fetch(**kwargs)
        try:
            resp = fetch(**kwargs)
        except OSError as exc:  # requests' connection errors, re-raised by download once its retries are spent
            if isinstance(exc, ValueError):  # InvalidURL / InvalidHeader: the request is malformed, not the host down
                raise
            reason = type(exc).__name__
        else:
            if not _failed(resp):
                return resp
            reason = f"HTTP {getattr(resp, 'status_code', None)}"
        fallback = FALLBACK_HOST + url[len(PRIMARY_HOST) :]
        logger.warning("Kalshi %s failed (%s); retrying on %s", PRIMARY_HOST, reason, FALLBACK_HOST)
        return fetch(**{**kwargs, "url": fallback})

    return call


def _get(url: str, params: dict | None = None, **kwargs: Any) -> Any:
    """GET a Kalshi Trade API v2 URL as JSON, failing over to the second host (see the module docstring).

    A caller's ``transport=`` replaces ``download`` on both hosts; the failover still applies.

    Raises:
        NoDataError: Kalshi answered 404.
        ValueError: Kalshi answered 400 / 422.
        AssetFetchError: both hosts failed; the primary answered another non-2xx (401/403/408/429), which
            never fails over; or a host answered 2xx with an empty or non-JSON body.
    """
    return _rt._get(url, params, transport=_failover(kwargs.pop("transport", None)), **kwargs)
