"""Shared runtime for the European hockey flat-API families (``hockey/<lg>/``).

Every European provider is wrapped politely: at least one second between two requests to
the same host. :func:`_polite_wait` is the one throttle, so a family runtime with its own
transport (KHL's session, HockeyAllsvenskan's POST bodies, VHL's pinned resolve) calls it
before each request instead of writing another. :func:`_get` is the plain JSON getter for
families that need nothing else: the shared ``_codegen_runtime._get`` over :data:`SESSION`,
whose every request (each retry ``download()`` makes included) waits its turn, so the error
vocabulary (404 -> ``NoDataError``, 400/422 -> ``ValueError``, anything else ->
``AssetFetchError``) holds unchanged.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from sportsdataverse import _codegen_runtime as _rt

__all__ = ["MIN_INTERVAL", "SESSION", "PoliteSession", "_get", "_polite_wait"]

#: Minimum seconds between two requests to one host (Phase B ruling: at least 1 s).
MIN_INTERVAL = 1.0

_last: Dict[str, float] = {}
_lock = threading.Lock()


def _polite_wait(url: str, min_interval: float = MIN_INTERVAL) -> None:
    """Sleep until ``min_interval`` seconds have passed since the last request to ``url``'s host.

    Args:
        url: the URL about to be requested; only its host is used.
        min_interval: the spacing to enforce, in seconds.
    """
    host = urlsplit(url).netloc
    with _lock:  # reserve this call's slot, then sleep outside the lock so other hosts are not held up
        now = time.monotonic()
        slot = max(now, _last.get(host, float("-inf")) + min_interval)
        _last[host] = slot
    if slot > now:
        time.sleep(slot - now)


class PoliteSession(requests.Session):
    """A ``requests.Session`` whose every request first calls :func:`_polite_wait`.

    ``dl_utils.download()`` retries inside one call; handing it this session spaces every
    attempt (and records it in the per-host clock), not just the first one.
    """

    def request(self, method: str | bytes, url: str | bytes, *args: Any, **kwargs: Any) -> requests.Response:
        """Wait for ``url``'s host slot, then send the request as ``requests.Session`` does."""
        _polite_wait(url if isinstance(url, str) else url.decode())
        return super().request(method, url, *args, **kwargs)


#: The pooled, throttled session every European hockey JSON request goes through.
SESSION = PoliteSession()
for _scheme in ("https://", "http://"):
    SESSION.mount(_scheme, HTTPAdapter(pool_connections=16, pool_maxsize=32))


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET ``url`` as JSON; every attempt waits at least :data:`MIN_INTERVAL` after its host's last.

    Args:
        url: fully-built request URL.
        params: query-string parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse._codegen_runtime._get`; ``session``
            defaults to :data:`SESSION`.

    Returns:
        The parsed JSON body.
    """
    kwargs.setdefault("session", SESSION)
    return _rt._get(url, params, **kwargs)
