"""Shared runtime for the international basketball flat-API families (``basketball/<lg>/``).

Every provider is wrapped politely: at least one second between two requests to the same host.
:func:`_polite_wait` is the one throttle, so a family runtime with its own transport (a signed
header, a decrypted body, an HTML page) calls it before each request instead of writing another.
:data:`SESSION` is a ``requests.Session`` whose every request waits its turn, so
``dl_utils.download()`` retries are spaced too. :func:`_get` is the plain JSON getter: the shared
``_codegen_runtime._get`` over :data:`SESSION`, with an optional proxy read from a named
environment variable (:func:`proxy_from_env`; Genius' CloudFront and the CBA host refuse
datacenter IPs). The proxy URL can carry credentials, so it is never logged or put in an error.

**HTTP 500 policy (one rule for every family).** A 500 is retried (with the other transient statuses 429 / 502 /
503 / 504) unless the provider uses 500 for a deterministic answer, in which case retrying only adds load. Not
retried, because their refs captures show 500 for an unknown or rejected id, body or parameter: ``cba`` (a body it
dislikes), ``cebl``, ``easl`` (an unknown fixture), ``fiba_hub`` (a missing parameter), ``genius`` (the 402-byte
"A website error has occurred" page), ``ibba`` (the ASMX stack trace), ``kbl``, ``lba``, ``dbl`` (sportality),
``plg`` (both specs: an unknown id) and ``tbf``; ``tpbl`` retries 500 except on the player game log (an unknown
``division_id``). Retried: ``aba``, ``acb``, ``bleague``, ``bnxt``, ``feb``, ``lkl``, ``scb`` and the families on the package default (``atrium``,
``kzs``, ``nbl_au``, ``vtb``, ``wjbl``), whose captures show no 500 for an unknown id. A new family follows
the same rule: list it here when its captures show a deterministic 500.

**Redirects.** A request that carries a credential header (``cebl`` X-Api-Key, ``bnxt`` X-Authorization, ``scb``
x-api-key / x-api-secret) goes through :data:`NOREDIRECT_SESSION`: a 3xx is never followed, so the header cannot
leave the host, and it surfaces as ``AssetFetchError`` (any non-2xx).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError

__all__ = [
    "MIN_INTERVAL",
    "NOREDIRECT_SESSION",
    "SESSION",
    "NoRedirectSession",
    "PoliteSession",
    "_get",
    "_polite_wait",
    "proxy_from_env",
]

#: Minimum seconds between two requests to one host (Phase B ruling: at least 1 s).
MIN_INTERVAL = 1.0

_last: Dict[str, float] = {}
_lock = threading.Lock()

# A proxied request logs nothing through dl_utils: its retry lines quote the transport exception,
# which can carry the proxy URL (credentials included).
_QUIET = logging.getLogger("sdv.basketball.proxied")
_QUIET.addHandler(logging.NullHandler())
_QUIET.propagate = False


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


class NoRedirectSession(PoliteSession):
    """The polite session with redirects off: a 3xx is returned, never followed (credential headers stay on-host)."""

    def request(self, method: str | bytes, url: str | bytes, *args: Any, **kwargs: Any) -> requests.Response:
        """Send the request with ``allow_redirects=False``."""
        kwargs["allow_redirects"] = False
        return super().request(method, url, *args, **kwargs)


#: The pooled, throttled session every international basketball request goes through.
SESSION = PoliteSession()
#: The same, with redirects off, for requests that carry a credential header.
NOREDIRECT_SESSION = NoRedirectSession()
for _scheme in ("https://", "http://"):
    SESSION.mount(_scheme, HTTPAdapter(pool_connections=16, pool_maxsize=32))
    NOREDIRECT_SESSION.mount(_scheme, HTTPAdapter(pool_connections=8, pool_maxsize=16))


def proxy_from_env(env_var: str) -> Optional[Dict[str, str]]:
    """The ``requests`` ``proxies=`` mapping for the proxy URL in ``$env_var``, or ``None`` when unset.

    Args:
        env_var: the NAME of the environment variable (``GENIUS_PROXY``, ``CBA_PROXY``). Its value
            is never logged, returned in an error or stored.

    Returns:
        ``{"http": url, "https": url}``, or ``None`` when the variable is unset or blank.
    """
    url = os.environ.get(env_var, "").strip()
    return {"http": url, "https": url} if url else None


def _get(url: str, params: Optional[dict] = None, *, proxy_env: Optional[str] = None, **kwargs: Any) -> Any:
    """GET ``url`` as JSON; every attempt waits at least :data:`MIN_INTERVAL` after its host's last.

    Args:
        url: fully-built request URL.
        params: query-string parameters; ``None`` values are dropped.
        proxy_env: name of an environment variable holding a proxy URL; used when set.
        **kwargs: forwarded to :func:`sportsdataverse._codegen_runtime._get`; ``session``
            defaults to :data:`SESSION`.

    Returns:
        The parsed JSON body.

    Raises:
        NoDataError: the host answered 404.
        ValueError: the host answered 400 / 422.
        AssetFetchError: any other failed fetch. Through a proxy the message names the variable
            and the cause class only, never the proxy URL.
    """
    kwargs.setdefault("session", SESSION)
    proxy = proxy_from_env(proxy_env) if proxy_env else None
    if proxy is None:
        return _rt._get(url, params, **kwargs)
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    kwargs.setdefault("proxy", proxy)
    kwargs.setdefault("logger", _QUIET)
    try:
        return _rt._download_json(_rt.download, url, params=clean, **kwargs)
    except AssetFetchError as exc:
        cause = type(exc.__cause__).__name__ if exc.__cause__ is not None else "a non-2xx or unreadable answer"
        raise AssetFetchError(f"{_rt._where(url)}: the fetch through {proxy_env} failed ({cause})") from None
