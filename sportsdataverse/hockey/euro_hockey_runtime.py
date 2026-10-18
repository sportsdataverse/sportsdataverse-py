"""Shared runtime for the European hockey flat-API families (``hockey/<lg>/``).

Every European provider is wrapped politely: at least one second between two requests to
the same host. :func:`_polite_wait` is the one throttle, so a family runtime with its own
transport (KHL's session, HockeyAllsvenskan's POST bodies, VHL's pinned resolve) calls it
before each request instead of writing another. :func:`_get` is the plain JSON getter for
families that need nothing else: the shared ``_codegen_runtime._get`` over :data:`SESSION`,
whose every request (each retry ``download()`` makes included) waits its turn, so the error
vocabulary (404 -> ``NoDataError``, 400/422 -> ``ValueError``, anything else ->
``AssetFetchError``) holds unchanged.

:func:`get_text` is the one getter for the families whose answer is a text page (VHL's HTML,
Ligue Magnus' game sheet page, the ELH S3 listing): it fetches, decodes STRICTLY in the declared
charset, and refuses a non-200, a body of the wrong content type, an empty body and (for a
transport that follows redirects itself) a redirect, all under the same error vocabulary.
Shell-page and truncation checks need the page's parser, so each family runs them on the text it
returns.
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode, urlsplit

import requests
from requests.adapters import HTTPAdapter

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__ = ["MIN_INTERVAL", "SESSION", "PoliteSession", "_get", "_polite_wait", "get_text"]

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

    See Also:
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
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


#: Statuses :func:`get_text` retries on a ``transport`` it drives itself, and the attempt budget
#: (first try included). A 429 / 503 may send ``Retry-After`` (seconds, capped at ``_MAX_RETRY_AFTER``).
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
ATTEMPTS = 3
_MAX_RETRY_AFTER = 120.0


def _retry_after(resp: Any) -> float:
    try:
        return min(float((getattr(resp, "headers", None) or {}).get("retry-after", 0)), _MAX_RETRY_AFTER)
    except (TypeError, ValueError):  # an HTTP-date Retry-After falls back to the exponential delay
        return 0.0


def _decode(url: str, content: bytes | bytearray, content_type: str) -> str:
    """Body bytes -> text, STRICTLY in the declared charset (UTF-8 when none is declared)."""
    m = re.search(r"charset=[\"']?([\w.:-]+)", content_type or "", re.I)
    charset = m.group(1) if m else "utf-8"
    try:
        text = (content or b"").decode(charset)
    except (LookupError, UnicodeDecodeError) as exc:
        raise AssetFetchError(f"{_rt._where(url)}: body is not valid {charset!r} ({type(exc).__name__})") from None
    if "\ufffd" in text:
        raise AssetFetchError(f"{_rt._where(url)}: replacement characters in the body (charset mismatch)")
    return text


def _send(url: str, call: Callable[[], Any]) -> Any:
    """``call()`` (one request to ``url``), each attempt throttled, retried on a dead connection / 429 / 5xx.

    Returns the last response (a retryable status only once the budget is spent); a connection that
    fails every attempt raises :class:`~sportsdataverse.errors.AssetFetchError`.
    """
    resp: Any = None
    for attempt in range(ATTEMPTS):
        _polite_wait(url)
        delay = 2.0 * 2**attempt
        try:
            resp = call()
        except OSError as exc:  # curl_cffi's RequestException subclasses OSError
            if attempt == ATTEMPTS - 1:
                raise AssetFetchError(
                    f"{_rt._where(url)}: fetch failed after {ATTEMPTS} attempts: {type(exc).__name__}"
                ) from exc
        else:
            status = getattr(resp, "status_code", None)
            if status not in RETRY_STATUSES or attempt == ATTEMPTS - 1:
                return resp
            if status in (429, 503):  # 429 and 503 may both send Retry-After
                delay = max(delay, _retry_after(resp))
        time.sleep(delay)
    return resp


def get_text(
    url: str,
    params: Optional[dict] = None,
    *,
    transport: Optional[Callable[[str], Any]] = None,
    expect: Optional[str] = None,
    is_missing: Optional[Callable[[str], bool]] = None,
    **kwargs: Any,
) -> str:
    """GET a text page (HTML / XML) and return its strictly decoded body, or the error the answer calls for.

    Two ways to fetch, one set of refusals:

    * ``transport=None`` (the default): ``dl_utils.download`` over :data:`SESSION`, which retries and
      waits its turn per attempt itself; ``kwargs`` are forwarded to it.
    * ``transport=fn``: ``fn(full_url)`` returns one response (a ``curl_cffi`` GET with a pinned IP for
      VHL); this function waits for the host slot before every attempt and retries a dead connection,
      a 429 and a 5xx (:data:`ATTEMPTS`, 2 s / 4 s apart, longer when a 429 / 503 sends ``Retry-After``).
      Only a 200 is a page on this path, and a redirect to another URL is refused.

    Args:
        url: the page URL, without the query string.
        params: query parameters; ``None`` values are dropped.
        transport: see above.
        expect: a substring the ``Content-Type`` must contain (``"html"``, ``"xml"``); ``None`` skips
            the check.
        is_missing: with ``transport``, called with the final URL of a redirected answer; ``True`` means
            "this is the site's not-found page" (:class:`~sportsdataverse.errors.NoDataError`), anything
            else redirected is an :class:`~sportsdataverse.errors.AssetFetchError`.
        **kwargs: forwarded to ``download`` (no ``transport``).

    Returns:
        The decoded body text.

    Raises:
        NoDataError: HTTP 404, or a redirect ``is_missing`` calls the not-found page.
        ValueError: HTTP 400 / 422.
        AssetFetchError: any other non-200 (after the retries), a dead connection, a body of the wrong
            content type, bytes that do not decode in the declared charset (or decode to replacement
            characters), an empty body, or any other redirect.

    See Also:
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    where = _rt._where(url)
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    full = url + ("?" + urlencode(clean) if clean else "")
    if transport is None:
        with _rt._transport_errors(url):
            resp = _rt.download(url=url, params=clean, **{"session": SESSION, **kwargs})
        _rt._check_response(resp, url)
    else:
        resp = _send(url, lambda: transport(full))
        status = getattr(resp, "status_code", None)
        if status != 200:
            _rt._check_status(url, status, (getattr(resp, "content", b"") or b"")[:400].decode("utf-8", "replace"))
            raise AssetFetchError(f"{where} answered HTTP {status}, not 200")
    status = getattr(resp, "status_code", 200)
    ctype = (getattr(resp, "headers", None) or {}).get("content-type", "") or ""
    if expect and expect not in ctype.lower():
        raise AssetFetchError(f"{where} answered a non-{expect.upper()} body ({ctype or 'no content type'})")
    content = getattr(resp, "content", None)
    text = (
        _decode(url, content, ctype) if isinstance(content, (bytes, bytearray)) else (getattr(resp, "text", "") or "")
    )
    if not text.strip():
        raise AssetFetchError(f"{where} answered HTTP {status} with an empty body")
    final = getattr(resp, "url", None) or ""
    if transport is not None and final.rstrip("/") != full.rstrip("/"):
        if is_missing is not None and is_missing(final):
            raise NoDataError(f"{where}: no such page (redirected to {_rt._where(final)})")
        raise AssetFetchError(f"{where}: redirected to {_rt._where(final) if final else 'an unknown URL'}")
    return text
