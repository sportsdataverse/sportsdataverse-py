"""Runtime getter for the generated PFF Premium Stats wrappers (:mod:`sportsdataverse.nfl.pff_core`).

PFF Premium Stats (``premium.pff.com/api/v1``) is **paywalled**: every request must carry the
authenticated-session cookies from a logged-in browser. Unlike stats.nba.com, PFF does **not**
JA3/TLS-fingerprint-block plain ``requests`` -- the only gate is the cookies -- so the live
transport goes through the shared :func:`sportsdataverse.dl_utils.download` gateway (retry loop +
pooling) rather than ``curl_cffi``.

**Auth precedence:**

1. an explicit ``cookies=`` dict on the call,
2. environment: a raw ``SDV_PY_PFF_COOKIES`` cookie string (``k=v; k=v``), or the pair
   ``SDV_PY_PFF_PREMIUM_KEY`` (the entitlement cookie ``_premium_key``) + optional
   ``SDV_PY_PFF_SESSION`` (the Clerk ``__session`` JWT),
3. ``SDV_PY_PFF_STORAGE_STATE`` -- a path to a saved Playwright ``storage_state`` JSON
   (captured once from a headed login). The runtime **replays it headlessly** so Clerk
   re-mints the short-lived ``__session`` and extracts fresh ``_premium_key`` +
   ``__session`` cookies, cached in-process for ``SDV_PY_PFF_STORAGE_STATE_TTL`` seconds
   (default 300 -- one browser launch per window, not per request). Needs the optional
   ``playwright`` extra (``pip install sportsdataverse[pff]`` then ``playwright install
   chromium``); a missing install raises a clear :class:`ImportError`,
4. otherwise a clear :class:`RuntimeError` telling the caller how to authenticate.

Both the HTTP call (``transport=``) and the browser refresh (``refresher=`` on
:func:`_cookies_from_storage_state`) are injectable, so wrappers and tests run fully
offline with no Playwright install and no live session. :func:`pff_login` is an
experimental stub -- see its docstring.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Dict, Optional

from sportsdataverse.dl_utils import download

__all__ = ["_get", "pff_login"]

Transport = Callable[[str, dict, dict, dict], tuple[int, str]]
Refresher = Callable[[str], dict[str, str]]

_COOKIE_ENV = ("SDV_PY_PFF_PREMIUM_KEY", "SDV_PY_PFF_SESSION", "SDV_PY_PFF_COOKIES")

# storage_state auth (tier 3): a saved Playwright storage_state replayed headlessly.
_STORAGE_STATE_ENV = "SDV_PY_PFF_STORAGE_STATE"
_STORAGE_STATE_TTL_ENV = "SDV_PY_PFF_STORAGE_STATE_TTL"
_DEFAULT_STORAGE_STATE_TTL = 300.0  # seconds; reuse a refreshed cookie set within this window
_PFF_COOKIE_NAMES = ("_premium_key", "__session")
# in-process cache: abspath(storage_state) -> (cookies, monotonic_expiry). Bounds browser
# launches to one per TTL rather than one per request; capped (realistic cardinality is ~1).
_STORAGE_STATE_CACHE_MAX = 8
_storage_state_cache: dict[str, tuple[dict[str, str], float]] = {}


def _pff_headers() -> Dict[str, str]:
    """Build minimal browser-mimicking request headers for premium.pff.com.

    Returns:
        A dict of HTTP request headers suitable for :func:`sportsdataverse.dl_utils.download`.

    Example:
        Quick start::

            from sportsdataverse.nfl.pff_runtime import _pff_headers
            print(_pff_headers()["Accept"])  # "application/json, text/plain, */*"
    """
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://premium.pff.com/",
        "Origin": "https://premium.pff.com",
    }


def _cookies_from_env() -> Dict[str, str]:
    """Assemble a PFF cookie dict from environment variables.

    Reads a raw ``SDV_PY_PFF_COOKIES`` cookie string first (``k=v; k=v``); otherwise
    builds ``{_premium_key, __session}`` from ``SDV_PY_PFF_PREMIUM_KEY`` /
    ``SDV_PY_PFF_SESSION``. Returns ``{}`` when none are set.

    Returns:
        A ``dict`` of cookie name -> value (possibly empty).
    """
    raw = os.environ.get("SDV_PY_PFF_COOKIES")
    if raw:
        out: Dict[str, str] = {}
        for part in raw.split(";"):
            key, _, value = part.partition("=")
            if key.strip():
                out[key.strip()] = value.strip()
        return out
    cookies: Dict[str, str] = {}
    premium_key = os.environ.get("SDV_PY_PFF_PREMIUM_KEY")
    if premium_key:
        cookies["_premium_key"] = premium_key
    session = os.environ.get("SDV_PY_PFF_SESSION")
    if session:
        cookies["__session"] = session
    return cookies


def _storage_state_ttl() -> float:
    """Cache TTL (seconds) for a refreshed cookie set, from env or the 300s default."""
    raw = os.environ.get(_STORAGE_STATE_TTL_ENV)
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return _DEFAULT_STORAGE_STATE_TTL


def _playwright_refresh(path: str, *, base_url: str = "https://premium.pff.com/") -> Dict[str, str]:
    """Replay a saved Playwright ``storage_state`` headlessly to mint fresh PFF cookies.

    Loads ``path`` into a headless Chromium context, navigates to ``base_url`` so Clerk
    re-mints the short-lived ``__session`` JWT, and returns the ``_premium_key`` +
    ``__session`` cookies from the resulting jar.

    Args:
        path: Filesystem path to a Playwright ``storage_state`` JSON.
        base_url: PFF origin to navigate for the session refresh.

    Returns:
        A cookie ``dict`` (a name absent from the jar is simply omitted).

    Raises:
        ImportError: When the optional ``playwright`` dependency is not installed.

    Example:
        Live use (residential IP, one-time headed capture already saved)::

            import os
            os.environ["SDV_PY_PFF_STORAGE_STATE"] = "dev/pff_auth/storage_state.json"
            from sportsdataverse.nfl.pff_runtime import _get
            raw = _get("https://premium.pff.com/api/v1/leagues", {})
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError(
            "PFF storage_state auth needs Playwright: `pip install sportsdataverse[pff]` "
            "(or `pip install playwright`) then `playwright install chromium`. Alternatively "
            "supply cookies via SDV_PY_PFF_PREMIUM_KEY / SDV_PY_PFF_SESSION / SDV_PY_PFF_COOKIES."
        ) from exc
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(storage_state=path)
            page = ctx.new_page()
            page.goto(base_url, wait_until="networkidle", timeout=45000)
            page.wait_for_timeout(1500)  # let Clerk finish refreshing the session token
            jar = {c["name"]: c["value"] for c in ctx.cookies()}
        finally:
            browser.close()
    return {name: jar[name] for name in _PFF_COOKIE_NAMES if name in jar}


def _cookies_from_storage_state(
    path: str,
    *,
    refresher: Optional[Refresher] = None,
    _clock: Callable[[], float] = time.monotonic,
) -> Dict[str, str]:
    """Return PFF cookies refreshed from a Playwright ``storage_state``, cached per TTL.

    The refresh launches a headless browser (:func:`_playwright_refresh`), so the result
    is cached in-process for :func:`_storage_state_ttl` seconds -- one launch per window,
    not per request. ``refresher`` is injectable so tests run without Playwright or a
    live session.

    Args:
        path: Path to the saved ``storage_state`` JSON.
        refresher: Callable ``(path) -> cookie dict``. Defaults to :func:`_playwright_refresh`.
        _clock: Monotonic clock (injectable for tests).

    Returns:
        A cookie ``dict`` (empty when the refresh yields nothing).
    """
    key = os.path.abspath(path)  # dedup logically-identical paths (cwd-relative vs absolute)
    now = _clock()
    cached = _storage_state_cache.get(key)
    if cached is not None and now < cached[1]:
        return dict(cached[0])
    refresh = refresher or _playwright_refresh
    cookies = refresh(key) or {}
    if cookies:
        if key not in _storage_state_cache and len(_storage_state_cache) >= _STORAGE_STATE_CACHE_MAX:
            _storage_state_cache.pop(next(iter(_storage_state_cache)))  # FIFO-evict the oldest
        _storage_state_cache[key] = (dict(cookies), now + _storage_state_ttl())
    return cookies


def _reset_storage_state_cache() -> None:
    """Clear the in-process storage_state cookie cache (test / rotation helper)."""
    _storage_state_cache.clear()


def _resolve_cookies(
    explicit: Optional[Dict[str, str]],
    *,
    storage_state_refresher: Optional[Refresher] = None,
) -> Dict[str, str]:
    """Return the cookie dict to send, or raise when none can be resolved.

    Precedence: explicit ``cookies`` > ``SDV_PY_PFF_*`` env cookies >
    ``SDV_PY_PFF_STORAGE_STATE`` (headless Playwright refresh).

    Args:
        explicit: Cookies passed directly on the call; wins over everything.
        storage_state_refresher: Injectable browser refresher for the storage_state tier
            (defaults to :func:`_playwright_refresh` via :func:`_cookies_from_storage_state`).

    Returns:
        A non-empty cookie ``dict``.

    Raises:
        RuntimeError: When no cookies can be resolved from any tier.
    """
    cookies = explicit or _cookies_from_env()
    if cookies:
        return cookies
    path = os.environ.get(_STORAGE_STATE_ENV)
    if path:
        cookies = _cookies_from_storage_state(path, refresher=storage_state_refresher)
        if cookies:
            return cookies
    raise RuntimeError(
        "PFF Premium is paywalled -- supply cookies via the cookies= arg, the "
        "SDV_PY_PFF_PREMIUM_KEY / SDV_PY_PFF_SESSION (or SDV_PY_PFF_COOKIES) env vars, or point "
        "SDV_PY_PFF_STORAGE_STATE at a saved Playwright storage_state JSON for headless "
        "auto-refresh (needs the optional `playwright` extra)."
    )


def _default_transport(url: str, params: dict, headers: dict, cookies: dict) -> tuple:
    """Live transport: fold cookies into a ``Cookie`` header and go through ``download``.

    :func:`sportsdataverse.dl_utils.download` has no ``cookies=`` parameter (it shares a
    pooled cookie jar), so the resolved cookies are serialized into a per-request ``Cookie``
    header instead.

    Args:
        url: Fully-qualified premium.pff.com URL.
        params: Query-string params (``None`` values already stripped by :func:`_get`).
        headers: Base request headers.
        cookies: Resolved auth cookie dict.

    Returns:
        ``(status_code, response_text)``.
    """
    hdrs = dict(headers or {})
    if cookies:
        hdrs["Cookie"] = "; ".join(f"{name}={value}" for name, value in cookies.items())
    resp = download(url=url, params=params, headers=hdrs, timeout=30)
    if hasattr(resp, "raise_for_status"):
        resp.raise_for_status()
    return resp.status_code, resp.text


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    *,
    cookies: Optional[Dict[str, str]] = None,
    transport: Optional[Transport] = None,
    **kwargs: Any,
) -> Dict:
    """GET a premium.pff.com endpoint and return its parsed JSON body.

    Resolves auth cookies (explicit arg > environment), attaches them, and issues the
    request through the injectable *transport*. Non-200 status, a blank body, or a JSON
    decode error all return ``{}`` so the parsers can chain without null-checks.

    Args:
        url: Fully-qualified premium.pff.com URL built by the generated wrapper.
        params: Query-string parameters; ``None`` values are stripped.
        headers: HTTP headers dict. Defaults to :func:`_pff_headers`.
        cookies: PFF auth cookies (e.g. ``{"_premium_key": "..."}``). Falls back to the
            ``SDV_PY_PFF_*`` environment variables; a :class:`RuntimeError` is raised when
            neither is available.
        transport: Callable ``(url, params, headers, cookies) -> (status_code, text)``.
            Defaults to :func:`_default_transport`. Inject a fake to run offline.
        **kwargs: Accepted for forward-compatibility with generated callers; unused.

    Returns:
        Parsed JSON ``dict``, or ``{}`` on non-200 status, blank body, or JSON error.

    Raises:
        RuntimeError: When no auth cookies can be resolved.

    Example:
        Quick start (offline -- inject a transport, no cookies needed on the wire)::

            import json
            from sportsdataverse.nfl.pff_runtime import _get

            def fake(url, params, headers, cookies):
                return 200, json.dumps({"passing_summary": []})

            data = _get(
                "https://premium.pff.com/api/v1/facet/passing/summary",
                {"league": "nfl"},
                cookies={"_premium_key": "PK"},
                transport=fake,
            )

        Live use (residential IP, logged-in session)::

            import os
            os.environ["SDV_PY_PFF_PREMIUM_KEY"] = "<your _premium_key cookie>"
            raw = _get("https://premium.pff.com/api/v1/leagues", {})
    """
    clean: Dict[str, Any] = {k: v for k, v in (params or {}).items() if v is not None}
    resolved = _resolve_cookies(cookies)
    _transport = transport or _default_transport
    status, text = _transport(url, clean, headers or _pff_headers(), resolved)
    if status != 200 or not (text or "").strip():
        return {}
    try:
        body = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return body if isinstance(body, dict) else {}


def pff_login(
    email: Optional[str] = None,
    password: Optional[str] = None,
    *,
    transport: Optional[Callable] = None,
) -> Dict[str, str]:
    """Experimental: mint PFF session cookies from credentials (NOT IMPLEMENTED).

    The Wave-0 auth spike (a live Clerk sign-in flow against ``clerk.pff.com`` that exchanges
    email/password for the ``_premium_key`` + ``__session`` cookies) was **deferred** -- there
    was no live PFF session to reverse-engineer the flow against. Two caveats make the
    credential path fragile even once built: the Clerk ``__session`` JWT is short-lived
    (~60 s, refreshed by the browser), and the ``_premium_key`` entitlement cookie is what
    actually authorizes the API for its TTL.

    **Supported path instead:** supply the cookies you already hold from a logged-in browser
    session -- pass ``cookies=`` to a wrapper / :func:`_get`, or set ``SDV_PY_PFF_PREMIUM_KEY``
    (+ optional ``SDV_PY_PFF_SESSION``) / ``SDV_PY_PFF_COOKIES`` in the environment.

    Args:
        email: PFF account email (or ``SDV_PY_PFF_EMAIL``). Never logged.
        password: PFF account password (or ``SDV_PY_PFF_PASSWORD``). Never logged.
        transport: Injectable Clerk transport for a future implementation / tests.

    Returns:
        A cookie dict ``{"_premium_key": ..., "__session": ...}`` once implemented.

    Raises:
        NotImplementedError: Always -- use the supported cookie-supply path (see above).
    """
    raise NotImplementedError(
        "pff_login (Clerk credential sign-in) is not implemented -- the auth spike was deferred. "
        "Supply your logged-in browser cookies instead: pass cookies= to a wrapper / _get, or set "
        "SDV_PY_PFF_PREMIUM_KEY / SDV_PY_PFF_SESSION / SDV_PY_PFF_COOKIES in the environment."
    )
