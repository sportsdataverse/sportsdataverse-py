"""Shared login + proxy + HTML-table layer for subscription scraping sources.

Two sdv-py sources sit behind a **username/password** paywall and serve HTML
tables rather than an API: KenPom (:mod:`sportsdataverse.mbb.kenpom_runtime`,
men's college basketball) and Her Hoop Stats
(:mod:`sportsdataverse.wbb.herhoopstats`, women's). Both use the exact
same flow, which is what this module implements once:

1. ``GET`` the login page on a private :class:`requests.Session` -- establishes
   the session cookie and, for a Django site, the CSRF token,
2. ``POST`` the credentials to the form's own ``action`` URL,
3. verify the login actually took (a *failed* login silently returns the
   logged-out page, whose free tables have different columns -- scraping that
   and calling it subscriber data is the failure mode worth a hard error),
4. reuse the authenticated session for every subsequent page GET, which goes
   through :func:`sportsdataverse.dl_utils.download` so the shared retry loop
   and backoff apply.

This mirrors hoopR's ``login()`` / ``.kp_get_page()`` and wehoop's
``.hhs_login()`` / ``.hhs_doc()``, with three deliberate improvements:

* **Session caching.** The R wrappers call ``login()`` on *every* function call,
  so a 20-season pull is 20 logins. Here an authenticated session is cached per
  ``(site, email, proxy)`` for :data:`_SESSION_TTL` seconds.
* **Proxy support** at every tier -- explicit argument, per-site env var, or the
  package-wide ``SDV_PY_PROXY``. The proxy is bound to the session, so the login
  POST and the page GETs share one egress IP (a subscription site that sees the
  login and the scrape from different IPs will invalidate the session).
* **Generic header flattening** -- :func:`sportsdataverse._html_tables.html_tables`,
  re-exported here, instead of the ~44 hardcoded ``header_cols`` vectors the R port
  needs.

Credentials are never logged, never defaulted, and never bundled.
"""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

import requests

from sportsdataverse._codegen_runtime import _text_body, _transport_errors, _where
from sportsdataverse._html_tables import html_tables
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError

__all__ = [
    "SubscriptionSite",
    "resolve_proxy",
    "resolve_credentials",
    "has_credentials",
    "login",
    "get_html",
    "html_tables",  # re-exported from _html_tables for the auth'd scrapers
    "clear_session_cache",
]

# Browser-ish UA. Both sites serve their member tables to a plain client -- unlike
# stats.nba.com there is no TLS/JA3 gate here -- so `requests` is sufficient.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Package-wide proxy fallback, consulted after a site's own PROXY env var.
GLOBAL_PROXY_ENV = "SDV_PY_PROXY"

_SESSION_TTL = 1800.0  # seconds; a subscription cookie outlives this comfortably
_SESSION_CACHE_MAX = 8
# key -> (session, expiry). ponytail: module-global dict + one lock; per-key locks
# only if a caller ever logs into many sites concurrently (realistic cardinality is 1-2).
_sessions: dict[tuple[str, str, str, str], tuple[requests.Session, float]] = {}
_lock = threading.Lock()
_login_lock = threading.Lock()  # serializes a cache-miss login (see login())


@dataclass(frozen=True)
class SubscriptionSite:
    """Static description of one username/password-gated scraping source.

    Attributes:
        name: Human label used in error messages (e.g. ``"KenPom"``).
        base_url: Origin, no trailing slash (e.g. ``"https://kenpom.com"``).
        login_url: Page carrying the login form.
        user_field: Form field name for the e-mail/username (e.g. ``"email"``).
        password_field: Form field name for the password.
        email_env: Env var names checked in order for the e-mail.
        password_env: Env var names checked in order for the password.
        proxy_env: Site-specific proxy env var, checked before
            :data:`GLOBAL_PROXY_ENV`.
        extra_form: Constant extra form fields to POST (e.g. ``{"submit": "Login"}``).
        csrf_field: Hidden CSRF input name to lift off the login page and echo
            back (Django sites); ``None`` when the site has no CSRF token.
        default_action: Fallback form ``action`` when the login page has no
            discoverable form (relative to ``base_url`` or absolute).
        signup_url: Where to buy a subscription; quoted in the "no credentials"
            error so the message is actionable.
        free_tier_marker: Regex that matches only a page served WITHOUT a live
            subscription, for a site whose free tier has no login form to detect (Her
            Hoop Stats serves a 5-row teaser table instead); ``None`` when the login
            form is the only marker.
    """

    name: str
    base_url: str
    login_url: str
    user_field: str
    password_field: str
    email_env: Tuple[str, ...]
    password_env: Tuple[str, ...]
    proxy_env: str
    extra_form: Dict[str, str] = field(default_factory=dict)
    csrf_field: Optional[str] = None
    default_action: Optional[str] = None
    signup_url: str = ""
    free_tier_marker: Optional[str] = None


def _env_first(names: Tuple[str, ...]) -> Optional[str]:
    """First non-empty environment value among ``names``, else ``None``."""
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def resolve_proxy(site: SubscriptionSite, proxy: Any = None) -> Optional[Dict[str, str]]:
    """Resolve the proxy to use, in the ``requests`` ``proxies=`` shape.

    Precedence is **explicit argument > site env var > package env var > none**.
    A plain string (``"http://user:pw@host:8080"``) is expanded to both the
    ``http`` and ``https`` schemes; a dict is passed through unchanged.

    Args:
        site: The site being scraped (supplies ``proxy_env``).
        proxy: Explicit proxy -- a URL ``str``, a ``proxies=`` ``dict``, or
            ``None`` to fall back to the environment.

    Returns:
        A ``{"http": ..., "https": ...}`` dict, or ``None`` when no proxy is
        configured anywhere.

    Example:
        Explicit beats environment::

            from sportsdataverse.mbb.kenpom_runtime import KENPOM
            from sportsdataverse._subscription_http import resolve_proxy

            resolve_proxy(KENPOM, "http://127.0.0.1:8888")
            # {'http': 'http://127.0.0.1:8888', 'https': 'http://127.0.0.1:8888'}
    """
    if isinstance(proxy, dict):
        return dict(proxy) or None
    url = proxy or _env_first((site.proxy_env, GLOBAL_PROXY_ENV))
    if not url:
        return None
    return {"http": url, "https": url}


def resolve_credentials(
    site: SubscriptionSite,
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> Tuple[str, str]:
    """Resolve subscription credentials, or raise with instructions.

    Precedence is **explicit arguments > environment variables**. Values are
    returned to the caller and never logged.

    Args:
        site: The site being scraped (supplies the env var names).
        email: Explicit account e-mail / username.
        password: Explicit account password.

    Returns:
        An ``(email, password)`` tuple, both non-empty.

    Raises:
        RuntimeError: When either half cannot be resolved. The message names the
            environment variables and the subscription URL.
    """
    resolved_email = (email or "").strip() or _env_first(site.email_env)
    resolved_pw = password or _env_first(site.password_env)
    if resolved_email and resolved_pw:
        return resolved_email, resolved_pw
    signup = f" A subscription is required: {site.signup_url}." if site.signup_url else ""
    raise RuntimeError(
        f"{site.name} is a paid subscription service -- pass email=/password= to the call, "
        f"or set the {' / '.join(site.email_env)} and {' / '.join(site.password_env)} "
        f"environment variables.{signup}"
    )


def has_credentials(site: SubscriptionSite) -> bool:
    """Whether credentials for ``site`` are resolvable from the environment.

    The Python counterpart of hoopR's ``has_kp_user_and_pw()``. Useful to gate a
    live test without triggering a login.

    Args:
        site: The site to check.

    Returns:
        ``True`` when both an e-mail and a password are set in the environment.
    """
    return bool(_env_first(site.email_env) and _env_first(site.password_env))


def _form_action(html: str, site: SubscriptionSite) -> str:
    """Absolute URL to POST the login form to.

    Reads the ``action`` of the first ``<form>`` containing the site's user
    field; falls back to ``site.default_action`` then to ``site.login_url``.

    Args:
        html: Login-page HTML.
        site: The site being scraped.

    Returns:
        A fully-qualified URL.
    """
    action = None
    for match in re.finditer(r"<form\b[^>]*>.*?</form>", html, re.IGNORECASE | re.DOTALL):
        block = match.group(0)
        if re.search(rf"""name=["']{re.escape(site.user_field)}["']""", block, re.IGNORECASE):
            attr = re.search(r"""\baction=["']([^"']*)["']""", block, re.IGNORECASE)
            action = attr.group(1).strip() if attr else None
            break
    action = action or site.default_action
    if not action:
        return site.login_url
    if action.startswith(("http://", "https://")):
        return action
    return f"{site.base_url}/{action.lstrip('/')}"


def _hidden_value(html: str, name: str) -> Optional[str]:
    """Value of a hidden ``<input name=...>`` (e.g. a Django CSRF token)."""
    pattern = rf"""<input[^>]*\bname=["']{re.escape(name)}["'][^>]*\bvalue=["']([^"']*)["']"""
    match = re.search(pattern, html, re.IGNORECASE)
    if match:
        return match.group(1)
    # attribute order is not guaranteed -- try value-before-name too
    pattern = rf"""<input[^>]*\bvalue=["']([^"']*)["'][^>]*\bname=["']{re.escape(name)}["']"""
    match = re.search(pattern, html, re.IGNORECASE)
    return match.group(1) if match else None


def _looks_logged_out(html: str, site: SubscriptionSite) -> bool:
    """Whether ``html`` is a page served without a live subscription.

    A rejected login on both sites returns HTTP 200 with the login page, not an
    error status, and an expired session gets the same page (KenPom) or a free-tier
    teaser (Her Hoop Stats, matched by ``site.free_tier_marker``). Detecting that is
    what stops logged-out tables from reaching a caller as subscriber data.

    Args:
        html: A response body (the login POST's, or any member page's).
        site: The site being scraped.

    Returns:
        ``True`` when a password input is present, or the site's free-tier marker matches.
    """
    text = html or ""
    if site.free_tier_marker and re.search(site.free_tier_marker, text):
        return True
    return bool(re.search(rf"""<input[^>]*\bname=["']{re.escape(site.password_field)}["']""", text, re.IGNORECASE))


def login(
    site: SubscriptionSite,
    email: Optional[str] = None,
    password: Optional[str] = None,
    *,
    proxy: Any = None,
    session: Optional[requests.Session] = None,
    use_cache: bool = True,
    timeout: float = 30.0,
) -> requests.Session:
    """Log into ``site`` and return an authenticated :class:`requests.Session`.

    The returned session carries the subscription cookie *and* the resolved proxy,
    so every later request shares one egress IP -- a site that sees the login and
    the scrape from different addresses will drop the session.

    Successful sessions are cached per ``(site, email, password, proxy)`` for 30 minutes, so
    a multi-season pull logs in once rather than once per call (the R wrappers log
    in on every call).

    Args:
        site: The site to authenticate against.
        email: Account e-mail; falls back to ``site.email_env``.
        password: Account password; falls back to ``site.password_env``.
        proxy: Proxy URL ``str`` or ``proxies=`` ``dict``; falls back to
            ``site.proxy_env`` then ``SDV_PY_PROXY``.
        session: Pre-built session to authenticate (advanced: custom adapters,
            mounted retries, or a test double). A fresh one is created when omitted.
        use_cache: Set ``False`` to force a new login and bypass the cache.
        timeout: Per-request timeout in seconds for the login round trip.

    Returns:
        An authenticated :class:`requests.Session`.

    Raises:
        RuntimeError: When credentials cannot be resolved, or when the site
            rejects them (the response is still the logged-out or free-tier page).
        NoDataError: The login page answered 404.
        ValueError: The site answered 400 / 422.
        AssetFetchError: Any other non-2xx, an empty body, or a connection failure.

    Example:
        Log in once and reuse (offline-safe pattern -- credentials from env)::

            import os
            from sportsdataverse._subscription_http import login
            from sportsdataverse.mbb.kenpom_runtime import KENPOM

            os.environ["KENPOM_EMAIL"] = "you@example.com"
            os.environ["KENPOM_PW"] = "..."
            sess = login(KENPOM, proxy="http://user:pw@proxy.example:8080")
    """
    resolved_email, resolved_pw = resolve_credentials(site, email, password)
    proxies = resolve_proxy(site, proxy)
    # The password's digest is in the key, so a changed or wrong password logs in again instead
    # of reusing the old session; the password itself is never stored.
    pw_digest = hashlib.sha256(resolved_pw.encode("utf-8")).hexdigest()
    key = (site.name, resolved_email, pw_digest, repr(sorted((proxies or {}).items())))

    if not (use_cache and session is None):
        return _login(site, resolved_email, resolved_pw, proxies, session, timeout)
    # One login at a time: two threads that miss together would both log in, and a second login
    # can invalidate the first session. The re-check under the lock reuses the first one's.
    with _login_lock:
        with _lock:
            cached = _sessions.get(key)
            if cached is not None and time.monotonic() < cached[1]:
                return cached[0]
        sess = _login(site, resolved_email, resolved_pw, proxies, None, timeout)
        with _lock:
            if key not in _sessions and len(_sessions) >= _SESSION_CACHE_MAX:
                _sessions.pop(next(iter(_sessions)))  # FIFO-evict the oldest
            _sessions[key] = (sess, time.monotonic() + _SESSION_TTL)
        return sess


def _login(
    site: SubscriptionSite,
    resolved_email: str,
    resolved_pw: str,
    proxies: Any,
    session: requests.Session | None,
    timeout: float,
) -> requests.Session:
    """One login round trip (no cache): GET the form, POST the credentials, check the answer."""
    sess = session or requests.Session()
    sess.headers.update({"User-Agent": USER_AGENT})
    if proxies:
        sess.proxies.update(proxies)

    # A-L64: both legs answer in the error vocabulary (404 NoDataError, 400/422 ValueError,
    # anything else or a dead connection AssetFetchError), never a bare requests error.
    # The form GET goes through ``download`` uncached (its CSRF token is per session).
    with _transport_errors(site.login_url):
        landing = download(
            url=site.login_url,
            headers={"User-Agent": USER_AGENT},
            proxy=sess.proxies or None,
            session=sess,
            timeout=timeout,
            cache_ttl=0,
        )
    landing_html = _text_body(landing, site.login_url)

    form = dict(site.extra_form)
    form[site.user_field] = resolved_email
    form[site.password_field] = resolved_pw
    if site.csrf_field:
        token = _hidden_value(landing_html, site.csrf_field)
        if token:
            form[site.csrf_field] = token

    action = _form_action(landing_html, site)
    # ponytail: ``download`` is GET-only, so the POST stays on the session; its answer is still classified.
    with _transport_errors(action):
        posted = sess.post(action, data=form, headers={"Referer": site.login_url}, timeout=timeout)
    if _looks_logged_out(_text_body(posted, action), site):
        raise RuntimeError(
            f"{site.name} rejected the supplied credentials (the response is still the logged-out "
            f"or free-tier page). Check {' / '.join(site.email_env)} and "
            f"{' / '.join(site.password_env)}, and that the subscription is active."
        )
    return sess


def clear_session_cache() -> None:
    """Drop every cached authenticated session (credential rotation / tests)."""
    with _lock:
        _sessions.clear()


def get_html(
    site: SubscriptionSite,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    email: Optional[str] = None,
    password: Optional[str] = None,
    proxy: Any = None,
    session: Optional[requests.Session] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> str:
    """GET an authenticated page from ``site`` and return its HTML body.

    Logs in (or reuses a cached session), then fetches through
    :func:`sportsdataverse.dl_utils.download` so the shared retry loop, backoff
    and ``Retry-After`` handling apply to the scrape.

    Args:
        site: The site to fetch from.
        url: Fully-qualified page URL. A path (``"/index.php"``) is joined to
            ``site.base_url``.
        params: Query-string parameters; ``None`` values are dropped.
        email: Account e-mail (see :func:`resolve_credentials`).
        password: Account password.
        proxy: Proxy URL or ``proxies=`` dict (see :func:`resolve_proxy`).
        session: An already-authenticated session to reuse verbatim; skips login.
        headers: Extra request headers, merged over the defaults (the auth itself
            is the session cookie, so this is for niceties like ``Accept-Language``).
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`
            (``timeout``, ``num_retries``, ...).

    Returns:
        The response body as ``str``.

    Raises:
        RuntimeError: When credentials cannot be resolved or are rejected.
        NoDataError: The page answered 404.
        ValueError: The site answered 400 / 422 -- the request is wrong.
        AssetFetchError: Any other non-2xx or a connection failure after retries, or
            an empty 200 -- its error page is not data. Also a 200 that is the login page or
            the free-tier teaser (the session expired, was replaced, or the subscription
            lapsed): the cached session is dropped, so the next call logs in again.

    Example:
        One page, explicit proxy::

            from sportsdataverse._subscription_http import get_html
            from sportsdataverse.mbb.kenpom_runtime import KENPOM

            html = get_html(KENPOM, "/index.php", {"y": 2025}, proxy="http://127.0.0.1:8888")
    """
    sess = session or login(site, email, password, proxy=proxy)
    full = url if url.startswith(("http://", "https://")) else f"{site.base_url}/{url.lstrip('/')}"
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    hdrs = {"User-Agent": USER_AGENT, "Referer": site.base_url + "/"}
    hdrs.update(headers or {})
    with _transport_errors(full):
        resp = download(
            url=full,
            params=clean,
            headers=hdrs,
            proxy=sess.proxies or None,
            session=sess,
            **kwargs,
        )
    body = _text_body(resp, full)
    if _looks_logged_out(body, site):
        # A-M19: the session died (server expiry, or KenPom's one-session-per-account rule let a
        # second login replace it). Drop it so the next call logs in afresh; never parse the page.
        with _lock:
            for key in [k for k, (cached, _) in _sessions.items() if cached is sess]:
                del _sessions[key]
        raise AssetFetchError(
            f"{site.name} answered {_where(full)} with its logged-out page (login form or free-tier teaser): "
            "the session expired, another login replaced it, or the subscription lapsed. Call again to log in afresh."
        )
    return body
