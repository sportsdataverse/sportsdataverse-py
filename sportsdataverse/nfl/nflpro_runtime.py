"""Runtime getter for the generated NFL Pro wrappers (:mod:`sportsdataverse.nfl.nflpro`).

`pro.nfl.com` serves the Next Gen Stats data that `nextgenstats.nfl.com` stopped
serving; the `/api/secured/*` tier needs a **user-bound** bearer token carrying an
active NFL+ Premium entitlement. A client-credentials token is not user-bound and
returns 401 on every secured route, so this module never mints one: it resolves a
real token, or raises with instructions.

Three things here are load-bearing and were established by measuring the live API:

* **An unsupported query param returns HTTP 200 with an EMPTY body**, not a 400,
  and with no error envelope to detect. :func:`_get` raises on that rather than
  handing back an empty string that a caller would then ``.get()`` on.
* **Responses truncate silently at the page size.** The envelope reports ``total``
  independently of what it returned, so :func:`_get` pages on ``offset`` until it
  has them all -- otherwise a default call to a 1,005-row route quietly returns 500.
* Playwright is an **optional** dependency, imported lazily and only when a token
  must be obtained by logging in.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Union

from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nflpro_parsers import _COLLECTION_KEYS

__all__ = ["_get", "nflpro_token", "nflpro_headers_gen", "NFLProAuthError"]

_HOST = "https://pro.nfl.com"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
_MAX_PAGES = 40
_LOGGER = logging.getLogger(__name__)
_token_cache: Dict[str, Any] = {"token": None}


class NFLProAuthError(RuntimeError):
    """Raised when no user-bound NFL Pro token can be resolved."""


def _claims(token: str) -> Dict[str, Any]:
    """Decode a JWT payload without verifying it (we only read our own token)."""
    import base64

    part = token.split(".")[1]
    part += "=" * (-len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(part))


def _entitled(token: str) -> bool:
    """True if the token carries an active NFL+ plan.

    The check that matters: an anonymous token and one minted with a Gigya UID
    have byte-identical claims, so "the login call succeeded" proves nothing.
    Only the presence of an ``NFL_PLUS_*`` plan distinguishes a user-bound token.
    """
    try:
        plans = _claims(token).get("plans") or []
    except Exception:
        return False
    for plan in plans:
        if not str(plan.get("plan", "")).startswith("NFL_PLUS"):
            continue
        # An expired subscription still lists its plan; only ACTIVE grants access.
        if str(plan.get("status", "ACTIVE")).upper() != "ACTIVE":
            continue
        return True
    return False


def _fresh(token: str, skew: int = 120) -> bool:
    try:
        return float(_claims(token).get("exp", 0)) - skew > time.time()
    except Exception:
        return False


def _browser_login(email: str, password: str, timeout_ms: int = 60000) -> str:
    """Complete the real id.nfl.com login and return the user-bound access token.

    The flow is three steps in a non-fixed order (email -> passkey offer ->
    password), so it is driven as a state machine. ``/account/sign-in-biometric``
    reads like a post-login passkey *enrolment* page and is actually a sign-in
    *offer* with no password field -- a fixed email-then-password sequence stops
    there having submitted nothing.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - depends on optional extra
        raise NFLProAuthError(
            "NFL Pro login needs the optional 'playwright' extra "
            "(pip install sportsdataverse[nflpro]; playwright install chromium), "
            "or set NFLPRO_TOKEN to a token obtained elsewhere."
        ) from exc

    email_sel = "input[type=email], input[name*=email i], input[id*=email i], input[name=loginID]"
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            page = browser.new_context(user_agent=_UA, viewport={"width": 1440, "height": 900}).new_page()
            page.goto(_HOST + "/", wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(6000)
            # The header collapses the control off-screen; dispatch the handler
            # rather than waiting for a visibility that never comes.
            page.evaluate(
                "() => document.querySelectorAll('.login-button, [aria-label=\"Sign In\"]').forEach(el => el.click())"
            )
            page.wait_for_timeout(8000)

            submitted = False
            for _ in range(6):
                page.wait_for_timeout(2500)
                field = page.locator("input[type=password]:visible").first
                if field.count():
                    field.fill(password, timeout=8000)
                    field.press("Enter")
                    submitted = True
                    page.wait_for_timeout(9000)
                    continue
                if page.evaluate(
                    """() => { const el = [...document.querySelectorAll('button,a')]
                        .find(e => /sign in with password|use password/i.test(e.innerText || ''));
                        if (el) { el.click(); return true; } return false; }"""
                ):
                    continue
                field = page.locator(email_sel).first
                if field.count() and field.is_visible():
                    field.fill(email, timeout=8000)
                    field.press("Enter")
                    continue
                break
            if not submitted:
                raise NFLProAuthError("id.nfl.com login: the password step was never reached")

            page.wait_for_timeout(8000)
            if "pro.nfl.com" not in page.url:
                page.goto(_HOST + "/", wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(10000)
            blobs = page.evaluate(
                """() => { const out = []; for (let i = 0; i < localStorage.length; i++)
                    out.push(localStorage.getItem(localStorage.key(i)) || ''); return out; }"""
            )
        finally:
            browser.close()

    import re

    for blob in blobs:
        for match in re.finditer(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", blob):
            if _entitled(match.group(0)):
                return match.group(0)
    raise NFLProAuthError(
        "Signed in, but no token carrying an active NFL_PLUS_* plan was found -- "
        "the account may not hold an NFL+ Premium subscription."
    )


def nflpro_token(
    token: Optional[str] = None,
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """Resolve a user-bound NFL Pro token, caching it until it expires.

    Resolution order: an explicit ``token``, then ``NFLPRO_TOKEN``, then a browser
    login using ``email``/``password`` or ``NFLPRO_EMAIL``/``NFLPRO_PW``.

    Args:
        token: An access token to use as-is.
        email: NFL account email; defaults to ``$NFLPRO_EMAIL``.
        password: NFL account password; defaults to ``$NFLPRO_PW``.

    Returns:
        A bearer token carrying an active NFL+ entitlement.

    Raises:
        NFLProAuthError: No credentials could be resolved, or the resolved token
            is expired or not user-bound (carries no active ``NFL_PLUS_*`` plan).

    Example::

        from sportsdataverse.nfl.nflpro_runtime import nflpro_token

        token = nflpro_token()  # reads NFLPRO_TOKEN, else NFLPRO_EMAIL/NFLPRO_PW
    """
    token = token or os.environ.get("NFLPRO_TOKEN")
    if token:
        if not _entitled(token):
            raise NFLProAuthError(
                "The supplied NFL Pro token carries no active NFL_PLUS_* plan, so every "
                "/api/secured/* route will 401. A client-credentials token is not user-bound."
            )
        # Expiry is checked here too: only the browser-login cache was consulting
        # _fresh, so a stale NFLPRO_TOKEN sailed through and 401'd on every route.
        if not _fresh(token):
            raise NFLProAuthError(
                "The supplied NFL Pro token has expired. Obtain a fresh one (unset NFLPRO_TOKEN to log in again)."
            )
        return token

    cached = _token_cache.get("token")
    if cached and _fresh(cached):
        return str(cached)

    email = email or os.environ.get("NFLPRO_EMAIL")
    password = password or os.environ.get("NFLPRO_PW")
    if not email or not password:
        raise NFLProAuthError(
            "No NFL Pro credentials: pass email=/password=, or set NFLPRO_EMAIL and "
            "NFLPRO_PW, or set NFLPRO_TOKEN to an already-obtained token."
        )
    fresh = _browser_login(email, password)
    _token_cache["token"] = fresh
    return fresh


def nflpro_headers_gen(
    token: Optional[str] = None,
    email: Optional[str] = None,
    password: Optional[str] = None,
) -> Dict[str, str]:
    """Build a reusable header dict for authenticated NFL Pro requests.

    Reuse ONE dict across many calls: resolving a token can cost a browser login,
    so minting fresh headers per request is the expensive way to do this.

    Args:
        token: An access token to use as-is; defaults to ``$NFLPRO_TOKEN``.
        email: NFL account email; defaults to ``$NFLPRO_EMAIL``.
        password: NFL account password; defaults to ``$NFLPRO_PW``.

    Returns:
        A header dict carrying the bearer token, plus the ``Referer`` and
        ``Accept`` values ``pro.nfl.com`` expects.

    Raises:
        NFLProAuthError: No user-bound token could be resolved, or the resolved
            token is expired or carries no active ``NFL_PLUS_*`` plan.

    Example::

        from sportsdataverse.nfl import nfl_pro_players_offense_passing_season
        from sportsdataverse.nfl.nflpro_runtime import nflpro_headers_gen

        headers = nflpro_headers_gen()
        df = nfl_pro_players_offense_passing_season(season=2024, headers=headers)
    """
    return {
        "User-Agent": _UA,
        "Referer": _HOST + "/",
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {nflpro_token(token, email, password)}",
    }


def _collection_key(body: Dict[str, Any]) -> Optional[str]:
    """Name the envelope key holding the records.

    The envelope echoes request params back alongside the data, and some echoes
    are themselves lists, so 'the first list value' is not a safe rule. Prefer the
    known collection names and fall back to the longest list.
    """
    for key in _COLLECTION_KEYS:
        if isinstance(body.get(key), list):
            return key
    lists = [(k, v) for k, v in body.items() if isinstance(v, list)]
    if not lists:
        return None
    return max(lists, key=lambda kv: len(kv[1]))[0]


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    paginate: bool = True,
    max_pages: int = _MAX_PAGES,
    **kwargs: Any,
) -> Union[Dict, List]:
    """GET a JSON payload from ``pro.nfl.com``, authenticating and de-truncating.

    Args:
        url: Absolute ``pro.nfl.com`` URL.
        params: Query params; ``None`` values are stripped.
        headers: A :func:`nflpro_headers_gen` dict to reuse; built fresh when ``None``.
        paginate: Follow ``offset`` until the returned row count reaches the
            envelope's ``total``. Leave on unless you deliberately want one page.
        max_pages: Hard cap on follow-up requests.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The parsed JSON body, with the record collection completed across pages.

    Raises:
        NFLProAuthError: No user-bound token could be resolved.
        ValueError: The API returned HTTP 200 with an empty body -- how it signals
            rejected query params, since it sends no error envelope.
    """
    if headers is None:
        headers = nflpro_headers_gen()
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    kwargs.setdefault("timeout", 45)
    # download() retries 403 by default because that is ESPN under load. Here a
    # 401/403 means the token lacks the entitlement, which no retry can change.
    kwargs.setdefault("retry_statuses", {408, 429, 500, 502, 503, 504})

    def _fetch(query: Dict[str, Any]) -> Dict[str, Any]:
        resp = download(url=url, params=query, headers=headers, **kwargs)
        if getattr(resp, "status_code", 200) in (401, 403):
            raise NFLProAuthError(
                f"pro.nfl.com refused {url} ({resp.status_code}). The token is expired, "
                "not user-bound, or carries no active NFL_PLUS_* plan."
            )
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        text = getattr(resp, "text", "") or ""
        if not text.strip():
            raise ValueError(
                f"pro.nfl.com returned HTTP 200 with an empty body for {url} with "
                f"{query!r}. That is how this API rejects unsupported query params "
                "(note: `week` is a path scope, not a query param)."
            )
        return resp.json()

    body = _fetch(clean)
    if not paginate or not isinstance(body, dict):
        return body
    key = _collection_key(body)
    if key is None:
        return body
    # Never mutate the payload in place: in memory cache mode download() hands back
    # the cached body BY REFERENCE, so writing the merged list into it would poison
    # the cache entry for every later caller.
    body = dict(body)

    start = int(clean.get("offset") or 0)
    limit = clean.get("limit")
    limit = limit if isinstance(limit, int) else None
    items = list(body.get(key) or [])
    try:
        total: Optional[int] = int(body["total"])
    except (KeyError, TypeError, ValueError):
        total = None

    def _more_expected() -> bool:
        if total is not None:
            # Offsets are absolute: page from the caller's offset, not from zero,
            # or the head of the collection is silently skipped.
            return start + len(items) < total
        # With no `total` a page whose size equals the requested limit is
        # indistinguishable from a complete response -- keep going until a short
        # page proves the end.
        return limit is not None and bool(items) and len(items) % limit == 0

    truncated, pages = False, 1
    while _more_expected():
        if pages >= max_pages:
            truncated = True
            break
        chunk = (_fetch({**clean, "offset": start + len(items)}) or {}).get(key) or []
        if not chunk:
            truncated = total is not None and start + len(items) < total
            break
        if items and chunk[0] == items[0]:
            # The server accepted `offset` and ignored it; extending here would
            # pile up duplicates until the count happened to reach `total`.
            raise ValueError(
                f"pro.nfl.com ignored `offset` for {url}: page {pages + 1} repeated "
                "the first page. Refusing to return duplicated rows."
            )
        items.extend(chunk)
        pages += 1

    body[key] = items
    if truncated:
        # A partial collection must never be indistinguishable from a complete one.
        body["_truncated"] = True
        _LOGGER.warning(
            "pro.nfl.com %s: returning %d of %s rows (stopped after %d pages).",
            url,
            len(items),
            total,
            pages,
        )
    return body
