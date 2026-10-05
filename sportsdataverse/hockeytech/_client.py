"""HockeyTech HTTP client: build the JSONP URL, fetch, strip the callback
wrapper, and parse JSON. One retrying, rate-limited entry point shared by every
league family.
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
from typing import Any, Dict, Optional, Union

from sportsdataverse._codegen_runtime import _check_status
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockeytech._leagues import get_config, resolve_api_key

_UA = "Mozilla/5.0 (compatible; sportsdataverse/hockeytech)"
_CALLBACK_RE = re.compile(r"^[A-Za-z_$][\w.$]*\(")
# The only "source never has this" reply: plain text, HTTP 200 (MJHL gc/gamesummary).
_ACCESS_DENIED_RE = re.compile(r"^\s*Feed type access denied\.?\s*$", re.IGNORECASE)
# Credential query params a transport error can quote back with the request URL.
_CREDENTIAL_QS_RE = re.compile(r"(?i)\b(key|api_?key|access_token|token|password|secret)=[^&\s'\"<>)]+")
_RATE_LIMIT_S = 0.4
_last_request_ts = 0.0

# League-specific Referer headers. Falls back to no Referer for unknown leagues.
_LEAGUE_REFERER: Dict[str, str] = {
    "pwhl": "https://www.thepwhl.com/",
    "ahl": "https://www.theahl.com/",
    "ohl": "https://www.ontariohockeyleague.com/",
    "whl": "https://www.whl.ca/",
    "qmjhl": "https://www.theqmjhl.ca/",
}


def _strip_jsonp(text: str) -> str:
    """Strip an ``angular.callbacks._N( ... )`` or bare ``( ... )`` JSONP wrapper."""
    text = text.strip()
    if _CALLBACK_RE.match(text) and text.endswith(")"):
        text = text[text.index("(") + 1 : -1]
    elif text.startswith("(") and text.endswith(")"):
        text = text[1:-1]
    return text.strip()


def _invalid_view_reason(payload: Any) -> Optional[str]:
    """Return a human reason when ``payload`` is a HockeyTech invalid-view sentinel.

    HockeyTech reports an unknown view with **HTTP 200 and an error in the body**,
    so a sentinel response would otherwise parse straight through to a zero-row
    frame; :func:`hockeytech_api` raises on it instead. Two shapes exist:

    - ``modulekit`` / ``gc``: ``{"SiteKit"|"GC": {..., "Undefined": "Undefined Tab <view>"}}``
    - ``statviewfeed``: ``{"error": "InvalidView error: <view>"}``; any non-empty
      top-level ``error`` string is treated the same way (as sportsdataverse-js does).

    Returns ``None`` for any healthy payload.
    """
    if not isinstance(payload, dict):
        return None
    err = payload.get("error")
    if isinstance(err, str) and err:
        return err
    for root in ("SiteKit", "GC"):
        node = payload.get(root)
        if isinstance(node, dict) and node.get("Undefined"):
            return str(node["Undefined"])
    return None


def _redact(text: str, secret: str = "") -> str:
    """Mask the feed key (and any other credential query param) in text that may quote the URL."""
    text = _CREDENTIAL_QS_RE.sub(r"\1=REDACTED", text)
    return text.replace(secret, "REDACTED") if secret else text


def _build_url(league: str, feed: str, view: str, params: Optional[Dict[str, Any]] = None) -> str:
    cfg = get_config(league)
    merged = {
        "feed": feed,
        "key": resolve_api_key(league, view=view),
        "client_code": cfg.client_code,
        "site_id": str(cfg.site_id),
        "lang": "en",
    }
    # The gc feed uses a ``tab`` parameter to select the view; all other feeds
    # (modulekit, statviewfeed, …) use ``view``.
    if feed == "gc":
        merged["tab"] = view
    else:
        merged["view"] = view
    if params:
        merged.update({k: str(v) for k, v in params.items() if v is not None})
    return cfg.base_url + "?" + urllib.parse.urlencode(merged)


def hockeytech_api(
    league: str,
    feed: str,
    view: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    timeout: int = 30,
    max_retries: int = 3,
    **kwargs,
) -> Union[Dict[str, Any], list]:
    """Fetch + parse one HockeyTech feed call.

    Returns the parsed JSON (dict or list). The one reply the source sends for
    "this key never has that feed" -- the plain-text ``Feed type access denied.``
    (MJHL's public key on ``gc``) -- returns ``{}``, which every parser reads as a
    zero-row frame.

    Raises:
        ValueError: ``league`` is not in the registry (before any request).
        NoDataError: the host answered HTTP 404.
        AssetFetchError: the fetch failed and the answer is unknown -- a transport
            error, a non-2xx status that outlived the retries (403, 429, 5xx), an
            empty or unparseable body, or an HTTP-200 error sentinel
            (``Undefined Tab <view>`` / ``InvalidView error: <view>``).
    """
    global _last_request_ts
    url = _build_url(league, feed, view, params)
    referer = _LEAGUE_REFERER.get(league)
    headers: Dict[str, str] = {"User-Agent": _UA, "Accept": "application/json"}
    if referer:
        headers["Referer"] = referer
    where = f"hockeytech_api({league}/{feed}/{view})"  # no URL: it carries the key
    key = resolve_api_key(league, view=view)

    elapsed = time.monotonic() - _last_request_ts
    if elapsed < _RATE_LIMIT_S:
        time.sleep(_RATE_LIMIT_S - elapsed)

    # Error text from requests / download() quotes the URL, which carries the key: redact
    # it, and raise outside the except block so the original is not chained either.
    failure: Optional[Exception] = None
    try:
        resp = download(url, headers=headers, timeout=timeout, num_retries=max_retries)
    except NoDataError as exc:
        failure = NoDataError(_redact(str(exc), key))
    except Exception as exc:  # noqa: BLE001 - any transport failure is a failed fetch
        failure = AssetFetchError(f"{where}: fetch failed: {_redact(f'{type(exc).__name__}: {exc}', key)}")
    finally:
        _last_request_ts = time.monotonic()
    if failure is not None:
        raise failure

    text = resp.text or ""
    # 400/422 -> ValueError, any other non-2xx -> AssetFetchError (the shared rule).
    _check_status(url, getattr(resp, "status_code", 200), _redact(text, key), label=where)
    if _ACCESS_DENIED_RE.match(text):
        return {}
    try:
        payload, parsed = json.loads(_strip_jsonp(text)), True
    except ValueError:  # its .doc is the whole body (key included): never chain it
        payload, parsed = None, False
    if not parsed:
        raise AssetFetchError(f"{where}: empty or unparseable body {_redact(text[:80], key)!r}")
    reason = _invalid_view_reason(payload)
    if reason:
        raise AssetFetchError(
            f"{where}: upstream rejected the view ({reason!r}); an error sentinel, not an empty result"
        )
    return payload
