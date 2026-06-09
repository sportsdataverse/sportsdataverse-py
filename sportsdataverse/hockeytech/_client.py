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

from sportsdataverse.dl_utils import download
from sportsdataverse.hockeytech._leagues import get_config, resolve_api_key

_UA = "Mozilla/5.0 (compatible; sportsdataverse/hockeytech)"
_CALLBACK_RE = re.compile(r"^[A-Za-z_$][\w.$]*\(")
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
) -> Union[Dict[str, Any], list, None]:
    """Fetch + parse one HockeyTech feed call. Returns parsed JSON (dict/list) or None."""
    global _last_request_ts
    url = _build_url(league, feed, view, params)
    referer = _LEAGUE_REFERER.get(league)
    headers: Dict[str, str] = {"User-Agent": _UA, "Accept": "application/json"}
    if referer:
        headers["Referer"] = referer

    elapsed = time.monotonic() - _last_request_ts
    if elapsed < _RATE_LIMIT_S:
        time.sleep(_RATE_LIMIT_S - elapsed)

    try:
        resp = download(url, headers=headers, timeout=timeout, num_retries=max_retries)
        _last_request_ts = time.monotonic()
        return json.loads(_strip_jsonp(resp.text))
    except Exception as exc:  # noqa: BLE001
        _last_request_ts = time.monotonic()
        from sportsdataverse._codegen_runtime import cli_warn

        cli_warn(f"hockeytech_api({league}/{feed}/{view}) failed: {exc}")
        return None
