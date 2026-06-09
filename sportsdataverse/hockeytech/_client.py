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

import requests

from sportsdataverse.hockeytech._leagues import get_config, resolve_api_key

_UA = "Mozilla/5.0 (compatible; sportsdataverse/hockeytech)"
_CALLBACK_RE = re.compile(r"^[A-Za-z_$][\w.$]*\(")
_RATE_LIMIT_S = 0.4
_last_request_ts = 0.0


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
    headers = {"User-Agent": _UA, "Accept": "application/json", "Referer": "https://www.thepwhl.com/"}
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        elapsed = time.monotonic() - _last_request_ts
        if elapsed < _RATE_LIMIT_S:
            time.sleep(_RATE_LIMIT_S - elapsed)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, **kwargs)
            _last_request_ts = time.monotonic()
            if resp.status_code == 200:
                return json.loads(_strip_jsonp(resp.text))
            if resp.status_code == 429:
                time.sleep(2**attempt)
                continue
            # Unexpected status (404/500/etc.): record it so the final cli_warn
            # fires, and back off briefly instead of spinning the retries.
            last_exc = RuntimeError(f"HTTP {resp.status_code} for {url}")
            time.sleep(1)
        except (requests.RequestException, json.JSONDecodeError) as exc:
            last_exc = exc
            time.sleep(1)
    if last_exc is not None:
        from sportsdataverse._codegen_runtime import cli_warn

        cli_warn(f"hockeytech_api({league}/{feed}/{view}) failed: {last_exc}")
    return None
