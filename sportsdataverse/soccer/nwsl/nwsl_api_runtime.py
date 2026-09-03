"""Runtime getter for the generated ``nwsl_api`` wrappers.

``api-sdp.nwslsoccer.com`` (StatsPerform's Sports Data Platform, fronted by
Akamai + istio-envoy) is **auth-free** -- no key, no token, CORS ``*`` -- but the
reference capture notes record a ``Referer: https://www.nwslsoccer.com/`` on
every request that worked. The shared ``_codegen_runtime._get`` sends no headers
at all, so this thin wrapper adds that Referer plus a browser User-Agent and
otherwise defers entirely to :func:`sportsdataverse.dl_utils.download` -- same
retry budget, same 404 -> ``NoDataError`` behaviour, no second HTTP path.

Composite ids (``nwsl::Football_{Entity}::{32-hex}``) are already substituted
into ``url`` by the wrapper and are passed through untouched -- the ``::`` must
reach the host literally, never percent-encoded.

Callers can still override either header by passing ``headers=`` through; the
supplied mapping wins key-by-key.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from sportsdataverse.dl_utils import download

REFERER = "https://www.nwslsoccer.com/"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

__all__ = ["REFERER", "_get"]


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, list]:
    """GET ``url`` as JSON with the NWSL site headers. Returns ``{}`` on failure.

    Args:
        url: fully-built request URL (host + substituted path).
        params: query-string parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`. A
            ``headers`` mapping is merged over the defaults.

    Returns:
        The parsed JSON envelope, or ``{}`` when the response is missing or not
        JSON.

    Raises:
        sportsdataverse.errors.NoDataError: the host returned 404.

    Example:
        Basic use::

            from sportsdataverse.soccer.nwsl.nwsl_api_runtime import _get

            body = _get(
                "https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions",
                params={"locale": "en-US"},
            )
            print(sorted(body))
    """
    headers = {"Referer": REFERER, "User-Agent": _UA, **(kwargs.pop("headers", None) or {})}
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    resp = download(url=url, params=clean, headers=headers, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}
