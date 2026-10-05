"""Runtime getter for the generated ``mlb_statcast`` wrappers.

Baseball Savant (``baseballsavant.mlb.com``) is heterogeneous: leaderboards
return **CSV** when called with ``csv=true`` (``text/csv``), the per-game feed
``/gf`` and ``/schedule`` return **JSON** (``application/json``), and a couple of
leaderboards (``fielding-run-value``, ``statcast-park-factors``) return **HTML**
with the data embedded in a ``<script>`` blob even with ``csv=true``.

The shared no-auth runtime (:mod:`sportsdataverse._codegen_runtime`) always does
``response.json()`` and returns ``{}`` on any non-JSON body — which silently
drops every CSV/HTML payload. This module supplies a drop-in ``_get`` that
returns the **parsed JSON dict for JSON bodies and the raw text for CSV/HTML
bodies**, so each endpoint's registered parser receives the shape it expects
(``parse_mlb_statcast_leaderboard`` consumes CSV text, ``parse_mlb_statcast_gamefeed``
consumes the JSON dict, the embedded-JSON parser consumes HTML text).

The generated module imports ``_get`` from here because the YAML sets
``getter_module: sportsdataverse.mlb.mlb_statcast_runtime``. Transform helpers
(``bool_str`` etc.) are re-exported so codegen ``runtime_imports`` keep resolving
against this module.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from sportsdataverse._codegen_runtime import _as_season_list, _csv, bool_str  # noqa: F401  (re-export for generated imports)
from sportsdataverse._codegen_runtime import _check_response, _json_body, _text_body, _transport_errors
from sportsdataverse.dl_utils import download


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, str]:
    """GET ``url`` and return JSON (``dict``) or raw text (``str``).

    Content-type drives the shape: ``application/json`` is parsed to a ``dict``;
    anything else (``text/csv``, ``application/download`` for the search export,
    ``text/html`` for embedded-JSON leaderboards) is returned as the raw response
    text. ``None`` params are stripped.

    Args:
        url: fully-qualified endpoint URL.
        params: query parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        ``dict`` for JSON responses, ``str`` for CSV/HTML responses.

    Raises:
        NoDataError: Savant answered 404.
        ValueError: Savant answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx or a connection failure after retries, or
            an empty 200, or a JSON-labelled body that does not decode -- its error
            page is not data.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    with _transport_errors(url):
        resp = download(url=url, params=clean, **kwargs)
    _check_response(resp, url)
    ctype = (resp.headers.get("content-type") or "").lower() if getattr(resp, "headers", None) else ""
    if "json" in ctype:
        # A JSON-labelled body that will not decode is a failed fetch, never text to parse.
        return _json_body(resp, url)
    return _text_body(resp, url)
