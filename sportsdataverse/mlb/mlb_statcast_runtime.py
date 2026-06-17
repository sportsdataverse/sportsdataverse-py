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
from sportsdataverse.dl_utils import download


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, str]:
    """GET ``url`` and return JSON (``dict``) or raw text (``str``).

    Content-type drives the shape: ``application/json`` is parsed to a ``dict``;
    anything else (``text/csv``, ``application/download`` for the search export,
    ``text/html`` for embedded-JSON leaderboards) is returned as the raw response
    text. ``None`` params are stripped. Returns ``{}`` on transport failure (no
    response) so JSON consumers can chain without a null-check, and ``""`` only
    if a body is present but unreadable.

    Args:
        url: fully-qualified endpoint URL.
        params: query parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        ``dict`` for JSON responses, ``str`` for CSV/HTML responses, ``{}`` when
        the request yields no response.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    ctype = (resp.headers.get("content-type") or "").lower() if getattr(resp, "headers", None) else ""
    if "json" in ctype:
        try:
            return resp.json()
        except Exception:
            pass
    try:
        return resp.text
    except Exception:
        # No content-type hint and no text -- last-ditch JSON attempt.
        try:
            return resp.json()
        except Exception:
            return ""
