"""Runtime getter for the generated ``api.nfl.com`` wrappers (:mod:`sportsdataverse.nfl.nfl_api`).

The generated flat-API module imports ``_get`` from here (via the
``getter_module`` field of ``tools/codegen/endpoints/nfl_api.yaml``) instead of
the shared no-auth :mod:`sportsdataverse._codegen_runtime`, because ``api.nfl.com``
requires a bearer token. Auth is shared with :mod:`sportsdataverse.nfl.nfl_games`:
when no ``headers`` dict is supplied, a fresh anonymous ``WEB_DESKTOP`` token is
minted via :func:`sportsdataverse.nfl.nfl_games.nfl_headers_gen`. Reuse one
``headers`` dict across many calls to avoid re-minting tokens.
"""

from __future__ import annotations

from typing import Dict, Optional

import requests

from sportsdataverse.nfl.nfl_games import nfl_headers_gen

__all__ = ["_get", "_bool_str"]


def _bool_str(value: bool) -> str:
    """Render a Python bool as the lowercase ``"true"``/``"false"`` api.nfl.com expects."""
    return str(value).lower()


def _get(
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs,
) -> Dict:
    """GET a JSON payload from ``api.nfl.com``, minting a token if needed.

    Args:
        url: Absolute ``api.nfl.com`` URL.
        params: Query params; ``None`` values are stripped.
        headers: A :func:`nfl_headers_gen` dict to reuse; minted fresh when ``None``.
        **kwargs: Forwarded to :func:`requests.get` (e.g. ``timeout``).

    Returns:
        The parsed JSON response body.
    """
    if headers is None:
        headers = nfl_headers_gen()
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    kwargs.setdefault("timeout", 30)
    resp = requests.get(url, headers=headers, params=clean, **kwargs)
    resp.raise_for_status()
    return resp.json()
