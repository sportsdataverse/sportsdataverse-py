"""Runtime getter for the generated ``api.nfl.com`` wrappers (:mod:`sportsdataverse.nfl.nfl_api`).

The generated flat-API module imports ``_get`` from here (via the
``getter_module`` field of ``tools/codegen/endpoints/nfl_api.yaml``) instead of
the shared no-auth :mod:`sportsdataverse._codegen_runtime`, because ``api.nfl.com``
requires a bearer token. Auth is shared with :mod:`sportsdataverse.nfl.nfl_games`:
when no ``headers`` dict is supplied, a fresh anonymous ``WEB_DESKTOP`` token is
minted via :func:`sportsdataverse.nfl.nfl_games.nfl_headers_gen`. Reuse one
``headers`` dict across many calls to avoid re-minting tokens.

Like every other wrapper in the package, the actual HTTP call goes through the
shared :func:`sportsdataverse.dl_utils.download` gateway (retry loop + cache +
ESPN-aware error handling) rather than calling :mod:`requests` directly.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union

from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_games import nfl_headers_gen

__all__ = ["_get", "_bool_str"]


def _bool_str(value: Optional[bool]) -> Optional[str]:
    """Render a Python bool as the lowercase ``"true"``/``"false"`` api.nfl.com expects.

    Passes ``None`` through unchanged so ``_get`` still strips it from the query
    string (mirrors :func:`sportsdataverse._codegen_runtime.bool_str`); otherwise a
    ``None`` flag would serialize to the literal ``"none"`` and reach the wire.
    """
    if value is None:
        return None
    return str(value).lower()


def _get(
    url: str,
    params: Optional[Dict] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs,
) -> Union[Dict, List]:
    """GET a JSON payload from ``api.nfl.com``, minting a token if needed.

    Args:
        url: Absolute ``api.nfl.com`` URL.
        params: Query params; ``None`` values are stripped.
        headers: A :func:`nfl_headers_gen` dict to reuse; minted fresh when ``None``.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`
            (e.g. ``timeout``, ``proxy``, ``num_retries``).

    Returns:
        The parsed JSON response body -- a ``dict`` for most endpoints, or a
        ``list`` for bare-array endpoints (e.g. ``weekly-game-details``).
    """
    if headers is None:
        headers = nfl_headers_gen()
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    kwargs.setdefault("timeout", 30)
    resp = download(url=url, params=clean, headers=headers, **kwargs)
    # download() returns a requests.Response on success (re-raising on retry
    # exhaustion); a cache hit returns a stored-200 shim with no raise_for_status.
    if hasattr(resp, "raise_for_status"):
        resp.raise_for_status()
    return resp.json()
