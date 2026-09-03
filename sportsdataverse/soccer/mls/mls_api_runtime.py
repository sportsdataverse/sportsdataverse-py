"""Runtime getter for the generated ``mls_api`` wrappers.

All three mlssoccer.com hosts (``stats-api``, ``sportapi``, ``dapi``) are
**auth-free** -- no token, cookie or key -- but they are only exercised in the
wild from the site itself, and the reference capture notes record a
``Referer: https://www.mlssoccer.com/`` on every request that worked. The shared
``_codegen_runtime._get`` sends no headers at all, so this thin wrapper adds that
Referer plus a browser User-Agent and otherwise defers entirely to
:func:`sportsdataverse.dl_utils.download` -- same retry budget, same 404 ->
``NoDataError`` behaviour, no second HTTP path.

Callers can still override either header by passing ``headers=`` through; the
supplied mapping wins key-by-key.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from sportsdataverse.dl_utils import download

REFERER = "https://www.mlssoccer.com/"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

__all__ = ["REFERER", "_get", "bool_str"]


def bool_str(value: Any) -> Optional[str]:
    """Coerce a truthy/falsey value to the lowercase ``"true"``/``"false"`` the API expects.

    Re-exported from the shared runtime so the generated module can import every
    helper it needs from one place. ``None`` passes through so ``_get`` still
    strips it.

    Args:
        value: any truthy/falsey value, or ``None``.

    Returns:
        ``"true"`` / ``"false"``, or ``None`` when ``value`` is ``None``.

    Example:
        Basic use::

            from sportsdataverse.soccer.mls.mls_api_runtime import bool_str

            bool_str(True)
            # 'true'
    """
    from sportsdataverse._codegen_runtime import bool_str as _shared

    return _shared(value)


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Union[Dict, list]:
    """GET ``url`` as JSON with the MLS site headers. Returns ``{}`` on failure.

    Args:
        url: fully-built request URL (host + substituted path).
        params: query-string parameters; ``None`` values are dropped.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`. A
            ``headers`` mapping is merged over the defaults.

    Returns:
        The parsed JSON body -- a ``dict`` for the enveloped and single-entity
        routes, a ``list`` for the ``sportapi`` batch routes -- or ``{}`` when the
        response is missing or not JSON.

    Raises:
        sportsdataverse.errors.NoDataError: the host returned 404.

    Example:
        Basic use::

            from sportsdataverse.soccer.mls.mls_api_runtime import _get

            body = _get("https://stats-api.mlssoccer.com/competitions")
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
