"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sportsdataverse.dl_utils import download


def _get(url: str, params: Optional[dict] = None, **kwargs) -> Dict:
    """GET ``url`` as JSON. Returns ``{}`` on failure. Strips ``None`` params."""
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    resp = download(url=url, params=clean, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


def _csv(values: Any) -> Optional[str]:
    """Join an iterable into a comma-separated string; pass scalar / None through."""
    if values is None:
        return None
    if isinstance(values, (list, tuple, set)):
        return ",".join(str(v) for v in values)
    return str(values)
