"""Runtime for the generated EuroLeague wrappers: the API answers XML unless asked for JSON."""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse import _codegen_runtime as _rt

ACCEPT_JSON = {"Accept": "application/json"}


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET ``url`` as JSON with ``Accept: application/json`` merged into any caller headers."""
    headers = {**ACCEPT_JSON, **(kwargs.pop("headers", None) or {})}
    return _rt._get(url, params, headers=headers, **kwargs)
