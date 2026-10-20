"""Runtime getter for the generated CBS Sports NAPI wrappers (:mod:`sportsdataverse.cbs.cbs_napi`).

The generated module imports ``_get`` from here (``getter_module`` in
``tools/codegen/endpoints/cbs_napi.yaml``, written by ``tools/codegen/gen_cbs.py``). It is the
shared :func:`sportsdataverse._codegen_runtime._get` plus one rule: NAPI answers an unknown id
with HTTP 200 and ``{"warnings": [{"code": 404, "type": "NotFoundException", ...}]}``
(``tests/fixtures/cbs_napi/warnings_404.json``). That is "nothing here", so it raises
:class:`~sportsdataverse.errors.NoDataError` instead of parsing to a zero-row frame (B-L60).
"""

from __future__ import annotations

from typing import Any, Optional

from sportsdataverse import _codegen_runtime
from sportsdataverse.errors import NoDataError

__all__ = ["_get"]


def _get(url: str, params: Optional[dict] = None, **kwargs: Any) -> Any:
    """GET a NAPI route as JSON (see :func:`sportsdataverse._codegen_runtime._get`).

    Raises:
        NoDataError: the host answered 404, or HTTP 200 with a ``warnings`` entry whose
            ``code`` is 404 (NAPI's unknown-id answer).
        ValueError: the host answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx after retries, a connection failure after
            retries, or a 2xx whose body is empty (not 204/205) or not JSON.
    """
    body = _codegen_runtime._get(url, params, **kwargs)
    warnings = body.get("warnings") if isinstance(body, dict) else None
    for warning in warnings if isinstance(warnings, list) else []:
        if isinstance(warning, dict) and warning.get("code") == 404:
            raise NoDataError(
                f"{_codegen_runtime._where(url)} answered HTTP 200 with a 404 warning: {warning.get('message', '')}"
            )
    return body
