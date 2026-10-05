"""Runtime getter for the generated Yahoo Sports wrappers (:mod:`sportsdataverse.yahoo.yahoo_shangrila`).

The generated flat-API module imports ``_get`` from here (via the
``getter_module`` field of ``tools/codegen/endpoints/yahoo_shangrila.yaml``)
instead of the shared :mod:`sportsdataverse._codegen_runtime`, because the two
Yahoo hosts need two things the shared getter does not send:

* **Origin / Referer headers.** ``graphite-secure.sports.yahoo.com`` and
  ``api-secure.sports.yahoo.com`` are NOT authenticated -- there is no token,
  cookie or crumb -- but they reject a request that does not look like it came
  from ``sports.yahoo.com``. That is the whole "auth" story, which is why the
  YAML sets ``getter_module`` but NOT ``auth: true``.
* **Locale defaults.** Every path in both specs declares optional
  ``lang``/``region``/``tz`` parameters with defaults. Sending them here keeps
  three no-op arguments off all 107 generated signatures; a caller who needs a
  different locale passes ``params={"lang": "fr-FR"}``, which the generated
  wrapper merges over these defaults.

Like every other wrapper in the package the actual HTTP call goes through the
shared :func:`sportsdataverse.dl_utils.download` gateway (retry loop + cache +
error handling) rather than calling :mod:`requests` directly. Tests substitute the
module-level ``download`` name to run entirely offline.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from sportsdataverse._codegen_runtime import _json_body, _transport_errors
from sportsdataverse.dl_utils import download

__all__ = ["_get"]

#: Yahoo rejects requests that do not present a sports.yahoo.com browser context.
_HEADERS = {
    "Origin": "https://sports.yahoo.com",
    "Referer": "https://sports.yahoo.com/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
}

#: Spec defaults for the locale parameters every Yahoo path declares.
_LOCALE = {"lang": "en-US", "region": "US", "tz": "America/Chicago"}


def _get(url: str, params: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Dict:
    """GET a Yahoo Sports JSON route and return its parsed body.

    Args:
        url: full route URL built by the generated wrapper (``host`` + path).
        params: query parameters; ``None`` values are dropped, and the caller's
            values win over the :data:`_LOCALE` defaults.
        **kwargs: forwarded to :func:`sportsdataverse.dl_utils.download`
            (``timeout``, ``proxy``, ``num_retries``, extra ``headers``).

    Returns:
        The parsed JSON ``dict``; ``{}`` for a 204/205 or a JSON body that is not
        an object.

    Raises:
        NoDataError: the route answered 404.
        ValueError: Yahoo answered 400 / 422 -- e.g. the HTTP 400
            ``{"errors": [...]}`` a bad persisted query gets. An error body is not data.
        AssetFetchError: any other non-2xx or a connection failure after retries, or
            a 2xx whose body is empty (not 204/205) or not JSON.

    Example:
        Quick start::

            from sportsdataverse.yahoo.yahoo_shangrila_runtime import _get
            raw = _get("https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames")
            print(sorted(raw.get("data", {})))
    """
    headers = {**_HEADERS, **(kwargs.pop("headers", None) or {})}
    query = {**_LOCALE, **{k: v for k, v in (params or {}).items() if v is not None}}
    with _transport_errors(url):
        resp = download(url=url, params=query, headers=headers, **kwargs)
    body = _json_body(resp, url)
    return body if isinstance(body, dict) else {}
