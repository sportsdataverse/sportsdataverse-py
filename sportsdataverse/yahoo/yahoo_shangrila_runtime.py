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

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import NoDataError

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
        The parsed JSON ``dict``; ``{}`` when the route 404s
        (:class:`~sportsdataverse.errors.NoDataError`), when the body is not JSON,
        or when it is not a JSON object. Yahoo answers a bad persisted query with
        HTTP 400 and a ``{"errors": [...]}`` body, which is returned as-is so the
        parser can turn it into a zero-row frame.

    Raises:
        requests.exceptions.RequestException: Connection-level failure after
            ``dl_utils.download`` exhausts its retries.

    Example:
        Quick start::

            from sportsdataverse.yahoo.yahoo_shangrila_runtime import _get
            raw = _get("https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueNames")
            print(sorted(raw.get("data", {})))
    """
    headers = {**_HEADERS, **(kwargs.pop("headers", None) or {})}
    query = {**_LOCALE, **{k: v for k, v in (params or {}).items() if v is not None}}
    try:
        resp = download(url=url, params=query, headers=headers, **kwargs)
    except NoDataError:
        return {}
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}
