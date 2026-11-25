"""Runtime getter for the PGA Tour GraphQL API (``orchestrator.pgatour.com/graphql``).

Runtime only: every ``pgatour_*`` wrapper sends one GraphQL query when it is called. No release
assets are built from this API, and nothing here bulk-downloads or backfills it.

Each generated wrapper names its operation in a fixed ``operation`` body field.
:func:`graphql_body` (the family's ``body_builder``) turns that flat body into the GraphQL
request, and :func:`_get` POSTs it to ``/graphql``:

* the query document is ``pgatour_queries/<operation>.graphql``, shipped with the package;
* the ``x-api-key`` header is the public key pgatour.com's own frontend sends. Set the
  ``PGA_API_KEY`` environment variable to use another key. Without it, the key is read from the
  site's JavaScript bundle on first use and kept for the rest of the process. No key is stored in
  the package;
* a ``*Compressed*`` query answers ``{"id": ..., "payload": "<base64 gzip JSON>"}``, and the
  payload is decoded before the wrapper sees it.

Retries, backoff and the error vocabulary come from :func:`sportsdataverse._codegen_runtime._get`.
"""

from __future__ import annotations

import base64
import binascii
import functools
import gzip
import json as _json
import os
import re
from pathlib import Path
from typing import Any

from sportsdataverse._codegen_runtime import _get as _http_get
from sportsdataverse._codegen_runtime import _integral, _wire_text
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list = []

HOST = "https://orchestrator.pgatour.com"
GRAPHQL_URL = f"{HOST}/graphql"
SITE_URL = "https://www.pgatour.com/"
#: Environment variable that overrides the API key read from the site.
KEY_ENV = "PGA_API_KEY"
QUERY_DIR = Path(__file__).with_name("pgatour_queries")

_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
_APP_CHUNK = re.compile(r"/_next/static/chunks/pages/_app-[0-9a-f]+\.js")
# The production block of the site's API config: {"prod":{..."apiKey":"<key>","queryEndpoint":...}}
_PROD_KEY = re.compile(r'"prod":\{[^{}]*?"apiKey":"([^"]+)"')


def _text(url: str) -> str:
    """``url``'s body, fetched fresh (never from the response cache, so a rotated key is re-read).

    Raises:
        AssetFetchError: the page did not answer (any status, a 404 included).
    """
    try:
        return download(url, headers={"User-Agent": _UA}, cache_ttl=0).text
    except NoDataError as exc:
        raise AssetFetchError(f"{url} answered 404 while reading the API key; set {KEY_ENV} to an API key") from exc


@functools.lru_cache(maxsize=1)
def _site_api_key() -> str:
    """The public API key in pgatour.com's ``_app`` bundle (fetched once per process).

    Raises:
        AssetFetchError: the page or bundle did not answer, or no longer carries the key.
    """
    page = _text(SITE_URL)
    chunk = _APP_CHUNK.search(page)
    if not chunk:
        raise AssetFetchError(f"{SITE_URL} no longer links its _app bundle; set {KEY_ENV} to an API key")
    found = _PROD_KEY.search(_text(f"https://www.pgatour.com{chunk.group(0)}"))
    if not found:
        raise AssetFetchError(f"the pgatour.com _app bundle no longer carries an API key; set {KEY_ENV}")
    return found.group(1)


def api_key() -> str:
    """The ``x-api-key`` the requests send.

    Returns:
        ``$PGA_API_KEY`` when set, else the public key read from pgatour.com (once per process).

    Raises:
        AssetFetchError: no environment key, and the site no longer carries one.
    """
    return os.environ.get(KEY_ENV) or _site_api_key()


@functools.cache
def query_document(operation: str) -> str:
    """The GraphQL document for ``operation``.

    Args:
        operation: The query's root field (``leaderboardCompressedV3``), also its file name.

    Returns:
        The text of ``pgatour_queries/<operation>.graphql``.

    Raises:
        ValueError: no document of that name ships with the package.
    """
    path = QUERY_DIR / f"{operation}.graphql"
    if not path.is_file():
        raise ValueError(f"no PGA Tour query named {operation!r}")
    return path.read_text(encoding="utf-8")


def decode_payload(value: Any) -> Any:
    """``value`` with a base64 + gzip JSON ``payload`` decoded in place; anything else unchanged.

    Args:
        value: One operation's ``data`` value (``{"id": ..., "payload": "<base64>"}`` when compressed).

    Returns:
        ``value`` with ``payload`` replaced by the decoded JSON; a value without one, as given.

    Raises:
        AssetFetchError: the payload is not base64-encoded gzip JSON.
    """
    if not (isinstance(value, dict) and isinstance(value.get("payload"), str)):
        return value
    try:
        decoded = _json.loads(gzip.decompress(base64.b64decode(value["payload"])))
    except (binascii.Error, OSError, EOFError, ValueError) as exc:
        raise AssetFetchError(f"{GRAPHQL_URL} sent a payload that is not base64 gzip JSON") from exc
    return {**value, "payload": decoded}


#: Variables the API types as ``Int``. ``schedule(year:)`` is the one ``String`` year.
_INT_VARS = frozenset({"year", "round", "hole"})
_STRING_VARS = {"schedule": frozenset({"year"})}
#: ``ID`` / ``String`` variables: a caller's ``120`` or ``28237`` is sent as text.
_TEXT_VARS = frozenset({"id", "tournamentId", "playerId", "statId", "tourCode", "courseId"})
#: ``[ID!]`` variables: one id or a list of them, each sent as text.
_LIST_VARS = frozenset({"ids"})
#: ``tournamentPastResults(year:)`` names an edition by its five-digit season (``20250`` is 2025): a
#: four-digit year is not an error to the API, it silently answers the current edition instead.
_EDITION_YEAR_OPS = frozenset({"tournamentPastResults"})


def _whole(value: Any) -> int | None:
    """``2026`` / ``"2026"`` / ``2026.0`` -> 2026; ``2026.7``, ``True``, ``"last"`` -> None."""
    if isinstance(value, str):
        text = value.strip()
        return int(text) if text.lstrip("-").isdigit() else None
    return _integral(value)


def _edition_year(operation: str, year: int) -> int:
    """A season as the five-digit edition value (``2025`` -> ``20250``); a five-digit one as given.

    Raises:
        ValueError: ``year`` is neither four nor five digits.
    """
    if 1000 <= year <= 9999:
        return year * 10
    if 10000 <= year <= 99999:
        return year
    raise ValueError(f"PGA Tour {operation}: year must be a season such as 2025 (or 20250), got {year!r}")


def _variables(operation: str, body: dict | None) -> dict[str, Any]:
    """The non-None variables of ``body``, each sent as the type the API declares.

    Raises:
        ValueError: an ``Int`` variable is not a whole number, or an edition year is not a season.
    """
    out: dict[str, Any] = {k: v for k, v in (body or {}).items() if v is not None}
    for key, value in list(out.items()):
        if key in _LIST_VARS:
            many = value if isinstance(value, (list, tuple)) else [value]
            out[key] = [_wire_text(v) for v in many if v is not None]
        elif key in _TEXT_VARS or key in _STRING_VARS.get(operation, frozenset()):
            out[key] = _wire_text(value)
        elif key in _INT_VARS:
            whole = _whole(value)
            if whole is None:
                raise ValueError(f"PGA Tour {operation}: {key} must be a whole number, got {value!r}")
            if key == "year" and operation in _EDITION_YEAR_OPS:
                whole = _edition_year(operation, whole)
            out[key] = whole
    return out


def graphql_body(body: dict[str, Any]) -> dict[str, Any]:
    """The wrapper's flat body (its variables plus a fixed ``operation``) as a GraphQL request.

    ``{"operation": "statDetails", "statId": "120", ...}`` becomes ``{"operationName":
    "StatDetails", "query": <pgatour_queries/statDetails.graphql>, "variables": {...}}``.

    Args:
        body: The generated wrapper's body: its variables (None for unset) and ``operation``.

    Returns:
        The JSON body to POST to ``/graphql``.

    Raises:
        ValueError: an unknown operation, or a variable of the wrong type.
    """
    body = dict(body)
    operation = str(body.pop("operation"))
    return {
        "operationName": operation[0].upper() + operation[1:],
        "query": query_document(operation),
        "variables": _variables(operation, body),
    }


def _graphql_error(operation: str, errors: Any) -> Exception:
    """The error-vocabulary exception for a GraphQL ``errors`` list.

    The API names the failure in ``errorType`` (``ValidationError``, ``MalformedHttpRequestException``,
    ``UnauthorizedException``, ...); the message text is the fallback when it is absent.
    """
    errors = [e for e in errors if isinstance(e, dict)] if isinstance(errors, list) else []
    text = "; ".join(str(e.get("message", "")) for e in errors) or "unknown GraphQL error"
    kinds = " ".join(str(e.get("errorType") or (e.get("extensions") or {}).get("code") or "") for e in errors).lower()
    lowered = f"{kinds} {text.lower()}"
    if "unauthorized" in lowered or "forbidden" in lowered or "api key" in lowered:
        return AssetFetchError(f"PGA Tour {operation}: the API key was refused ({text}); set {KEY_ENV}")
    if "notfound" in kinds or "not found" in lowered or "does not exist" in lowered:
        return NoDataError(f"PGA Tour {operation}: {text}")
    if "validation" in lowered or "malformed" in lowered or "variable" in lowered:
        return ValueError(f"PGA Tour {operation}: {text}")
    return AssetFetchError(f"PGA Tour {operation}: {text}")


# 401 / 403 are a refused key, not load: answered at once (see _post), never backed off.
_RETRY = frozenset({429, 500, 502, 503, 504})


def _send(body: dict[str, Any], headers: dict[str, str], **kwargs: Any) -> Any:
    return _http_get(
        GRAPHQL_URL, json=body, headers={"x-api-key": api_key(), **headers}, retry_statuses=_RETRY, **kwargs
    )


def _post(body: dict[str, Any], headers: dict[str, str], **kwargs: Any) -> Any:
    """POST ``body`` with the API key; a refused key read from the site is re-read once (it may have rotated)."""
    try:
        return _send(body, headers, **kwargs)
    except AssetFetchError as exc:
        if os.environ.get(KEY_ENV) or not any(f"HTTP {code}" in str(exc) for code in (401, 403)):
            raise
    _site_api_key.cache_clear()
    return _send(body, headers, **kwargs)


def _get(url: str, params: dict | None = None, json: dict | None = None, **kwargs: Any) -> Any:
    """POST the GraphQL request ``json`` (built by :func:`graphql_body`) to ``url``.

    Returns the operation's ``data`` field (a dict), with a compressed ``payload`` decoded.

    Raises:
        NoDataError: the API answers ``null`` for the operation (an unknown id) or says it does not exist.
        ValueError: the API rejects the query or its variables.
        AssetFetchError: a refused key, any other GraphQL error, a transport failure after retries,
            or a payload that does not decode.
    """
    body = json if isinstance(json, dict) and "query" in json else graphql_body({**(json or {}), **(params or {})})
    field = body["operationName"][0].lower() + body["operationName"][1:]
    headers = {
        "User-Agent": _UA,
        "Origin": "https://www.pgatour.com",
        "Referer": SITE_URL,
    }
    headers.update(kwargs.pop("headers", None) or {})
    answer = _post(body, headers, **kwargs)
    data = answer.get("data") if isinstance(answer, dict) else None
    value = data.get(field) if isinstance(data, dict) else None
    if value is None:
        errors = answer.get("errors") if isinstance(answer, dict) else None
        if errors:
            raise _graphql_error(field, errors)
        raise NoDataError(f"PGA Tour {field} answered null for {body['variables']}")
    return decode_payload(value)
