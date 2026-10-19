"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional
from urllib.parse import urlsplit

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import (
    AssetFetchError,
    NoDataError,
    _error_envelope_code,
    _redact_secrets,
    _string_route,
)
from sportsdataverse.errors import SeasonNotFoundError  # noqa: F401  (re-export for generated loaders)

logger = logging.getLogger(__name__)

# Release / raw-data hosts for the generated dataset loaders.
_SDV_RELEASES = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
_RAW_DATA = "https://raw.githubusercontent.com/sportsdataverse/"


def _cast_ids_int64(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """Canonicalize id columns to ``Int64`` at the loader boundary.

    Producers have shipped the same ESPN id as ``String``, ``Int32`` and ``Int64``
    across releases (CFB ``team_id`` was String on the summaries/ratings family and
    Int64 on the box/pbp/adv family). Joining across two such datasets matches
    **nothing** -- silently, with no error and a structurally valid frame -- so the
    dtype is pinned once here rather than left to every caller.

    Conservative by construction: a column is only converted when every non-null
    value survives the cast. A String column holding a genuinely non-numeric id, or
    one with leading zeros that would change meaning, is left exactly as-is rather
    than corrupted or nulled. Float-origin ids go straight to Int64 rather than
    through a string (which would yield ``"123.0"``).

    Args:
        df: frame to normalize (may be empty).
        cols: id column names to canonicalize; missing ones are ignored.

    Returns:
        The frame with each named column cast to ``Int64`` where safe.
    """
    if df.height == 0:
        return df
    for col in cols:
        if col not in df.columns or df.schema[col] == pl.Int64:
            continue
        src = df[col]
        cast = src.cast(pl.Int64, strict=False)
        # Refuse if the cast would invent nulls -- a value did not survive, so the
        # column is not really an integer id.
        if cast.null_count() != src.null_count():
            continue
        # Widening one integer type to another is always lossless. For every other
        # source dtype require an exact round-trip, because "no new nulls" is
        # necessary but NOT sufficient: "007" casts cleanly to 7 and 1.5 truncates
        # to 1, both silently changing the id. Zero-padded and fractional values
        # must keep their original column untouched.
        if not src.dtype.is_integer() and not cast.cast(src.dtype).equals(src):
            continue
        df = df.with_columns(cast.alias(col))
    return df


def _where(url: str) -> str:
    """``host/path`` of ``url``: never the query string, which can carry an API key."""
    parts = urlsplit(url)
    return f"{parts.netloc}{parts.path}"


def _excerpt(text: str) -> str:
    """A bounded, single-line head of an error body (the error classes redact it)."""
    return " ".join((text or "").split())[:200]


def _check_status(url: str, status: Any, text: str = "", *, label: Optional[str] = None) -> None:
    """Raise unless ``status`` is 2xx, so a failed fetch never reaches a parser as data.

    * 404 -> :class:`~sportsdataverse.errors.NoDataError`: the host answered "nothing here".
    * 400 / 422 -> :class:`ValueError`: the request itself is wrong; retrying cannot help.
    * anything else (a 401/403/429/5xx that outlived the retries) ->
      :class:`~sportsdataverse.errors.AssetFetchError`: the answer is unknown.

    Every message names host, path and status (or ``label``, for a caller with a more
    telling name for the request) plus a bounded excerpt of the body; the error
    classes redact credentials, and the query string is never quoted.
    """
    if isinstance(status, int) and 200 <= status < 300:
        return
    where = label or _where(url)
    if status == 404:
        raise NoDataError(f"{where} answered HTTP 404")
    if status in (400, 422):
        raise ValueError(_redact_secrets(f"{where} rejected the request: HTTP {status}: {_excerpt(text)}"))
    raise AssetFetchError(f"{where} answered HTTP {status}: {_excerpt(text)}")


def _json_text(url: str, status: Any, text: str) -> Any:
    """Decode a ``(status, text)`` transport answer under the error vocabulary.

    2xx with a JSON object or array -> the body; 204 / 205 (no content by definition) -> ``{}``.
    A 200 with an EMPTY body is not "nothing": barttorvik's block, pro.nfl.com's
    rejected params and stats-host throttling all answer that way, so it raises
    :class:`~sportsdataverse.errors.AssetFetchError`, as does any non-JSON 2xx body.
    A non-2xx raises per :func:`_check_status`; so does a 2xx carrying an error
    envelope or a bare JSON scalar (see :func:`_json_data`).
    """
    _check_status(url, status, text)
    if status in (204, 205):
        return {}
    if not (text or "").strip():
        raise AssetFetchError(f"{_where(url)} answered HTTP {status} with an empty body")
    try:
        body = json.loads(text)
    except ValueError:  # its ``.doc`` is the whole body: never chain it as __context__
        pass
    else:
        return _json_data(url, status, body)
    raise AssetFetchError(f"{_where(url)} answered HTTP {status} with a non-JSON body: {_excerpt(text)}")


def _json_data(url: str, status: Any, body: Any) -> Any:
    """A decoded 2xx JSON body, refused when it is not data.

    * A JSON ``null``, number, string or boolean is no payload any parser reads, so it
      raises :class:`~sportsdataverse.errors.AssetFetchError` (B-GB-16): the fetch failed.
      The one exception is a string from a route that answers one by design
      (:data:`~sportsdataverse.errors._STRING_ROUTES`, OpenLigaDB's last-change timestamp).
    * ESPN's ``{"code": 4xx/5xx, "message": ...}`` error envelope is classified by its
      code like an HTTP status (A-M13): 404 -> ``NoDataError``, 400 / 422 ->
      ``ValueError``, anything else -> ``AssetFetchError``.
    """
    if isinstance(body, str) and _string_route(url):
        return body
    if not isinstance(body, (dict, list)):
        raise AssetFetchError(
            f"{_where(url)} answered HTTP {status} with a JSON {type(body).__name__}, not an object or array: "
            f"{_excerpt(json.dumps(body))}"
        )
    code = _error_envelope_code(body)
    if code is not None:
        _check_status(url, code, str(body["message"]), label=f"{_where(url)} (a 2xx error envelope)")
    return body


@contextmanager
def _transport_errors(url: str) -> Iterator[None]:
    """Surface a connection failure that outlived the retries as ``AssetFetchError``.

    ``dl_utils.download`` re-raises the last ``requests.RequestException`` (a timeout,
    a reset, DNS) once its retry budget is spent; that is a failed fetch too, so it is
    re-raised as :class:`~sportsdataverse.errors.AssetFetchError` chained to the
    original, which ``download`` has already redacted (``hockeytech._client`` does the
    same). Wrap the transport call only::

        with _transport_errors(url):
            resp = download(url=url, params=params)

    ponytail: catches ``OSError``, the common base of ``requests.RequestException``
    and curl_cffi's ``RequestException`` (the 247Sports transports), so one guard
    covers both; ``NoDataError`` is not an ``OSError`` and passes through.
    """
    try:
        yield
    except OSError as exc:
        raise AssetFetchError(f"{_where(url)}: fetch failed after retries: {type(exc).__name__}") from exc


def _check_response(resp: Any, url: str) -> None:
    """:func:`_check_status` for a :func:`~sportsdataverse.dl_utils.download` response.

    ``download`` has already raised :class:`~sportsdataverse.errors.NoDataError` for a
    404 and for ESPN's 200-with-``code: 404`` body, and retried 403/408/429/5xx; what
    reaches here is a final answer. The body is only decoded on the failure path.
    """
    if resp is None:
        raise AssetFetchError(f"{_where(url)}: no response")
    status = getattr(resp, "status_code", 200)
    if not (isinstance(status, int) and 200 <= status < 300):
        _check_status(url, status, getattr(resp, "text", "") or "")


def _text_body(resp: Any, url: str, *, html: bool = True) -> str:
    """The checked body of a text (CSV / HTML) response; an empty 200 raises like :func:`_json_text`.

    ``html=False`` is for a getter that never serves a page (a CSV or JSON host): a 2xx
    body that opens with ``<`` is then a challenge or error page, not data, and raises
    :class:`~sportsdataverse.errors.AssetFetchError` (A-L63). No CSV starts with ``<``.
    """
    _check_response(resp, url)
    text = getattr(resp, "text", "") or ""
    status = getattr(resp, "status_code", 200)
    if not text.strip() and status not in (204, 205):
        raise AssetFetchError(f"{_where(url)} answered HTTP {status} with an empty body")
    if not html and text.lstrip("\ufeff \t\r\n").startswith("<"):
        raise AssetFetchError(f"{_where(url)} answered HTTP {status} with a page, not data: {_excerpt(text)}")
    return text


def _json_body(resp: Any, url: str) -> Any:
    """Decode a :func:`~sportsdataverse.dl_utils.download` response like :func:`_json_text`."""
    _check_response(resp, url)
    status = getattr(resp, "status_code", 200)
    try:
        body = resp.json()
    except ValueError:  # decoded again below, outside this handler, so the error never chains it
        pass
    else:
        return _json_data(url, status, body)
    return _json_text(url, status, getattr(resp, "text", "") or "")


#: Re-fetches of a 2xx JSON body that arrived cut off (see :func:`_download_json`).
_TRUNCATED_JSON_RETRIES = 2
_TRUNCATED_JSON_DELAY = 1.0


def _truncated_json(resp: Any) -> bool:
    """A 2xx body that opens like JSON (``{`` / ``[``) but does not decode.

    A body that decodes was refused for what it says (an error envelope), not cut off.
    """
    status = getattr(resp, "status_code", 200)
    text = (getattr(resp, "text", "") or "").lstrip()
    if not (isinstance(status, int) and 200 <= status < 300 and text[:1] in ("{", "[")):
        return False
    try:
        json.loads(text)
    except ValueError:
        return True
    return False


def _download_json(transport: Callable[..., Any], url: str, **kwargs: Any) -> Any:
    """``transport(url=url, **kwargs)`` decoded under the error vocabulary (see :func:`_get`).

    The hand-written scrapers pass their own module-level ``download`` as
    ``transport``, so a test that monkeypatches ``<module>.download`` still
    intercepts the request.

    A 2xx body that starts as JSON but does not parse was cut off in transit, so it
    is fetched again, up to :data:`_TRUNCATED_JSON_RETRIES` times, before it raises
    :class:`~sportsdataverse.errors.AssetFetchError`. Yahoo's CFB scoreboard did this
    on about 1 week in 35 (2026-10-12); the crosswalk skipped the week and dropped
    its Yahoo matches. An HTML or empty 2xx body is a real answer and raises at once.
    """
    for attempt in range(_TRUNCATED_JSON_RETRIES + 1):
        with _transport_errors(url):
            resp = transport(url=url, **kwargs)
        try:
            return _json_body(resp, url)
        except AssetFetchError:
            if attempt == _TRUNCATED_JSON_RETRIES or not _truncated_json(resp):
                raise
        logger.warning(
            "%s answered a truncated JSON body; re-fetching [%d/%d]", _where(url), attempt + 1, _TRUNCATED_JSON_RETRIES
        )
        time.sleep(_TRUNCATED_JSON_DELAY * (attempt + 1))
    raise AssertionError("unreachable")  # pragma: no cover


def _get(url: str, params: Optional[dict] = None, **kwargs) -> Any:
    """GET ``url`` as JSON. Strips ``None`` params.

    Raises:
        NoDataError: the host answered 404 (or ESPN 200 with ``code: 404``).
        ValueError: the host answered 400 / 422 -- the request is wrong.
        AssetFetchError: any other non-2xx after retries, a connection failure after
            retries, or a 2xx whose body is empty (not 204/205) or not JSON.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    return _download_json(download, url, params=clean, **kwargs)


def _csv(values: Any) -> Optional[str]:
    """Join an iterable into a comma-separated string; pass scalar / None through."""
    if values is None:
        return None
    if isinstance(values, (list, tuple, set)):
        return ",".join(str(v) for v in values)
    return str(values)


def bool_str(value: Any) -> Optional[str]:
    """Coerce a truthy/falsey value to the lowercase ``"true"``/``"false"`` ESPN expects.

    Passes ``None`` through unchanged so ``_get`` still strips it.
    """
    if value is None:
        return None
    return "true" if value else "false"


def _as_season_list(seasons: Any) -> List[int]:
    """Normalize an int / iterable of seasons to a list of ints."""
    if isinstance(seasons, (int,)) and not isinstance(seasons, bool):
        return [seasons]
    if isinstance(seasons, str):
        return [int(seasons)]
    return [int(s) for s in seasons]


def cli_warn(msg: str) -> None:
    """Emit a non-fatal warning (used by 404-safe loaders for skipped seasons)."""
    import warnings

    warnings.warn(msg, stacklevel=2)


def _fetch_release_parquet(url: str) -> pl.DataFrame:
    """Read a release parquet, RAISING when the asset is missing.

    The read stays a direct read, and the bytes deliberately do NOT go through the
    HTTP gateway: :func:`_release_bytes` reads the whole body with urllib, as polars'
    own URL read does, and pyarrow parses it from memory, while a trip through
    ``download`` holds an extra ``requests`` copy of the compressed file. Measured on the
    59 MB ``play_by_play_2025.parquet`` (best of 3, one read per process, identical
    imports): direct 1010 MB / 6.0s versus buffered 1081 MB / 10.5s -- +7% peak RSS and
    +75% wall. Routing through a temp file or a
    zero-copy ``pa.py_buffer`` was no better. See issue #397.

    What DOES go through the gateway is the *classification* of a failure. When the
    direct read fails we ask ``download`` what the server actually said instead of
    pattern-matching the reader's exception message (:func:`_classify_release_failure`):

    * a 404 raises :class:`~sportsdataverse.errors.NoDataError` -- the asset is
      genuinely absent;
    * any other non-2xx -- notably a 403 or a rate limit that outlived the retry
      budget -- and a refused or reset connection raise
      :class:`~sportsdataverse.errors.AssetFetchError`, so a fetch that FAILED is never
      confused with an asset that is ABSENT;
    * a reachable asset is read once more, so a failure in transit (B-L74) recovers; a
      second transport failure is classified the same way (404 -> ``NoDataError``,
      anything else -> ``AssetFetchError``).

    Only a transport failure (``OSError``, ``http.client.HTTPException``) is classified.
    A parse or schema error from a body that arrived whole is the asset's own and is
    re-raised at once, without a second fetch. The classification is a one-byte ranged
    GET, so it never re-downloads the asset (A-N10).

    polars' URL read sets no socket timeout, so a server that accepts the connection and
    never answers still hangs the read; ``SDV_PY_HTTP_TIMEOUT`` bounds only the probe.

    R-producer assets can carry an ``arrow.r.vctrs`` field-extension whose metadata is
    raw RDS bytes (not UTF-8) -- e.g. a ``glue`` character column. polars' arrow-FFI
    import panics on that metadata (``pyo3 PanicException``, a ``BaseException`` that
    an ``except Exception`` handler never sees), so on panic we re-read via pyarrow
    with all field/schema metadata stripped; the storage types are plain, so this is
    lossless.

    Use :func:`_read_release_parquet` instead when a missing season should be skipped
    rather than raised.
    """
    import http.client
    import urllib.error

    # The fetch and the parse are separate steps: pyarrow reports a corrupt footer as a
    # plain OSError ("Couldn't deserialize thrift"), which must not read as a failed fetch.
    try:
        data = _release_bytes(url)
    except (OSError, http.client.HTTPException) as e:  # a transport failure: classified below
        original: BaseException = e
    else:
        return _parse_release(url, data)  # a parse error is the asset's own: raised at once

    _classify_release_failure(url, original)
    # Reachable (B-L74): a read that failed in transit succeeds now. A second transport
    # failure is classified like the first.
    try:
        data = _release_bytes(url)  # the second read, after the probe
    except urllib.error.HTTPError as e:
        e.close()  # its unread body holds the socket open
        if e.code == 404:
            raise NoDataError(f"{_where(url)}: release asset not found (HTTP 404)") from e
        raise AssetFetchError(f"release asset fetch failed with HTTP {e.code}: {url}") from e
    except (OSError, http.client.HTTPException) as e:
        raise AssetFetchError(f"{_where(url)}: release asset read failed: {type(e).__name__}") from e
    return _parse_release(url, data)  # the asset, read after the probe


def _release_bytes(url: str) -> bytes:
    """The asset's whole body, read the way polars reads a URL (urllib, no gateway; #397)."""
    import urllib.request

    with urllib.request.urlopen(url) as f:
        return f.read()


def _parse_release(url: str, data: bytes) -> pl.DataFrame:
    """Parse release bytes with pyarrow; on pyo3's metadata panic, re-read with the metadata stripped."""
    import io

    try:
        return pl.read_parquet(io.BytesIO(data), use_pyarrow=True)
    except BaseException as e:  # pyo3's PanicException does not subclass Exception
        if type(e).__name__ != "PanicException":
            raise  # a parse error is the asset's own; never turn Ctrl-C / SystemExit into an HTTP call
        return _read_parquet_stripped_metadata(url)


#: One byte answers "is the asset there?" without re-downloading a 20-59 MB file (A-N10).
_PROBE_HEADERS = {"Range": "bytes=0-0"}


def _classify_release_failure(url: str, original: BaseException) -> None:
    """Raise what the server says about an asset Arrow failed to read; return when it is reachable.

    A 404 raises :class:`~sportsdataverse.errors.NoDataError` (from ``download``); a refused
    or reset connection after the retries (A-M9) and any other non-2xx raise
    :class:`~sportsdataverse.errors.AssetFetchError` chained to ``original``. The release
    CDN answers the ranged GET 206, a host that ignores ``Range`` 200, and a zero-byte
    asset 416: all three are reachable, so the caller reads again.
    """
    import urllib.error

    if isinstance(original, urllib.error.HTTPError):
        original.close()  # the first read's error response holds its socket open (R898-13)
    with _transport_errors(url):
        resp = download(url, headers=_PROBE_HEADERS)
    status = getattr(resp, "status_code", None)
    if isinstance(status, int) and not (200 <= status < 300 or status == 416):
        raise AssetFetchError(f"release asset fetch failed with HTTP {status}: {url}") from original


def _read_release_parquet(url: str) -> Optional[pl.DataFrame]:
    """404-safe read: ``None`` when the asset is absent (season-gap-tolerant loaders).

    Thin wrapper over :func:`_fetch_release_parquet` -- identical read path and
    identical failure classification. The ONLY difference is that a genuinely missing
    asset becomes ``None`` so the caller can skip that season, while a failed fetch
    (``AssetFetchError``) still propagates. Keeping the two apart in one place is what
    stops a rate-limited season from being recorded as an empty one.
    """
    from sportsdataverse.errors import NoDataError

    try:
        return _fetch_release_parquet(url)
    except NoDataError:
        return None


def _read_parquet_stripped_metadata(url: str) -> pl.DataFrame:
    """Fetch a parquet and load it with every field/schema metadata entry dropped.

    Classifies the refetch the same way the caller does. This path runs only after
    a panic, so it issues its OWN request -- and that one can fail even though the
    first read reached the asset. Without the status check, a 403 or 5xx body would
    be handed to ``pq.read_table`` and surface as a parquet parse error, which is
    precisely the "failed fetch wearing the wrong clothes" this module exists to
    prevent. A 404 still arrives as ``NoDataError`` from ``download``.
    """
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    with _transport_errors(url):  # A-M9: a dead connection is a failed fetch, not a requests error
        resp = download(url)
    status = getattr(resp, "status_code", None)
    if status is not None and status != 200:
        raise AssetFetchError(f"release asset fetch failed with HTTP {status}: {url}")
    tbl = pq.read_table(io.BytesIO(resp.content))
    plain = pa.schema([pa.field(f.name, f.type) for f in tbl.schema])
    out = pl.from_arrow(tbl.cast(plain).replace_schema_metadata(None))
    assert isinstance(out, pl.DataFrame)
    return out


def _fetch_csv(url: str, **read_csv_kwargs: Any) -> pl.DataFrame:
    """Read a remote CSV through the HTTP gateway, under the error vocabulary (A-M11).

    A CSV is small, so unlike :func:`_fetch_release_parquet` its bytes go through
    ``download``, and its 429/5xx backoff and the vocabulary come with them. Handed a
    URL, polars 2.0's own reader raises a bare ``OSError`` for every HTTP failure, so a
    missing file, a rate limit and a dead connection were indistinguishable.

    Raises:
        NoDataError: the host answered 404.
        ValueError: the host answered 400 / 422.
        AssetFetchError: any other non-2xx after retries, a connection failure after
            retries, or a 2xx whose body is empty or a page rather than CSV.
    """
    import io

    with _transport_errors(url):
        resp = download(url)
    _text_body(resp, url, html=False)
    return pl.read_csv(io.BytesIO(resp.content), **read_csv_kwargs)


def format_nhl_season(season: Any) -> Optional[str]:
    """Normalize an NHL season to the 8-digit ``"20242025"`` form the api-web host wants.

    Accepts a 4-digit end year (``2025`` -> ``"20242025"``) or an already-8-digit
    string/int (``"20242025"`` -> ``"20242025"``). ``None`` passes through.
    """
    if season is None:
        return None
    s = str(season)
    if len(s) == 8 and s.isdigit():
        return s
    if len(s) == 4 and s.isdigit():
        return f"{int(s) - 1}{s}"
    raise ValueError(f"Unrecognized NHL season {season!r}")
