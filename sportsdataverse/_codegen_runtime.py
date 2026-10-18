"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

import inspect
import json
import logging
import numbers
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, List, Optional
from urllib.parse import quote, urlsplit

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


def _integral(value: Any) -> Optional[int]:
    """``value`` as an ``int`` when it is a whole number (an int, a numpy integer, ``2024.0``), else ``None``.

    A bool is not a number here: ``True`` as a season or an id is a caller's mistake.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real) and float(value).is_integer():
        return int(float(value))
    return None


def _is_many(value: Any) -> bool:
    """An iterable of values (list, tuple, set, range, numpy array, polars / pandas Series), not one value."""
    return not isinstance(value, (str, bytes)) and hasattr(value, "__iter__")


def _wire_text(value: Any) -> str:
    """One query / path value as the text the host receives (A-M16).

    A whole number goes through ``int``, so ``2024.0`` and ``np.int64(2024)`` are
    ``"2024"``, never ``"2024.0"``; a bool is the lowercase ``"true"``/``"false"``.

    Raises:
        ValueError: ``value`` is itself a collection.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    whole = _integral(value)
    if whole is not None:
        return str(whole)
    if _is_many(value):
        raise ValueError(f"expected a single value, got {type(value).__name__} {value!r}")
    return str(value)


def _csv(values: Any) -> Optional[str]:
    """Join an iterable into a comma-separated string; pass scalar / None through.

    Any iterable joins (a numpy array, a polars Series and a range included); a set
    joins sorted, so the request does not depend on hash order. Each item is
    :func:`_wire_text`.
    """
    if values is None:
        return None
    if not _is_many(values):
        return _wire_text(values)
    items = sorted(values) if isinstance(values, (set, frozenset)) else values
    return ",".join(_wire_text(v) for v in items)


def _path_seg(value: Any) -> str:
    """A path parameter as one URL segment: :func:`_wire_text`, percent-quoted (A-N2).

    ``team_id="13/roster"`` is the segment ``13%2Froster``; unquoted, it changed the route.
    """
    return quote(_wire_text(value), safe="")


#: Keyword arguments a generated ESPN wrapper forwards through ``_get`` to ``download``.
_GET_KWARGS = frozenset(inspect.signature(download).parameters) - {"url", "params"}


def _check_kwargs(fn: str, kwargs: dict) -> None:
    """Refuse a keyword ``fn`` cannot use before any request (A-N3).

    A misspelled query kwarg (``date=`` for ``dates=``) used to fail inside ``download``
    as ``download() got an unexpected keyword argument``, naming the wrong function.

    Raises:
        TypeError: ``kwargs`` holds a name ``download`` does not take.
    """
    bad = sorted(set(kwargs) - _GET_KWARGS)
    if bad:
        names = ", ".join(repr(k) for k in bad)
        raise TypeError(
            f"{fn}() got an unexpected keyword argument {names}; "
            f"besides its own parameters it accepts {', '.join(sorted(_GET_KWARGS))} and params"
        )


_TRUE_TEXT = frozenset({"true", "1"})
_FALSE_TEXT = frozenset({"false", "0"})


def bool_str(value: Any, name: str = "value") -> Optional[str]:
    """Coerce a truthy/falsey value to the lowercase ``"true"``/``"false"`` ESPN expects.

    Passes ``None`` through unchanged so ``_get`` still strips it. A string is parsed,
    not tested for truth: ``"false"`` and ``"0"`` are ``"false"`` (A-M17).

    Raises:
        ValueError: ``value`` is a string other than true / false / 1 / 0 (any case);
            the message names the parameter ``name``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE_TEXT:
            return "true"
        if text in _FALSE_TEXT:
            return "false"
        raise ValueError(f"{name} must be a bool or one of 'true'/'false'/'1'/'0'; got {value!r}")
    return "true" if value else "false"


def _season(value: Any) -> int:
    """One season as an ``int`` year (see :func:`_as_season_list`)."""
    whole = _integral(value)
    if whole is not None:
        return whole
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    raise ValueError(f"seasons must be a year or an iterable of years; got {value!r}")


def _as_season_list(seasons: Any) -> List[int]:
    """Normalize an int / iterable of seasons to a list of ints (A-M6).

    A numpy integer, ``2024.0``, a numpy array and a polars Series all work; a
    fractional year, a bool, ``None`` and non-numeric text raise ``ValueError``
    before any request (``2024.5`` used to truncate to ``2024``).
    """
    if _is_many(seasons):
        return [_season(s) for s in seasons]
    return [_season(seasons)]


def cli_warn(msg: str) -> None:
    """Emit a non-fatal warning (used by 404-safe loaders for skipped seasons)."""
    import warnings

    warnings.warn(msg, stacklevel=2)


def _fetch_release_parquet(url: str) -> pl.DataFrame:
    """Read a release parquet, RAISING when the asset is missing.

    The read stays a direct Arrow fetch, and the bytes deliberately do NOT go
    through the HTTP gateway. Arrow overlaps the fetch with decoding and range-reads
    column chunks; buffering the asset first serializes the two and holds an extra copy
    of the compressed file. Measured on the 59 MB ``play_by_play_2025.parquet`` (best of
    3, one read per process, identical imports): direct 1010 MB / 6.0s versus buffered
    1081 MB / 10.5s -- +7% peak RSS and +75% wall. Routing through a temp file or a
    zero-copy ``pa.py_buffer`` was no better. See issue #397.

    What DOES go through the gateway is the *classification* of a failure. When the
    direct read fails we ask ``download`` what the server actually said instead of
    pattern-matching the reader's exception message:

    * a 404 raises :class:`~sportsdataverse.errors.NoDataError` -- the asset is
      genuinely absent;
    * any other non-200 -- notably a 403 or a rate limit that outlived the retry
      budget -- raises :class:`~sportsdataverse.errors.AssetFetchError`, so a fetch
      that FAILED is never confused with an asset that is ABSENT;
    * a reachable, readable asset means the failure was a genuine parse/schema error,
      which is re-raised untouched.

    The extra round trip happens only on the failure path and costs ~0.1s.

    R-producer assets can carry an ``arrow.r.vctrs`` field-extension whose metadata is
    raw RDS bytes (not UTF-8) -- e.g. a ``glue`` character column. polars' arrow-FFI
    import panics on that metadata (``pyo3 PanicException``, a ``BaseException`` that
    an ``except Exception`` handler never sees), so on panic we re-read via pyarrow
    with all field/schema metadata stripped; the storage types are plain, so this is
    lossless.

    Use :func:`_read_release_parquet` instead when a missing season should be skipped
    rather than raised.
    """
    from sportsdataverse.errors import AssetFetchError

    try:
        return pl.read_parquet(url, use_pyarrow=True)
    except Exception as e:  # noqa: BLE001 -- classified below, by the server not the message
        original: BaseException = e
    except BaseException as e:  # pyo3 PanicException does not subclass Exception
        if type(e).__name__ != "PanicException":
            raise  # never swallow KeyboardInterrupt / SystemExit into an HTTP call
        return _read_parquet_stripped_metadata(url)

    resp = download(url)  # raises NoDataError on a 404 -- the "absent" signal

    status = getattr(resp, "status_code", None)
    if status is not None and status != 200:
        raise AssetFetchError(f"release asset fetch failed with HTTP {status}: {url}") from original
    raise original


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

    from sportsdataverse.errors import AssetFetchError

    resp = download(url)
    status = getattr(resp, "status_code", None)
    if status is not None and status != 200:
        raise AssetFetchError(f"release asset fetch failed with HTTP {status}: {url}")
    tbl = pq.read_table(io.BytesIO(resp.content))
    plain = pa.schema([pa.field(f.name, f.type) for f in tbl.schema])
    out = pl.from_arrow(tbl.cast(plain).replace_schema_metadata(None))
    assert isinstance(out, pl.DataFrame)
    return out


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
