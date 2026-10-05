"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

import json
from typing import Any, List, Optional
from urllib.parse import urlsplit

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.errors import SeasonNotFoundError  # noqa: F401  (re-export for generated loaders)

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


def _check_status(url: str, status: Any, text: str = "") -> None:
    """Raise unless ``status`` is 2xx, so a failed fetch never reaches a parser as data.

    A 404 raises :class:`~sportsdataverse.errors.NoDataError` (the host answered
    "nothing here"); every other status -- a 401/403/429/5xx that outlived the
    retries, a 400 -- raises :class:`~sportsdataverse.errors.AssetFetchError` naming
    host, path and status. The error classes redact credentials from the message.
    """
    if isinstance(status, int) and 200 <= status < 300:
        return
    if status == 404:
        raise NoDataError(f"{_where(url)} answered HTTP 404")
    raise AssetFetchError(f"{_where(url)} answered HTTP {status}: {(text or '').strip()[:200]}")


def _json_text(url: str, status: Any, text: str) -> Any:
    """Decode a ``(status, text)`` transport answer under the error vocabulary.

    2xx with a JSON body -> the body; 2xx with an empty body (204) -> ``{}``, a
    deliberate "nothing"; 2xx with any other body, or a non-2xx -> raises (see
    :func:`_check_status`).
    """
    _check_status(url, status, text)
    if not (text or "").strip():
        return {}
    try:
        return json.loads(text)
    except ValueError:  # its ``.doc`` is the whole body: never chain it as __context__
        pass
    raise AssetFetchError(f"{_where(url)} answered HTTP {status} with a non-JSON body: {text.strip()[:200]}")


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


def _json_body(resp: Any, url: str) -> Any:
    """Decode a :func:`~sportsdataverse.dl_utils.download` response like :func:`_json_text`."""
    _check_response(resp, url)
    try:
        return resp.json()
    except ValueError:
        pass
    return _json_text(url, getattr(resp, "status_code", 200), getattr(resp, "text", "") or "")


def _get(url: str, params: Optional[dict] = None, **kwargs) -> Any:
    """GET ``url`` as JSON. Strips ``None`` params.

    Raises:
        NoDataError: the host answered 404 (or ESPN 200 with ``code: 404``).
        AssetFetchError: any other non-2xx after retries, or a 2xx non-JSON body.
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    return _json_body(download(url=url, params=clean, **kwargs), url)


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
