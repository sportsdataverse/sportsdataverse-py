"""Minimal RDS (version 2, XDR) writer for polars DataFrames.

Implements the subset of R's serialization format (``serialize.c``) needed to
``saveRDS()``-equivalently persist a data.frame of atomic columns: integer,
double, logical, character, Date, and POSIXct vectors with NA fidelity, plus
arbitrary string / POSIXct attributes on the frame (``sportsdataverse_type`` /
``sportsdataverse_timestamp`` parity with the sportsdataversedata R package).

Why hand-rolled: ``pyreadr`` was deliberately dropped from this project
(sdist build pain, and librdata cannot write attributes) and ``rdata``'s
writer cannot convert pandas-3-era string/boolean arrays. The format itself
is small and stable; the output is validated against a live ``readRDS()``
oracle in ``tests/release/``.

Scope limits (raise ``ValueError``): List / Struct / nested columns. Int64
values outside int32 range are written as doubles (R has no 64-bit integer).
"""

from __future__ import annotations

import gzip
import os
import secrets
import struct
from contextlib import nullcontext
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Protocol, Union

import numpy as np
import polars as pl

__all__ = ["write_rds"]

# SEXP type codes + serialization bit masks (R serialize.c, format version 2)
_SYMSXP = 1
_LISTSXP = 2
_CHARSXP = 9
_LGLSXP = 10
_INTSXP = 13
_REALSXP = 14
_STRSXP = 16
_VECSXP = 19
_NILVALUE = 254

_UTF8_LEVS = 8  # CHARSXP gp UTF8_MASK
_ASCII_LEVS = 64  # CHARSXP gp ASCII_MASK (R flags pure-ASCII strings this way)
_IS_OBJECT = 1 << 8
_HAS_ATTR = 1 << 9
_HAS_TAG = 1 << 10

_NA_INT = -0x80000000
_NA_REAL_BYTES = b"\x7f\xf0\x00\x00\x00\x00\x07\xa2"
_INT32_MAX = 2**31 - 1

# writer R version stamped in the header (informational); min-required 2.3.0
_R_VERSION = (4 << 16) | (5 << 8) | 3
_R_MIN_VERSION = (2 << 16) | (3 << 8) | 0

# O_EXCL retries when claiming a temp name; a collision needs a 64-bit token clash
_TEMP_NAME_ATTEMPTS = 8

# runtime-evaluated alias: `str | datetime` needs py3.10+, floor is 3.9
_AttrValue = Union[str, datetime]
_ColumnWriter = Callable[[], None]


def _create_temp_sibling(out_path: Path) -> tuple[int, Path]:
    """Create a uniquely-named sibling of ``out_path``; return its fd and path.

    ``tempfile.mkstemp`` would do this but hardcodes mode 0600, and the rename that
    publishes the file preserves that mode -- so every output would silently become
    owner-only. Creating with 0666 lets the kernel subtract the caller's umask, which
    is exactly what ``open(path, "wb")`` did before the atomic-write change.

    Reading the umask in-process instead (``os.umask(0)`` then restore) would mutate
    process-global state and race any other thread creating a file in that window.

    Raises:
        OSError: If no unique name could be claimed.
    """
    for _ in range(_TEMP_NAME_ATTEMPTS):
        candidate = out_path.with_name(f".{out_path.name}.{secrets.token_hex(8)}.partial")
        try:
            fd = os.open(candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
        except FileExistsError:
            continue
        return fd, candidate
    raise OSError(f"could not claim a unique temp name next to {out_path}")


class _ByteSink(Protocol):
    """Any write-only binary sink: ``GzipFile``, ``BufferedWriter``, ``BytesIO``.

    Structural rather than ``IO[bytes]`` because ``gzip.GzipFile`` does not
    satisfy the full ``IO`` protocol -- the writer only ever calls ``write``.
    """

    def write(self, data: bytes, /) -> int: ...


class _RdsWriter:
    """Serializes one R object as an XDR byte stream.

    With a ``sink`` the stream is written straight through and nothing is
    retained; without one it accumulates in memory and :meth:`payload` returns it.

    Streaming is not an optimization here, it is what makes season-sized frames
    writable at all. Buffering costs roughly 10x the frame: every value becomes
    its own small ``bytes`` object (each carrying ~33 bytes of object overhead),
    and ``payload()`` then joins them into a second, contiguous copy of the whole
    stream. Serializing NHL's 1.1M x 94 play-by-play frame that way needed ~6.8GB
    on top of the frame itself and was OOM-killed mid-write.
    """

    # Bytes to gather before handing the sink a single write. Serialization emits
    # a few bytes at a time, so writing each one straight through means millions
    # of compressor calls; batching them cuts that by ~5 orders of magnitude while
    # keeping memory bounded by this constant rather than by the frame.
    _SINK_BUFFER_BYTES = 4 * 1024 * 1024

    def __init__(self, sink: _ByteSink | None = None) -> None:
        self._sink = sink
        self._chunks: list[bytes] = []
        self._pending: list[bytes] = []
        self._pending_len = 0
        # R's serializer refs repeated symbols instead of re-serializing them
        # (serialize.c HashAdd/OutRefIndex); 1-based, first-appearance order
        self._sym_refs: dict[str, int] = {}

    def payload(self) -> bytes:
        """The buffered stream. Only valid on a writer constructed without a sink.

        Raises:
            RuntimeError: If a sink was given -- the bytes went to it, so returning
                the (empty) buffer would look like a successfully serialized object.
        """
        if self._sink is not None:
            raise RuntimeError("payload() is unavailable on a streaming writer")
        return b"".join(self._chunks)

    def flush(self) -> None:
        """Hand any buffered bytes to the sink. Must be called before closing it."""
        if self._sink is None or not self._pending:
            return
        self._sink.write(b"".join(self._pending))
        self._pending.clear()
        self._pending_len = 0

    def _raw(self, data: bytes) -> None:
        if self._sink is None:
            self._chunks.append(data)
            return
        self._pending.append(data)
        self._pending_len += len(data)
        if self._pending_len >= self._SINK_BUFFER_BYTES:
            self.flush()

    def _int(self, value: int) -> None:
        self._raw(struct.pack(">i", value))

    def _flags(
        self,
        sexp_type: int,
        *,
        levs: int = 0,
        is_object: bool = False,
        has_attr: bool = False,
        has_tag: bool = False,
    ) -> None:
        flags = sexp_type | (levs << 12)
        if is_object:
            flags |= _IS_OBJECT
        if has_attr:
            flags |= _HAS_ATTR
        if has_tag:
            flags |= _HAS_TAG
        self._int(flags)

    def header(self) -> None:
        self._raw(b"X\n")
        self._int(2)
        self._int(_R_VERSION)
        self._int(_R_MIN_VERSION)

    # -- atomic vectors ------------------------------------------------------

    def charsxp(self, value: str | None) -> None:
        if value is None:
            # NA_STRING is a CHARSXP with length -1 and no encoding bits
            self._flags(_CHARSXP)
            self._int(-1)
            return
        encoded = value.encode("utf-8")
        levs = _ASCII_LEVS if value.isascii() else _UTF8_LEVS
        self._flags(_CHARSXP, levs=levs)
        self._int(len(encoded))
        self._raw(encoded)

    def strsxp(
        self,
        values: Iterable[str | None],
        *,
        length: int,
        attributes: list[tuple[str, _ColumnWriter]] | None = None,
        is_object: bool = False,
    ) -> None:
        self._flags(_STRSXP, is_object=is_object, has_attr=bool(attributes))
        self._int(length)
        for value in values:
            self.charsxp(value)
        if attributes:
            self.attr_pairlist(attributes)

    def intsxp_raw(
        self,
        data: bytes,
        *,
        length: int,
        sexp_type: int = _INTSXP,
        attributes: list[tuple[str, _ColumnWriter]] | None = None,
        is_object: bool = False,
    ) -> None:
        self._flags(sexp_type, is_object=is_object, has_attr=bool(attributes))
        self._int(length)
        self._raw(data)
        if attributes:
            self.attr_pairlist(attributes)

    def realsxp(
        self,
        values: np.ndarray,
        null_mask: np.ndarray,
        *,
        attributes: list[tuple[str, _ColumnWriter]] | None = None,
        is_object: bool = False,
    ) -> None:
        self._flags(_REALSXP, is_object=is_object, has_attr=bool(attributes))
        self._int(len(values))
        buf = bytearray(np.ascontiguousarray(values, dtype=">f8").tobytes())
        for idx in np.flatnonzero(null_mask):
            offset = int(idx) * 8
            buf[offset : offset + 8] = _NA_REAL_BYTES
        self._raw(bytes(buf))
        if attributes:
            self.attr_pairlist(attributes)

    # -- attributes ----------------------------------------------------------

    def symbol(self, name: str) -> None:
        """SYMSXP on first use; packed REFSXP (255 | index << 8) after."""
        ref = self._sym_refs.get(name)
        if ref is not None:
            if ref <= 0x7FFFFF:  # MAX_PACKED_INDEX
                self._int((ref << 8) | 255)
            else:  # pragma: no cover - needs 8M+ distinct symbols
                self._int(255)
                self._int(ref)
            return
        self._sym_refs[name] = len(self._sym_refs) + 1
        self._flags(_SYMSXP)
        self.charsxp(name)

    def attr_pairlist(self, attributes: list[tuple[str, _ColumnWriter]]) -> None:
        """LISTSXP chain of (tag symbol, value); terminated by NILVALUE."""
        for name, write_value in attributes:
            self._flags(_LISTSXP, has_tag=True)
            self.symbol(name)
            write_value()
        self._int(_NILVALUE)

    def scalar_string(self, value: str) -> None:
        self.strsxp([value], length=1)

    def class_attr(self, classes: list[str]) -> tuple[str, _ColumnWriter]:
        return ("class", lambda: self.strsxp(list(classes), length=len(classes)))

    def posixct_scalar(self, value: datetime) -> None:
        if value.tzinfo is None:
            epoch = value.replace(tzinfo=timezone.utc).timestamp()
        else:
            epoch = value.timestamp()
        self.realsxp(
            np.array([epoch]),
            np.array([False]),
            attributes=[self.class_attr(["POSIXct", "POSIXt"])],
            is_object=True,
        )


def _int_column_bytes(series: pl.Series) -> bytes:
    filled = series.cast(pl.Int64).fill_null(_NA_INT)
    return filled.to_numpy().astype(">i4").tobytes()


def _column_writer(writer: _RdsWriter, series: pl.Series) -> _ColumnWriter:
    """Return a thunk that serializes one polars column as an R vector."""
    dtype = series.dtype
    length = series.len()

    if dtype in (pl.Categorical, pl.Enum):
        series = series.cast(pl.Utf8)
        dtype = pl.Utf8

    if dtype == pl.Boolean:
        as_int = series.cast(pl.Int32)
        data = _int_column_bytes(as_int)
        return lambda: writer.intsxp_raw(data, length=length, sexp_type=_LGLSXP)

    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.UInt8, pl.UInt16):
        data = _int_column_bytes(series)
        return lambda: writer.intsxp_raw(data, length=length)

    if dtype in (pl.Int64, pl.UInt32, pl.UInt64):
        non_null = series.drop_nulls()
        fits_int32 = non_null.is_empty() or (int(non_null.min()) >= -_INT32_MAX and int(non_null.max()) <= _INT32_MAX)
        if fits_int32:
            data = _int_column_bytes(series)
            return lambda: writer.intsxp_raw(data, length=length)
        # R has no 64-bit integer; out-of-range ids become doubles like R would
        values = series.cast(pl.Float64).fill_null(0.0).to_numpy()
        mask = series.is_null().to_numpy()
        return lambda: writer.realsxp(values, mask)

    if dtype in (pl.Float32, pl.Float64):
        values = series.cast(pl.Float64).fill_null(0.0).to_numpy()
        mask = series.is_null().to_numpy()
        return lambda: writer.realsxp(values, mask)

    if dtype == pl.Utf8:
        items = series.to_list()
        return lambda: writer.strsxp(items, length=length)

    if dtype == pl.Date:
        # R Date: days since 1970-01-01 as double, class "Date"
        values = series.to_physical().cast(pl.Float64).fill_null(0.0).to_numpy()
        mask = series.is_null().to_numpy()
        return lambda: writer.realsxp(values, mask, attributes=[writer.class_attr(["Date"])], is_object=True)

    if isinstance(dtype, pl.Datetime):
        scale = {"ms": 1e3, "us": 1e6, "ns": 1e9}[dtype.time_unit]
        values = (series.to_physical().cast(pl.Float64).fill_null(0.0) / scale).to_numpy()
        mask = series.is_null().to_numpy()
        attributes = [writer.class_attr(["POSIXct", "POSIXt"])]
        if dtype.time_zone is not None:
            tz = str(dtype.time_zone)
            attributes.append(("tzone", lambda: writer.scalar_string(tz)))
        return lambda: writer.realsxp(values, mask, attributes=attributes, is_object=True)

    if dtype == pl.Null:
        # all-null column: R reads it most naturally as logical NA
        data = struct.pack(">i", _NA_INT) * length
        return lambda: writer.intsxp_raw(data, length=length, sexp_type=_LGLSXP)

    raise ValueError(
        f"Column {series.name!r} has dtype {dtype}, which has no R atomic-vector "
        f"equivalent supported by the RDS writer (nested/list columns are out "
        f"of scope)."
    )


def write_rds(
    df: pl.DataFrame,
    path: str | Path,
    *,
    attributes: Mapping[str, _AttrValue] | None = None,
    cls: Sequence[str] | None = None,
    compress: bool = True,
) -> None:
    """Write ``df`` as an R data.frame in RDS (version 2) format.

    Args:
        df: Frame with atomic-typed columns (int / float / bool / str /
            Categorical / Date / Datetime).
        path: Destination ``.rds`` path.
        attributes: Extra attributes to attach to the data.frame — ``str``
            values become length-1 character vectors, ``datetime`` values
            become ``POSIXct`` scalars (matching R ``attr(df, ...) <-``).
        cls: The frame's S3 ``class`` vector. Defaults to ``["data.frame"]``.
            Pass the league's own chain to match what the R producers stamp,
            e.g. ``["hoopR_data", "tbl_df", "tbl", "data.table", "data.frame"]``
            (``hoopR:::make_hoopR_data``) — the class is load-bearing, not
            cosmetic: hoopR/wehoop register S3 methods on it (``print.hoopR_data``),
            so a released rds without it prints differently for every user.
        compress: Gzip the stream like ``saveRDS(compress = TRUE)``.

    Raises:
        ValueError: On column dtypes with no R atomic-vector equivalent, or a
            ``cls`` that does not end in ``"data.frame"``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._rds import write_rds
            write_rds(pl.DataFrame({"season": [2024]}), "frame.rds")

        Stamp a league class + attributes the way the R producers do::

            from datetime import datetime, timezone
            write_rds(
                df,
                "player_core_2015.rds",
                cls=["hoopR_data", "tbl_df", "tbl", "data.table", "data.frame"],
                attributes={
                    "hoopR_type": "ESPN NBA player_core from hoopR data repository",
                    "hoopR_timestamp": datetime.now(timezone.utc),
                },
            )
    """
    if cls is not None:
        cls = list(cls)
        if not cls:
            raise ValueError("cls must be a non-empty class vector")
        # R only dispatches data.frame methods when data.frame is in the chain,
        # and it must be last -- an rds whose class omits it stops behaving like
        # a data.frame on read, which is far worse than a wrong print method.
        if cls[-1] != "data.frame":
            raise ValueError(f"cls must end with 'data.frame'; got {cls!r}")
    out_path = Path(path)

    # Serialize straight into the file rather than buffering the stream and
    # writing it at the end: a season-sized frame costs ~10x its own size to
    # buffer (see _RdsWriter) and gets OOM-killed. Peak memory is now flat in the
    # size of the output.
    #
    # Streaming does introduce a failure mode the buffered version could not
    # have: a mid-write error leaves a truncated file where previously no file
    # was created at all. A truncated .rds is worse than a missing one -- it
    # reads as a corrupt object and can be published as if it were complete --
    # so write to a temp sibling and rename on success, which is atomic within a
    # filesystem and never exposes a partial file at `path`. The temp name is unique
    # per writer: two processes writing the same `path` (two compiles sharing an
    # out_dir) would otherwise interleave their bytes into one fixed temp file.
    # Opened O_EXCL with mode 0666 rather than via tempfile.mkstemp: mkstemp forces
    # 0600, and rename preserves the mode, so the output would silently become
    # owner-only where opening the destination directly gave the usual 0644. Passing
    # 0666 lets the kernel subtract the caller's umask exactly as open() would --
    # reading the umask in-process instead would mean os.umask(), which mutates
    # process-global state and races any other thread creating a file meanwhile.
    fd, tmp_path = _create_temp_sibling(out_path)
    try:
        with os.fdopen(fd, "wb") as raw:
            # gzip stamps the output filename into its header, so name it for the
            # final path -- opening the temp path directly would embed the temp name
            # and make the bytes differ from a plain gzip.open(path). Built only when
            # compressing: GzipFile writes its header at construction.
            sink_ctx = gzip.GzipFile(filename=out_path.name, mode="wb", fileobj=raw) if compress else nullcontext(raw)
            with sink_ctx as f:
                writer = _RdsWriter(sink=f)
                writer.header()

                column_writers = [_column_writer(writer, df[name]) for name in df.columns]

                # Attribute order is mutation-history-dependent in R; this matches a
                # data.frame that went through `$<-` column assignment (as in
                # sportsdataverse_save's season/week coercion): names, row.names, class.
                # Readers accept any order; the byte-golden fixture pins this one.
                frame_attributes: list[tuple[str, _ColumnWriter]] = [
                    ("names", lambda: writer.strsxp(list(df.columns), length=df.width)),
                    (
                        "row.names",
                        # compact internal form: c(NA_integer_, -nrow)
                        lambda: writer.intsxp_raw(struct.pack(">ii", _NA_INT, -df.height), length=2),
                    ),
                    writer.class_attr(cls if cls is not None else ["data.frame"]),
                ]
                for attr_name, attr_value in (attributes or {}).items():
                    if isinstance(attr_value, datetime):
                        frame_attributes.append((attr_name, partial(writer.posixct_scalar, attr_value)))
                    else:
                        frame_attributes.append((attr_name, partial(writer.scalar_string, attr_value)))

                writer._flags(_VECSXP, is_object=True, has_attr=True)
                writer._int(df.width)
                for write_column in column_writers:
                    write_column()
                writer.attr_pairlist(frame_attributes)
                writer.flush()  # the tail of the stream is still buffered
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    tmp_path.replace(out_path)
