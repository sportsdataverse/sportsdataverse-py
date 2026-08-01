"""Runtime helpers for codegen-emitted wrappers (HTTP + value coercion).

Hand-written and stable; generated modules import ``_get`` / ``_csv`` from here so
the ~1,000 generated functions share one tested HTTP path instead of inlining it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import polars as pl

from sportsdataverse.dl_utils import download
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


def _read_release_parquet(url: str) -> Optional[pl.DataFrame]:
    """Read a release parquet; return ``None`` on 404 / missing asset (404-safe loaders).

    Re-raises anything that isn't a missing-asset error so genuine parse/schema bugs
    aren't silently swallowed. The token list is deliberately narrow: ``403/forbidden``
    (rate-limit / auth) and generic ``could not`` (parse/type failures) are NOT treated
    as missing assets, so they surface instead of being masked as "no data".

    R-producer assets can carry an ``arrow.r.vctrs`` field-extension whose metadata is
    raw RDS bytes (not UTF-8) — e.g. a ``glue`` character column. polars' arrow-FFI
    import panics on that metadata (``pyo3 PanicException``, a ``BaseException`` the
    404 classifier never sees), so on panic we re-read via pyarrow with all
    field/schema metadata stripped; the storage types are plain, so this is lossless.
    """
    try:
        return pl.read_parquet(url, use_pyarrow=True)
    except Exception as e:  # noqa: BLE001 -- classify fetch/parse failures
        msg = str(e).lower()
        if any(tok in msg for tok in ("404", "not found", "no such")):
            return None
        raise
    except BaseException as e:  # pyo3 PanicException does not subclass Exception
        if type(e).__name__ != "PanicException":
            raise
        return _read_parquet_stripped_metadata(url)


def _read_parquet_stripped_metadata(url: str) -> pl.DataFrame:
    """Fetch a parquet and load it with every field/schema metadata entry dropped."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    resp = download(url)
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
