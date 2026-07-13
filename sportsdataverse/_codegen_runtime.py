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
    """
    try:
        return pl.read_parquet(url, use_pyarrow=True)
    except Exception as e:  # noqa: BLE001 -- classify fetch/parse failures
        msg = str(e).lower()
        if any(tok in msg for tok in ("404", "not found", "no such")):
            return None
        raise


def _read_release_csv(url: str) -> Optional[pl.DataFrame]:
    """Read a release CSV; return ``None`` on 404 / missing asset.

    The CSV counterpart of :func:`_read_release_parquet`, for release tags whose
    producer publishes an asset in csv/rds but not parquet. Same narrow
    missing-asset classification, same re-raise-everything-else contract.
    """
    try:
        return pl.read_csv(url)
    except Exception as e:  # noqa: BLE001 -- classify fetch/parse failures
        msg = str(e).lower()
        if any(tok in msg for tok in ("404", "not found", "no such")):
            return None
        raise


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
