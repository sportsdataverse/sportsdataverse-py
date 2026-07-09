"""Parser for the generated ``sports247_site_pages`` wrappers (247sports.com
front-end page models).

Every ``247sports.com/*.json`` route resolves to one of two JSON shapes — a
single flat object (the detail routes: ``Institution/{key}``, ``Player/{key}``,
``Coach/{key}``, ...) or a bare array of such objects (the list/feed routes:
``Season/{season}/Recruits``, ``Institution``, ``League/{id}/DraftPicks``, ...)
— both handled by :func:`parse_sports247_site_page`. Three captured gotchas are
baked in:

* **String-numeric fields.** The page models serialize numeric measures as JSON
  *strings* (``"Latitude": "0.000000"``, ``"CompositeRating": "0.9421"``,
  ``"OverallRank": "12"``). Each ``Utf8`` column is trial-cast to ``Int64`` then
  ``Float64`` and the cast is kept only when it introduces **no new nulls** (every
  non-null original value parsed) — so genuine strings (``Name``, ``Height="6-5"``,
  ``Rankable="True"``) stay ``Utf8`` while true numerics land as real dtypes.
* **Bare-integer foreign keys.** Nested entities are *not* inlined — they arrive
  as bare integer FK columns (``"Location": 32605`` -> traverse via
  ``/Institution/{Location}/Location.json``). The parser **surfaces** the FK
  columns (Int64 from ``json_normalize``); it does not traverse them (no graph
  resolver — walk each FK through its own ``.json`` sub-route yourself).
* **Inlined object stubs.** The few routes that *do* embed a sub-object
  (``Recruit.Player``, ``Player.Hometown``, ``TimelineEvent.Author``) flatten
  under ``json_normalize(sep="_")`` to ``player_*`` / ``hometown_*`` / ``author_*``
  leaf columns.

Package parser contract throughout: polars by default, pandas via
``return_as_pandas=True``, zero-row frame on empty / malformed payloads,
snake-cased columns via :func:`sportsdataverse.dl_utils.underscore`.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_sports247_site_page"]


def _extract_rows(raw: Any) -> List[Dict[str, Any]]:
    """Normalize any site-page payload to a list of row dicts.

    ``list`` -> as-is; a single ``dict`` (the detail routes) -> ``[raw]``;
    anything else (``None`` / scalar / str) -> ``[]``.
    """
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict) and r]
    if isinstance(raw, dict):
        return [raw] if raw else []
    return []


def _cast_numeric_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Cast string-numeric columns to Int64/Float64 when lossless.

    For each ``Utf8`` column, trial ``Int64`` then ``Float64``; keep the cast
    only when it adds no new nulls (every non-null original value parsed). FK/PK
    columns already arrive Int64 from ``json_normalize`` and are untouched.
    """
    casts: List[pl.Expr] = []
    for col, dtype in df.schema.items():
        if dtype != pl.Utf8:
            continue
        s = df[col]
        non_null = s.len() - s.null_count()
        if non_null == 0:
            continue  # all-null: nothing to widen, leave as Utf8
        for target in (pl.Int64, pl.Float64):
            trial = s.cast(target, strict=False)
            if trial.null_count() == s.null_count():  # no NEW nulls introduced
                casts.append(pl.col(col).cast(target, strict=False))
                break
    return df.with_columns(casts) if casts else df


def parse_sports247_site_page(
    raw: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse any 247sports.com site-page ``*.json`` payload into a tidy frame.

    Flattens the object / array payload with ``pandas.json_normalize(sep="_")``
    (turning inline sub-objects ``Recruit.Player`` / ``Player.Hometown`` /
    ``TimelineEvent.Author`` into ``player_*`` / ``hometown_*`` / ``author_*``
    columns while leaving bare integer FKs as scalar int columns), snake-cases
    the columns, then losslessly casts string-numeric columns to real dtypes.

    Nested entities are surfaced as **bare integer foreign-key columns**, not
    traversed — walk each FK through its own ``.json`` sub-route (e.g. an
    ``Institution`` row's ``location`` FK via ``Institution/{location}/Location.json``).

    Args:
        raw: any site-page payload — a single detail object or an array of them
            (``institution``, ``recruits_season``, ``draft_picks``, ...).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per entity; zero-row frame when the payload is empty or
        malformed.

    Example:
        Quick start::

            from sportsdataverse.cfb import sports247_site_pages_institution
            df = sports247_site_pages_institution(key=24099)
            print(df.shape)

        Traverse a surfaced foreign key (no auto-resolver)::

            from sportsdataverse.cfb import sports247_site_pages_institution_location
            loc = sports247_site_pages_institution_location(key=df["location"][0])

    See Also:
        * `recruitR`_ -- college recruiting data in R (CFBD-backed).

    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    rows = _extract_rows(raw)
    if not rows:
        df = pl.DataFrame()
        return df.to_pandas() if return_as_pandas else df
    flat = pd.json_normalize(rows, sep="_")
    flat.columns = [underscore(str(c)) for c in flat.columns]
    for c in flat.columns:
        if flat[c].dtype == object:
            flat[c] = flat[c].map(lambda v: v if v is None or isinstance(v, str) else str(v))
    df = _cast_numeric_strings(pl.from_pandas(flat))
    return df.to_pandas() if return_as_pandas else df
