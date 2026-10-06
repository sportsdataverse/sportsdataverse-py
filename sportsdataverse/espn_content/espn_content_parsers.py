"""Parser for the generated ``espn_content`` wrappers (content.core.api.espn.com/v1).

Every route answers the same envelope: a ``headlines`` list plus scalar siblings
(``resultsOffset``, ``resultsLimit``, ``resultsCount``). Rows come from
``headlines`` -- the single-story route returns a one-element list, so it parses
to exactly one row with no special case. Nested ``categories``, ``images`` and
``keywords`` are JSON-encoded cells: their shape varies per story and flattening
them would make the column set depend on which headlines came back. The
fixed-key ``links`` object flattens to ``links_api_self_href``-style columns, the
same treatment the Sleeper parser gives a page object's fixed-key children.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, snake_cased columns, ids pinned to ``Utf8``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_espn_content"]

_ID = re.compile(r"(^|_)(id|code)$")


def _as_rows(raw: Any) -> List[Dict[str, Any]]:
    if isinstance(raw, list):
        return [r for r in raw if isinstance(r, dict)]
    if not isinstance(raw, dict):
        return []
    headlines = raw.get("headlines")
    if isinstance(headlines, list):
        return [h for h in headlines if isinstance(h, dict)]
    return [raw] if raw else []


def _encode(value: Any) -> Any:
    return json.dumps(value, default=str) if isinstance(value, (list, dict)) else value


def _to_frame(rows: List[Dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    pdf = pd.json_normalize(rows, sep="_")
    pdf.columns = [underscore(str(c)) for c in pdf.columns]
    for name in pdf.columns:
        if pdf[name].dtype == object:
            pdf[name] = pdf[name].map(_encode)
    df = pl.from_pandas(pdf)
    ids = []
    for name, dtype in df.schema.items():
        if not _ID.search(name) or dtype == pl.String:
            continue
        expr = pl.col(name)
        if dtype.is_float():
            expr = expr.cast(pl.Int64, strict=False)
        ids.append(expr.cast(pl.String).alias(name))
    return df.with_columns(ids) if ids else df


def parse_espn_content(
    raw: Union[Dict[str, Any], List[Any], None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse an ESPN content.core v1 body into a tidy headlines frame.

    Args:
        raw: a decoded content.core body -- the ``{"headlines": [...], ...}``
            envelope every route returns.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per headline, snake_cased, with ``id`` / ``*_id`` columns pinned
        to ``Utf8`` and nested ``categories`` / ``images`` / ``keywords``
        JSON-encoded. A zero-row frame when the payload is ``None``, empty or
        malformed.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.espn_content import espn_content_league_news

            df = espn_content_league_news(sport_slug="football", league_slug="nfl", limit=5)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("id", "headline", "published")

    See Also:
        * `ESPN content.core endpoints`_ -- the captured route reference.

    .. _ESPN content.core endpoints: https://content.core.api.espn.com/v1/sports/news
    """
    df = _to_frame(_as_rows(raw))
    return df.to_pandas() if return_as_pandas else df
