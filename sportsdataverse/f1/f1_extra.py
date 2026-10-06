"""Hand-written ``f1`` helpers: ``f1_laps``, every lap time of a race, paged over ``offset``.

Lives in ``f1_extra`` (not ``f1_laps``) so the module never shadows the function of the same
name on ``sportsdataverse.f1``.

``/{season}/{round}/laps.json`` answers at most 100 timing rows per call (``limit``
is clamped to 100 and echoed in ``MRData.limit``), while a race carries ~1,100
(20 drivers x ~57 laps): the generated :func:`sportsdataverse.f1.f1.f1_laps_page`
is one page, this loop is the whole race.
"""

from __future__ import annotations

import time
from typing import Any, Union

import pandas as pd
import polars as pl

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.f1.f1 import _PARSER_COLUMNS, f1_laps_page
from sportsdataverse.f1.f1_parsers import parse_f1_mrdata
from sportsdataverse.soccer._frames import as_output

__all__ = ["f1_laps"]

#: The host's page cap; a larger ``limit`` is clamped to this and echoed back.
PAGE_LIMIT = 100
#: Pause between pages, to stay under the 4 requests/second burst limit.
_PAGE_PAUSE_S = 0.25


def f1_laps(
    season: str,
    round: str,  # noqa: A002 -- the Ergast/f1dataR argument name
    *,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Lap times of one race: one row per driver per lap (1996+), all pages.

    Endpoint: ``GET https://api.jolpi.ca/ergast/f1/{season}/{round}/laps.json``,
    requested with ``limit=100`` and ``offset`` advanced page by page until
    ``MRData.total`` timings have been read.

    Args:
        season: Four-digit season year, or ``current``.
        round: Round number within the season, or ``last`` / ``next``.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: Forwarded to the underlying HTTP getter.

    Returns:
        A polars/pandas DataFrame with the ``laps_page`` columns (``season``,
        ``round``, ``race_*``, ``lap_number``, ``driver_id``, ``position``, ``time``),
        concatenated across pages; zero rows (same columns) when the race has no lap
        data (before 1996, or a round not yet run).

    Raises:
        sportsdataverse.errors.NoDataError: the Jolpica API returned 404 (unknown season or round).
        ValueError: The host answered 400 / 422 -- the request is wrong; retrying cannot help.
        AssetFetchError: The fetch failed (a non-2xx answer or a connection failure after retries,
            or an empty or unreadable 200 body), or a page came back empty before ``MRData.total``
            timings were read -- the answer is unknown, not empty.

    Example:
        Quick start::

            from sportsdataverse.f1 import f1_laps

            df = f1_laps(2024, 1)
            print(df.shape)  # (1129, 17) -- 12 requests

        Pipeline next step (one line)::

            import polars as pl

            df.filter(pl.col("driver_id") == "max_verstappen").select("lap_number", "position", "time")

        See Also:
            * `f1dataR`_ - ``load_laps()``, the R twin
            * `FastF1`_ - per-lap timing and telemetry sdv-py does not wrap

        .. _f1dataR: https://scottyd22.github.io/f1dataR/
        .. _FastF1: https://docs.fastf1.dev/

    Notes:
        * Data license CC BY-NC-SA 4.0 (non-commercial, attribution, share-alike): wrap-only,
          never republished as release assets.
        * Rate limit 4 requests/second burst, **500 requests/hour** sustained, per IP and shared
          by every process on the machine. One race is ``ceil(total / 100)`` requests (~12), so a
          24-race season is ~300 of the hourly budget; a multi-season backfill must be paced
          across hours.
    """
    offset = 0
    frames = []
    while True:
        raw = f1_laps_page(season, round, limit=str(PAGE_LIMIT), offset=str(offset), return_parsed=False, **kwargs)
        page = parse_f1_mrdata(raw, columns=_PARSER_COLUMNS["laps_page"])
        meta = (raw or {}).get("MRData") or {}
        total = int(meta.get("total") or 0)
        if page.height == 0 and offset < total:
            # a 200 with an empty table mid-race is a failed fetch, not an empty one
            raise AssetFetchError(
                f"f1_laps({season!r}, {round!r}): empty page at offset {offset} of {total} timings",
            )
        frames.append(page)
        offset += int(meta.get("limit") or PAGE_LIMIT)  # the echoed (clamped) page size
        if offset >= total:
            break
        time.sleep(_PAGE_PAUSE_S)
    df = pl.concat(frames, how="diagonal_relaxed")
    return as_output(df, return_as_pandas=return_as_pandas)
