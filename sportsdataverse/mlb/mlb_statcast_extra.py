"""Hand-written Statcast wrappers that need logic beyond a passthrough — the
25,000-row search has to be date-chunked + truncation-checked."""

from __future__ import annotations
import warnings
from datetime import date, timedelta
from typing import List, Tuple, Union

import polars as pl

from sportsdataverse.dl_utils import download
from sportsdataverse.mlb.mlb_statcast_parsers import _csv_to_frame

_SEARCH_URL = "https://baseballsavant.mlb.com/statcast_search/csv"


def _date_chunks(start: str, end: str, days: int = 7) -> List[Tuple[str, str]]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    out: List[Tuple[str, str]] = []
    cur = s
    while cur <= e:
        chunk_end = min(cur + timedelta(days=days - 1), e)
        out.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return out


def _fetch_chunk(gt: str, lt: str, player_type: str, filters: dict) -> pl.DataFrame:
    params = {"all": "true", "type": "details", "player_type": player_type, "game_date_gt": gt, "game_date_lt": lt}
    params.update(filters)
    resp = download(_SEARCH_URL, params=params)
    text = getattr(resp, "text", resp if isinstance(resp, str) else "")
    return _csv_to_frame(text)


def mlb_statcast_search(
    start_dt: str,
    end_dt: str,
    *,
    player_type: str = "batter",
    chunk_days: int = 7,
    return_as_pandas: bool = False,
    **filters,
) -> "Union[pl.DataFrame, object]":
    frames: list[pl.DataFrame] = []
    for gt, lt in _date_chunks(start_dt, end_dt, days=chunk_days):
        df = _fetch_chunk(gt, lt, player_type, filters)
        if df.height >= 25000 and chunk_days > 1:
            # truncated -> recurse on this sub-range with a smaller window
            df = mlb_statcast_search(gt, lt, player_type=player_type, chunk_days=max(1, chunk_days // 2), **filters)
        elif df.height >= 25000:
            warnings.warn(
                f"statcast_search: {gt}..{lt} hit the 25,000-row Savant cap at the "
                f"1-day floor; results for that day may be truncated.",
                stacklevel=2,
            )
        frames.append(df)
    frames = [f for f in frames if f.height]
    out = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if return_as_pandas:
        return out.to_pandas()
    return out
