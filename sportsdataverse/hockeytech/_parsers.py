"""Pure HockeyTech parsers: parsed JSON (dict/list) -> snake_cased polars frame.

No network here -- tests drive these from captured fixtures. Every parser
tolerates empty/None payloads by returning a zero-row frame.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(str(c)).replace(".", "_").replace("__", "_") for c in df.columns]
    return df


def _to_frame(records: List[Dict[str, Any]], return_as_pandas: bool) -> Any:
    pdf = pd.json_normalize(records or [], sep="_")
    pdf = _snake_columns(pdf)
    if return_as_pandas:
        return pdf
    return pl.from_pandas(pdf) if len(pdf) else pl.DataFrame()


def _sitekit(payload: Any, key: str) -> Any:
    return ((payload or {}).get("SiteKit", {}) or {}).get(key)


def _derive_season_year(name: str) -> Optional[int]:
    m = re.search(r"(\d{4})-(\d{2})", name or "")
    if m:
        return int(m.group(1)[:2]) * 100 + int(m.group(2))
    m2 = re.search(r"(\d{4})", name or "")
    return int(m2.group(1)) if m2 else None


def _game_type_label(name: str) -> str:
    n = (name or "").lower()
    if re.search(r"pre[- ]?season", n):
        return "preseason"
    if re.search(r"playoff|post", n):
        return "playoffs"
    return "regular"


def parse_seasons(payload: Any, return_as_pandas: bool = False) -> Any:
    """Parse a HockeyTech ``modulekit/seasons`` JSON payload into a flat frame.

    Returns a :class:`polars.DataFrame` by default; pass ``return_as_pandas=True``
    for a :class:`pandas.DataFrame`. An empty/None payload returns a zero-row
    frame of the same type, never raises.
    """
    raw = _sitekit(payload, "Seasons") or []
    rows = []
    for s in raw:
        name = s.get("season_name")
        rows.append(
            {
                "season_id": int(s.get("season_id")) if s.get("season_id") else None,
                "season_name": name,
                "season_short": s.get("shortname"),
                "career": s.get("career", "0"),
                "playoff": s.get("playoff", "0"),
                "start_date": s.get("start_date"),
                "end_date": s.get("end_date"),
                "season_yr": _derive_season_year(name),
                "game_type_label": _game_type_label(name),
            }
        )
    return _to_frame(rows, return_as_pandas)
