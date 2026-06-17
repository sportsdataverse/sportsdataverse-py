"""Parsers for Baseball Savant / Statcast payloads. Universal sdv-py parser
contract: polars by default, pandas via return_as_pandas, snake-case columns,
zero-row frame on empty/malformed input."""

from __future__ import annotations

import json
import re
from io import StringIO
from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(str(c)).replace(".", "_") for c in df.columns]
    return df


def _empty_frame(return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    return pd.DataFrame() if return_as_pandas else pl.DataFrame()


def _to_output(df: pd.DataFrame, return_as_pandas: bool) -> pl.DataFrame | pd.DataFrame:
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        # polars rejected a mixed/list-valued column — stringify object columns so the frame still converts (sdv-py parser convention).
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _csv_to_frame(text: str, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    if not text or not text.strip():
        return _empty_frame(return_as_pandas)
    try:
        df = pd.read_csv(StringIO(text))
    except Exception:
        return _empty_frame(return_as_pandas)
    if df.empty:
        return _empty_frame(return_as_pandas)
    return _to_output(_snake_columns(df), return_as_pandas)


def _html_script_json(html: str, var_name: str) -> Dict:
    if not html:
        return {}
    m = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*", html)
    if not m:
        return {}
    try:
        obj, _ = json.JSONDecoder().raw_decode(html, m.end())
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
