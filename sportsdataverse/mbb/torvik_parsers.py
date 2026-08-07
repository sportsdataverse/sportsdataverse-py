"""Parsers for the Bart Torvik (T-Rank) data-file wrappers.

One generic CSV parser covers every self-describing Torvik file
(``{year}_team_results.csv``, ``{year}_fffinal.csv``, and their women's
``/ncaaw`` mirrors): the header row is cleaned to snake_case (janitor-style:
``%`` -> ``_percent``, leading digits get an ``x`` prefix, duplicates get a
``_2``/``_3`` suffix) and the rows are type-inferred. The output keeps the
``team`` / ``conf`` columns the basketball crosswalks consume.
"""

from __future__ import annotations

import io
import re
from typing import TYPE_CHECKING, List, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_torvik_csv"]


def _clean_col(name: str) -> str:
    """janitor::make_clean_names-style cleaner for a single Torvik CSV header."""
    s = str(name).strip().replace("%", " percent ")
    # fold a digit-glued capital ("3P", "2p%D") to lowercase so it stays one
    # token ("x3p_percent"), instead of splitting to "x3_p_percent"
    s = re.sub(r"(?<=\d)([A-Z])(?=[^a-z]|$)", lambda m: m.group(1).lower(), s)
    s = underscore(s)
    s = re.sub(r"[^0-9a-z]+", "_", s.lower()).strip("_")
    s = re.sub(r"_+", "_", s) or "col"
    if s[0].isdigit():
        s = "x" + s
    return s


def _clean_cols(names: List[str]) -> List[str]:
    """Clean a header row and de-duplicate repeats with ``_2``/``_3`` suffixes."""
    out: List[str] = []
    seen: dict = {}
    for n in names:
        c = _clean_col(n)
        seen[c] = seen.get(c, 0) + 1
        out.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
    return out


def parse_torvik_csv(payload: object, return_as_pandas: bool = False) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse a Torvik CSV-with-header payload into a tidy frame.

    Args:
        payload: CSV text returned by a barttorvik.com data-file endpoint
            (e.g. ``{year}_team_results.csv`` or ``{year}_fffinal.csv``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A polars (or pandas) DataFrame, one row per team, with snake-cased,
        de-duplicated column names; zero rows on empty/malformed input.

    Example:
        Quick start::

            from sportsdataverse.mbb import torvik_ratings
            from sportsdataverse.mbb.torvik_parsers import parse_torvik_csv
            df = parse_torvik_csv(torvik_ratings(year=2025, return_parsed=False))
    """
    text = payload if isinstance(payload, str) else ""
    if not text.strip() or "\n" not in text.strip():
        df = pl.DataFrame()
    else:
        header = next(iter(pl.read_csv(io.StringIO(text), has_header=False, n_rows=1).rows()))
        names = _clean_cols([str(h) if h is not None and str(h).strip() else "unnamed" for h in header])
        df = pl.read_csv(
            io.StringIO(text),
            has_header=False,
            skip_rows=1,
            new_columns=names,
            infer_schema_length=10000,
        )
    if return_as_pandas:
        return df.to_pandas()
    return df
