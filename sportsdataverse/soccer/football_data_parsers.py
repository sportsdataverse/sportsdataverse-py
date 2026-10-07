"""Parser for the generated ``football_data`` wrappers (football-data.co.uk).

football-data.co.uk is a static **CSV** archive, not a JSON API, so this parser
takes the raw CSV text that
:func:`sportsdataverse.soccer.football_data_runtime._get` returns and reads it
into a frame. The files are historical results plus closing betting odds: 132
columns for a main-league season (``/mmz4281/{season}/{div}.csv``), 25 for an
'extra' league (``/new/{country}.csv``) and 94 for the fixtures file.

Two source quirks are handled here rather than pushed onto callers:

* **Trailing commas** leave empty header cells, which read as ``""`` /
  ``_duplicated_N`` columns; they carry no data and are dropped.
* **Whole-file schema inference** (``infer_schema_length=None``): odds columns
  are sparse and a short scan types a column from its first rows and then fails
  on a later value.

Column names are snake_cased, so the source's ``HomeTeam`` / ``FTHG`` /
``B365H`` become ``home_team`` / ``fthg`` / ``b365_h``; the glossary for the
abbreviations is ``/notes.txt``, transcribed in
``sdv-internal-refs/football-data-co-uk/football-data-co-uk-returns.md``. The
over/under and Asian-handicap columns keep the source's punctuation
(``B365>2.5`` -> ``b365>2.5``), because the threshold is part of the market name.

Follows the package-wide parser contract: polars by default, pandas via
``return_as_pandas=True``, a zero-row frame (never an exception) on an empty or
malformed payload, snake_cased columns.
"""

from __future__ import annotations

import io
from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

__all__ = ["parse_football_data"]


def _is_blank_name(name: str) -> bool:
    """True for a header cell that carries no name (a trailing-comma artifact)."""
    bare = name.strip()
    return not bare or bare.startswith("_duplicated_") or bare.lower().startswith("unnamed")


def parse_football_data(
    raw: Union[str, bytes, None],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Parse a football-data.co.uk CSV body into a tidy frame.

    Args:
        raw: the raw CSV text (or bytes) of one archive file, as returned by
            :func:`sportsdataverse.soccer.football_data_runtime._get`.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        One row per match (or per upcoming fixture), snake_cased, with the
        source's unnamed trailing columns dropped. A zero-row frame when the
        payload is ``None``, blank or header-only. Text that is not CSV cannot be
        recognised as such -- a one-line HTML body is a valid one-column CSV -- so
        the runtime rejects an HTML response before it reaches here.

    Raises:
        None: malformed payloads yield a zero-row frame rather than an exception.

    Example:
        Quick start::

            from sportsdataverse.soccer import football_data_league_season

            df = football_data_league_season(season="2526", div="E0")
            print(df.shape)

        Pipeline next step (one line)::

            df.select("date", "home_team", "away_team", "fthg", "ftag")

    See Also:
        * `Football-Data.co.uk`_ -- the archive and its column glossary.

    .. _Football-Data.co.uk: https://www.football-data.co.uk/data.php
    """
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8-sig", errors="replace")
    if not isinstance(raw, str) or not raw.strip():
        return pl.DataFrame().to_pandas() if return_as_pandas else pl.DataFrame()
    try:
        df = pl.read_csv(
            io.StringIO(raw),
            infer_schema_length=None,
            truncate_ragged_lines=True,
            null_values=["", "NA"],
        )
        keep = [c for c in df.columns if not _is_blank_name(c)]
        # Inside the try: two source headers can snake_case to one name (a future season adding
        # a ``fthg`` beside ``FTHG``), and ``rename`` raises DuplicateError on that.
        df = df.select(keep).rename({c: underscore(c) for c in keep})
    except Exception:
        # Not CSV at all (an error page, a glossary), or headers that collapse onto each other:
        # a zero-row frame, never a raise.
        return pl.DataFrame().to_pandas() if return_as_pandas else pl.DataFrame()
    return df.to_pandas() if return_as_pandas else df
