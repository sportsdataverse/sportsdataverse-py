"""Table readers shared by the stats.ncaa.org box-score parsers (B-L28).

One body each for the cell, row, id, integer and frame helpers that the basketball
(:mod:`sportsdataverse.mbb.mbb_ncaa_box_tabs`), football
(:mod:`sportsdataverse.cfb.cfb_ncaa_box`) and college-baseball
(:mod:`sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box`) box tabs read
their HTML tables with. stats.ncaa.org renders every sport's box tabs from one template.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

import polars as pl

if TYPE_CHECKING:
    import pandas as pd


def _cells(tr: "Any") -> "list[str]":
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def _rows(table: "Any") -> "list[list[str]]":
    return [_cells(tr) for tr in table.find_all("tr")]


def _cid(contest_id: "str | int | None") -> "str | None":
    return str(contest_id) if contest_id is not None else None


def _int(v: str) -> "int | None":
    return int(v) if v and v.lstrip("-").isdigit() else None


def _finish(rows: "list[dict]", schema: dict, return_as_pandas: bool) -> "Union[pl.DataFrame, pd.DataFrame]":
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df
