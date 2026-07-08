"""Recruiting/production -> NFL draft projection for CFB (T2.2 model ⑤).

Draft outcomes come from the nflverse draft-picks dataset
(:func:`sportsdataverse.nfl.load_nfl_draft_picks`) rather than the ESPN
season-draft endpoint, which 404s for recent years. The picks carry the
college name, the PFR player name, and (for recent drafts) the ESPN
``cfb_player_id`` — the join keys the projection matches recruits on.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.nfl import load_nfl_draft_picks

__all__ = ["load_draft_outcomes"]

_DRAFT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "draft_year": pl.Int64,
    "college": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "round": pl.Int64,
    "pick": pl.Int64,
    "position": pl.Utf8,
}


def load_draft_outcomes(years: int | list[int], *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """NFL draft picks with the college of each pick, for the requested draft years.

    Args:
        years: A draft year or list of draft years.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per pick: ``draft_year`` (Int64), ``college`` (Utf8 PFR-style
        college name), ``player_id`` (Utf8 ESPN college athlete id; null for
        older drafts), ``player_name`` (Utf8), ``round`` / ``pick`` (Int64),
        ``position`` (Utf8). Zero-row (typed) when the source is unavailable.

    Example:
        Quick start::

            from sportsdataverse.cfb import load_draft_outcomes
            picks = load_draft_outcomes([2023, 2024])
            picks.group_by("college").len().sort("len", descending=True).head()

    See Also:
        * `nflreadpy`_ -- the picks dataset's canonical Python surface.
        * `recruitR`_ -- the R companion for CFB recruiting data.

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _recruitR: https://github.com/sportsdataverse/recruitR
    """
    year_list = [years] if isinstance(years, int) else list(years)
    raw = load_nfl_draft_picks()
    if isinstance(raw, pd.DataFrame):
        raw = pl.from_pandas(raw)
    if raw.height == 0 or "season" not in raw.columns:
        empty = pl.DataFrame(schema=_DRAFT_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    out = (
        raw.filter(pl.col("season").is_in(year_list))
        .select(
            pl.col("season").cast(pl.Int64).alias("draft_year"),
            pl.col("college").cast(pl.Utf8),
            pl.col("cfb_player_id").cast(pl.Utf8).alias("player_id"),
            pl.col("pfr_player_name").cast(pl.Utf8).alias("player_name"),
            pl.col("round").cast(pl.Int64),
            pl.col("pick").cast(pl.Int64),
            pl.col("position").cast(pl.Utf8),
        )
        .sort("draft_year", "pick")
    )
    return out.to_pandas() if return_as_pandas else out
