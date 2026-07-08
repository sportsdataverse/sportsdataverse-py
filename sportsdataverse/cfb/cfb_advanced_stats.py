"""Opponent-adjusted CFB advanced efficiency / explosiveness / havoc stats.

Compute-on-demand team-season advanced stats layered on ``load_cfb_pbp``,
following the canonical published methodology (Bill Connelly / GameOnPaper /
collegefootballdata): garbage-time-filtered success rate, EPA/play, isoPPP
explosiveness, and havoc -- raw and opponent-adjusted (iterative solver in
:mod:`sportsdataverse.cfb.cfb_opponent_adjust`).

Data availability: the hosted ``load_cfb_pbp`` parquet covers **2002-2021
only** (2022+ assets 404 -- cfb-data producer backfill pending), so build and
validate on seasons <= 2021.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.cfb.cfb_advanced_constants import AdjustConfig, rank_desc
from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp
from sportsdataverse.cfb.cfb_opponent_adjust import (
    DEFAULT_PBP_COLS,
    build_play_long,
    opponent_adjust,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_advanced_stats"]

#: metric -> (long-frame value column, output stem)
_ADJUSTED_METRICS: dict[str, str] = {
    "epa": "epa_play",
    "success": "success_rate",
    "explosive": "explosive_rate",
    "havoc": "havoc",
}

_RAW_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Utf8(),
    "plays": pl.Int64(),
    "off_success_rate": pl.Float64(),
    "def_success_rate": pl.Float64(),
    "off_epa_play": pl.Float64(),
    "def_epa_play": pl.Float64(),
    "off_iso_ppp": pl.Float64(),
    "def_iso_ppp": pl.Float64(),
    "off_explosive_rate": pl.Float64(),
    "def_explosive_rate": pl.Float64(),
    "def_havoc": pl.Float64(),
    "off_havoc_allowed": pl.Float64(),
    "off_epa_success_rate": pl.Float64(),
}

_ADJ_SCHEMA: dict[str, pl.DataType] = {
    **{f"adj_off_{stem}": pl.Float64() for stem in ("epa_play", "success_rate", "explosive_rate")},
    **{f"adj_def_{stem}": pl.Float64() for stem in ("epa_play", "success_rate", "explosive_rate")},
    "adj_def_havoc": pl.Float64(),
    "adj_off_havoc_allowed": pl.Float64(),
    "off_epa_rank": pl.Int64(),
    "def_epa_rank": pl.Int64(),
}


def _team_season_raw(long: pl.DataFrame) -> pl.DataFrame:
    """Aggregate the play long frame to raw per-(season, team_id) stats."""
    succ_epa = pl.col("epa").filter(pl.col("success") == True)  # noqa: E712
    off = long.group_by(["season", "team_id"]).agg(
        plays=pl.len().cast(pl.Int64),
        off_success_rate=pl.col("success").cast(pl.Float64).mean(),
        off_epa_play=pl.col("epa").mean(),
        off_iso_ppp=succ_epa.mean(),
        off_explosive_rate=pl.col("explosive").cast(pl.Float64).mean(),
        off_havoc_allowed=pl.col("havoc").cast(pl.Float64).mean(),
        off_epa_success_rate=(pl.col("epa") > 0).cast(pl.Float64).mean(),
    )
    dfn = (
        long.group_by(["season", "opp_team_id"])
        .agg(
            def_success_rate=pl.col("success").cast(pl.Float64).mean(),
            def_epa_play=pl.col("epa").mean(),
            def_iso_ppp=succ_epa.mean(),
            def_explosive_rate=pl.col("explosive").cast(pl.Float64).mean(),
            def_havoc=pl.col("havoc").cast(pl.Float64).mean(),
        )
        .rename({"opp_team_id": "team_id"})
    )
    assert off.schema["team_id"] == dfn.schema["team_id"]
    return off.join(dfn, on=["season", "team_id"], how="full", coalesce=True)


def _adjusted(long: pl.DataFrame, config: Optional[AdjustConfig]) -> pl.DataFrame:
    """Opponent-adjust each metric per season; one row per (season, team_id)."""
    frames: list[pl.DataFrame] = []
    for (season,), season_long in long.group_by(["season"], maintain_order=True):
        out: Optional[pl.DataFrame] = None
        for value_col, stem in _ADJUSTED_METRICS.items():
            adj = opponent_adjust(season_long, value_col=value_col, config=config)
            if stem == "havoc":
                renames = {
                    "adj_off": "adj_off_havoc_allowed",
                    "adj_def": "adj_def_havoc",
                }
            else:
                renames = {"adj_off": f"adj_off_{stem}", "adj_def": f"adj_def_{stem}"}
            adj = adj.select(["team_id", "adj_off", "adj_def"]).rename(renames)
            if out is None:
                out = adj
            else:
                assert out.schema["team_id"] == adj.schema["team_id"]
                out = out.join(adj, on="team_id", how="full", coalesce=True)
        assert out is not None
        frames.append(out.with_columns(season=pl.lit(season, dtype=pl.Int64)))
    return pl.concat(frames, how="vertical_relaxed")


@overload
def cfb_advanced_stats(
    seasons: Union[int, list[int]],
    *,
    adjust: bool = ...,
    exclude_garbage: bool = ...,
    as_of_date: Optional[datetime.date] = ...,
    config: Optional[AdjustConfig] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def cfb_advanced_stats(
    seasons: Union[int, list[int]],
    *,
    adjust: bool = ...,
    exclude_garbage: bool = ...,
    as_of_date: Optional[datetime.date] = ...,
    config: Optional[AdjustConfig] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def cfb_advanced_stats(
    seasons: Union[int, list[int]],
    *,
    adjust: bool = True,
    exclude_garbage: bool = True,
    as_of_date: Optional[datetime.date] = None,
    config: Optional[AdjustConfig] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team-season CFB advanced stats: efficiency, explosiveness, havoc.

    Loads play-by-play via ``load_cfb_pbp``, builds the garbage-filtered
    per-play long frame, aggregates raw per-team offense/defense success
    rate, EPA/play, isoPPP (mean EPA on successful plays), explosive rate
    and havoc, and (default) opponent-adjusts each metric with the
    iterative solver.

    Args:
        seasons: season or list of seasons (hosted pbp covers 2002-2021).
        adjust: add ``adj_*`` opponent-adjusted columns + EPA ranks.
        exclude_garbage: drop Connelly garbage-time plays.
        as_of_date: leakage boundary -- only plays strictly before this
            date contribute.
        config: :class:`AdjustConfig` for the solver.
        return_as_pandas: return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (season, team_id) with the raw columns (and ``adj_*``
        plus ``off_epa_rank``/``def_epa_rank`` when ``adjust=True``).
        Empty input returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_advanced_stats
            df = cfb_advanced_stats([2021])
            print(df.shape)

        Raw only, garbage time kept::

            df_raw = cfb_advanced_stats(2021, adjust=False, exclude_garbage=False)

        Pipeline next step (one line)::

            df.sort("adj_off_epa_play", descending=True).head()

    See Also:
        * `cfbfastR`_ -- R sister package (CFBD advanced stats wrappers)

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    pbp = load_cfb_pbp(season_list)
    long = build_play_long(
        pbp if isinstance(pbp, pl.DataFrame) else pl.DataFrame(pbp),
        cols=DEFAULT_PBP_COLS,
        exclude_garbage=exclude_garbage,
        as_of_date=as_of_date,
    )
    schema = {**_RAW_SCHEMA, **(_ADJ_SCHEMA if adjust else {})}
    if long.height == 0:
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    out = _team_season_raw(long)
    if adjust:
        adj = _adjusted(long, config)
        assert out.schema["team_id"] == adj.schema["team_id"]
        out = out.join(adj, on=["season", "team_id"], how="left")
        out = out.with_columns(
            off_epa_rank=rank_desc(pl.col("adj_off_epa_play")).over("season"),
            def_epa_rank=pl.col("adj_def_epa_play").rank(method="dense").cast(pl.Int64).over("season"),
        )
    out = out.select(list(schema)).sort(["season", "team_id"])
    return out.to_pandas() if return_as_pandas else out
