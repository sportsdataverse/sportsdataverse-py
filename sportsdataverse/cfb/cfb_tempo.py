"""Opponent-adjusted, situation-neutral CFB tempo / pace (⑤).

Plays per game and seconds per play on situation-neutral snaps (scrimmage
plays, Connelly garbage time excluded, kneels/spikes dropped), opponent-
adjusted with the shared iterative solver on per-game team pace.

Data availability: hosted ``load_cfb_pbp`` covers 2002-2021 only (2022+
404s -- cfb-data producer backfill pending).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.cfb.cfb_advanced_constants import AdjustConfig, rank_desc
from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp
from sportsdataverse.cfb.cfb_opponent_adjust import (
    DEFAULT_PBP_COLS,
    flag_garbage_time,
    opponent_adjust,
)

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_adjusted_tempo"]

#: rename entries (real released pbp -> canonical) for the tempo fields.
TEMPO_PBP_COLS: dict[str, str] = {
    **DEFAULT_PBP_COLS,
    "type.text": "play_type",
    "start.TimeSecsRem": "start_time_secs_rem",
    "end.TimeSecsRem": "end_time_secs_rem",
}

_OUT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Utf8(),
    "games": pl.Int64(),
    "raw_plays_game": pl.Float64(),
    "adj_plays_game": pl.Float64(),
    "raw_sec_play": pl.Float64(),
    "adj_sec_play": pl.Float64(),
    "pace_rank": pl.Int64(),
}


def _team_games(pbp: pl.DataFrame, *, exclude_garbage: bool) -> pl.DataFrame:
    """One row per (season, game, offense team): situation-neutral plays + seconds."""
    required = {
        "season",
        "game_id",
        "period",
        "pos_team_id",
        "def_pos_team_id",
        "scrimmage_play",
    }
    if pbp.height == 0 or not required <= set(pbp.columns):
        return pl.DataFrame(
            schema={
                "season": pl.Int64(),
                "game_id": pl.Utf8(),
                "team_id": pl.Utf8(),
                "opp_team_id": pl.Utf8(),
                "plays": pl.Int64(),
                "secs": pl.Float64(),
            }
        )
    pbp = flag_garbage_time(pbp)
    out = pbp.filter(pl.col("scrimmage_play").cast(pl.Boolean) == True)  # noqa: E712
    if exclude_garbage:
        out = out.filter(pl.col("garbage_time") == False)  # noqa: E712
    if "play_type" in out.columns:
        out = out.filter(
            pl.col("play_type").str.contains("(?i)kneel|spike").fill_null(False) == False  # noqa: E712
        )
    if {"start_time_secs_rem", "end_time_secs_rem"} <= set(out.columns):
        elapsed = (pl.col("start_time_secs_rem") - pl.col("end_time_secs_rem")).cast(pl.Float64).clip(0.0, 60.0)
    else:
        elapsed = pl.lit(None, dtype=pl.Float64)
    return (
        out.group_by(["season", "game_id", "pos_team_id", "def_pos_team_id"])
        .agg(plays=pl.len().cast(pl.Int64), secs=elapsed.sum())
        .select(
            pl.col("season").cast(pl.Int64),
            pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
            pl.col("pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
            pl.col("def_pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("opp_team_id"),
            pl.col("plays"),
            pl.col("secs").cast(pl.Float64),
        )
    )


@overload
def cfb_adjusted_tempo(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = ...,
    config: Optional[AdjustConfig] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def cfb_adjusted_tempo(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = ...,
    config: Optional[AdjustConfig] = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def cfb_adjusted_tempo(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = True,
    config: Optional[AdjustConfig] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team-season situation-neutral, opponent-adjusted tempo / pace.

    Counts scrimmage plays per team-game (garbage time and kneels/spikes
    dropped) and per-play elapsed seconds, then opponent-adjusts both with
    the iterative solver on the per-game values (a fast team facing slow
    defenses gets ``adj_plays_game > raw_plays_game``).

    Args:
        seasons: season or list of seasons (hosted pbp covers 2002-2021).
        exclude_garbage: drop Connelly garbage-time plays.
        config: :class:`AdjustConfig` for the solver.
        return_as_pandas: return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (season, team_id): ``games, raw_plays_game,
        adj_plays_game, raw_sec_play, adj_sec_play, pace_rank`` (rank 1 =
        fastest adjusted pace). Zero-row frame with the documented schema
        on empty input.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_adjusted_tempo
            df = cfb_adjusted_tempo([2021])
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("pace_rank").head()

    See Also:
        * `cfbfastR`_ -- R sister package (CFBD advanced stats wrappers)

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    pbp = load_cfb_pbp(season_list)
    if not isinstance(pbp, pl.DataFrame):
        pbp = pl.DataFrame(pbp)
    if pbp.height > 0:
        mapping = {k: v for k, v in TEMPO_PBP_COLS.items() if k in pbp.columns}
        clobber = [v for k, v in mapping.items() if v in pbp.columns and v != k]
        pbp = pbp.drop(clobber).rename(mapping)
    tg = _team_games(pbp, exclude_garbage=exclude_garbage)
    if tg.height == 0:
        out = pl.DataFrame(schema=_OUT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    tg = tg.with_columns(sec_play=pl.col("secs") / pl.col("plays"))
    raw = tg.group_by(["season", "team_id"]).agg(
        games=pl.col("game_id").n_unique().cast(pl.Int64),
        raw_plays_game=pl.col("plays").cast(pl.Float64).mean(),
        raw_sec_play=(pl.col("secs").sum() / pl.col("plays").sum()),
    )
    frames: list[pl.DataFrame] = []
    for (season,), s_tg in tg.group_by(["season"], maintain_order=True):
        adj_p = opponent_adjust(s_tg, value_col="plays", config=config).select(
            "team_id", adj_plays_game=pl.col("adj_off")
        )
        adj_s = opponent_adjust(s_tg, value_col="sec_play", config=config).select(
            "team_id", adj_sec_play=pl.col("adj_off")
        )
        assert adj_p.schema["team_id"] == adj_s.schema["team_id"]
        frames.append(
            adj_p.join(adj_s, on="team_id", how="full", coalesce=True).with_columns(
                season=pl.lit(season, dtype=pl.Int64)
            )
        )
    adj = pl.concat(frames, how="vertical_relaxed")
    assert raw.schema["team_id"] == adj.schema["team_id"]
    out = (
        raw.join(adj, on=["season", "team_id"], how="left")
        .with_columns(pace_rank=rank_desc(pl.col("adj_plays_game")).over("season"))
        .select(list(_OUT_SCHEMA))
        .sort(["season", "team_id"])
    )
    return out.to_pandas() if return_as_pandas else out
