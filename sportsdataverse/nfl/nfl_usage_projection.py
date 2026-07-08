"""NFL usage projection: target share, air-yards share, and WOPR (weighted
opportunity rating), season-level and projected via the shared Marcel blend.

Methodology: WOPR = ``1.5 * target_share + 0.7 * air_yards_share`` (Josh
Hermsmeyer's published formula, cited per the methodology-attribution
convention; no code copied).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Literal, Union, overload

import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_player_stats
from sportsdataverse.nfl.nfl_projection import _marcel_blend
from sportsdataverse.nfl.nfl_projection_constants import as_of_season_split, get_position_constants

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_USAGE_SCHEMA: dict = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "team": pl.Utf8,
    "position_group": pl.Utf8,
    "games": pl.Int64,
    "targets": pl.Float64,
    "air_yards": pl.Float64,
    "target_share": pl.Float64,
    "air_yards_share": pl.Float64,
    "wopr": pl.Float64,
}


def season_usage_shares(weekly: pl.DataFrame) -> pl.DataFrame:
    """Season-level target share, air-yards share, and WOPR per player-team.

    Aggregates weekly targets and receiving air yards to (player_id, season,
    team), window-sums the team-season totals, and computes each player's
    share plus ``wopr = 1.5 * target_share + 0.7 * air_yards_share``.

    Args:
        weekly (pl.DataFrame): nflverse weekly offense stats (needs
            ``player_id, season, recent_team, targets, receiving_air_yards``).

    Returns:
        pl.DataFrame: One row per (player_id, season, team): ``player_id:Utf8,
        season:Int64, team:Utf8, position_group:Utf8, games:Int64,
        targets:Float64, air_yards:Float64, target_share:Float64,
        air_yards_share:Float64, wopr:Float64``. Empty/malformed input returns
        a zero-row frame with that schema.

    Example:
        Quick start::

            import sportsdataverse.nfl as nfl
            from sportsdataverse.nfl.nfl_usage_projection import season_usage_shares
            shares = season_usage_shares(nfl.load_nfl_player_stats())

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    required = {"player_id", "season", "recent_team", "targets"}
    if weekly.height == 0 or not required.issubset(weekly.columns):
        return pl.DataFrame(schema=_USAGE_SCHEMA)
    air = pl.col("receiving_air_yards").fill_null(0.0) if "receiving_air_yards" in weekly.columns else pl.lit(0.0)
    games_expr = pl.col("week").n_unique() if "week" in weekly.columns else pl.len()
    pos_expr = (
        pl.col("position_group").drop_nulls().first()
        if "position_group" in weekly.columns
        else pl.lit(None, dtype=pl.Utf8)
    )
    agg = (
        weekly.with_columns(
            pl.col("player_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
            pl.col("recent_team").cast(pl.Utf8).alias("team"),
        )
        .group_by("player_id", "season", "team")
        .agg(
            pos_expr.alias("position_group"),
            games_expr.cast(pl.Int64).alias("games"),
            pl.col("targets").fill_null(0.0).sum().cast(pl.Float64).alias("targets"),
            air.sum().cast(pl.Float64).alias("air_yards"),
        )
    )
    team_tot = [
        pl.col("targets").sum().over("team", "season").alias("_team_targets"),
        pl.col("air_yards").sum().over("team", "season").alias("_team_air_yards"),
    ]
    out = (
        agg.with_columns(team_tot)
        .with_columns(
            (pl.col("targets") / pl.max_horizontal(pl.col("_team_targets"), pl.lit(1e-9)))
            .cast(pl.Float64)
            .alias("target_share"),
            (pl.col("air_yards") / pl.max_horizontal(pl.col("_team_air_yards"), pl.lit(1e-9)))
            .cast(pl.Float64)
            .alias("air_yards_share"),
        )
        .with_columns((1.5 * pl.col("target_share") + 0.7 * pl.col("air_yards_share")).cast(pl.Float64).alias("wopr"))
        .drop("_team_targets", "_team_air_yards")
    )
    return out.select(list(_USAGE_SCHEMA.keys()))


_PROJ_SCHEMA: dict = {
    "player_id": pl.Utf8,
    "target_season": pl.Int64,
    "position_group": pl.Utf8,
    "proj_team": pl.Utf8,
    "proj_target_share": pl.Float64,
    "proj_air_yards_share": pl.Float64,
    "proj_wopr": pl.Float64,
    "proj_targets": pl.Float64,
    "proj_air_yards": pl.Float64,
}


@overload
def nfl_usage_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def nfl_usage_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...


def nfl_usage_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Project next-season target share, air-yards share, and WOPR.

    Projects each player's shares via the shared Marcel blend
    (:func:`sportsdataverse.nfl.nfl_projection._marcel_blend` — the same
    recency/shrinkage engine as the rate projection), assigns each player to
    their most recent team, **renormalizes shares within each projected team to
    sum to 1.0** (the share invariant), and converts shares to volumes with a
    team-level carry-forward of pass attempts (team targets) and air yards.
    As-of-date clean: only seasons strictly before ``target_season`` are used.

    Args:
        seasons (List[int]): History seasons to load.
        target_season (int): The season being projected.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: ``player_id:Utf8, target_season:Int64,
        position_group:Utf8, proj_team:Utf8, proj_target_share:Float64,
        proj_air_yards_share:Float64, proj_wopr:Float64, proj_targets:Float64,
        proj_air_yards:Float64``. Empty history returns a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_usage_projection import nfl_usage_projection
            usage = nfl_usage_projection([2021, 2022, 2023], 2024)
            usage.sort("proj_wopr", descending=True).head()

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    weekly = load_nfl_player_stats()
    if "season" in weekly.columns and seasons:
        weekly = weekly.filter(pl.col("season").is_in(list(seasons)))
    shares = season_usage_shares(weekly)
    hist = as_of_season_split(shares, target_season)
    if hist.height == 0:
        result = pl.DataFrame(schema=_PROJ_SCHEMA)
        return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result

    # blend reliability is keyed on targets (the share's opportunity volume)
    hist_b = hist.with_columns(pl.col("targets").alias("volume"))
    frames = []
    for pos in sorted([p for p in hist_b["position_group"].unique().to_list() if p is not None]):
        consts = get_position_constants(pos)
        sub = hist_b.filter(pl.col("position_group") == pos)
        blend = _marcel_blend(sub, value_cols=["target_share", "air_yards_share"], consts=consts)
        frames.append(blend.with_columns(pl.lit(pos).alias("position_group")))
    blended = pl.concat(frames, how="vertical")

    # most recent team per player (max season, tiebreak: most targets)
    last_team = (
        hist.sort("season", "targets", descending=[True, True])
        .group_by("player_id", maintain_order=True)
        .agg(pl.col("team").first().alias("proj_team"))
    )
    assert blended.schema["player_id"] == last_team.schema["player_id"]
    out = blended.join(last_team, on="player_id", how="left")

    # renormalize shares within projected team
    out = out.with_columns(
        (pl.col("_blend_target_share") / pl.col("_blend_target_share").sum().over("proj_team"))
        .cast(pl.Float64)
        .alias("proj_target_share"),
        (pl.col("_blend_air_yards_share") / pl.col("_blend_air_yards_share").sum().over("proj_team"))
        .cast(pl.Float64)
        .alias("proj_air_yards_share"),
    ).with_columns(
        (1.5 * pl.col("proj_target_share") + 0.7 * pl.col("proj_air_yards_share")).cast(pl.Float64).alias("proj_wopr")
    )

    # team-level carry-forward volumes from the most recent visible season per team
    team_season = hist.group_by("team", "season").agg(
        pl.col("targets").sum().alias("_team_targets"),
        pl.col("air_yards").sum().alias("_team_air_yards"),
    )
    team_last = (
        team_season.sort("season", descending=True)
        .group_by("team", maintain_order=True)
        .agg(pl.col("_team_targets").first(), pl.col("_team_air_yards").first())
        .rename({"team": "proj_team"})
    )
    assert out.schema["proj_team"] == team_last.schema["proj_team"]
    out = out.join(team_last, on="proj_team", how="left").with_columns(
        (pl.col("proj_target_share") * pl.col("_team_targets")).cast(pl.Float64).alias("proj_targets"),
        (pl.col("proj_air_yards_share") * pl.col("_team_air_yards")).cast(pl.Float64).alias("proj_air_yards"),
    )
    result = out.with_columns(pl.lit(target_season, dtype=pl.Int64).alias("target_season")).select(
        list(_PROJ_SCHEMA.keys())
    )
    return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result
