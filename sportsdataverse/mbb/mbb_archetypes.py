"""College player role/archetype assignment from bundled KMeans centers.

Model ② of the MBB/WBB player-value spine. Per-100 box features (+ a roster
position score) are standardized with the fit-time moments and each
player-season is assigned the nearest bundled center; the center labels were
hand-assigned once at fit time and are frozen in the artifact.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_player_value_constants import (
    aggregate_player_seasons,
    get_player_value_constants,
    load_artifact,
    player_per100_features,
)

__all__ = ["mbb_archetypes"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "min": pl.Float64,
    "archetype": pl.Utf8,
    "cluster": pl.Int64,
    "dist_to_center": pl.Float64,
}


@overload
def mbb_archetypes(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_archetypes(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_archetypes(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-player-season role archetype from the bundled KMeans centers.

    Aggregates the season's player boxscores, builds the per-100 feature
    vector (+ roster position score), standardizes with the artifact's
    fit-time mean/sd, and assigns each player-season to the nearest center.
    ``dist_to_center`` is the euclidean distance in z-space -- small = a
    prototypical example of the archetype, large = a hybrid.

    Args:
        seasons: A season (e.g. ``2025``) or list of seasons.
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (player_id, season, team_id): ``player_id:Utf8, player,
        season, team_id:Utf8, min, archetype, cluster:Int64,
        dist_to_center:Float64``. Empty input returns the schema with zero
        rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_archetypes
            roles = mbb_archetypes(2025)

        Pipeline next step (one line)::

            roles.filter(pl.col("archetype") == "rim protector").sort("dist_to_center").head(10)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    agg = aggregate_player_seasons(seasons_list, league=league)
    if agg.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = load_artifact(f"{get_player_value_constants(league).bundle_prefix}_archetypes")
    feats = player_per100_features(agg)
    if "pos_score" in art["feature_cols"]:
        feats = feats.join(
            agg.select("player_id", "season", "team_id", "position"),
            on=["player_id", "season", "team_id"],
            how="left",
        ).with_columns(
            pl.when(pl.col("position").fill_null("").str.contains("(?i)C"))
            .then(1.0)
            .when(pl.col("position").fill_null("").str.contains("(?i)F"))
            .then(0.5)
            .otherwise(0.0)
            .alias("pos_score")
        )
    cols = art["feature_cols"]
    mu = np.array([art["feature_mean"][c] for c in cols])
    sd = np.array([art["feature_sd"][c] for c in cols])
    Z = (feats.select(cols).fill_null(0.0).to_numpy() - mu) / sd
    centers = np.asarray(art["centers"], dtype=float)
    d = np.sqrt(((Z[:, None, :] - centers[None, :, :]) ** 2).sum(-1))
    cluster = d.argmin(1)
    labels = art["labels"]
    out = (
        feats.select("player_id", "season", "team_id", "min")
        .with_columns(
            pl.Series("cluster", cluster.astype(np.int64), dtype=pl.Int64),
            pl.Series("archetype", [labels[j] for j in cluster], dtype=pl.Utf8),
            pl.Series("dist_to_center", d.min(1), dtype=pl.Float64),
        )
        .join(agg.select("player_id", "season", "team_id", "player"), on=["player_id", "season", "team_id"], how="left")
        .select(list(_SCHEMA))
        .sort("season", "cluster", "dist_to_center")
    )
    return out.to_pandas() if return_as_pandas else out
