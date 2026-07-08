"""College box-BPM from the bundled team-constrained ridge artifact.

Model ① of the MBB/WBB player-value spine. Follows the published BPM
methodology: per-100 box features are scored through ridge coefficients fit
at the TEAM level (minutes-weighted team feature aggregates regressed onto
the shipped ``mbb_team_ratings`` adjusted efficiencies), then a uniform team
adjustment makes each team's minutes-weighted player scores sum to the team's
adjusted rating (the BPM constraint). No lineup data is required at fit or
call time.

Methodology reference: Basketball-Reference's Box Plus/Minus 2.0 write-up
(Daniel Myers) -- methodology only, no ported code.
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
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings

__all__ = ["mbb_box_bpm"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "min": pl.Float64,
    "box_obpm": pl.Float64,
    "box_dbpm": pl.Float64,
    "box_bpm": pl.Float64,
}


@overload
def mbb_box_bpm(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_box_bpm(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_box_bpm(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-player-season box Plus/Minus (offense, defense, total).

    Aggregates the season's player boxscores, scores the per-100 features
    through the bundled team-constrained coefficients, and applies the BPM
    team adjustment so each team's minutes-weighted player scores sum to its
    adjusted efficiency margin (points per 100 possessions above league
    average; positive = good on both ends).

    Args:
        seasons: A season (e.g. ``2025``) or list of seasons.
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (player_id, season, team_id): ``player_id:Utf8, player,
        season, team_id:Utf8, min, box_obpm, box_dbpm, box_bpm``. Empty
        input returns the schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_box_bpm
            bpm = mbb_box_bpm(2025)

        Pipeline next step (one line)::

            bpm.filter(pl.col("min") >= 400).sort("box_bpm", descending=True).head(15)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    agg = aggregate_player_seasons(seasons_list, league=league)
    if agg.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = load_artifact(f"{get_player_value_constants(league).bundle_prefix}_box_bpm")
    feats = player_per100_features(agg)
    if any(c == "pos_score" or c.endswith("__x_pos") for c in art["feature_cols"]):
        # position interactions (BPM 2.0-style): join the roster position and
        # interact every base feature with a numeric G=0 / F=0.5 / C=1 score
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
        feats = feats.with_columns(
            [
                (pl.col(c.removesuffix("__x_pos")) * pl.col("pos_score")).alias(c)
                for c in art["feature_cols"]
                if c.endswith("__x_pos")
            ]
        )
    # standardize with the fit-time moments and clip z-scores: tiny-minute
    # players carry insane per-100 rates that would otherwise corrupt scores
    # and the team sums. Player raw score uses the slopes only (the intercept
    # is team-level; the team adjustment absorbs it).
    cols = art["feature_cols"]
    mu = np.array([art["feature_mean"][c] for c in cols])
    sd = np.array([art["feature_sd"][c] for c in cols])
    zc = float(art.get("z_clip", 4.0))
    X = feats.select(cols).fill_null(0.0).to_numpy()
    Z = np.clip((X - mu) / sd, -zc, zc)
    raw_o = Z @ np.asarray(art["obpm_coef"], dtype=float)[1:]
    raw_d = Z @ np.asarray(art["dbpm_coef"], dtype=float)[1:]
    scored = feats.select("player_id", "season", "team_id", "min").with_columns(
        pl.Series("_raw_o", raw_o, dtype=pl.Float64),
        pl.Series("_raw_d", raw_d, dtype=pl.Float64),
    )

    # D1 centering only: the ratings frame includes every opponent ever seen
    ratings = (
        mbb_team_ratings(seasons_list, league=league)
        .filter(pl.col("games") >= 10)
        .select("season", "team_id", "adj_o", "adj_d")
    )
    assert scored.schema["team_id"] == ratings.schema["team_id"] == pl.Utf8
    team_y = ratings.with_columns(
        (pl.col("adj_o") - pl.col("adj_o").mean().over("season")).alias("_y_o"),
        (-(pl.col("adj_d") - pl.col("adj_d").mean().over("season"))).alias("_y_d"),
    ).select("season", "team_id", "_y_o", "_y_d")

    # uniform team adjustment: minutes-weighted (w_i sums to 5) player scores
    # must sum to the team rating -> c = (y_t - sum_i w_i * raw_i) / 5.
    # Weights use qualified minutes only (>= the artifact's fit floor); bench
    # slivers still get scored + the team constant, but can't distort the sum.
    min_floor = float(art.get("min_minutes", 0.0))
    out = (
        scored.join(team_y, on=["season", "team_id"], how="left")
        .with_columns((pl.col("min") >= min_floor).alias("_qual"))
        .with_columns(
            pl.when(pl.col("_qual") == True)  # noqa: E712
            .then(
                5.0 * pl.col("min") / pl.col("min").filter(pl.col("_qual") == True).sum().over("season", "team_id")  # noqa: E712
            )
            .otherwise(0.0)
            .alias("_w")
        )
        .with_columns(
            ((pl.col("_y_o") - (pl.col("_w") * pl.col("_raw_o")).sum().over("season", "team_id")) / 5.0).alias("_c_o"),
            ((pl.col("_y_d") - (pl.col("_w") * pl.col("_raw_d")).sum().over("season", "team_id")) / 5.0).alias("_c_d"),
        )
        .with_columns(
            (pl.col("_raw_o") + pl.col("_c_o").fill_null(0.0)).alias("box_obpm"),
            (pl.col("_raw_d") + pl.col("_c_d").fill_null(0.0)).alias("box_dbpm"),
        )
        .with_columns((pl.col("box_obpm") + pl.col("box_dbpm")).alias("box_bpm"))
        .join(agg.select("player_id", "season", "team_id", "player"), on=["player_id", "season", "team_id"], how="left")
        .select(list(_SCHEMA))
        .sort("season", "box_bpm", descending=[False, True])
    )
    return out.to_pandas() if return_as_pandas else out
