"""Availability model ③ -- projected games-available % (NOT skill).

Projects ``avail_pct``, a strictly separate availability channel: it is
never folded into a value/skill projection. Consumes prior-season GP%, career
GP% history, and age from ``nba_stats_playercareerstats``, and applies a
bundled logistic artifact fit offline in ``dev/nba_draft/fit_availability.py``.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_draft_constants import get_constants
from sportsdataverse.nba.nba_stats import nba_stats_leaguedashplayerstats

__all__ = ["availability_features", "nba_availability", "score_availability"]

_LOOKBACK_SEASONS = 5  # prior seasons pulled (bulk, one call each) to build career_gp_pct

_FEATURE_COLS = ["age", "prior_gp_pct", "career_gp_pct", "age_sq", "bmi"]

_SCHEMA = {"player_id": pl.Utf8, "season": pl.Int64, "avail_pct": pl.Float64}


def availability_features(career: pl.DataFrame, *, league: str = "nba") -> pl.DataFrame:
    """Build per-(player_id, season) availability features from career GP history.

    Args:
        career: Per-season rows with at least ``player_id:Utf8, season:Int64,
            age:Float64 (or Int64), gp:Int64``. Optionally ``bmi:Float64``.
        league: League key -- selects the full-season game count for the GP%
            denominator via :func:`sportsdataverse.nba.nba_draft_constants.get_constants`.

    Returns:
        Frame ``player_id, season, age, prior_gp_pct, career_gp_pct, age_sq,
        bmi`` -- one row per player-season. ``prior_gp_pct`` is the
        **strictly prior** season's GP% (null/median-imputed for a player's
        first observed season, never the current season's own GP -- avoids
        feeding the label into its own feature). ``career_gp_pct`` is the
        career-to-date mean GP% using only seasons strictly before the
        current one. Empty input -> zero-row frame with the full schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_availability import availability_features
            career = pl.DataFrame({
                "player_id": ["1", "1"], "season": [2018, 2019],
                "age": [22, 23], "gp": [70, 60],
            })
            availability_features(career)
    """
    schema = {
        "player_id": pl.Utf8,
        "season": pl.Int64,
        "age": pl.Float64,
        "prior_gp_pct": pl.Float64,
        "career_gp_pct": pl.Float64,
        "age_sq": pl.Float64,
        "bmi": pl.Float64,
    }
    if career.is_empty():
        return pl.DataFrame(schema=schema)

    full_season = float(get_constants(league).games_full_season)
    df = career.sort("player_id", "season").with_columns(
        (pl.col("gp").cast(pl.Float64) / full_season).clip(0.0, 1.0).alias("_gp_pct"),
        pl.col("age").cast(pl.Float64),
    )
    df = df.with_columns(
        pl.col("_gp_pct").shift(1).over("player_id").alias("prior_gp_pct"),
        pl.col("_gp_pct").shift(1).over("player_id").cum_sum().over("player_id").alias("_cum_prior_sum"),
        pl.int_range(0, pl.len()).over("player_id").alias("_season_idx"),
    )
    df = df.with_columns(
        pl.when(pl.col("_season_idx") > 0)
        .then(pl.col("_gp_pct").shift(1).over("player_id").cum_sum().over("player_id") / pl.col("_season_idx"))
        .otherwise(None)
        .alias("career_gp_pct"),
        (pl.col("age") ** 2).alias("age_sq"),
    )
    league_median_gp = df["_gp_pct"].median() or 0.75
    df = df.with_columns(
        pl.col("prior_gp_pct").fill_null(league_median_gp),
        pl.col("career_gp_pct").fill_null(league_median_gp),
    )
    if "bmi" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("bmi"))
    bmi_median = df["bmi"].median()
    df = df.with_columns(pl.col("bmi").fill_null(bmi_median if bmi_median is not None else 24.0))
    return df.select("player_id", "season", "age", "prior_gp_pct", "career_gp_pct", "age_sq", "bmi")


def _load_artifact(league: str) -> dict:
    prefix = get_constants(league).artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_availability.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


@overload
def nba_availability(
    seasons: "int | list[int]", *, league: str = "nba", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_availability(
    seasons: "int | list[int]", *, league: str = "nba", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_availability(
    seasons: "int | list[int]", *, league: str = "nba", return_as_pandas: bool = False
) -> "pl.DataFrame | pd.DataFrame":
    """Project games-available % for a season (or seasons) from career GP history.

    ``avail_pct`` is **availability, not skill** -- it is the only output of
    this function and is never combined into a value/rating column by this
    module (:func:`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
    reports it as a separate column too).

    Args:
        seasons: A season (start year, e.g. ``2019``) or list of seasons.
        league: ``"nba"``, ``"wnba"``, or ``"gleague"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, season:Int64, avail_pct:Float64`` (clipped to
        ``[0, 1]``). Empty ``seasons`` -> zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_availability
            proj = nba_availability(2019)
            print(proj.sort("avail_pct").head())

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if not season_list:
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    earliest = min(season_list) - _LOOKBACK_SEASONS
    latest = max(season_list)
    frames = []
    for start_year in range(earliest, latest + 1):
        season_str = f"{start_year}-{str(start_year + 1)[-2:]}"
        if league == "wnba":
            # WNBA plays a single-year season label (no cross-year split);
            # wnba_stats mirrors the nba_stats parameter shape on its own host.
            from sportsdataverse.wnba.wnba_stats import wnba_stats_leaguedashplayerstats  # noqa: PLC0415

            bulk = wnba_stats_leaguedashplayerstats(season=str(start_year))
        elif league == "gleague":
            bulk = nba_stats_leaguedashplayerstats(season=season_str, league_id="20")
        else:
            bulk = nba_stats_leaguedashplayerstats(season=season_str)
        if bulk.is_empty():
            continue
        frames.append(
            bulk.select(
                pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
                pl.lit(start_year).cast(pl.Int64).alias("season"),
                pl.col("age").cast(pl.Float64),
                pl.col("gp").cast(pl.Int64),
            )
        )
    career = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if career.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    feats = availability_features(career, league=league)
    scored = score_availability(feats, league=league)
    out = scored.filter(pl.col("season").is_in(season_list))
    return out.to_pandas() if return_as_pandas else out


def score_availability(features: pl.DataFrame, *, league: str = "nba") -> pl.DataFrame:
    """Apply the bundled availability artifact to a pre-built feature frame.

    Args:
        features: Output of :func:`availability_features` (or an
            equivalently-shaped frame).
        league: League key for the bundled artifact.

    Returns:
        Frame ``player_id:Utf8, season:Int64, avail_pct:Float64``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_availability import availability_features, score_availability
            feats = availability_features(career_df)
            score_availability(feats)
    """
    if features.is_empty():
        return pl.DataFrame(schema=_SCHEMA)
    art = _load_artifact(league)
    cols = art["features"]
    X = features.select(cols).fill_null(0.0).to_numpy()
    logit = float(art["intercept"]) + X @ np.asarray(art["coef"], dtype=float)
    p = 1.0 / (1.0 + np.exp(-logit))
    return features.select("player_id", "season").with_columns(
        pl.Series("avail_pct", np.clip(p, 0.0, 1.0), dtype=pl.Float64)
    )
