"""Availability model ③ -- projected games-available % (NOT skill).

Projects ``avail_pct``, a strictly separate availability channel: it is
never folded into a value/skill projection. Consumes prior-season GP%, career
GP% history, and age from the bulk ``nba_stats_leaguedashplayerstats`` (GP +
age per player-season), and applies a bundled logistic artifact fit offline
in ``dev/nba_draft/fit_availability.py``.
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


def availability_features(
    career: pl.DataFrame, *, league: str = "nba", median_ref: "dict[str, float] | None" = None
) -> pl.DataFrame:
    """Build per-(player_id, season) availability features from career GP history.

    Args:
        career: Per-season rows with at least ``player_id:Utf8, season:Int64,
            age:Float64 (or Int64), gp:Int64``. Optionally ``bmi:Float64``.
        league: League key -- selects the full-season game count for the GP%
            denominator via :func:`sportsdataverse.nba.nba_draft_constants.get_constants`.
        median_ref: Optional ``{"gp_pct": float, "bmi": float}`` imputation
            scalars. When the FIT script splits into train/holdout it must
            pass **train-derived** medians here so the holdout distribution
            never leaks into the imputed values baked across the split. When
            ``None`` (the runtime-inference path, where every row passed IS a
            row being scored) the medians are computed from ``career`` itself.

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
    # `prior_gp_pct` must be the *immediately preceding season* -- a
    # row-position `.shift(1)` silently jumps across multi-season gaps
    # (injury years, overseas stints, retirement-and-return) instead of
    # nulling out, which corrupts the feature for any player with a gap in
    # their captured career. Join on `season - 1` instead (the same
    # gap-safe pattern `nba_aging_curve.build_aging_deltas` already uses).
    prior = df.select("player_id", (pl.col("season") + 1).alias("season"), pl.col("_gp_pct").alias("prior_gp_pct"))
    df = df.join(prior, on=["player_id", "season"], how="left")
    df = df.with_columns(pl.int_range(0, pl.len()).over("player_id").alias("_season_idx"))
    # career_gp_pct = mean GP% over all STRICTLY PRIOR *observed* seasons
    # (position-based cumulative average is fine here -- gaps in calendar
    # time don't invalidate an average of the seasons actually observed).
    df = df.with_columns(
        pl.when(pl.col("_season_idx") > 0)
        .then(pl.col("_gp_pct").shift(1).over("player_id").cum_sum().over("player_id") / pl.col("_season_idx"))
        .otherwise(None)
        .alias("career_gp_pct"),
        (pl.col("age") ** 2).alias("age_sq"),
    )
    ref = median_ref or {}
    league_median_gp = ref.get("gp_pct") if "gp_pct" in ref else (df["_gp_pct"].median() or 0.75)
    df = df.with_columns(
        pl.col("prior_gp_pct").fill_null(league_median_gp),
        pl.col("career_gp_pct").fill_null(league_median_gp),
    )
    if "bmi" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("bmi"))
    if "bmi" in ref:
        bmi_fill = ref["bmi"]
    else:
        bmi_median = df["bmi"].median()
        bmi_fill = bmi_median if bmi_median is not None else 24.0
    df = df.with_columns(pl.col("bmi").fill_null(bmi_fill))
    return df.select("player_id", "season", "age", "prior_gp_pct", "career_gp_pct", "age_sq", "bmi")


def _load_artifact(league: str) -> dict:
    prefix = get_constants(league).artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_availability.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


@overload
def nba_availability(
    seasons: "int | list[int]",
    *,
    league: str = "nba",
    gleague_bridge: bool = False,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_availability(
    seasons: "int | list[int]",
    *,
    league: str = "nba",
    gleague_bridge: bool = False,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_availability(
    seasons: "int | list[int]",
    *,
    league: str = "nba",
    gleague_bridge: bool = False,
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Project games-available % for a season (or seasons) from career GP history.

    ``avail_pct`` is **availability, not skill** -- it is the only output of
    this function and is never combined into a value/rating column by this
    module (:func:`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
    reports it as a separate column too).

    Args:
        seasons: A season (end year, e.g. ``2020``) or list of seasons.
        league: ``"nba"``, ``"wnba"``, or ``"gleague"``.
        gleague_bridge: When ``True`` (and ``league != "gleague"``), also
            pulls each season's G-League (``league_id="20"``) bulk GP as a
            development-outcome bridge feature before scoring. Best-effort:
            gracefully absent (never raises) when the G-League bulk call
            returns no rows for a season; the returned schema is unaffected
            either way since the bridge column isn't part of the bundled
            artifact's scored features.
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
    for end_year in range(earliest, latest + 1):
        # Public `seasons` is now the END year; derive the START year to
        # build the stats.nba.com "YYYY-YY" season string.
        start_year = end_year - 1
        season_str = f"{start_year}-{str(end_year)[-2:]}"
        if league == "wnba":
            # WNBA plays a single-year season label (no cross-year split, so
            # there's no start/end distinction to derive) -- unaffected by
            # the NBA end-year migration; wnba_stats mirrors the nba_stats
            # parameter shape on its own host.
            from sportsdataverse.wnba.wnba_stats import wnba_stats_leaguedashplayerstats  # noqa: PLC0415

            bulk = wnba_stats_leaguedashplayerstats(season=str(end_year))
        elif league == "gleague":
            bulk = nba_stats_leaguedashplayerstats(season=season_str, league_id="20")
        else:
            bulk = nba_stats_leaguedashplayerstats(season=season_str)
        if bulk.is_empty():
            continue
        # leaguedashplayerstats is a per-player-season aggregate (one row per
        # player), but guard against a duplicated player_id anyway so a
        # traded/duplicated row can't double-count a player's GP within a
        # season the way the playercareerstats TOT rows did in the offline
        # corpus (that dedup lived only in the fixture; this is the runtime
        # equivalent). keep="first" is a first-writer-wins reduction.
        frames.append(
            bulk.select(
                pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
                pl.lit(end_year).cast(pl.Int64).alias("season"),
                pl.col("age").cast(pl.Float64),
                pl.col("gp").cast(pl.Int64),
            ).unique(subset=["player_id"], keep="first")
        )
    career = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if career.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    if gleague_bridge and league != "gleague":
        bridge_frames = []
        for end_year in range(earliest, latest + 1):
            start_year = end_year - 1
            season_str = f"{start_year}-{str(end_year)[-2:]}"
            try:
                gbulk = nba_stats_leaguedashplayerstats(season=season_str, league_id="20")
            except Exception:  # pragma: no cover - defensive, matches "never raises"
                continue
            if gbulk.is_empty() or "player_id" not in gbulk.columns or "gp" not in gbulk.columns:
                continue
            bridge_frames.append(
                gbulk.select(
                    pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
                    pl.lit(end_year).cast(pl.Int64).alias("season"),
                    pl.col("gp").cast(pl.Int64).alias("gleague_gp"),
                )
            )
        bridge = pl.concat(bridge_frames, how="diagonal_relaxed") if bridge_frames else pl.DataFrame()
        if not bridge.is_empty():
            assert career.schema["player_id"] == bridge.schema["player_id"]
            career = career.join(bridge, on=["player_id", "season"], how="left")

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
