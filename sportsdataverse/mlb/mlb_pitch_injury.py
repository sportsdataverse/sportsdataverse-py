"""Pitcher injury-risk index ⑦ — as-of-date trailing velocity/workload composite.

An unsupervised composite z-score index over leakage-safe trailing features
(no IL-transactions label exists in the shipped Statcast data, so this ships
as a documented risk index, not a supervised classifier — see the plan's
autonomous decision #4). Every trailing feature for appearance G uses only
appearances strictly before G (:func:`sportsdataverse.mlb.mlb_pitching_constants.as_of_split`
is the shared leakage boundary this module honors).
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Optional, Union

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import as_of_split

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["pitcher_appearance_trends", "mlb_injury_risk"]

_FASTBALL_TYPES = ("FF", "SI", "FC")

_TRENDS_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "game_pk": pl.Int64,
    "game_date": pl.Date,
    "fb_velo": pl.Float64,
    "velo_trend": pl.Float64,
    "velo_drop": pl.Float64,
    "pitches_game": pl.Int64,
    "trailing_workload": pl.Float64,
    "days_rest": pl.Float64,
}
_INJURY_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "game_pk": pl.Int64,
    "game_date": pl.Date,
    "injury_risk_index": pl.Float64,
}

#: equal weights on the standardized adverse features (documented default;
#: Task 8.2 notes this may be fitted to the self-supervised velo-decline
#: label if the rank gate needs it -- not needed here, see the oracle test).
_INDEX_WEIGHTS: dict = {"velo_trend": -1.0, "velo_drop": -1.0, "trailing_workload": 1.0, "days_rest": -1.0}


def _ensure_date_dtype(df: pl.DataFrame) -> pl.DataFrame:
    """Cast ``game_date`` to ``pl.Date`` if it arrived as the raw Savant
    search CSV's "YYYY-MM-DD" string (a no-op if already ``pl.Date``)."""
    if "game_date" in df.columns and df.schema["game_date"] == pl.Utf8:
        return df.with_columns(pl.col("game_date").str.to_date())
    return df


def _ols_slope(y: np.ndarray) -> float:
    """OLS slope of ``y`` against ``0..len(y)-1``; NaN if fewer than 2 points."""
    n = len(y)
    if n < 2:
        return float("nan")
    x: np.ndarray = np.arange(n, dtype=float)
    x_mean, y_mean = x.mean(), y.mean()
    denom = ((x - x_mean) ** 2).sum()
    if denom == 0:
        return float("nan")
    return float(((x - x_mean) * (y - y_mean)).sum() / denom)


def _per_pitcher_trends(group: pl.DataFrame, window: int) -> pl.DataFrame:
    group = group.sort("game_date")
    dates = group["game_date"].to_list()
    fb_velos = group["fb_velo"].to_list()
    pitches = group["pitches_game"].to_list()

    velo_trend, velo_drop, trailing_workload, days_rest = [], [], [], []
    for i in range(len(group)):
        prior_start = max(0, i - window)
        prior_velos = [v for v in fb_velos[prior_start:i] if v is not None]
        prior_pitches = pitches[prior_start:i]

        velo_trend.append(_ols_slope(np.array(prior_velos)) if len(prior_velos) >= 2 else None)
        if prior_velos and fb_velos[i] is not None:
            velo_drop.append(float(np.mean(prior_velos)) - fb_velos[i])
        else:
            velo_drop.append(None)
        trailing_workload.append(float(np.mean(prior_pitches)) if prior_pitches else None)
        days_rest.append(float((dates[i] - dates[i - 1]).days) if i > 0 else None)

    return group.with_columns(
        pl.Series("velo_trend", velo_trend, dtype=pl.Float64),
        pl.Series("velo_drop", velo_drop, dtype=pl.Float64),
        pl.Series("trailing_workload", trailing_workload, dtype=pl.Float64),
        pl.Series("days_rest", days_rest, dtype=pl.Float64),
    )


def pitcher_appearance_trends(
    pitches: pl.DataFrame, *, window: int = 5, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Leakage-safe per-appearance trailing velocity/workload trends.

    Args:
        pitches: Raw (or feature-substrate) pitch frame carrying ``pitcher``,
            ``game_pk``, ``game_date``, ``pitch_type``, ``release_speed``.
        window: Number of trailing PRIOR appearances used for the rolling
            statistics (never includes the current appearance).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(pitcher, game_pk, game_date)``: ``fb_velo`` (this game's mean
        fastball ``release_speed``), ``velo_trend`` (OLS slope of ``fb_velo``
        over the trailing ``window`` prior appearances), ``velo_drop``
        (trailing-baseline mean minus this game's ``fb_velo``),
        ``pitches_game``, ``trailing_workload`` (mean ``pitches_game`` over
        the trailing window), ``days_rest``. The first appearance for a
        pitcher has null trailing stats (no prior data). Empty input returns
        a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_injury import pitcher_appearance_trends
            out = pitcher_appearance_trends(raw_pitches, window=5)
            print(out.select("game_date", "velo_drop", "days_rest").tail())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    required = ("pitcher", "game_pk", "game_date", "pitch_type", "release_speed")
    if pitches is None or pitches.height == 0 or not all(c in pitches.columns for c in required):
        out = pl.DataFrame(schema=_TRENDS_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = pitches.with_columns(pl.col("pitcher").cast(pl.Int64), pl.col("game_pk").cast(pl.Int64))
    df = _ensure_date_dtype(df)
    fb = df.filter(pl.col("pitch_type").is_in(list(_FASTBALL_TYPES)))
    per_game = (
        df.group_by("pitcher", "game_pk", "game_date")
        .agg(pl.len().alias("pitches_game"))
        .join(
            fb.group_by("pitcher", "game_pk", "game_date").agg(pl.col("release_speed").mean().alias("fb_velo")),
            on=["pitcher", "game_pk", "game_date"],
            how="left",
        )
    )

    results = [
        _per_pitcher_trends(per_game.filter(pl.col("pitcher") == pid), window)
        for pid in per_game["pitcher"].unique(maintain_order=True).to_list()
    ]
    out = pl.concat(results, how="diagonal_relaxed").select(
        "pitcher",
        "game_pk",
        "game_date",
        "fb_velo",
        "velo_trend",
        "velo_drop",
        "pitches_game",
        "trailing_workload",
        "days_rest",
    )
    return out.to_pandas() if return_as_pandas else out


def _standardize(col: pl.Expr) -> pl.Expr:
    mu, sd = col.mean(), col.std()
    return pl.when(sd > 0).then((col - mu) / sd).otherwise(0.0)


def mlb_injury_risk(
    pitches: pl.DataFrame,
    *,
    as_of_date: Optional[dt.date] = None,
    window: int = 5,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Composite pitcher injury-risk index from leakage-safe trailing trends.

    Args:
        pitches: Raw pitch frame (from :func:`sportsdataverse.mlb.mlb_statcast_search`).
        as_of_date: When given, restricts input to
            ``game_date < as_of_date`` via
            :func:`sportsdataverse.mlb.mlb_pitching_constants.as_of_split`
            before computing trends (an additional leakage boundary on top of
            the per-appearance trailing-window logic).
        window: Trailing-window size passed to :func:`pitcher_appearance_trends`.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``game_pk``, ``game_date``, ``injury_risk_index`` —
        equal-weighted sum of standardized adverse features (``-velo_trend``,
        ``-velo_drop``, ``trailing_workload``, ``-days_rest``; higher =
        more risk). Empty input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_injury import mlb_injury_risk
            out = mlb_injury_risk(raw_pitches)
            print(out.sort("injury_risk_index", descending=True).head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0:
        out = pl.DataFrame(schema=_INJURY_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    dated = _ensure_date_dtype(pitches)
    df = as_of_split(dated, as_of_date) if as_of_date is not None else dated
    if df.height == 0:
        out = pl.DataFrame(schema=_INJURY_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    trends = pitcher_appearance_trends(df, window=window)
    trends = trends.with_columns(
        (
            _INDEX_WEIGHTS["velo_trend"] * _standardize(pl.col("velo_trend"))
            + _INDEX_WEIGHTS["velo_drop"] * _standardize(pl.col("velo_drop"))
            + _INDEX_WEIGHTS["trailing_workload"] * _standardize(pl.col("trailing_workload"))
            + _INDEX_WEIGHTS["days_rest"] * _standardize(pl.col("days_rest"))
        ).alias("injury_risk_index")
    )
    out = trends.select("pitcher", "game_pk", "game_date", "injury_risk_index")
    return out.to_pandas() if return_as_pandas else out
