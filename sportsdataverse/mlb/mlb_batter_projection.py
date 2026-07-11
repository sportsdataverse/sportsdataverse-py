"""④ Batter aging curve + Marcel projection -- delta-method aging (Tango/Lichtman)
and a Marcel-the-Monkey-style weighted, regressed, age-adjusted xwOBA
projection built entirely on ① ``mlb_expected_stats`` player-season history.

The as-of-date leakage boundary is enforced via
:func:`sportsdataverse.mlb.mlb_hitting_constants.as_of_seasons_split`: a
projection for season *Y* only ever sees rows with ``season < Y``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Tuple, Union, overload

import polars as pl

from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats
from sportsdataverse.mlb.mlb_hitting_constants import as_of_seasons_split

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

_PROJECTION_SCHEMA = {"batter": pl.Int64, "age": pl.Int64, "proj_xwoba": pl.Float64, "proj_pa": pl.Float64}

_AGING_CURVE_SCHEMA = {"age": pl.Int64, "delta": pl.Float64, "curve": pl.Float64}

#: Fitted defaults for the Marcel regression (dev/mlb_hitting/fit_marcel.py
#: backtest, 2021-2023 -> 2024 xwOBA OOS sweep): 1200 phantom league-average
#: PAs and the 2021-2023 committed-history league mean.
DEFAULT_REGRESSION_PA = 1200.0
DEFAULT_WEIGHTS: Tuple[float, float, float] = (5.0, 4.0, 3.0)


def aging_curve(player_seasons: pl.DataFrame, *, metric: str = "xwoba", min_pa: int = 200) -> pl.DataFrame:
    """Delta-method (Tango/Lichtman) aging curve from a player-season panel.

    For every player with two consecutive seasons (age *a* and age *a+1*),
    computes the year-over-year change in ``metric``, weighted by
    ``min(pa_a, pa_a+1)``. The age-level weighted mean delta is cumulatively
    summed and re-centered so the curve's peak (its argmax) sits at 0 --
    the shape, not the level, is what the oracle gate validates. This method
    has a well-documented survivor-bias caveat at the tails (only players who
    keep a roster spot contribute a delta); the shape near the peak is robust
    to it.

    Args:
        player_seasons: Frame with ``batter``, ``season``, ``age``, ``pa``,
            and the metric column (default ``xwoba``).
        metric: Column name to build the aging curve for.
        min_pa: Minimum PA in EITHER season for a delta to be included.

    Returns:
        One row per ``age``: ``delta`` (Float64, min-PA-weighted mean
        year-over-year change into that age) and ``curve`` (Float64,
        cumulative sum of ``delta``, re-centered at its own peak).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_batter_projection import aging_curve

            curve = aging_curve(player_season_history)
            peak_age = curve.sort("curve", descending=True)["age"][0]
    """
    prev = player_seasons.select(
        pl.col("batter"),
        pl.col("age").alias("_age_from"),
        (pl.col("age") + 1).alias("age"),
        pl.col(metric).alias("_metric_from"),
        pl.col("pa").alias("_pa_from"),
    )
    cur = player_seasons.select(
        pl.col("batter"), pl.col("age"), pl.col(metric).alias("_metric_to"), pl.col("pa").alias("_pa_to")
    )
    pairs = prev.join(cur, on=["batter", "age"], how="inner").filter(
        (pl.col("_pa_from") >= min_pa) & (pl.col("_pa_to") >= min_pa)
    )
    if pairs.height == 0:
        return pl.DataFrame(schema=_AGING_CURVE_SCHEMA)

    pairs = pairs.with_columns(
        (pl.col("_metric_to") - pl.col("_metric_from")).alias("_delta"),
        pl.min_horizontal("_pa_from", "_pa_to").alias("_weight"),
    )
    by_age = (
        pairs.group_by("age")
        .agg(((pl.col("_delta") * pl.col("_weight")).sum() / pl.col("_weight").sum()).alias("delta"))
        .sort("age")
    )
    curve = by_age.with_columns(pl.col("delta").cum_sum().alias("_raw_curve"))
    peak = curve["_raw_curve"].max()
    return (
        curve.with_columns((pl.col("_raw_curve") - pl.lit(peak)).alias("curve"))
        .select("age", "delta", "curve")
        .cast(_AGING_CURVE_SCHEMA)
    )


def marcel_projection(
    player_seasons: pl.DataFrame,
    target_season: int,
    aging: pl.DataFrame,
    *,
    weights: Tuple[float, float, float] = DEFAULT_WEIGHTS,
    regression_pa: float = DEFAULT_REGRESSION_PA,
    league_xwoba: Optional[float] = None,
) -> pl.DataFrame:
    """Marcel-the-Monkey xwOBA projection for ``target_season``, age-adjusted.

    Uses **only** ``as_of_seasons_split(player_seasons, target_season)`` (the
    leakage boundary) -- rows at or after ``target_season`` never influence
    the projection. Blends each batter's 3 most recent prior seasons with
    ``weights`` (most recent first) x PA, regresses toward ``league_xwoba``
    by ``regression_pa`` phantom league-average PAs, then applies the
    ``aging`` curve's age-adjustment for the batter's age in ``target_season``.

    Args:
        player_seasons: Full history frame (``batter``, ``season``, ``age``,
            ``xwoba``, ``pa``) -- filtered internally to ``season <
            target_season``.
        target_season: The season being projected.
        aging: Output of :func:`aging_curve`.
        weights: Per-season-back weights (most recent season first),
            multiplied by that season's PA.
        regression_pa: Phantom league-average PAs regression strength.
        league_xwoba: League-average xwOBA to regress toward; if ``None``,
            computed as the PA-weighted mean of the as-of history.

    Returns:
        One row per ``batter``: ``proj_xwoba``, ``proj_pa`` (Float64).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_batter_projection import aging_curve, marcel_projection

            curve = aging_curve(history)
            proj = marcel_projection(history, 2024, curve)
    """
    asof = as_of_seasons_split(player_seasons, target_season)
    if asof.height == 0:
        return pl.DataFrame(schema={"batter": pl.Int64, "proj_xwoba": pl.Float64, "proj_pa": pl.Float64})

    if league_xwoba is None:
        league_xwoba = float((asof["xwoba"] * asof["pa"]).sum() / asof["pa"].sum())

    ranked = asof.with_columns((pl.lit(target_season) - pl.col("season")).alias("_years_back")).filter(
        (pl.col("_years_back") >= 1) & (pl.col("_years_back") <= len(weights))
    )

    weight_map = {i + 1: w for i, w in enumerate(weights)}
    ranked = ranked.with_columns(
        pl.col("_years_back").replace_strict(weight_map, default=0.0, return_dtype=pl.Float64).alias("_year_weight")
    )
    ranked = ranked.with_columns((pl.col("_year_weight") * pl.col("pa")).alias("_w"))

    per_batter = ranked.group_by("batter").agg(
        (pl.col("xwoba") * pl.col("_w")).sum().alias("_wsum"),
        pl.col("_w").sum().alias("_wtotal"),
        pl.col("pa").sum().alias("_recent_pa"),
        pl.col("age").sort_by("season", descending=True).first().alias("_last_age"),
    )
    per_batter = per_batter.with_columns(
        (
            (pl.col("_wsum") + pl.lit(league_xwoba) * pl.lit(regression_pa))
            / (pl.col("_wtotal") + pl.lit(regression_pa))
        ).alias("_regressed_xwoba"),
        (pl.col("_last_age") + 1).alias("_target_age"),
    )

    aging_lookup = aging.select(pl.col("age"), pl.col("delta"))
    per_batter = per_batter.join(aging_lookup, left_on="_target_age", right_on="age", how="left").with_columns(
        pl.col("delta").fill_null(0.0).alias("_age_adj")
    )

    return per_batter.with_columns(
        (pl.col("_regressed_xwoba") + pl.col("_age_adj")).alias("proj_xwoba"),
        pl.col("_recent_pa").cast(pl.Float64).alias("proj_pa"),
        pl.col("_target_age").alias("age"),
    ).select("batter", "age", "proj_xwoba", "proj_pa")


@overload
def mlb_batter_projection(
    target_season: int, *, history: Optional[pl.DataFrame] = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...
@overload
def mlb_batter_projection(
    target_season: int, *, history: Optional[pl.DataFrame] = ..., return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...
def mlb_batter_projection(
    target_season: int,
    *,
    history: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Next-season xwOBA projection (Marcel + delta-method aging) for every batter.

    If ``history`` is ``None``, builds player-season xwOBA history via
    :func:`sportsdataverse.mlb.mlb_expected_stats.mlb_expected_stats` across
    the three seasons before ``target_season`` (ages must already be present
    on a supplied ``history`` frame -- this convenience path is intended for
    callers who already maintain an age-joined roster history).

    Args:
        target_season: The season being projected.
        history: Pre-built player-season history (``batter``, ``season``,
            ``age``, ``xwoba``, ``pa``). If ``None``, uses
            ``mlb_expected_stats`` over ``target_season - 3 .. target_season - 1``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``batter``: ``age``, ``proj_xwoba``, ``proj_pa``. Empty
        history returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_batter_projection import mlb_batter_projection

            proj = mlb_batter_projection(2024, history=player_season_history)
            print(proj.shape)

        Pipeline next step (one line)::

            proj.sort("proj_xwoba", descending=True).head()

        See Also:
            * `baseballr`_ -- Statcast helper functions (R)

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if history is None:
        frames = []
        for yr in (target_season - 3, target_season - 2, target_season - 1):
            frames.append(mlb_expected_stats(f"{yr}-01-01", f"{yr}-12-01"))
        history = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()

    if history is None or history.height == 0:
        empty = pl.DataFrame(schema=_PROJECTION_SCHEMA)
        if return_as_pandas:
            return empty.to_pandas()
        return empty

    history = history.with_columns(pl.col("batter").cast(pl.Int64))
    curve = aging_curve(history)
    proj = marcel_projection(history, target_season, curve)
    result = proj.select("batter", "age", "proj_xwoba", "proj_pa").cast(_PROJECTION_SCHEMA).sort("batter")

    if return_as_pandas:
        return result.to_pandas()
    return result
