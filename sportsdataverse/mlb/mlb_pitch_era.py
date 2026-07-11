"""xERA + SIERA-like ③ — parametric ERA estimators from Statcast expected wOBA.

Two ERA-scale estimators, both compute-on-demand (no bundled artifact):

- :func:`x_era` — a **parametric** wOBA-to-runs conversion:
  ``x_era = league_era + ((x_woba - league_woba) / woba_scale) * pa_per_9``,
  with per-season league baselines from
  :func:`sportsdataverse.mlb.mlb_pitching_constants.get_baselines`.
- :func:`siera_like` — the published SIERA functional form
  ``b0 + b1*k_pct + b2*bb_pct + b3*gb_pct + b4*gb_pct**2 + b5*(k_pct*gb_pct)``
  with OLS-fitted coefficients from ``mlb_pitching_constants.siera_coef``
  (see ``dev/mlb_pitching/fit_era_siera.py``).

Follows the published SIERA methodology (cited as a reference; no code
copied, so no license obligation).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import get_baselines

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["x_era", "siera_like", "mlb_pitch_era"]

_STRIKEOUT_EVENTS = {"strikeout", "strikeout_double_play"}
_WALK_EVENTS = {"walk", "intent_walk", "hit_by_pitch"}
_GROUND_BALL_TYPES = {"ground_ball"}

_XERA_SCHEMA: dict = {"pitcher": pl.Int64, "season": pl.Int64, "x_woba": pl.Float64, "x_era": pl.Float64}
_SIERA_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "season": pl.Int64,
    "k_pct": pl.Float64,
    "bb_pct": pl.Float64,
    "gb_pct": pl.Float64,
    "siera_like": pl.Float64,
}
_COMBINED_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "season": pl.Int64,
    "x_woba": pl.Float64,
    "x_era": pl.Float64,
    "k_pct": pl.Float64,
    "bb_pct": pl.Float64,
    "gb_pct": pl.Float64,
    "siera_like": pl.Float64,
}


def _pitcher_rates(pitches: pl.DataFrame, season: int) -> pl.DataFrame:
    """Per-pitcher batters-faced/strikeout/walk/ground-ball rates for ``season``."""
    df = pitches.with_columns(pl.col("pitcher").cast(pl.Int64))
    pa_ending = df.filter(pl.col("events").is_not_null() & (pl.col("events") != ""))
    per_pitcher = pa_ending.group_by("pitcher").agg(
        pl.len().alias("batters_faced"),
        pl.col("events").is_in(list(_STRIKEOUT_EVENTS)).sum().alias("strikeouts"),
        pl.col("events").is_in(list(_WALK_EVENTS)).sum().alias("walks"),
    )
    if "bb_type" in df.columns:
        batted = df.filter(pl.col("bb_type").is_not_null() & (pl.col("bb_type") != ""))
        gb = batted.group_by("pitcher").agg(
            pl.len().alias("batted_balls"),
            pl.col("bb_type").is_in(list(_GROUND_BALL_TYPES)).sum().alias("ground_balls"),
        )
        per_pitcher = per_pitcher.join(gb, on="pitcher", how="left")
    else:
        per_pitcher = per_pitcher.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("batted_balls"), pl.lit(None, dtype=pl.Int64).alias("ground_balls")
        )

    per_pitcher = per_pitcher.with_columns(
        (pl.col("strikeouts") / pl.col("batters_faced")).alias("k_pct"),
        (pl.col("walks") / pl.col("batters_faced")).alias("bb_pct"),
        pl.when(pl.col("batted_balls") > 0)
        .then(pl.col("ground_balls") / pl.col("batted_balls"))
        .otherwise(None)
        .alias("gb_pct"),
    )
    return per_pitcher.with_columns(pl.lit(season).cast(pl.Int64).alias("season"))


def x_era(pitches: pl.DataFrame, season: int, *, return_as_pandas: bool = False) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parametric xERA from Statcast expected wOBA allowed.

    Args:
        pitches: Raw (or feature-substrate) pitch frame carrying ``pitcher``
            and ``estimated_woba_using_speedangle``.
        season: Season year (selects the league baseline via
            :func:`sportsdataverse.mlb.mlb_pitching_constants.get_baselines`).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``season``, ``x_woba``, ``x_era``. Empty input returns a
        zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_era import x_era
            out = x_era(raw_pitches, 2024)
            print(out.sort("x_era").head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0 or "estimated_woba_using_speedangle" not in pitches.columns:
        out = pl.DataFrame(schema=_XERA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    b = get_baselines(season)
    df = pitches.with_columns(pl.col("pitcher").cast(pl.Int64))
    batted = df.filter(pl.col("estimated_woba_using_speedangle").is_not_null())
    per_pitcher = batted.group_by("pitcher").agg(pl.col("estimated_woba_using_speedangle").mean().alias("x_woba"))
    per_pitcher = per_pitcher.with_columns(
        pl.lit(season).cast(pl.Int64).alias("season"),
        (b.league_era + ((pl.col("x_woba") - b.league_woba) / b.woba_scale) * b.pa_per_9).alias("x_era"),
    )
    out = per_pitcher.select("pitcher", "season", "x_woba", "x_era")
    return out.to_pandas() if return_as_pandas else out


def siera_like(
    pitches: pl.DataFrame, season: int, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """SIERA-like ERA estimator from K%/BB%/GB% (fitted OLS coefficients).

    Args:
        pitches: Raw pitch frame carrying ``pitcher``, ``events``, and
            (optionally) ``bb_type``.
        season: Season year (unused in the formula itself, carried through
            for join convenience with :func:`x_era`).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``season``, ``k_pct``, ``bb_pct``, ``gb_pct``,
        ``siera_like``. Empty input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_era import siera_like
            out = siera_like(raw_pitches, 2024)
            print(out.sort("siera_like").head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0 or "events" not in pitches.columns:
        out = pl.DataFrame(schema=_SIERA_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    b = get_baselines(season)
    b0, b1, b2, b3, b4, b5 = b.siera_coef
    rates = _pitcher_rates(pitches, season)
    rates = rates.with_columns(
        (
            b0
            + b1 * pl.col("k_pct")
            + b2 * pl.col("bb_pct")
            + b3 * pl.col("gb_pct").fill_null(0.0)
            + b4 * pl.col("gb_pct").fill_null(0.0) ** 2
            + b5 * (pl.col("k_pct") * pl.col("gb_pct").fill_null(0.0))
        ).alias("siera_like")
    )
    out = rates.select("pitcher", "season", "k_pct", "bb_pct", "gb_pct", "siera_like")
    return out.to_pandas() if return_as_pandas else out


def mlb_pitch_era(
    pitches: pl.DataFrame, seasons: int, *, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Combined xERA + SIERA-like estimator (model ③).

    Args:
        pitches: Raw pitch frame (from :func:`sportsdataverse.mlb.mlb_statcast_search`).
        seasons: Season year.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``season``, ``x_woba``, ``x_era``, ``k_pct``, ``bb_pct``,
        ``gb_pct``, ``siera_like``. Empty input returns a zero-row frame with
        this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_era import mlb_pitch_era
            out = mlb_pitch_era(raw_pitches, 2024)
            print(out.select("pitcher", "x_era", "siera_like").head())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0:
        out = pl.DataFrame(schema=_COMBINED_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    xe = x_era(pitches, seasons)
    sr = siera_like(pitches, seasons)
    assert xe.schema["pitcher"] == sr.schema["pitcher"]
    out = xe.join(sr.drop("season"), on="pitcher", how="full", coalesce=True)
    out = out.select("pitcher", "season", "x_woba", "x_era", "k_pct", "bb_pct", "gb_pct", "siera_like")
    return out.to_pandas() if return_as_pandas else out
