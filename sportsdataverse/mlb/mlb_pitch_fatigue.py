"""Times-through-order / fatigue ④ — fitted run-value penalty by TTO.

:func:`tto_penalty_table` aggregates observed run value by
``times_through_order`` (compute-on-demand, no fitted model).
:func:`mlb_times_through_order` applies the **fitted** per-TTO marginal
penalty (``mlb_pitching_constants.get_baselines(season).tto_penalty``,
fitted by ``dev/mlb_pitching/fit_fatigue.py`` via OLS on real Statcast data)
as a per-pitch ``fatigue_rv_adj`` column.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_pitching_constants import get_baselines

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports
    import pandas as pd

__all__ = ["tto_penalty_table", "add_velo_drop_from_start", "mlb_times_through_order"]

_TTO_TABLE_SCHEMA: dict = {
    "times_through_order": pl.Int64,
    "mean_run_value": pl.Float64,
    "penalty_vs_first": pl.Float64,
    "n": pl.Int64,
}
_TTO_OUTPUT_SCHEMA: dict = {
    "pitcher": pl.Int64,
    "game_pk": pl.Int64,
    "at_bat_number": pl.Int64,
    "pitch_number": pl.Int64,
    "times_through_order": pl.Int64,
    "fatigue_rv_adj": pl.Float64,
}


def tto_penalty_table(feats: pl.DataFrame, *, return_as_pandas: bool = False) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Observed mean run value by times-through-order, with the penalty vs TTO=1.

    Args:
        feats: Output of
            :func:`sportsdataverse.mlb.mlb_pitch_features.add_sequence_features`
            (needs ``times_through_order`` and ``run_value``).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``times_through_order``, ``mean_run_value``, ``penalty_vs_first``
        (``mean_run_value`` minus the TTO=1 mean run value), ``n``. Empty
        input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
            from sportsdataverse.mlb.mlb_pitch_fatigue import tto_penalty_table
            feats = add_sequence_features(pitch_features(raw_pitches))
            out = tto_penalty_table(feats)
            print(out.sort("times_through_order"))

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    required = ("times_through_order", "run_value")
    if feats is None or feats.height == 0 or not all(c in feats.columns for c in required):
        out = pl.DataFrame(schema=_TTO_TABLE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    table = feats.group_by("times_through_order").agg(
        pl.col("run_value").mean().alias("mean_run_value"),
        pl.len().alias("n"),
    )
    first_mean = table.filter(pl.col("times_through_order") == 1)["mean_run_value"]
    baseline = float(first_mean[0]) if first_mean.len() > 0 else 0.0
    table = table.with_columns((pl.col("mean_run_value") - baseline).alias("penalty_vs_first"))
    out = table.sort("times_through_order").select("times_through_order", "mean_run_value", "penalty_vs_first", "n")
    return out.to_pandas() if return_as_pandas else out


def add_velo_drop_from_start(feats: pl.DataFrame) -> pl.DataFrame:
    """Add ``velo_drop_from_start`` = this-game first-pitch fastball velo minus current velo.

    Positive values mean velocity has dropped since the pitcher's first pitch
    of the game (a fatigue signal). Computed within ``(game_pk, pitcher)``,
    sorted by ``(at_bat_number, pitch_number)`` -- no cross-game leakage.

    Args:
        feats: Output of
            :func:`sportsdataverse.mlb.mlb_pitch_features.add_sequence_features`
            (needs ``release_speed``).

    Returns:
        ``feats`` plus ``velo_drop_from_start``.
    """
    if "release_speed" not in feats.columns:
        return feats.with_columns(pl.lit(None, dtype=pl.Float64).alias("velo_drop_from_start"))
    df = feats.sort(["game_pk", "pitcher", "at_bat_number", "pitch_number"])
    first_velo = pl.col("release_speed").first().over(["game_pk", "pitcher"])
    return df.with_columns((first_velo - pl.col("release_speed")).alias("velo_drop_from_start"))


def mlb_times_through_order(
    pitches: pl.DataFrame, *, season: int = 2024, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-pitch fitted times-through-order fatigue adjustment.

    Args:
        pitches: Output of
            :func:`sportsdataverse.mlb.mlb_pitch_features.add_sequence_features`
            (needs ``times_through_order``).
        season: Season year, selects the fitted ``tto_penalty`` coefficients
            via :func:`sportsdataverse.mlb.mlb_pitching_constants.get_baselines`.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        ``pitcher``, ``game_pk``, ``at_bat_number``, ``pitch_number``,
        ``times_through_order``, ``fatigue_rv_adj`` (the fitted marginal
        penalty for that TTO level). Empty input returns a zero-row frame
        with this schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_pitch_features import pitch_features, add_sequence_features
            from sportsdataverse.mlb.mlb_pitch_fatigue import mlb_times_through_order
            feats = add_sequence_features(pitch_features(raw_pitches))
            out = mlb_times_through_order(feats, season=2024)
            print(out.select("times_through_order", "fatigue_rv_adj").unique())

        See Also:
            * `baseballr`_ -- companion R package for Statcast-based pitching analysis.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches is None or pitches.height == 0 or "times_through_order" not in pitches.columns:
        out = pl.DataFrame(schema=_TTO_OUTPUT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    penalties = get_baselines(season).tto_penalty
    tto_expr = (
        pl.when(pl.col("times_through_order") <= len(penalties))
        .then(
            pl.col("times_through_order").replace_strict(
                {i + 1: p for i, p in enumerate(penalties)}, default=penalties[-1], return_dtype=pl.Float64
            )
        )
        .otherwise(penalties[-1])
    )

    df = pitches.with_columns(tto_expr.alias("fatigue_rv_adj"))
    out = df.select("pitcher", "game_pk", "at_bat_number", "pitch_number", "times_through_order", "fatigue_rv_adj")
    return out.to_pandas() if return_as_pandas else out
