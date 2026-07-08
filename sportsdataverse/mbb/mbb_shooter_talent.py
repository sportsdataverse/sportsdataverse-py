"""Shooter true-talent, model ③ of the shot-quality spine.

Per-shooter make% over expected (``oe_pct``), empirical-Bayes-regressed by
``n / (n + k)`` where ``k`` is FITTED split-half (``fit_shrinkage_k``): the
``k`` minimizing the error of the regressed first half predicting the raw
second half. Standard stabilization methodology (Tango-style regression to
the mean) -- methodology only, no ported code.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants

__all__ = ["fit_shrinkage_k", "mbb_shooter_talent", "talent_split_mse"]

_SCHEMA = {
    "shooter_id": pl.Utf8,
    "n_shots": pl.Int64,
    "make_rate": pl.Float64,
    "xmake_mean": pl.Float64,
    "oe_pct": pl.Float64,
    "oe_pct_regressed": pl.Float64,
    "points_over_expected": pl.Float64,
    "poe_per_100": pl.Float64,
}


@overload
def mbb_shooter_talent(
    scored: pl.DataFrame,
    *,
    league: str = "mens",
    k: "float | None" = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_shooter_talent(
    scored: pl.DataFrame,
    *,
    league: str = "mens",
    k: "float | None" = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_shooter_talent(
    scored: pl.DataFrame,
    *,
    league: str = "mens",
    k: "float | None" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-shooter EB-regressed make% over expected + points over expected.

    Args:
        scored: ``mbb_shot_quality`` output (needs ``shooter_id, made,
            point_value, xmake, xpoints``).
        league: ``"mens"`` or ``"womens"`` (default ``k`` source).
        k: Shrinkage pseudo-shots; ``None`` uses
            ``get_constants(league).shrink_k_talent`` (fitted split-half).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per shooter: ``shooter_id:Utf8, n_shots, make_rate,
        xmake_mean, oe_pct, oe_pct_regressed, points_over_expected,
        poe_per_100``. Empty input returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shooter_talent
            talent = mbb_shooter_talent(mbb_shot_quality(mbb_shot_data(2025)))

        Pipeline next step (one line)::

            talent.filter(pl.col("n_shots") >= 200).sort("oe_pct_regressed", descending=True).head(15)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if scored.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    kk = float(k) if k is not None else float(get_constants(league).shrink_k_talent)
    out = (
        scored.filter(pl.col("shooter_id").is_not_null())
        .group_by("shooter_id")
        .agg(
            pl.len().cast(pl.Int64).alias("n_shots"),
            pl.col("made").cast(pl.Float64).mean().alias("make_rate"),
            pl.col("xmake").mean().alias("xmake_mean"),
            (
                (pl.col("point_value").cast(pl.Float64) * pl.col("made").cast(pl.Float64)).sum()
                - pl.col("xpoints").sum()
            ).alias("points_over_expected"),
        )
        .with_columns((pl.col("make_rate") - pl.col("xmake_mean")).alias("oe_pct"))
        .with_columns(
            (pl.col("oe_pct") * pl.col("n_shots") / (pl.col("n_shots") + kk)).alias("oe_pct_regressed"),
            (100.0 * pl.col("points_over_expected") / pl.col("n_shots")).alias("poe_per_100"),
        )
        .select(list(_SCHEMA))
        .sort("oe_pct_regressed", descending=True)
    )
    return out.to_pandas() if return_as_pandas else out


def _split_halves(scored: pl.DataFrame, seed: int) -> pl.DataFrame:
    """Seeded within-shooter half assignment (balanced even/odd interleave)."""
    rng = np.random.default_rng(seed)
    shuffled = scored.filter(pl.col("shooter_id").is_not_null()).with_columns(
        pl.Series("_r", rng.random(scored.filter(pl.col("shooter_id").is_not_null()).height))
    )
    return shuffled.sort("shooter_id", "_r").with_columns(
        (pl.int_range(pl.len()).over("shooter_id") % 2).alias("_half")
    )


def _half_oe(half: pl.DataFrame) -> pl.DataFrame:
    return half.group_by("shooter_id").agg(
        pl.len().cast(pl.Int64).alias("n"),
        (pl.col("made").cast(pl.Float64).mean() - pl.col("xmake").mean()).alias("oe"),
    )


def talent_split_mse(scored: pl.DataFrame, *, k: float, seed: int = 0) -> float:
    """Weighted MSE of the k-regressed first half predicting the raw second half.

    Args:
        scored: ``mbb_shot_quality`` output.
        k: Shrinkage pseudo-shots to evaluate.
        seed: Split seed.

    Returns:
        ``sum(n_h2 * (oe_h1 * n_h1/(n_h1+k) - oe_h2)^2) / sum(n_h2)``.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shooter_talent import talent_split_mse
            talent_split_mse(scored, k=200.0)
    """
    halves = _split_halves(scored, seed)
    h1 = _half_oe(halves.filter(pl.col("_half") == 0))
    h2 = _half_oe(halves.filter(pl.col("_half") == 1))
    assert h1.schema["shooter_id"] == h2.schema["shooter_id"]
    j = h1.join(h2, on="shooter_id", how="inner", suffix="_h2")
    assert j.height > 0, "split-half join produced no shooters"
    pred = j.get_column("oe").to_numpy() * j.get_column("n").to_numpy() / (j.get_column("n").to_numpy() + k)
    err = pred - j.get_column("oe_h2").to_numpy()
    w = j.get_column("n_h2").to_numpy().astype(float)
    return float((w * err**2).sum() / w.sum())


def fit_shrinkage_k(scored: pl.DataFrame, *, seed: int = 0) -> float:
    """Fit the talent shrinkage ``k`` split-half (see module docstring).

    Args:
        scored: ``mbb_shot_quality`` output.
        seed: Split seed (deterministic fit).

    Returns:
        The ``k`` in ``[1, 5000]`` minimizing :func:`talent_split_mse`.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_shooter_talent import fit_shrinkage_k
            k = fit_shrinkage_k(scored)
    """
    from scipy.optimize import minimize_scalar  # noqa: PLC0415 - optional heavy import

    res = minimize_scalar(
        lambda k: talent_split_mse(scored, k=float(k), seed=seed), bounds=(1.0, 5000.0), method="bounded"
    )
    return float(res.x)
