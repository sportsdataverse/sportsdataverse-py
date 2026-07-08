"""Expected-points-per-shot (xPoints), model ① of the shot-quality spine.

Compute-on-demand empirical-Bayes make-rate table over ``zone x type`` cells
(each cell shrunk toward its parent-zone mean by ``n / (n + k)``), plus the
per-shot scorer that joins it back. No bundled artifact -- the model is a
returned frame the caller can persist or refit at will.

Methodology reference: Hoop-Math-style shot zones + standard
empirical-Bayes shrinkage -- methodology only, no ported code.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_shot_quality_constants import get_constants

__all__ = ["mbb_shot_quality", "mbb_shot_quality_model"]

_MODEL_SCHEMA = {
    "shot_zone": pl.Utf8,
    "shot_type": pl.Utf8,
    "n": pl.Int64,
    "make_rate_raw": pl.Float64,
    "make_rate_shrunk": pl.Float64,
    "point_value": pl.Float64,
    "xpoints": pl.Float64,
}


@overload
def mbb_shot_quality_model(
    shots: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_shot_quality_model(
    shots: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_shot_quality_model(
    shots: pl.DataFrame,
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Empirical-Bayes ``zone x type`` make-rate / xPoints table.

    Each cell's raw make rate is shrunk toward its PARENT-ZONE mean by
    ``n / (n + k)`` with ``k = get_constants(league).shrink_k_zone``
    pseudo-attempts, so sparse cells (e.g. tip-ins in the mid zone) borrow
    strength from their zone; ``xpoints = make_rate_shrunk * point_value``
    (the cell's modal point value).

    Args:
        shots: Canonical shot frame (needs ``shot_zone, shot_type, made,
            point_value``).
        league: ``"mens"`` or ``"womens"`` (selects the shrinkage ``k``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``(shot_zone, shot_type)``: ``shot_zone, shot_type, n,
        make_rate_raw, make_rate_shrunk, point_value, xpoints``. Empty input
        returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality_model
            model = mbb_shot_quality_model(mbb_shot_data(2025))

        Pipeline next step (one line)::

            model.sort("xpoints", descending=True).head(5)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if shots.is_empty():
        out = pl.DataFrame(schema=_MODEL_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    k = float(get_constants(league).shrink_k_zone)
    cells = shots.group_by("shot_zone", "shot_type").agg(
        pl.len().cast(pl.Int64).alias("n"),
        pl.col("made").cast(pl.Float64).mean().alias("make_rate_raw"),
        pl.col("point_value").cast(pl.Float64).mode().first().alias("point_value"),
    )
    zones = shots.group_by("shot_zone").agg(pl.col("made").cast(pl.Float64).mean().alias("_zone_mean"))
    out = (
        cells.join(zones, on="shot_zone", how="left")
        .with_columns(
            ((pl.col("n") * pl.col("make_rate_raw") + k * pl.col("_zone_mean")) / (pl.col("n") + k)).alias(
                "make_rate_shrunk"
            )
        )
        .with_columns((pl.col("make_rate_shrunk") * pl.col("point_value")).alias("xpoints"))
        .select(list(_MODEL_SCHEMA))
        .sort("shot_zone", "shot_type")
    )
    return out.to_pandas() if return_as_pandas else out


@overload
def mbb_shot_quality(
    shots: pl.DataFrame,
    *,
    model: "pl.DataFrame | None" = None,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_shot_quality(
    shots: pl.DataFrame,
    *,
    model: "pl.DataFrame | None" = None,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_shot_quality(
    shots: pl.DataFrame,
    *,
    model: "pl.DataFrame | None" = None,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score each shot with ``xmake`` / ``xpoints`` from the cell table.

    Args:
        shots: Canonical shot frame.
        model: A ``mbb_shot_quality_model`` table. When ``None`` it is built
            from ``shots`` itself -- convenient, but leakage-safe evaluation
            should pass a model fit on PRIOR data.
        league: ``"mens"`` or ``"womens"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``shots``'s columns plus ``xmake:Float64, xpoints:Float64`` (null
        for cells absent from the model). Empty input returns the input
        schema plus the two columns, zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality
            scored = mbb_shot_quality(mbb_shot_data(2025))

        Pipeline next step (one line)::

            scored.group_by("team_id").agg(pl.col("xpoints").sum()).sort("xpoints", descending=True)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if shots.is_empty():
        out = shots.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("xmake"), pl.lit(None, dtype=pl.Float64).alias("xpoints")
        )
        return out.to_pandas() if return_as_pandas else out
    m = model if model is not None else mbb_shot_quality_model(shots, league=league)
    assert shots.schema["shot_zone"] == m.schema["shot_zone"], "join-key dtype mismatch: shot_zone"
    assert shots.schema["shot_type"] == m.schema["shot_type"], "join-key dtype mismatch: shot_type"
    out = shots.join(
        m.select("shot_zone", "shot_type", pl.col("make_rate_shrunk").alias("xmake"), "xpoints"),
        on=["shot_zone", "shot_type"],
        how="left",
    )
    return out.to_pandas() if return_as_pandas else out
