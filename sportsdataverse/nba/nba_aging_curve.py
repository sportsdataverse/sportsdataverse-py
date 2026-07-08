"""Aging curve / career trajectory ② -- delta-method population age curve.

Builds a per-age value-multiplier curve from within-player consecutive-age
deltas (the published "delta method" popularized by Nate Silver / Neil Paine
at FiveThirtyEight/Basketball-Reference, and used in Kevin Pelton's WARP
aging-curve research), then applies it to age a player's value forward or
back to a peak-centered curve. The bundled artifact is fit offline in
``dev/nba_draft/fit_aging_curve.py``.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_draft_constants import get_constants

__all__ = ["build_aging_deltas", "nba_aging_curve", "nba_career_trajectory"]


def build_aging_deltas(season_values: pl.DataFrame, *, min_minutes: float = 500.0) -> pl.DataFrame:
    """Chain within-player consecutive-age value deltas into a level curve.

    Keeps only within-player age-to-age+1 pairs (no cross-player leak),
    computes a minutes-weighted mean delta per age, chains the deltas into a
    level curve starting from the lowest observed age, and normalizes so the
    curve peaks at ``1.0``.

    Args:
        season_values: Per ``(player_id, age, season_value, minutes)`` rows.
        min_minutes: Minimum minutes for a season to count toward a delta pair.

    Returns:
        Frame ``age:Int64, rel_value:Float64, n_pairs:Int64``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_aging_curve import build_aging_deltas
            season_values = pl.DataFrame({
                "player_id": ["1", "1", "1"], "age": [24, 25, 26],
                "season_value": [10.0, 12.0, 11.0], "minutes": [2000.0, 2000.0, 2000.0],
            })
            build_aging_deltas(season_values)
    """
    df = season_values.filter(pl.col("minutes") >= min_minutes).sort("player_id", "age")
    paired = df.join(
        df.select(
            "player_id",
            (pl.col("age") - 1).alias("age"),
            pl.col("season_value").alias("next_value"),
            pl.col("minutes").alias("next_min"),
        ),
        on=["player_id", "age"],
        how="inner",
    ).with_columns(
        (pl.col("next_value") - pl.col("season_value")).alias("delta"),
        pl.min_horizontal("minutes", "next_min").alias("w"),
    )
    if paired.is_empty():
        return pl.DataFrame(schema={"age": pl.Int64, "rel_value": pl.Float64, "n_pairs": pl.Int64})

    per_age = (
        paired.group_by("age")
        .agg(((pl.col("delta") * pl.col("w")).sum() / pl.col("w").sum()).alias("delta"), pl.len().alias("n_pairs"))
        .sort("age")
    )
    ages = per_age["age"].to_list()
    deltas = per_age["delta"].to_list()
    level = np.concatenate([[0.0], np.cumsum(deltas)])
    curve_ages = ages + [ages[-1] + 1]
    rel = np.asarray(level)
    rel = rel - rel.max() + 1.0
    n_pairs = per_age["n_pairs"].to_list() + [per_age["n_pairs"][-1]]
    return pl.DataFrame({"age": curve_ages, "rel_value": rel, "n_pairs": n_pairs}).cast(
        {"age": pl.Int64, "n_pairs": pl.Int64}
    )


def _load_curve_artifact(league: str) -> dict:
    prefix = get_constants(league).artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_aging_curve.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))  # type: ignore[attr-defined]


@overload
def nba_aging_curve(*, league: str = "nba", return_as_pandas: Literal[False] = False) -> pl.DataFrame: ...


@overload
def nba_aging_curve(*, league: str = "nba", return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def nba_aging_curve(*, league: str = "nba", return_as_pandas: bool = False) -> "pl.DataFrame | pd.DataFrame":
    """Load the bundled per-age value-multiplier curve.

    Args:
        league: ``"nba"``, ``"wnba"``, or ``"gleague"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``age:Int64, rel_value:Float64, peak_age:Float64`` (``peak_age``
        repeated on every row for convenient filtering/joining).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_aging_curve
            curve = nba_aging_curve()
            print(curve.sort("rel_value", descending=True).head(1))

        Pipeline next step (one line)::

            curve.filter(pl.col("age").is_between(24, 30))

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    art = _load_curve_artifact(league)
    out = pl.DataFrame({"age": art["age"], "rel_value": art["rel_value"]}).cast(
        {"age": pl.Int64, "rel_value": pl.Float64}
    )
    out = out.with_columns(pl.lit(float(art["peak_age"])).alias("peak_age"))
    return out.to_pandas() if return_as_pandas else out


@overload
def nba_career_trajectory(
    player_values: pl.DataFrame, *, league: str = "nba", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_career_trajectory(
    player_values: pl.DataFrame, *, league: str = "nba", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_career_trajectory(
    player_values: pl.DataFrame, *, league: str = "nba", return_as_pandas: bool = False
) -> "pl.DataFrame | pd.DataFrame":
    """Age-adjust player-season values with the bundled aging curve.

    Args:
        player_values: Frame ``player_id, age:Int64, value:Float64``.
        league: ``"nba"``, ``"wnba"``, or ``"gleague"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        ``player_values`` plus ``age_adjusted_value`` (``value /
        rel_value(age)``, peak-centered) and ``proj_next_value`` (``value *
        rel_value(age+1) / rel_value(age)``). Ages outside the bundled curve's
        range fall back to ``rel_value = 1.0`` (no adjustment). Empty input
        returns the zero-row schema.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import nba_career_trajectory
            player_values = pl.DataFrame({"player_id": ["1"], "age": [24], "value": [10.0]})
            nba_career_trajectory(player_values)
    """
    schema = {
        "player_id": pl.Utf8,
        "age": pl.Int64,
        "value": pl.Float64,
        "age_adjusted_value": pl.Float64,
        "proj_next_value": pl.Float64,
    }
    if player_values.is_empty():
        return pl.DataFrame(schema=schema).to_pandas() if return_as_pandas else pl.DataFrame(schema=schema)

    curve = nba_aging_curve(league=league).select("age", "rel_value")
    rel_map = dict(zip(curve["age"].to_list(), curve["rel_value"].to_list()))

    def _rel(age: int) -> float:
        return float(rel_map.get(age, 1.0))

    ages = player_values["age"].to_list()
    rel_now = [_rel(a) for a in ages]
    rel_next = [_rel(a + 1) for a in ages]
    out = player_values.with_columns(
        pl.Series("age_adjusted_value", player_values["value"].to_numpy() / np.asarray(rel_now), dtype=pl.Float64),
        pl.Series(
            "proj_next_value",
            player_values["value"].to_numpy() * (np.asarray(rel_next) / np.asarray(rel_now)),
            dtype=pl.Float64,
        ),
    )
    return out.to_pandas() if return_as_pandas else out
