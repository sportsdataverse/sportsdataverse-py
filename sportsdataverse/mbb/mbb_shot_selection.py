"""Shot-selection value, model ② of the shot-quality spine.

Where a shooter/team CHOOSES to shoot from, valued against a league-average
shooter taking the same mix: ``selection_value = mean(xpoints) - league mean
(xpoints)``. Positive = a diet of high-value looks (rim + threes); the
attempt-weighted league sum is zero by construction.
"""

from __future__ import annotations

from typing import Literal, Union, overload

import pandas as pd
import polars as pl

__all__ = ["mbb_shot_selection"]

_GROUPS = ("shooter_id", "team_id")


@overload
def mbb_shot_selection(
    scored: pl.DataFrame,
    *,
    group: str = "shooter_id",
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_shot_selection(
    scored: pl.DataFrame,
    *,
    group: str = "shooter_id",
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_shot_selection(
    scored: pl.DataFrame,
    *,
    group: str = "shooter_id",
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per shooter/team expected points per attempt vs the league-average mix.

    Args:
        scored: ``mbb_shot_quality`` output (needs ``xpoints, point_value,
            made`` + the group column).
        group: ``"shooter_id"`` or ``"team_id"``.
        league: ``"mens"`` or ``"womens"`` (interface parity; the math is
            league-free).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per group: ``{group}:Utf8, n_shots:Int64, xppp,
        actual_ppp, selection_value, selection_value_total`` (all value
        columns Float64). The attempt-weighted ``selection_value`` sums to
        zero across the league. Empty input returns the zero-row schema.

    Raises:
        ValueError: Unknown ``group``.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shot_selection
            sel = mbb_shot_selection(mbb_shot_quality(mbb_shot_data(2025)), group="team_id")

        Pipeline next step (one line)::

            sel.sort("selection_value", descending=True).head(10)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if group not in _GROUPS:
        raise ValueError(f"unknown group {group!r}; expected one of {_GROUPS}")
    schema = {
        group: pl.Utf8,
        "n_shots": pl.Int64,
        "xppp": pl.Float64,
        "actual_ppp": pl.Float64,
        "selection_value": pl.Float64,
        "selection_value_total": pl.Float64,
    }
    if scored.is_empty():
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out
    league_avg_xppp = float(scored.get_column("xpoints").mean())
    out = (
        scored.group_by(group)
        .agg(
            pl.len().cast(pl.Int64).alias("n_shots"),
            pl.col("xpoints").mean().alias("xppp"),
            (pl.col("point_value").cast(pl.Float64) * pl.col("made").cast(pl.Float64)).mean().alias("actual_ppp"),
        )
        .with_columns((pl.col("xppp") - league_avg_xppp).alias("selection_value"))
        .with_columns((pl.col("selection_value") * pl.col("n_shots")).alias("selection_value_total"))
        .select(list(schema))
        .sort("selection_value", descending=True)
    )
    return out.to_pandas() if return_as_pandas else out
