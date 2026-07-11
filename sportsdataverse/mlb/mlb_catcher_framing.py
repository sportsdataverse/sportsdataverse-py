"""Catcher framing runs (T6.3, model (1)) -- a compute-on-demand
called-strike probability grid + per-catcher framing runs.

Owns :func:`called_strike_prob_grid` (the fitted surface) and
:func:`mlb_catcher_framing` (the public entry point). Both are pure
functions over an already-loaded pitch-level Statcast frame -- see
:mod:`sportsdataverse.mlb.mlb_run_values` for the shared run-value engine
and the wire-touching loader.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant catcher framing leaderboard -- the concurrent-validity
      oracle this model's runs are gated against
      (:func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_catcher_framing`).

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_run_values import count_strike_run_value

if TYPE_CHECKING:
    import pandas as pd

_TAKES = ["called_strike", "ball"]

_GRID_SCHEMA = {
    "stand": pl.Utf8,
    "px_bin": pl.Int64,
    "pz_bin": pl.Int64,
    "p_strike": pl.Float64,
    "n": pl.Int64,
}

_FRAMING_SCHEMA = {
    "catcher_id": pl.Utf8,
    "takes": pl.Int64,
    "framing_runs": pl.Float64,
    "strikes_gained": pl.Float64,
}


def _prep_takes(pitches: "pl.DataFrame", *, x_bin: float, z_bin: float) -> "pl.DataFrame":
    """Filter to takes and add the zone-relative bin columns shared by the grid + scorer."""
    takes = pitches.filter(pl.col("description").is_in(_TAKES)).with_columns(
        (pl.col("description") == "called_strike").cast(pl.Int64).alias("is_strike"),
        ((pl.col("plate_z") - pl.col("sz_bot")) / (pl.col("sz_top") - pl.col("sz_bot"))).alias("pz_norm"),
    )
    return takes.with_columns(
        (pl.col("plate_x") / x_bin).floor().cast(pl.Int64).alias("px_bin"),
        (pl.col("pz_norm") / z_bin).floor().cast(pl.Int64).alias("pz_bin"),
    )


def called_strike_prob_grid(
    pitches: "pl.DataFrame", *, x_bin: float = 0.1, z_bin: float = 0.1, alpha: float = 1.0
) -> "pl.DataFrame":
    """Empirical called-strike-probability grid over ``(stand, plate_x, pz_norm)``.

    Pitch height is normalized within the batter's strike zone
    (``pz_norm = (plate_z - sz_bot) / (sz_top - sz_bot)``) so the grid is
    zone-relative and comparable across batters; ``plate_x`` is kept raw
    (feet from the plate's center). Rate per bin is Laplace-smoothed:
    ``(strikes + alpha) / (n + 2 * alpha)``.

    Args:
        pitches: Pitch-level takes frame (``plate_x``, ``plate_z``,
            ``sz_top``, ``sz_bot``, ``stand``, ``description``).
        x_bin: Bin width for ``plate_x``, in feet. Defaults to ``0.1``.
        z_bin: Bin width for zone-normalized height. Defaults to ``0.1``.
        alpha: Laplace smoothing strength. Defaults to ``1.0``.

    Returns:
        pl.DataFrame: one row per observed ``(stand, px_bin, pz_bin)``.

        | Column | Type | Description |
        |---|---|---|
        | stand | Utf8 | Batter handedness (``L``/``R``) |
        | px_bin | Int64 | Horizontal plate-location bin index |
        | pz_bin | Int64 | Zone-normalized vertical bin index |
        | p_strike | Float64 | Laplace-smoothed empirical called-strike probability |
        | n | Int64 | Takes observed in this bin |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_catcher_framing import called_strike_prob_grid
            grid = called_strike_prob_grid(pitches, alpha=1.0)
    """
    if pitches.height == 0:
        return pl.DataFrame(schema=_GRID_SCHEMA)
    takes = _prep_takes(pitches, x_bin=x_bin, z_bin=z_bin)
    if takes.height == 0:
        return pl.DataFrame(schema=_GRID_SCHEMA)
    return (
        takes.group_by(["stand", "px_bin", "pz_bin"])
        .agg(pl.col("is_strike").sum().alias("k"), pl.len().alias("n"))
        .with_columns(((pl.col("k") + alpha) / (pl.col("n") + 2 * alpha)).alias("p_strike"))
        .select("stand", "px_bin", "pz_bin", "p_strike", "n")
        .sort("stand", "px_bin", "pz_bin")
    )


def mlb_catcher_framing(
    pitches: "pl.DataFrame",
    *,
    x_bin: float = 0.1,
    z_bin: float = 0.1,
    alpha: float = 1.0,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-catcher framing runs from the called-strike probability grid.

    For every take, ``framing_run = (actual_strike - expected_strike) *
    strike_run_value(count)`` where ``expected_strike`` is the grid lookup
    for that pitch's ``(stand, px_bin, pz_bin)`` and ``strike_run_value``
    is the count's defensive run value from
    :func:`sportsdataverse.mlb.mlb_run_values.count_strike_run_value`. Summed
    per catcher (Savant's ``fielder_2``, cast ``Utf8`` at the boundary).

    Args:
        pitches: Pitch-level frame with the take columns
            (``plate_x``/``plate_z``/``sz_top``/``sz_bot``/``stand``/
            ``description``/``balls``/``strikes``/``delta_run_exp``/
            ``fielder_2``). MiLB feeds (e.g.
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`)
            run through the same function -- there is simply no Savant
            leaderboard oracle to gate MiLB output against.
        x_bin: Grid bin width for ``plate_x``. Defaults to ``0.1``.
        z_bin: Grid bin width for zone-normalized height. Defaults to ``0.1``.
        alpha: Laplace smoothing strength for the grid. Defaults to ``1.0``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per catcher.

        | Column | Type | Description |
        |---|---|---|
        | catcher_id | Utf8 | Catcher MLBAM id (Savant ``fielder_2``) |
        | takes | Int64 | Called-strike + ball takes caught |
        | framing_runs | Float64 | Sum of (actual - expected strike) x count run-value |
        | strikes_gained | Float64 | Sum of (actual - expected strike), run-value-free |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_catcher_framing import mlb_catcher_framing
            framing = mlb_catcher_framing(pitches)

        Useful parameter combination::

            framing_pd = mlb_catcher_framing(pitches, alpha=2.0, return_as_pandas=True)

        Pipeline next step (one line)::

            framing.filter(pl.col("takes") >= 500).sort("framing_runs", descending=True)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant catcher framing leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches.height == 0:
        out = pl.DataFrame(schema=_FRAMING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    grid = called_strike_prob_grid(pitches, x_bin=x_bin, z_bin=z_bin, alpha=alpha)
    rv = count_strike_run_value(pitches)
    takes = _prep_takes(pitches, x_bin=x_bin, z_bin=z_bin).with_columns(
        pl.col("fielder_2").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("catcher_id"),
        pl.col("balls").cast(pl.Int64),
        pl.col("strikes").cast(pl.Int64),
    )
    if takes.height == 0 or grid.height == 0:
        out = pl.DataFrame(schema=_FRAMING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    assert takes.schema["px_bin"] == grid.schema["px_bin"], "px_bin dtype mismatch before framing join"
    assert takes.schema["pz_bin"] == grid.schema["pz_bin"], "pz_bin dtype mismatch before framing join"

    scored = (
        takes.join(grid.select("stand", "px_bin", "pz_bin", "p_strike"), on=["stand", "px_bin", "pz_bin"], how="left")
        .join(rv, on=["balls", "strikes"], how="left")
        .with_columns(
            pl.col("p_strike").fill_null(0.5),
            pl.col("strike_run_value").fill_null(0.0),
        )
        .with_columns(
            ((pl.col("is_strike") - pl.col("p_strike")) * pl.col("strike_run_value")).alias("framing_run"),
            (pl.col("is_strike") - pl.col("p_strike")).alias("strike_gain"),
        )
    )
    out = (
        scored.group_by("catcher_id")
        .agg(
            pl.len().alias("takes"),
            pl.col("framing_run").sum().alias("framing_runs"),
            pl.col("strike_gain").sum().alias("strikes_gained"),
        )
        .sort("framing_runs", descending=True)
        .select("catcher_id", "takes", "framing_runs", "strikes_gained")
    )
    return out.to_pandas() if return_as_pandas else out
