"""① Expected outcomes -- from-scratch xwOBA/xBA/xSLG via an EV x LA empirical grid.

The per-cell value is the mean of Savant's own ``woba_value`` / a hit indicator /
total bases over balls in play, re-fit per pull. Savant's
``estimated_woba_using_speedangle`` / ``estimated_ba_using_speedangle`` are used
only as a concurrent-validity oracle (see ``tests/mlb/test_mlb_hitting_oracle.py``),
never as a model input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union, overload

import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import GRID, HIT_EVENTS, TOTAL_BASES
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

_EXPECTED_STATS_SCHEMA = {
    "batter": pl.Int64,
    "season": pl.Int64,
    "pa": pl.Int64,
    "ab": pl.Int64,
    "xwoba": pl.Float64,
    "xba": pl.Float64,
    "xslg": pl.Float64,
}

_GRID_SCHEMA = {
    "ev_bin": pl.Int64,
    "la_bin": pl.Int64,
    "n": pl.Int64,
    "woba": pl.Float64,
    "ba": pl.Float64,
    "slg": pl.Float64,
}

#: ``events`` values whose contact-outcome value is deterministic (not
#: predicted from launch): walks, HBP, strikeouts, and other non-batted-ball
#: plate-appearance outcomes. Their realized ``woba_value`` is used as-is.
_NON_BATTED_EVENTS = {
    "walk",
    "hit_by_pitch",
    "strikeout",
    "strikeout_double_play",
}
#: Events that do NOT count toward at-bats (for the AB denominator).
_NON_AB_EVENTS = {
    "walk",
    "hit_by_pitch",
    "sac_fly",
    "sac_fly_double_play",
    "sac_bunt",
    "sac_bunt_double_play",
    "catcher_interf",
}


def _add_value_columns(pitches: pl.DataFrame) -> pl.DataFrame:
    """Add per-batted-ball value/bin columns used by :func:`build_outcome_grid`.

    Args:
        pitches: Raw Statcast pitch/batted-ball rows (``type``, ``events``,
            ``launch_speed``, ``launch_angle``, ``woba_value``).

    Returns:
        ``pitches`` with ``_hit`` (Int8 hit indicator), ``_total_bases``
        (Int8 total-bases-on-the-play), ``ev_bin``/``la_bin`` (Int64 grid
        cell indices) appended.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_stats import _add_value_columns
            _add_value_columns(pitches)
    """
    tb = pl.col("events").replace_strict(TOTAL_BASES, default=0, return_dtype=pl.Int8)
    hit = pl.col("events").is_in(list(HIT_EVENTS)).cast(pl.Int8)
    ev_bin = ((pl.col("launch_speed") - GRID.ev_min) / GRID.ev_width).floor().cast(pl.Int64)
    la_bin = ((pl.col("launch_angle") - GRID.la_min) / GRID.la_width).floor().cast(pl.Int64)
    return pitches.with_columns(
        _hit=hit,
        _total_bases=tb,
        ev_bin=ev_bin,
        la_bin=la_bin,
    )


def build_outcome_grid(pitches_with_values: pl.DataFrame) -> pl.DataFrame:
    """Build the EV x LA empirical outcome grid (cell-means of batted-ball value).

    Args:
        pitches_with_values: Output of :func:`_add_value_columns` -- must carry
            ``type``, ``launch_speed``, ``launch_angle``, ``ev_bin``, ``la_bin``,
            ``woba_value``, ``_hit``, ``_total_bases``.

    Returns:
        One row per (``ev_bin``, ``la_bin``) with ``n``, ``woba``, ``ba``,
        ``slg`` cell-means. Only balls in play (``type == "X"``) with non-null
        launch data are included. Empty input returns a zero-row frame with
        the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_stats import _add_value_columns, build_outcome_grid

            grid = build_outcome_grid(_add_value_columns(pitches))
    """
    bb = pitches_with_values.filter(
        (pl.col("type") == "X") & pl.col("launch_speed").is_not_null() & pl.col("launch_angle").is_not_null()
    )
    if bb.height == 0:
        return pl.DataFrame(schema=_GRID_SCHEMA)
    return (
        bb.group_by("ev_bin", "la_bin")
        .agg(
            pl.len().alias("n"),
            pl.col("woba_value").mean().alias("woba"),
            pl.col("_hit").mean().cast(pl.Float64).alias("ba"),
            pl.col("_total_bases").mean().cast(pl.Float64).alias("slg"),
        )
        .sort("ev_bin", "la_bin")
        .cast(_GRID_SCHEMA)
    )


def predict_contact_value(batted_balls: pl.DataFrame, grid: pl.DataFrame, *, value: str = "woba") -> pl.Series:
    """Predict per-batted-ball contact value from the EV x LA grid, with fallback.

    Cells with ``n < GRID.min_n`` (including missing cells) fall back to the
    launch-angle-marginal mean of ``value`` (n-weighted across EV bins for
    that LA bin); if the LA-marginal is itself unavailable, falls back to the
    grid's global n-weighted mean. The result is always finite (never null).

    Args:
        batted_balls: Batted-ball rows with ``ev_bin``/``la_bin`` (Int64,
            added by :func:`_add_value_columns`).
        grid: Output of :func:`build_outcome_grid`.
        value: Which cell statistic to predict -- ``"woba"``, ``"ba"``, or
            ``"slg"``.

    Returns:
        A ``Float64`` polars Series aligned to ``batted_balls``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_stats import predict_contact_value

            predict_contact_value(batted_balls, grid, value="woba")
    """
    assert batted_balls.schema.get("ev_bin") == grid.schema.get("ev_bin"), "ev_bin dtype mismatch before grid join"
    assert batted_balls.schema.get("la_bin") == grid.schema.get("la_bin"), "la_bin dtype mismatch before grid join"

    if grid.height == 0:
        return pl.Series(value, [None] * batted_balls.height, dtype=pl.Float64)

    global_mean = float((grid[value] * grid["n"]).sum() / grid["n"].sum())

    la_marginal = grid.group_by("la_bin").agg(
        ((pl.col(value) * pl.col("n")).sum() / pl.col("n").sum()).alias("la_marginal")
    )

    dense_cells = grid.filter(pl.col("n") >= GRID.min_n).select("ev_bin", "la_bin", pl.col(value).alias("cell_value"))

    out = (
        batted_balls.select("ev_bin", "la_bin")
        .join(dense_cells, on=["ev_bin", "la_bin"], how="left")
        .join(la_marginal, on="la_bin", how="left")
        .with_columns(pl.coalesce(["cell_value", "la_marginal", pl.lit(global_mean)]).alias(value))
    )
    return out[value]


@overload
def mlb_expected_stats(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def mlb_expected_stats(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def mlb_expected_stats(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = mlb_statcast_search,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per player-season xwOBA/xBA/xSLG from an on-the-fly EV x LA empirical grid.

    Pulls pitches via ``puller(start_dt, end_dt, player_type="batter")``, builds
    the outcome grid from the pull's own batted balls (season-agnostic
    algorithm, per-pull empirical constants -- see ``CLAUDE.md``), predicts
    contact ``woba``/``ba``/``slg`` per batted ball with the launch-angle-
    marginal fallback, then aggregates:

    * ``xwoba = (sum(predicted_woba over balls in play) + sum(woba_value over
      non-batted-ball PA outcomes)) / sum(woba_denom)``
    * ``xba = sum(predicted_ba over balls in play) / ab``
    * ``xslg = sum(predicted_tb over balls in play) / ab``

    Args:
        start_dt: Pull start date, ``YYYY-MM-DD``.
        end_dt: Pull end date, ``YYYY-MM-DD``.
        puller: Injectable Statcast search callable -- defaults to
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (``batter``, ``season``): ``pa``, ``ab``, ``xwoba``,
        ``xba``, ``xslg``. Empty pull returns a zero-row frame with the
        documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_stats import mlb_expected_stats

            df = mlb_expected_stats("2024-06-01", "2024-06-21")
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("xwoba", descending=True).head()

        See Also:
            * `baseballr`_ -- Statcast helper functions (R)

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    pitches = puller(start_dt, end_dt, player_type="batter")
    if pitches is None or pitches.height == 0:
        empty = pl.DataFrame(schema=_EXPECTED_STATS_SCHEMA)
        if return_as_pandas:
            return empty.to_pandas()
        return empty

    pitches = pitches.with_columns(pl.col("batter").cast(pl.Int64))
    if "game_year" in pitches.columns:
        season_expr = pl.col("game_year").cast(pl.Int64)
    elif pitches.schema["game_date"] == pl.Utf8:
        season_expr = pl.col("game_date").str.to_date().dt.year().cast(pl.Int64)
    else:
        season_expr = pl.col("game_date").cast(pl.Date).dt.year().cast(pl.Int64)
    pitches = pitches.with_columns(season_expr.alias("season"))

    pitches = _add_value_columns(pitches)
    grid = build_outcome_grid(pitches)

    bip_mask = (pl.col("type") == "X") & pl.col("launch_speed").is_not_null() & pl.col("launch_angle").is_not_null()
    bip = pitches.filter(bip_mask)
    non_bip = pitches.filter(~bip_mask)

    if bip.height > 0:
        bip = bip.with_columns(
            predict_contact_value(bip, grid, value="woba").alias("_pred_woba"),
            predict_contact_value(bip, grid, value="ba").alias("_pred_ba"),
            predict_contact_value(bip, grid, value="slg").alias("_pred_slg"),
        )
    else:
        bip = bip.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("_pred_woba"),
            pl.lit(None, dtype=pl.Float64).alias("_pred_ba"),
            pl.lit(None, dtype=pl.Float64).alias("_pred_slg"),
        )

    bip_agg = bip.group_by("batter", "season").agg(
        pl.col("_pred_woba").sum().alias("_bip_woba_sum"),
        pl.col("_pred_ba").sum().alias("_bip_ba_sum"),
        pl.col("_pred_slg").sum().alias("_bip_slg_sum"),
        pl.len().alias("_ab_from_bip"),
    )

    woba_denom_col = "woba_denom" if "woba_denom" in non_bip.columns else None
    non_bip_agg = non_bip.group_by("batter", "season").agg(
        pl.col("woba_value").fill_null(0.0).sum().alias("_non_bip_woba_sum"),
        (pl.col(woba_denom_col).fill_null(0.0).sum() if woba_denom_col else pl.lit(0.0)).alias("_non_bip_denom"),
        pl.col("events").is_in(list(_NON_AB_EVENTS)).sum().alias("_non_ab_events"),
        pl.len().alias("_pa_from_non_bip"),
    )

    bip_denom_agg = bip.group_by("batter", "season").agg(
        (pl.col(woba_denom_col).fill_null(1.0).sum() if woba_denom_col else pl.len().cast(pl.Float64)).alias(
            "_bip_denom"
        )
    )

    merged = bip_agg.join(non_bip_agg, on=["batter", "season"], how="full", coalesce=True).join(
        bip_denom_agg, on=["batter", "season"], how="full", coalesce=True
    )
    merged = merged.fill_null(0)

    result = (
        merged.with_columns(
            (pl.col("_ab_from_bip") + pl.col("_pa_from_non_bip")).alias("pa"),
            (pl.col("_ab_from_bip") + pl.col("_pa_from_non_bip") - pl.col("_non_ab_events")).alias("ab"),
            (pl.col("_bip_denom") + pl.col("_non_bip_denom")).alias("_woba_denom"),
        )
        .with_columns(
            pl.when(pl.col("_woba_denom") > 0)
            .then((pl.col("_bip_woba_sum") + pl.col("_non_bip_woba_sum")) / pl.col("_woba_denom"))
            .otherwise(None)
            .alias("xwoba"),
            pl.when(pl.col("ab") > 0).then(pl.col("_bip_ba_sum") / pl.col("ab")).otherwise(None).alias("xba"),
            pl.when(pl.col("ab") > 0).then(pl.col("_bip_slg_sum") / pl.col("ab")).otherwise(None).alias("xslg"),
        )
        .select("batter", "season", "pa", "ab", "xwoba", "xba", "xslg")
        .cast(_EXPECTED_STATS_SCHEMA)
    )

    if return_as_pandas:
        return result.to_pandas()
    return result
