"""① Expected outcomes -- from-scratch xwOBA/xBA/xSLG via an EV x LA empirical grid.

The per-cell value is the mean of Savant's own ``woba_value`` / a hit indicator /
total bases over balls in play, re-fit per pull. Savant's
``estimated_woba_using_speedangle`` / ``estimated_ba_using_speedangle`` are used
only as a concurrent-validity oracle (see ``tests/mlb/test_mlb_hitting_oracle.py``),
never as a model input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal, Optional, Union, overload

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
    "woba": pl.Float64,
    "ba": pl.Float64,
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
    "intent_walk",
    "hit_by_pitch",
    "sac_fly",
    "sac_fly_double_play",
    "sac_bunt",
    "sac_bunt_double_play",
    "catcher_interf",
}

#: PA-ending events that carry a ZERO wOBA denominator (standard wOBA denom =
#: AB + uBB + SF + HBP -- intentional walks, sac bunts and catcher
#: interference are excluded).
_WOBA_DENOM_ZERO_EVENTS = {
    "intent_walk",
    "sac_bunt",
    "sac_bunt_double_play",
    "catcher_interf",
}

#: Fixed fallback wOBA weights for PA-ending events whose ``woba_value`` is
#: null in a given cache vintage (older Savant extracts). These are
#: FanGraphs-scale constants (true seasonal weights drift by ~±0.01) used only
#: as a null-fill -- a documented approximation that is orders of magnitude
#: smaller than the corruption of dropping the event entirely. Events not in
#: this map fall back to 0.0.
_WOBA_VALUE_FALLBACK = {
    "walk": 0.69,
    "hit_by_pitch": 0.72,
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
    puller: Optional[Callable[..., pl.DataFrame]] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def mlb_expected_stats(
    start_dt: str,
    end_dt: str,
    *,
    puller: Optional[Callable[..., pl.DataFrame]] = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def mlb_expected_stats(
    start_dt: str,
    end_dt: str,
    *,
    puller: Optional[Callable[..., pl.DataFrame]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per player-season xwOBA/xBA/xSLG from an on-the-fly EV x LA empirical grid.

    Pulls pitches via ``puller(start_dt, end_dt, player_type="batter")``, builds
    the outcome grid from the pull's own batted balls (season-agnostic
    algorithm, per-pull empirical constants -- see ``CLAUDE.md``), predicts
    contact ``woba``/``ba``/``slg`` per batted ball with the launch-angle-
    marginal fallback, then aggregates:

    * ``xwoba = (sum(predicted_woba over balls in play) + sum(woba_value over
      non-batted-ball PA-ENDING outcomes)) / derived_woba_denom`` -- the
      denominator is DERIVED from ``events`` (PA enders minus intentional
      walks / sac bunts / catcher interference), never trusted from a cache
      vintage's ``woba_denom`` column. The numerator excludes those same
      zero-denominator events, and a PA-ending walk/HBP whose ``woba_value``
      is null in a given vintage is filled with the fixed weights .69 / .72.
    * ``xba = (sum(predicted_ba over TRACKED at-bat balls in play) +
      sum(realized hits over UNTRACKED ones)) / ab`` -- a ball in play with no
      launch data cannot be predicted from the grid, so it takes its realized
      outcome exactly as ``xwoba`` does, rather than counting in ``ab`` with a
      zero numerator (which deflated league-mean xBA by the untracked share).
    * ``xslg`` -- same construction on total bases.
    * ``woba`` / ``ba`` -- the OBSERVED counterparts on the same denominators,
      so ``xwoba - woba`` is a luck-vs-skill delta needing no second source.

    ``pa`` counts PLATE-APPEARANCE-ENDING rows only (``events`` non-null),
    never raw pitches -- a Statcast search pull carries every pitch, and
    counting them (the pre-fix behavior) inflated ``pa``/``ab`` by ~4x and
    corrupted ``xba``/``xslg`` scales.

    Args:
        start_dt: Pull start date, ``YYYY-MM-DD``.
        end_dt: Pull end date, ``YYYY-MM-DD``.
        puller: Injectable Statcast search callable -- defaults to
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (``batter``, ``season``): ``pa``, ``ab``, ``xwoba``,
        ``xba``, ``xslg``, plus the observed ``woba`` / ``ba``. Empty pull
        returns a zero-row frame with the documented schema.

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
    puller = puller or mlb_statcast_search
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
    # The outcome grid's cell means likewise use only PA-ending batted balls —
    # an events-less "batted ball" row carries no realized outcome to average.
    grid = build_outcome_grid(pitches.filter(pl.col("events").is_not_null() & (pl.col("events") != "")))

    bip_mask = (pl.col("type") == "X") & pl.col("launch_speed").is_not_null() & pl.col("launch_angle").is_not_null()
    # PA-ender discipline: pa / ab / the wOBA denominator count only rows that
    # END a plate appearance (``events`` non-null and non-empty). A Statcast
    # search pull carries EVERY PITCH; counting raw non-BIP pitch rows (the
    # pre-fix behavior) inflated pa/ab by the pitch count and, on cache
    # vintages with degenerate ``woba_value``/``woba_denom`` semantics,
    # corrupted the xwOBA scale itself. Denominators are DERIVED from events
    # (vintage-proof) rather than trusted from ``woba_denom``.
    pa_end_mask = pl.col("events").is_not_null() & (pl.col("events") != "")
    zero_denom = pl.col("events").is_in(list(_WOBA_DENOM_ZERO_EVENTS))
    non_ab = pl.col("events").is_in(list(_NON_AB_EVENTS))

    # A batted-ball row must ALSO be a PA ender: a type=="X" row with launch
    # data but a null/empty ``events`` is a feed artifact, and without this
    # gate it would count toward pa while the events-derived masks exclude it
    # from every denominator (the inconsistency this fix exists to remove).
    bip = pitches.filter(bip_mask & pa_end_mask)
    non_bip_pa = pitches.filter(~bip_mask & pa_end_mask)

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

    #: realized wOBA value with the vintage null-fill -- shared by the observed
    #: ``woba`` column and by the untracked-batted-ball fallback below.
    realized_woba = pl.coalesce(
        pl.col("woba_value"),
        pl.col("events").replace_strict(_WOBA_VALUE_FALLBACK, default=0.0, return_dtype=pl.Float64),
    )

    bip_agg = bip.group_by("batter", "season").agg(
        pl.col("_pred_woba").filter(~zero_denom).sum().alias("_bip_woba_sum"),
        pl.col("_pred_ba").filter(~non_ab).sum().alias("_bip_ba_sum"),
        pl.col("_pred_slg").filter(~non_ab).sum().alias("_bip_slg_sum"),
        realized_woba.filter(~zero_denom).sum().alias("_bip_obs_woba_sum"),
        pl.col("_hit").filter(~non_ab).sum().alias("_bip_obs_ba_sum"),
        (~zero_denom).sum().cast(pl.Float64).alias("_bip_denom"),
        (~non_ab).sum().alias("_ab_from_bip"),
        pl.len().alias("_pa_from_bip"),
    )

    # UNTRACKED-BATTED-BALL SYMMETRY (2026-09 follow-up to the #421 PA-ender
    # fix): 8-19% of PA-ending balls in play carry no launch_speed/launch_angle
    # and so cannot be predicted from the grid. They land here, in non_bip_pa.
    # xwOBA has always given them their REALIZED woba_value; xBA/xSLG did not,
    # counting them in the `ab` denominator with a zero numerator contribution.
    # That deflated league-mean xBA by almost exactly the untracked share times
    # the hit rate (2015: 19.4% untracked -> mean xBA .2026 vs observed BA
    # .2556; 2021: 8.3% -> .2198 vs .2442), and untracked balls are NOT
    # degenerate -- their hit rate matches tracked balls to 3 decimals (.325 vs
    # .324 in 2015). So they take the same realized-outcome fallback xwOBA
    # already uses. A strikeout/field_out contributes a realized 0, exactly as
    # before; only untracked batted balls change.
    non_bip_agg = non_bip_pa.group_by("batter", "season").agg(
        realized_woba.filter(~zero_denom).sum().alias("_non_bip_woba_sum"),
        pl.col("_hit").filter(~non_ab).sum().alias("_non_bip_ba_sum"),
        pl.col("_total_bases").filter(~non_ab).sum().alias("_non_bip_slg_sum"),
        (~zero_denom).sum().cast(pl.Float64).alias("_non_bip_denom"),
        (~non_ab).sum().alias("_ab_from_non_bip"),
        pl.len().alias("_pa_from_non_bip"),
    )

    merged = bip_agg.join(non_bip_agg, on=["batter", "season"], how="full", coalesce=True)
    merged = merged.fill_null(0)

    result = (
        merged.with_columns(
            (pl.col("_pa_from_bip") + pl.col("_pa_from_non_bip")).alias("pa"),
            (pl.col("_ab_from_bip") + pl.col("_ab_from_non_bip")).alias("ab"),
            (pl.col("_bip_denom") + pl.col("_non_bip_denom")).alias("_woba_denom"),
        )
        .with_columns(
            pl.when(pl.col("_woba_denom") > 0)
            .then((pl.col("_bip_woba_sum") + pl.col("_non_bip_woba_sum")) / pl.col("_woba_denom"))
            .otherwise(None)
            .alias("xwoba"),
            pl.when(pl.col("ab") > 0)
            .then((pl.col("_bip_ba_sum") + pl.col("_non_bip_ba_sum")) / pl.col("ab"))
            .otherwise(None)
            .alias("xba"),
            pl.when(pl.col("ab") > 0)
            .then((pl.col("_bip_slg_sum") + pl.col("_non_bip_slg_sum")) / pl.col("ab"))
            .otherwise(None)
            .alias("xslg"),
            # Observed counterparts on the SAME denominators, so a luck-vs-skill
            # delta is `xwoba - woba` / `xba - ba` with no second source.
            pl.when(pl.col("_woba_denom") > 0)
            .then((pl.col("_bip_obs_woba_sum") + pl.col("_non_bip_woba_sum")) / pl.col("_woba_denom"))
            .otherwise(None)
            .alias("woba"),
            pl.when(pl.col("ab") > 0)
            .then((pl.col("_bip_obs_ba_sum") + pl.col("_non_bip_ba_sum")) / pl.col("ab"))
            .otherwise(None)
            .alias("ba"),
        )
        .select("batter", "season", "pa", "ab", "xwoba", "xba", "xslg", "woba", "ba")
        .cast(_EXPECTED_STATS_SCHEMA)
    )

    if return_as_pandas:
        return result.to_pandas()
    return result
