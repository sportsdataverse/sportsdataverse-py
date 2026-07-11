"""③ Expected home runs -- from-scratch EV x LA x spray HR-probability grid,
park-adjusted via Savant's park-factors leaderboard.

The per-cell HR probability is the mean HR indicator over batted balls in
that 3-axis cell, re-fit per pull. Savant's ``mlb_statcast_leaderboard_home_runs``
xHR is used only as a concurrent-validity oracle, never as a model input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import GRID, MLB_TEAM_ID_BY_ABBREV, spray_angle
from sportsdataverse.mlb.mlb_statcast import mlb_statcast_leaderboard_park_factors
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

_EXPECTED_HR_SCHEMA = {
    "batter": pl.Int64,
    "season": pl.Int64,
    "hr": pl.Int64,
    "xhr_neutral": pl.Float64,
    "xhr_park_adj": pl.Float64,
    "hr_above_expected": pl.Float64,
}

_HR_GRID_SCHEMA = {"ev_bin": pl.Int64, "la_bin": pl.Int64, "spray_bin": pl.Int64, "n": pl.Int64, "p_hr": pl.Float64}


def _add_hr_bins(batted_balls: pl.DataFrame) -> pl.DataFrame:
    """Add EV/LA/spray grid-cell bins and the HR indicator to batted-ball rows.

    Args:
        batted_balls: Raw batted-ball rows (``events``, ``launch_speed``,
            ``launch_angle``, ``hc_x``, ``hc_y``, ``stand``).

    Returns:
        ``batted_balls`` with ``ev_bin``, ``la_bin``, ``spray_bin`` (Int64) and
        ``_is_hr`` (Int8, 1 iff ``events == "home_run"``) appended.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_home_runs import _add_hr_bins
            _add_hr_bins(batted_balls)
    """
    ev_bin = ((pl.col("launch_speed") - GRID.ev_min) / GRID.ev_width).floor().cast(pl.Int64)
    la_bin = ((pl.col("launch_angle") - GRID.la_min) / GRID.la_width).floor().cast(pl.Int64)
    spray = spray_angle(pl.col("hc_x"), pl.col("hc_y"), pl.col("stand"))
    spray_bin = (spray / GRID.spray_width).floor().cast(pl.Int64)
    is_hr = (pl.col("events") == "home_run").cast(pl.Int8)
    return batted_balls.with_columns(ev_bin=ev_bin, la_bin=la_bin, spray_bin=spray_bin, _is_hr=is_hr)


def build_hr_grid(batted_balls_with_bins: pl.DataFrame) -> pl.DataFrame:
    """Build the EV x LA x spray home-run probability grid.

    Args:
        batted_balls_with_bins: Output of :func:`_add_hr_bins` -- must carry
            ``type``, ``launch_speed``, ``launch_angle``, ``ev_bin``,
            ``la_bin``, ``spray_bin``, ``_is_hr``.

    Returns:
        One row per (``ev_bin``, ``la_bin``, ``spray_bin``): ``n`` (Int64),
        ``p_hr`` (Float64, mean ``_is_hr``). Only balls in play with non-null
        launch/spray data are included. Empty input returns a zero-row frame
        with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_home_runs import _add_hr_bins, build_hr_grid
            grid = build_hr_grid(_add_hr_bins(batted_balls))
    """
    bb = batted_balls_with_bins.filter(
        (pl.col("type") == "X")
        & pl.col("launch_speed").is_not_null()
        & pl.col("launch_angle").is_not_null()
        & pl.col("spray_bin").is_not_null()
    )
    if bb.height == 0:
        return pl.DataFrame(schema=_HR_GRID_SCHEMA)
    return (
        bb.group_by("ev_bin", "la_bin", "spray_bin")
        .agg(pl.len().alias("n"), pl.col("_is_hr").mean().cast(pl.Float64).alias("p_hr"))
        .sort("ev_bin", "la_bin", "spray_bin")
        .cast(_HR_GRID_SCHEMA)
    )


def predict_hr_prob(batted_balls: pl.DataFrame, grid: pl.DataFrame) -> pl.Series:
    """Predict per-batted-ball HR probability, with EV x LA-marginal (spray-collapsed) fallback.

    Cells with ``n < GRID.min_n`` (including missing cells) fall back to the
    EV x LA-marginal HR rate (spray collapsed, n-weighted); if that is also
    unavailable, falls back to the grid's global n-weighted mean.

    Args:
        batted_balls: Batted-ball rows with ``ev_bin``/``la_bin``/``spray_bin``
            (Int64, added by :func:`_add_hr_bins`).
        grid: Output of :func:`build_hr_grid`.

    Returns:
        A ``Float64`` polars Series aligned to ``batted_balls`` (never null).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_home_runs import predict_hr_prob
            predict_hr_prob(batted_balls, grid)
    """
    assert batted_balls.schema.get("ev_bin") == grid.schema.get("ev_bin"), "ev_bin dtype mismatch before grid join"
    assert batted_balls.schema.get("la_bin") == grid.schema.get("la_bin"), "la_bin dtype mismatch before grid join"
    assert batted_balls.schema.get("spray_bin") == grid.schema.get("spray_bin"), (
        "spray_bin dtype mismatch before grid join"
    )

    if grid.height == 0:
        return pl.Series("p_hr", [None] * batted_balls.height, dtype=pl.Float64)

    global_mean = float((grid["p_hr"] * grid["n"]).sum() / grid["n"].sum())

    ev_la_marginal = grid.group_by("ev_bin", "la_bin").agg(
        ((pl.col("p_hr") * pl.col("n")).sum() / pl.col("n").sum()).alias("ev_la_marginal")
    )

    dense_cells = grid.filter(pl.col("n") >= GRID.min_n).select(
        "ev_bin", "la_bin", "spray_bin", pl.col("p_hr").alias("cell_value")
    )

    out = (
        batted_balls.select("ev_bin", "la_bin", "spray_bin")
        .join(dense_cells, on=["ev_bin", "la_bin", "spray_bin"], how="left")
        .join(ev_la_marginal, on=["ev_bin", "la_bin"], how="left")
        .with_columns(pl.coalesce(["cell_value", "ev_la_marginal", pl.lit(global_mean)]).alias("p_hr"))
    )
    return out["p_hr"]


def park_adjust(batted_balls_with_phr: pl.DataFrame, park_factors: pl.DataFrame) -> pl.DataFrame:
    """Attach each batted ball's park HR factor and produce a park-adjusted probability.

    Args:
        batted_balls_with_phr: Batted-ball rows with ``home_team`` (Statcast
            abbreviation) and ``p_hr`` (the neutral HR probability).
        park_factors: Frame with ``team_id`` (Int64, MLBAM id) and
            ``hr_factor`` (Float64, index 100 = neutral).

    Returns:
        ``batted_balls_with_phr`` with ``p_hr_park_adj = p_hr * hr_factor / 100``
        appended.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_home_runs import park_adjust
            park_adjust(batted_balls, park_factors)
    """
    assert park_factors.schema.get("team_id") == pl.Int64, "park_factors.team_id must be Int64"
    team_id_expr = (
        pl.col("home_team").replace_strict(MLB_TEAM_ID_BY_ABBREV, default=None, return_dtype=pl.Int64).alias("_team_id")
    )
    out = batted_balls_with_phr.with_columns(team_id_expr).join(
        park_factors.select("team_id", "hr_factor"), left_on="_team_id", right_on="team_id", how="left"
    )
    return out.with_columns(
        (pl.col("p_hr") * pl.col("hr_factor").fill_null(100.0) / 100.0).alias("p_hr_park_adj")
    ).drop("_team_id")


@overload
def mlb_expected_home_runs(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = ...,
    park_factors: Optional[pl.DataFrame] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def mlb_expected_home_runs(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = ...,
    park_factors: Optional[pl.DataFrame] = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def mlb_expected_home_runs(
    start_dt: str,
    end_dt: str,
    *,
    puller: "object" = mlb_statcast_search,
    park_factors: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per player-season park-neutral xHR, park-adjusted xHR, and HR-above-expected.

    Pulls batted balls via ``puller(start_dt, end_dt, player_type="batter")``,
    builds the EV x LA x spray HR-probability grid from the pull's own batted
    balls (season-agnostic algorithm, per-pull empirical constants), predicts
    each ball's HR probability with the EV x LA-marginal fallback, park-adjusts
    via ``hr_factor`` (index 100 = neutral, joined on the Statcast
    ``home_team`` abbreviation -> MLBAM team id), then aggregates per batter.

    Args:
        start_dt: Pull start date, ``YYYY-MM-DD``.
        end_dt: Pull end date, ``YYYY-MM-DD``.
        puller: Injectable Statcast search callable -- defaults to
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`.
        park_factors: Pre-fetched park-factors frame (``team_id``, ``hr_factor``);
            if ``None``, fetched via
            :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_park_factors`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (``batter``, ``season``): ``hr``, ``xhr_neutral``,
        ``xhr_park_adj``, ``hr_above_expected`` (``hr - xhr_neutral``). Empty
        pull returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_expected_home_runs import mlb_expected_home_runs

            df = mlb_expected_home_runs("2024-06-01", "2024-06-21")
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("hr_above_expected", descending=True).head()

        See Also:
            * `baseballr`_ -- Statcast helper functions (R)

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    pitches = puller(start_dt, end_dt, player_type="batter")
    if pitches is None or pitches.height == 0:
        empty = pl.DataFrame(schema=_EXPECTED_HR_SCHEMA)
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

    pitches = _add_hr_bins(pitches)
    grid = build_hr_grid(pitches)

    bip_mask = (
        (pl.col("type") == "X")
        & pl.col("launch_speed").is_not_null()
        & pl.col("launch_angle").is_not_null()
        & pl.col("spray_bin").is_not_null()
    )
    bip = pitches.filter(bip_mask)
    if bip.height == 0:
        empty = pl.DataFrame(schema=_EXPECTED_HR_SCHEMA)
        if return_as_pandas:
            return empty.to_pandas()
        return empty

    bip = bip.with_columns(predict_hr_prob(bip, grid).alias("p_hr"))

    if park_factors is None:
        pf_raw = mlb_statcast_leaderboard_park_factors()
        park_factors = pf_raw.select(
            pl.col("main_team_id").cast(pl.Int64).alias("team_id"),
            pl.col("index_hr").cast(pl.Float64).alias("hr_factor"),
        )
    park_factors = park_factors.with_columns(pl.col("team_id").cast(pl.Int64))

    bip = park_adjust(bip, park_factors)

    result = (
        bip.group_by("batter", "season")
        .agg(
            pl.col("_is_hr").sum().alias("hr"),
            pl.col("p_hr").sum().alias("xhr_neutral"),
            pl.col("p_hr_park_adj").sum().alias("xhr_park_adj"),
        )
        .with_columns((pl.col("hr") - pl.col("xhr_neutral")).alias("hr_above_expected"))
        .select("batter", "season", "hr", "xhr_neutral", "xhr_park_adj", "hr_above_expected")
        .cast(_EXPECTED_HR_SCHEMA)
        .sort("batter", "season")
    )

    if return_as_pandas:
        return result.to_pandas()
    return result
