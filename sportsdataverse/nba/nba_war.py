"""WAR (points-above-replacement -> wins) layer for the NBA model zoo.

Per the WP4 spec's Open Decision (see the plan's "Open decisions" section):
this module ships NO invented numeric default for ``pts_per_win`` or
``replacement_level`` — both are closed-form-calibrated from real
compiled-season data via the two functions below, and ``nba_war`` (Task 4)
requires them as explicit keyword arguments.
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd
import polars as pl

#: Schema of the per-player frame returned by :func:`nba_war`.
WAR_SCHEMA: dict[str, pl.DataType] = {"player_id": pl.Int64, "war": pl.Float64}


def _empty_war_frame() -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series([], dtype=t) for c, t in WAR_SCHEMA.items()})


def calibrate_pts_per_win(team_season: pl.DataFrame) -> float:
    """Regress team wins on season point margin; return points-per-marginal-win.

    Fits ``wins ~ total_margin`` via ordinary least squares over one (or more,
    pooled) season's team-level rows and returns ``1 / slope`` — the amount of
    full-season point differential associated with one additional win. This is
    ``nba_war``'s ``pts_per_win`` input.

    Args:
        team_season: One row per team-season with ``team_id`` (any dtype),
            ``wins`` (numeric), and ``total_margin`` (numeric — the team's
            full-season point differential: points scored minus points allowed
            across all its games, NOT a per-game average).

    Returns:
        ``float`` points of season margin per marginal win.

    Raises:
        ValueError: If ``team_season`` has fewer than 3 rows (a degenerate
            regression), if ``total_margin`` has zero variance, or if the
            fitted slope is exactly zero (undefined points-per-win).

    Example:
        Calibrate from a compiled season's standings::

            from sportsdataverse.nba.nba_war import calibrate_pts_per_win
            pts_per_win = calibrate_pts_per_win(team_standings)  # team_id/wins/total_margin
            print(pts_per_win)
    """
    if team_season.height < 3:
        raise ValueError(f"calibrate_pts_per_win needs >=3 team-season rows, got {team_season.height}")
    x = team_season["total_margin"].to_numpy().astype(np.float64)
    y = team_season["wins"].to_numpy().astype(np.float64)
    if np.std(x) == 0:
        raise ValueError("calibrate_pts_per_win: total_margin has zero variance")
    slope, _intercept = np.polyfit(x, y, 1)
    if slope == 0:
        raise ValueError("calibrate_pts_per_win: fitted wins~total_margin slope is exactly zero")
    return float(1.0 / slope)


def calibrate_replacement_level(
    ratings: pl.DataFrame,
    poss: pl.DataFrame,
    *,
    pts_per_win: float,
    target_total_war: float,
    rating_col: str = "rating",
    poss_col: str = "poss",
) -> float:
    """Solve for the ``replacement_level`` that makes summed league WAR hit a target.

    WAR is affine in ``replacement_level``:
    ``war_i = (rating_i - replacement) * poss_i / 100 / pts_per_win``. Summed
    over all players this is a single linear equation in ``replacement_level``;
    this function solves it in closed form (not an iterative search) for the
    ``replacement_level`` that makes ``sum(war_i) == target_total_war``.

    ``target_total_war`` is a value the CALLER computes from real standings
    (e.g. total league wins above a chosen replacement-team win percentage) —
    this function does not assume or invent any such win-percentage convention.

    Args:
        ratings: Frame with ``player_id`` and ``rating_col``.
        poss: Frame with ``player_id`` and ``poss_col`` (total possessions played).
        pts_per_win: Points of season margin per marginal win
            (``calibrate_pts_per_win``'s output).
        target_total_war: The desired sum of every player's WAR.
        rating_col: Column in ``ratings`` holding the per-100-possession rating.
        poss_col: Column in ``poss`` holding total possessions played.

    Returns:
        ``float`` replacement_level solving the equation exactly.

    Raises:
        ValueError: If ``ratings`` and ``poss`` share no ``player_id`` (an
            inner join on two non-empty frames producing zero rows — a probable
            id-source/dtype mismatch), or if the resulting total possession
            weight sums to zero (the equation is then degenerate).

    Example:
        Solve replacement level against a chosen win-above-replacement target::

            from sportsdataverse.nba.nba_war import calibrate_replacement_level
            repl = calibrate_replacement_level(
                ratings, poss, pts_per_win=250.0, target_total_war=300.0,
            )
    """
    j = ratings.join(poss, on="player_id", how="inner")
    if j.is_empty():
        if ratings.is_empty() or poss.is_empty():
            raise ValueError("calibrate_replacement_level: ratings or poss is empty — nothing to calibrate")
        raise ValueError("calibrate_replacement_level: ratings and poss share no shared player_id")
    weight = j[poss_col].cast(pl.Float64) / 100.0 / pts_per_win
    total_weight = float(weight.sum())
    if total_weight == 0:
        raise ValueError("calibrate_replacement_level: total possession weight is zero")
    weighted_rating = float((j[rating_col].cast(pl.Float64) * weight).sum())
    return (weighted_rating - target_total_war) / total_weight


def nba_war(
    ratings: pl.DataFrame,
    poss: pl.DataFrame,
    *,
    replacement_level: float,
    pts_per_win: float,
    rating_col: str = "rating",
    poss_col: str = "poss",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Points-above-replacement -> wins for each player.

    ``war_i = (rating_i - replacement_level) * poss_i / 100 / pts_per_win``.

    Args:
        ratings: Per-player rating frame with ``player_id`` and ``rating_col``
            (e.g. ``nba_rapm``'s ``rapm`` column renamed, a ``nba_ratings_panel``
            row filtered to one date, or ``nba_bpm``'s ``bpm`` column).
        poss: Per-player possession-count frame with ``player_id`` and
            ``poss_col`` (e.g. ``off_poss + def_poss`` from ``nba_rapm``).
        replacement_level: Per-100-possession rating of a replacement-level
            player. No built-in default — calibrate via
            :func:`calibrate_replacement_level`.
        pts_per_win: Points of season point-margin per marginal win. No
            built-in default — calibrate via :func:`calibrate_pts_per_win`.
        rating_col: Column in ``ratings`` to score.
        poss_col: Column in ``poss`` giving total possessions played.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        Frame with :data:`WAR_SCHEMA` columns (``player_id``, ``war``). Empty
        (that schema) when either input is empty.

    Raises:
        ValueError: If both inputs are non-empty but share no ``player_id``
            (a probable id-source or dtype mismatch, not a normal empty case).

    Example:
        Score WAR from a calibrated replacement level and pts-per-win::

            from sportsdataverse.nba.nba_war import nba_war
            war = nba_war(rapm_df.rename({"rapm": "rating"}), poss_df,
                           replacement_level=-2.0, pts_per_win=250.0)
            print(war.sort("war", descending=True).head())

        Derive both required kwargs from real data first::

            from sportsdataverse.nba.nba_war import (
                calibrate_pts_per_win, calibrate_replacement_level, nba_war,
            )
            pts_per_win = calibrate_pts_per_win(team_standings)
            repl = calibrate_replacement_level(
                ratings, poss, pts_per_win=pts_per_win, target_total_war=300.0,
            )
            war = nba_war(ratings, poss, replacement_level=repl, pts_per_win=pts_per_win)
    """
    if ratings.is_empty() or poss.is_empty():
        return _empty_war_frame().to_pandas() if return_as_pandas else _empty_war_frame()
    j = ratings.join(poss, on="player_id", how="inner")
    if j.is_empty():
        raise ValueError("nba_war: ratings and poss share no shared player_id")
    out = j.select(
        pl.col("player_id").cast(pl.Int64),
        (
            (pl.col(rating_col).cast(pl.Float64) - replacement_level)
            * pl.col(poss_col).cast(pl.Float64)
            / 100.0
            / pts_per_win
        ).alias("war"),
    )
    return out.to_pandas() if return_as_pandas else out
