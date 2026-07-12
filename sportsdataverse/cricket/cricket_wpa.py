"""Cricket expected-runs + batting/bowling win-probability added (T7.3 model ②).

Consumes the output of
:func:`sportsdataverse.cricket.cricket_win_prob.cricket_win_probability` (which
carries ``proj_final``, ``resources_left``, ``overs_left`` and ``win_prob``) and
derives:

* :func:`cricket_expected_runs` — the projected runs still to come from each
  state (``exp_runs_remaining = proj_final - runs``) and the implied remaining
  run rate (``exp_run_rate = exp_runs_remaining / overs_left``); and
* :func:`cricket_wpa` — the per-delivery/over batting and bowling win-probability
  added, ``wpa_batting = win_prob - win_prob_before`` (the prior state within the
  same innings) and ``wpa_bowling = -wpa_batting``.

Both are compute-on-demand (no bundled artifact). The WPA lead is taken
``.over(["event_id", "innings_number"])`` so concatenated frames never leak a
win-probability change across matches or innings (as-of boundary).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    import pandas as pd


def cricket_expected_runs(state_wp: pl.DataFrame, *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Expected remaining runs + run rate from a win-probability-scored state frame.

    Args:
        state_wp: Output of
            :func:`~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability`
            (must carry ``proj_final``, ``runs``, ``overs_left``).
        return_as_pandas: When True, return a :class:`pandas.DataFrame`.

    Returns:
        The input rows plus ``exp_runs_remaining:Float64`` (``proj_final - runs``,
        floored at 0) and ``exp_run_rate:Float64`` (per remaining over; null when
        no overs remain). A zero-row input returns the schema with both columns
        appended (all null).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
            from sportsdataverse.cricket.cricket_wpa import cricket_expected_runs
            scored = cricket_win_probability(state)
            er = cricket_expected_runs(scored)
            er.select("exp_runs_remaining", "exp_run_rate").head()
    """
    if state_wp.height == 0:
        out = state_wp.with_columns(
            exp_runs_remaining=pl.lit(None, pl.Float64),
            exp_run_rate=pl.lit(None, pl.Float64),
        )
        return out.to_pandas() if return_as_pandas else out
    out = state_wp.with_columns(
        exp_runs_remaining=(pl.col("proj_final") - pl.col("runs")).clip(lower_bound=0.0).cast(pl.Float64)
    ).with_columns(
        exp_run_rate=pl.when(pl.col("overs_left") > 0)
        .then(pl.col("exp_runs_remaining") / pl.col("overs_left"))
        .otherwise(None)
        .cast(pl.Float64)
    )
    return out.to_pandas() if return_as_pandas else out


def cricket_wpa(state_wp: pl.DataFrame, *, return_as_pandas: bool = False) -> pl.DataFrame | pd.DataFrame:
    """Batting/bowling win-probability added per over/wicket transition.

    ``wpa_batting`` is the change in the batting team's win probability since the
    previous state within the same innings; ``wpa_bowling`` is its negation (the
    bowling side gains exactly what the batting side loses). The lead is taken
    ``.over(["event_id", "innings_number"])`` so no change leaks across matches or
    innings, and the first state of each innings has ``wpa_batting = 0``.

    Args:
        state_wp: Output of
            :func:`~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability`
            (must carry ``event_id``, ``innings_number``, ``balls_bowled``,
            ``win_prob``).
        return_as_pandas: When True, return a :class:`pandas.DataFrame`.

    Returns:
        The input rows (sorted by ``event_id, innings_number, balls_bowled``) plus
        ``win_prob_before:Float64``, ``wpa_batting:Float64`` and
        ``wpa_bowling:Float64``. A zero-row input returns the schema with those
        columns appended (all null).

    Example:
        Quick start::

            from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
            from sportsdataverse.cricket.cricket_wpa import cricket_wpa
            wpa = cricket_wpa(cricket_win_probability(state))
            wpa.select("wpa_batting", "wpa_bowling").head()
    """
    if state_wp.height == 0:
        out = state_wp.with_columns(
            win_prob_before=pl.lit(None, pl.Float64),
            wpa_batting=pl.lit(None, pl.Float64),
            wpa_bowling=pl.lit(None, pl.Float64),
        )
        return out.to_pandas() if return_as_pandas else out
    out = (
        state_wp.sort(["event_id", "innings_number", "balls_bowled"])
        .with_columns(win_prob_before=pl.col("win_prob").shift(1).over(["event_id", "innings_number"]))
        .with_columns(wpa_batting=(pl.col("win_prob") - pl.col("win_prob_before")).fill_null(0.0).cast(pl.Float64))
        .with_columns(wpa_bowling=(-pl.col("wpa_batting")).cast(pl.Float64))
    )
    return out.to_pandas() if return_as_pandas else out
