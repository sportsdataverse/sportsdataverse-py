"""Shared run-expectancy engine, run-value constants, and validation metrics
for the MLB fielding/catching/baserunning model spine (T6.3, league-agnostic
core all five sibling models import).

Run values are derived from Savant's per-pitch ``delta_run_exp`` (RE288,
batter's perspective) where present; :data:`RUN_VALUES` is a documented
fallback (Tango, Lichtman & Dolphin, *The Book: Playing the Percentages in
Baseball*, 2007; Statcast run-value methodology) used only when
``delta_run_exp`` is absent (pre-2015 feeds / MiLB) -- methodology reference,
no license obligation.

This module is the **single shared home** for the metrics
(:func:`pearson_corr`, :func:`spearman_corr`, :func:`mae`), the count-based
and event-based run-value helpers, the as-of-date leakage boundary
(:func:`as_of_split`), and the one thin wire-touching convenience loader
(:func:`_load_season_pitches`). The T6.1 (pitching) / T6.2 (hitting) sibling
spines share the same pitch-level Statcast loader -- see
:mod:`sportsdataverse.mlb.mlb_statcast_extra`.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Tango, Lichtman & Dolphin, *The Book* (2007) -- the run-value
      methodology this module's fallback constants and RE288 approach follow.

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import Any, List, Union

import numpy as np
import polars as pl
from scipy.stats import rankdata

from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

#: Documented linear-weight fallback (used only when ``delta_run_exp`` is
#: absent from the feed -- pre-2015 seasons / MiLB). Values follow Tango,
#: Lichtman & Dolphin, *The Book* (2007) and the published Statcast
#: run-value tables: stolen base, caught stealing, wild pitch/passed ball,
#: and a generic extra-base-taken credit.
RUN_VALUES: "dict[str, float]" = {
    "sb": 0.175,
    "cs": -0.467,
    "wp_pb": 0.27,
    "extra_base": 0.22,
}


def pearson_corr(a: "np.ndarray", b: "np.ndarray") -> float:
    """Pearson correlation coefficient between two 1-D arrays.

    Args:
        a: First sample array.
        b: Second sample array, same length as ``a``.

    Returns:
        float: Pearson's r. ``nan`` if either input has zero variance.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import pearson_corr
            r = pearson_corr(mine["framing_runs"].to_numpy(), sav["runs_extra_strikes"].to_numpy())
    """
    arr_a = np.asarray(a, dtype=float)
    arr_b = np.asarray(b, dtype=float)
    return float(np.corrcoef(arr_a, arr_b)[0, 1])


def spearman_corr(a: "np.ndarray", b: "np.ndarray") -> float:
    """Spearman rank correlation between two 1-D arrays.

    Args:
        a: First sample array.
        b: Second sample array, same length as ``a``.

    Returns:
        float: Spearman's rho (Pearson correlation of the ranks).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import spearman_corr
            rho = spearman_corr(mine["oaa"].to_numpy(), sav["outs_above_average"].to_numpy())
    """
    arr_a = np.asarray(a, dtype=float)
    arr_b = np.asarray(b, dtype=float)
    return float(np.corrcoef(rankdata(arr_a), rankdata(arr_b))[0, 1])


def mae(a: "np.ndarray", b: "np.ndarray") -> float:
    """Mean absolute error between two 1-D arrays.

    Args:
        a: Predicted / modeled array.
        b: Reference / observed array, same length as ``a``.

    Returns:
        float: ``mean(abs(a - b))``.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import mae
            gap = mae(deciles["mean_pred"].to_numpy(), deciles["mean_actual"].to_numpy())
    """
    arr_a = np.asarray(a, dtype=float)
    arr_b = np.asarray(b, dtype=float)
    return float(np.mean(np.abs(arr_a - arr_b)))


_COUNT_RV_SCHEMA = {"balls": pl.Int64, "strikes": pl.Int64, "strike_run_value": pl.Float64}


def count_strike_run_value(pitches: "pl.DataFrame") -> "pl.DataFrame":
    """Ball-to-strike run-expectancy delta per count, from ``delta_run_exp``.

    ``strike_run_value`` is positive = runs **saved by the defense** per
    stolen strike, since a called strike carries negative ``delta_run_exp``
    for the batting team relative to a ball in the same count:
    ``strike_run_value = -(E[delta_run_exp | called_strike, count] -
    E[delta_run_exp | ball, count])``.

    Args:
        pitches: Pitch-level frame with ``balls``, ``strikes``,
            ``description``, and ``delta_run_exp`` columns (a
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`
            frame). Rows other than ``called_strike``/``ball`` are ignored.

    Returns:
        pl.DataFrame: one row per observed count.

        | Column | Type | Description |
        |---|---|---|
        | balls | Int64 | Ball count (0-3) entering the pitch |
        | strikes | Int64 | Strike count (0-2) entering the pitch |
        | strike_run_value | Float64 | Runs saved by the defense per called strike vs. a ball in this count |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import count_strike_run_value
            rv = count_strike_run_value(pitches)
    """
    if pitches.height == 0 or "delta_run_exp" not in pitches.columns:
        return pl.DataFrame(schema=_COUNT_RV_SCHEMA)
    takes = pitches.filter(pl.col("description").is_in(["called_strike", "ball"])).with_columns(
        pl.col("balls").cast(pl.Int64), pl.col("strikes").cast(pl.Int64)
    )
    if takes.height == 0:
        return pl.DataFrame(schema=_COUNT_RV_SCHEMA)
    means = takes.group_by(["balls", "strikes", "description"]).agg(pl.col("delta_run_exp").mean().alias("mean_dre"))
    wide = means.pivot(values="mean_dre", index=["balls", "strikes"], on="description")
    if "called_strike" not in wide.columns or "ball" not in wide.columns:
        return pl.DataFrame(schema=_COUNT_RV_SCHEMA)
    return (
        wide.with_columns((-(pl.col("called_strike") - pl.col("ball"))).alias("strike_run_value"))
        .select("balls", "strikes", "strike_run_value")
        .sort("balls", "strikes")
    )


def event_run_value(pitches: "pl.DataFrame", events: "List[str]") -> float:
    """Empirical run value of an event set, from mean ``delta_run_exp``.

    Args:
        pitches: Pitch-level frame with an ``events`` column and
            ``delta_run_exp``.
        events: Statcast ``events`` values to average over (e.g.
            ``["stolen_base_2b"]``).

    Returns:
        float: Mean ``delta_run_exp`` over rows whose ``events`` is in
            ``events``. ``0.0`` if the frame is empty, lacks
            ``delta_run_exp``, or no rows match.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import event_run_value
            rv_sb = event_run_value(pitches, ["stolen_base_2b", "stolen_base_3b"])
    """
    if pitches.height == 0 or "delta_run_exp" not in pitches.columns or "events" not in pitches.columns:
        return 0.0
    sub = pitches.filter(pl.col("events").is_in(events))
    if sub.height == 0:
        return 0.0
    val = sub["delta_run_exp"].mean()
    return float(val) if val is not None else 0.0


def as_of_split(events: "pl.DataFrame", cutoff_date: Any, *, date_col: str = "game_date") -> "pl.DataFrame":
    """Leakage boundary: rows strictly before ``cutoff_date`` only.

    The predictive path of the stolen-base (and, where predictive,
    baserunning) model must derive runner/catcher features only from data
    known **before** the event being scored -- this helper is the one place
    that boundary is enforced, so every predictive caller shares it.

    Args:
        events: Any frame carrying a date column.
        cutoff_date: Exclusive upper bound (rows with ``date_col <
            cutoff_date`` are kept).
        date_col: Name of the date column. Defaults to ``"game_date"``.

    Returns:
        pl.DataFrame: the filtered frame (unchanged if empty or missing
            ``date_col``).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import as_of_split
            history = as_of_split(events, cutoff_date=dt.date(2024, 6, 15))
    """
    if events.height == 0 or date_col not in events.columns:
        return events
    return events.filter(pl.col(date_col) < cutoff_date)


def _load_season_pitches(
    seasons: "Union[int, List[int]]", *, return_as_pandas: bool = False, **filters: Any
) -> "pl.DataFrame":
    """Thin per-season :func:`mlb_statcast_search` wrapper -- the only
    wire-touching function in the T6.3 fielding/catching/baserunning spine.

    T6.1/T6.2 shared-loader boundary: if those spines land a cached
    ``mlb_statcast_season_pitches()`` loader, swap this body for it with no
    change to the pure-function model layer.

    Args:
        seasons: One season (int) or a list of seasons.
        return_as_pandas: Return a pandas DataFrame instead of polars.
        **filters: Forwarded to :func:`mlb_statcast_search` (e.g.
            ``player_type``).

    Returns:
        pl.DataFrame: concatenated pitch-level rows across the requested
            seasons (``diagonal_relaxed`` -- tolerant of season-to-season
            schema drift).

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_run_values import _load_season_pitches
            pitches = _load_season_pitches(2024)
    """
    yrs = [seasons] if isinstance(seasons, int) else list(seasons)
    frames = [mlb_statcast_search(f"{y}-01-01", f"{y}-12-31", season=y, **filters) for y in yrs]
    non_empty = [f for f in frames if f.height]
    out = pl.concat(non_empty, how="diagonal_relaxed") if non_empty else pl.DataFrame()
    return out.to_pandas() if return_as_pandas else out
