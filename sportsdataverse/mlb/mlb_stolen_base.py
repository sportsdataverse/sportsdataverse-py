"""Stolen-base success + run value (T6.3, model (5)).

Owns :func:`sb_attempts_from_pitches` (SB/CS attempt extraction, shared with
the catcher-throwing model in :mod:`sportsdataverse.mlb.mlb_catcher_defense`),
:func:`sb_success_surface` (the fitted P(success) grid),
:func:`mlb_stolen_base_value` (the public entry point), and
:func:`predict_sb_success` (the as-of-date predictive path -- the surface is
built from history strictly before the cutoff date via
:func:`sportsdataverse.mlb.mlb_run_values.as_of_split`, then used to score
upcoming attempts, so no same-day/future outcome leaks into the model).

**Real-capture finding (documented deviation from a literal ``events``-column
read):** a real 2024-06 capture confirms Baseball Savant's per-pitch
``mlb_statcast_search`` ``events`` column carries **zero**
``stolen_base_*``/``caught_stealing_*`` values across 116k pitches (a genuine
month of MLB should have several hundred attempts). Those attempts are
narrated only as a trailing clause in the terminal pitch's free-text ``des``
field (e.g. *"Jake Meyers strikes out swinging. Jeremy Peña to 3rd. Jeremy
Peña steals (8) 2nd base."*), attached to whichever batter's plate
appearance the pitch belongs to -- not tagged as their own ``events`` value.
:func:`sb_attempts_from_pitches` therefore detects attempts via a ``des``
regex and reads the runner off the pre-play occupancy column implied by the
attempted base (``on_1b`` for a 2B attempt, etc.), producing an explicit
``outcome`` column (``"success"`` \\| ``"caught"``) rather than trying to
match a Statcast ``events`` value that does not exist in this feed shape.
Because the narrating row's ``delta_run_exp`` bundles the *primary* batter
outcome together with the steal, it cannot isolate the steal's own run
value -- so :func:`mlb_stolen_base_value` uses the documented
:data:`sportsdataverse.mlb.mlb_run_values.RUN_VALUES` ``"sb"``/``"cs"``
fallback constants rather than
:func:`sportsdataverse.mlb.mlb_run_values.event_run_value` on these bundled
rows.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant basestealing run-value leaderboard -- concurrent-validity
      oracle
      (:func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_basestealing_run_value`).

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_run_values import RUN_VALUES, as_of_split

if TYPE_CHECKING:
    import pandas as pd

_ATTEMPTS_SCHEMA = {
    "game_date": pl.Date,
    "runner_id": pl.Utf8,
    "catcher_id": pl.Utf8,
    "base": pl.Utf8,
    "outcome": pl.Utf8,
}

_SURFACE_SCHEMA = {"speed_b": pl.Int64, "pop_b": pl.Int64, "base": pl.Utf8, "p_success": pl.Float64, "n": pl.Int64}

_VALUE_SCHEMA = {
    "runner_id": pl.Utf8,
    "attempts": pl.Int64,
    "p_success_mean": pl.Float64,
    "sb_run_value": pl.Float64,
}

_PREDICT_SCHEMA = {"runner_id": pl.Utf8, "base": pl.Utf8, "p_success": pl.Float64}

_REQUIRED_ATTEMPT_COLS = {"des", "fielder_2", "on_1b", "on_2b", "on_3b"}

#: `des` phrasing observed in a real 2024-06 capture: "steals (N) 2nd base",
#: "caught stealing 2nd, catcher ...", base name is 2nd/3rd/home.
_SB_PATTERN = r"(?i)steals?\s*(?:\(\d+\)\s*)?(2nd|3rd|home)\b"
_CS_PATTERN = r"(?i)caught stealing\s*(2nd|3rd|home)\b"
_BASE_LABELS = {"2nd": "2B", "3rd": "3B", "home": "HOME"}
_BASE_TO_PRE_COL = {"2B": "on_1b", "3B": "on_2b", "HOME": "on_3b"}


def sb_attempts_from_pitches(pitches: "pl.DataFrame") -> "pl.DataFrame":
    """Extract stolen-base / caught-stealing attempts from pitch-level Statcast rows.

    Detects attempts via a ``des`` regex (see module docstring for why --
    the ``events`` column does not carry these in the flat per-pitch search)
    and reads the attempting runner off the pre-play occupancy column
    implied by the attempted base (2B attempt -> ``on_1b``, 3B -> ``on_2b``,
    home -> ``on_3b``).

    Args:
        pitches: A :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`
            frame with ``des``, ``fielder_2``, ``on_1b``/``on_2b``/``on_3b``,
            and (if present) ``game_date``.

    Returns:
        pl.DataFrame: one row per attempt.

        | Column | Type | Description |
        |---|---|---|
        | game_date | Date | Game date (if present in the input) |
        | runner_id | Utf8 | Attempting runner's MLBAM id |
        | catcher_id | Utf8 | Catcher MLBAM id (Savant ``fielder_2``) |
        | base | Utf8 | ``2B`` \\| ``3B`` \\| ``HOME`` |
        | outcome | Utf8 | ``success`` \\| ``caught`` |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_stolen_base import sb_attempts_from_pitches
            sb_attempts = sb_attempts_from_pitches(pitches)
    """
    if pitches.height == 0 or not _REQUIRED_ATTEMPT_COLS.issubset(set(pitches.columns)):
        return pl.DataFrame(schema=_ATTEMPTS_SCHEMA)

    is_sb = pl.col("des").str.contains(_SB_PATTERN).fill_null(False)
    is_cs = pl.col("des").str.contains(_CS_PATTERN).fill_null(False)
    att = pitches.filter(is_sb | is_cs).with_columns(
        pl.col("fielder_2").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("catcher_id"),
        pl.when(is_cs).then(pl.lit("caught")).otherwise(pl.lit("success")).alias("outcome"),
        pl.coalesce(
            pl.col("des").str.extract(_CS_PATTERN, 1),
            pl.col("des").str.extract(_SB_PATTERN, 1),
        ).alias("base_raw"),
    )
    att = att.with_columns(
        pl.col("base_raw").replace_strict(_BASE_LABELS, default=None, return_dtype=pl.Utf8).alias("base"),
        pl.when(pl.col("base_raw") == "2nd")
        .then(pl.col("on_1b"))
        .when(pl.col("base_raw") == "3rd")
        .then(pl.col("on_2b"))
        .when(pl.col("base_raw") == "home")
        .then(pl.col("on_3b"))
        .otherwise(None)
        .alias("runner_raw"),
    )
    att = att.with_columns(pl.col("runner_raw").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("runner_id"))

    cols = ["runner_id", "catcher_id", "base", "outcome"]
    if "game_date" in att.columns:
        cols = ["game_date"] + cols
    out = att.filter(pl.col("runner_id").is_not_null() & pl.col("base").is_not_null()).select(cols)
    if "game_date" not in out.columns:
        out = out.with_columns(pl.lit(None, dtype=pl.Date).alias("game_date")).select(list(_ATTEMPTS_SCHEMA.keys()))
    return out


def sb_success_surface(
    sb_attempts: "pl.DataFrame",
    sprint_speed: "pl.DataFrame",
    poptime: "pl.DataFrame",
    *,
    speed_bin: float = 0.5,
    pop_bin: float = 0.05,
    pop_col: str = "pop_2b_sba",
    alpha: float = 2.0,
) -> "pl.DataFrame":
    """Empirical P(stolen-base success) surface over ``(sprint speed, pop time, base)``.

    Rate per bin is Laplace-smoothed: ``(successes + alpha) / (n + 2 * alpha)``.

    Args:
        sb_attempts: One row per attempt (``runner_id``, ``catcher_id``,
            ``base``, ``outcome``).
        sprint_speed: A
            :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_sprint_speed`
            frame with ``runner_id`` (Utf8) and ``sprint_speed``.
        poptime: A :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_poptime`
            frame with ``catcher_id`` (Utf8) and the pop-time column named
            by ``pop_col``.
        speed_bin: Bin width (ft/sec) for sprint speed. Defaults to ``0.5``.
        pop_bin: Bin width (seconds) for pop time. Defaults to ``0.05``.
        pop_col: Name of the pop-time column in ``poptime``. Defaults to
            ``"pop_2b_sba"``.
        alpha: Laplace smoothing strength. Defaults to ``2.0``.

    Returns:
        pl.DataFrame: one row per observed ``(speed_b, pop_b, base)``.

        | Column | Type | Description |
        |---|---|---|
        | speed_b | Int64 | Sprint-speed bin index |
        | pop_b | Int64 | Pop-time bin index |
        | base | Utf8 | Attempted base |
        | p_success | Float64 | Laplace-smoothed empirical success probability |
        | n | Int64 | Attempts observed in this bin |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_stolen_base import sb_success_surface
            surface = sb_success_surface(sb_attempts, sprint_speed, poptime)
    """
    if sb_attempts.height == 0:
        return pl.DataFrame(schema=_SURFACE_SCHEMA)
    att = sb_attempts.with_columns(
        pl.col("runner_id").cast(pl.Utf8),
        pl.col("catcher_id").cast(pl.Utf8),
        (pl.col("outcome") == "success").cast(pl.Int64).alias("is_success"),
    )
    spd = sprint_speed.with_columns(pl.col("runner_id").cast(pl.Utf8))
    pop = poptime.with_columns(pl.col("catcher_id").cast(pl.Utf8))
    assert att.schema["runner_id"] == spd.schema["runner_id"], "runner_id dtype mismatch before sprint-speed join"
    assert att.schema["catcher_id"] == pop.schema["catcher_id"], "catcher_id dtype mismatch before pop-time join"

    joined = att.join(spd.select("runner_id", "sprint_speed"), on="runner_id", how="left").join(
        pop.select("catcher_id", pop_col), on="catcher_id", how="left"
    )
    joined = joined.with_columns(
        (pl.col("sprint_speed") / speed_bin).floor().cast(pl.Int64, strict=False).alias("speed_b"),
        (pl.col(pop_col) / pop_bin).floor().cast(pl.Int64, strict=False).alias("pop_b"),
    )
    return (
        joined.group_by(["speed_b", "pop_b", "base"])
        .agg(pl.col("is_success").sum().alias("k"), pl.len().alias("n"))
        .with_columns(((pl.col("k") + alpha) / (pl.col("n") + 2 * alpha)).alias("p_success"))
        .select("speed_b", "pop_b", "base", "p_success", "n")
        .sort("speed_b", "pop_b", "base")
    )


def mlb_stolen_base_value(
    sb_attempts: "pl.DataFrame",
    sprint_speed: "pl.DataFrame",
    poptime: "pl.DataFrame",
    *,
    speed_bin: float = 0.5,
    pop_bin: float = 0.05,
    pop_col: str = "pop_2b_sba",
    alpha: float = 2.0,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-runner stolen-base run value: realized-vs-expected run contribution.

    ``sb_run_value = sum(p_success * RUN_VALUES["sb"] + (1 - p_success) *
    RUN_VALUES["cs"])`` per attempt -- the documented fallback constants
    (see module docstring for why, not
    :func:`sportsdataverse.mlb.mlb_run_values.event_run_value` on these
    bundled-``des`` rows), weighted by the surface's modeled success
    probability for that attempt's bin.

    Args:
        sb_attempts: One row per attempt (see :func:`sb_attempts_from_pitches`).
        sprint_speed: Sprint-speed leaderboard frame (``runner_id``, ``sprint_speed``).
        poptime: Pop-time leaderboard frame (``catcher_id``, ``pop_col``).
        speed_bin: Sprint-speed bin width. Defaults to ``0.5``.
        pop_bin: Pop-time bin width. Defaults to ``0.05``.
        pop_col: Pop-time column name in ``poptime``. Defaults to ``"pop_2b_sba"``.
        alpha: Laplace smoothing strength for the surface. Defaults to ``2.0``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per runner.

        | Column | Type | Description |
        |---|---|---|
        | runner_id | Utf8 | Runner MLBAM id |
        | attempts | Int64 | Stolen-base attempts |
        | p_success_mean | Float64 | Mean modeled success probability across attempts |
        | sb_run_value | Float64 | Sum of realized-vs-expected run contribution |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_stolen_base import mlb_stolen_base_value
            sb_value = mlb_stolen_base_value(sb_attempts, sprint_speed, poptime)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant basestealing run-value leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if sb_attempts.height == 0:
        out = pl.DataFrame(schema=_VALUE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    att = sb_attempts.with_columns(pl.col("runner_id").cast(pl.Utf8), pl.col("catcher_id").cast(pl.Utf8))
    surface = sb_success_surface(
        sb_attempts, sprint_speed, poptime, speed_bin=speed_bin, pop_bin=pop_bin, pop_col=pop_col, alpha=alpha
    )
    spd = sprint_speed.with_columns(pl.col("runner_id").cast(pl.Utf8))
    pop = poptime.with_columns(pl.col("catcher_id").cast(pl.Utf8))

    joined = (
        att.join(spd.select("runner_id", "sprint_speed"), on="runner_id", how="left")
        .join(pop.select("catcher_id", pop_col), on="catcher_id", how="left")
        .with_columns(
            (pl.col("sprint_speed") / speed_bin).floor().cast(pl.Int64, strict=False).alias("speed_b"),
            (pl.col(pop_col) / pop_bin).floor().cast(pl.Int64, strict=False).alias("pop_b"),
        )
    )
    joined = joined.join(
        surface.select("speed_b", "pop_b", "base", "p_success"), on=["speed_b", "pop_b", "base"], how="left"
    ).with_columns(pl.col("p_success").fill_null(0.5))

    rv_sb, rv_cs = RUN_VALUES["sb"], RUN_VALUES["cs"]
    joined = joined.with_columns((pl.col("p_success") * rv_sb + (1 - pl.col("p_success")) * rv_cs).alias("play_value"))
    out = (
        joined.group_by("runner_id")
        .agg(
            pl.len().alias("attempts"),
            pl.col("p_success").mean().alias("p_success_mean"),
            pl.col("play_value").sum().alias("sb_run_value"),
        )
        .sort("sb_run_value", descending=True)
        .select("runner_id", "attempts", "p_success_mean", "sb_run_value")
    )
    return out.to_pandas() if return_as_pandas else out


def predict_sb_success(
    upcoming: "pl.DataFrame",
    history: "pl.DataFrame",
    cutoff_date,
    *,
    speed_bin: float = 0.5,
    pop_bin: float = 0.05,
    pop_col: str = "pop_2b_sba",
    alpha: float = 2.0,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """As-of-date predictive P(success): the surface is fit on history strictly before ``cutoff_date``.

    The leakage boundary: :func:`sportsdataverse.mlb.mlb_run_values.as_of_split`
    drops every ``history`` row with ``game_date >= cutoff_date`` before the
    success-rate grid is built, so ``upcoming`` attempts are scored only
    against what was knowable at that date.

    Args:
        upcoming: Attempts to score, each carrying ``runner_id``,
            ``base``, ``sprint_speed``, and the ``pop_col`` pop-time column.
        history: Prior attempts with ``game_date``, ``outcome``,
            ``sprint_speed``, and ``pop_col`` -- used to fit the surface via
            :func:`as_of_split`.
        cutoff_date: Exclusive upper bound on ``history["game_date"]``.
        speed_bin: Sprint-speed bin width. Defaults to ``0.5``.
        pop_bin: Pop-time bin width. Defaults to ``0.05``.
        pop_col: Pop-time column name. Defaults to ``"pop_2b_sba"``.
        alpha: Laplace smoothing strength. Defaults to ``2.0``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per scored attempt.

        | Column | Type | Description |
        |---|---|---|
        | runner_id | Utf8 | Runner MLBAM id |
        | base | Utf8 | Attempted base |
        | p_success | Float64 | Modeled success probability, as-of ``cutoff_date`` |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_stolen_base import predict_sb_success
            preds = predict_sb_success(upcoming, history, cutoff_date=dt.date(2024, 6, 15))
    """
    hist = as_of_split(history, cutoff_date)
    if hist.height == 0 or upcoming.height == 0:
        out = pl.DataFrame(schema=_PREDICT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    hist2 = hist.with_columns(
        (pl.col("outcome") == "success").cast(pl.Int64).alias("is_success"),
        (pl.col("sprint_speed") / speed_bin).floor().cast(pl.Int64, strict=False).alias("speed_b"),
        (pl.col(pop_col) / pop_bin).floor().cast(pl.Int64, strict=False).alias("pop_b"),
    )
    surface = (
        hist2.group_by(["speed_b", "pop_b", "base"])
        .agg(pl.col("is_success").sum().alias("k"), pl.len().alias("n"))
        .with_columns(((pl.col("k") + alpha) / (pl.col("n") + 2 * alpha)).alias("p_success"))
    )

    up = upcoming.with_columns(
        pl.col("runner_id").cast(pl.Utf8),
        (pl.col("sprint_speed") / speed_bin).floor().cast(pl.Int64, strict=False).alias("speed_b"),
        (pl.col(pop_col) / pop_bin).floor().cast(pl.Int64, strict=False).alias("pop_b"),
    )
    out = (
        up.join(surface.select("speed_b", "pop_b", "base", "p_success"), on=["speed_b", "pop_b", "base"], how="left")
        .with_columns(pl.col("p_success").fill_null(0.5))
        .select("runner_id", "base", "p_success")
    )
    return out.to_pandas() if return_as_pandas else out
