"""Baserunning value (T6.3, model (4)) -- extra-bases-taken above expected.

Owns :func:`advancement_opportunities` (opportunity + outcome extraction from
Statcast search pre-play base-occupancy columns) and
:func:`mlb_baserunning_value` (the public entry point).

**Source-of-truth decision (resolves the design's Sec. 9 open item):**
advancement opportunities are derived from
:func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`'s
``on_1b``/``on_2b``/``on_3b`` pre-play occupancy columns (the *next* plate
appearance's pre-state is read as the *current* play's post-state, within
each ``game_pk``), not from ``mlb_pbp`` (statsapi). This keeps the whole
T6.3 spine on one data source and avoids a second network surface; the
tradeoff is that mid-plate-appearance events between two PAs (a caught
stealing before the next batter's first pitch) are folded into the next
PA's pre-state rather than modeled as a separate event -- acceptable since
:func:`mlb_stolen_base` (model (5)) covers stolen-base attempts on their own
terms. Run values use the documented :data:`sportsdataverse.mlb.mlb_run_values.RUN_VALUES`
``"extra_base"`` fallback rather than a bespoke RE288 read, since a
first-class RE288 value for "took the extra base" needs the full
base-*and*-out state transition (see :mod:`sportsdataverse.mlb.mlb_run_expectancy`),
which this occupancy-only approximation does not track.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant baserunning run-value leaderboard -- concurrent-validity
      oracle
      (:func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_baserunning_run_value`).

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_run_values import RUN_VALUES

if TYPE_CHECKING:
    import pandas as pd

_OPP_SCHEMA = {"runner_id": pl.Utf8, "opp_type": pl.Utf8, "took_extra": pl.Int8}

_VALUE_SCHEMA = {
    "runner_id": pl.Utf8,
    "opportunities": pl.Int64,
    "extra_bases_above_expected": pl.Float64,
    "baserunning_runs": pl.Float64,
}

_REQUIRED_COLS = {"game_pk", "at_bat_number", "on_1b", "on_2b", "on_3b", "events"}


def advancement_opportunities(events: "pl.DataFrame") -> "pl.DataFrame":
    """Extract first-to-third / second-to-home / tag-up opportunities and outcomes.

    One plate-appearance row (the terminal, non-null-``events`` pitch of
    each ``(game_pk, at_bat_number)``) is matched against the *next*
    plate appearance's pre-play occupancy (``on_1b``/``on_2b``/``on_3b``,
    shifted within ``game_pk``) to read the post-play base state.

    Args:
        events: Pitch-level frame (a
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`
            output) with ``game_pk``, ``at_bat_number``, ``on_1b``,
            ``on_2b``, ``on_3b``, ``events``.

    Returns:
        pl.DataFrame: one row per detected opportunity.

        | Column | Type | Description |
        |---|---|---|
        | runner_id | Utf8 | MLBAM id of the runner facing the advancement decision |
        | opp_type | Utf8 | ``first_to_third`` \\| ``second_to_home`` \\| ``tag_up`` |
        | took_extra | Int8 | 1 if the runner advanced the extra base, else 0 |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_baserunning import advancement_opportunities
            opps = advancement_opportunities(pitches)
    """
    if events.height == 0 or not _REQUIRED_COLS.issubset(set(events.columns)):
        return pl.DataFrame(schema=_OPP_SCHEMA)

    pa = (
        events.filter(pl.col("events").is_not_null())
        .sort(["game_pk", "at_bat_number"])
        .unique(subset=["game_pk", "at_bat_number"], keep="last")
    )
    if pa.height == 0:
        return pl.DataFrame(schema=_OPP_SCHEMA)

    pa = pa.sort(["game_pk", "at_bat_number"]).with_columns(
        pl.col("on_1b").shift(-1).over("game_pk").alias("post_1b"),
        pl.col("on_2b").shift(-1).over("game_pk").alias("post_2b"),
        pl.col("on_3b").shift(-1).over("game_pk").alias("post_3b"),
    )

    is_single = pl.col("events") == "single"
    is_fly_scoring_type = pl.col("events").is_in(["field_out", "sac_fly"])

    def _cast_runner(col: str) -> "pl.Expr":
        return pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8)

    first_to_third = (
        pa.filter(pl.col("on_1b").is_not_null() & is_single)
        .with_columns(
            pl.lit("first_to_third").alias("opp_type"),
            _cast_runner("on_1b").alias("runner_id"),
            (pl.col("post_3b") == pl.col("on_1b")).fill_null(False).cast(pl.Int8).alias("took_extra"),
        )
        .select("runner_id", "opp_type", "took_extra")
    )

    scored_away_1b = (pl.col("post_1b") != pl.col("on_2b")).fill_null(True)
    scored_away_2b = (pl.col("post_2b") != pl.col("on_2b")).fill_null(True)
    scored_away_3b = (pl.col("post_3b") != pl.col("on_2b")).fill_null(True)
    second_to_home = (
        pa.filter(pl.col("on_2b").is_not_null() & is_single)
        .with_columns(
            pl.lit("second_to_home").alias("opp_type"),
            _cast_runner("on_2b").alias("runner_id"),
            (scored_away_1b & scored_away_2b & scored_away_3b).cast(pl.Int8).alias("took_extra"),
        )
        .select("runner_id", "opp_type", "took_extra")
    )

    scored_away_1b3 = (pl.col("post_1b") != pl.col("on_3b")).fill_null(True)
    scored_away_2b3 = (pl.col("post_2b") != pl.col("on_3b")).fill_null(True)
    scored_away_3b3 = (pl.col("post_3b") != pl.col("on_3b")).fill_null(True)
    tag_up = (
        pa.filter(pl.col("on_3b").is_not_null() & is_fly_scoring_type)
        .with_columns(
            pl.lit("tag_up").alias("opp_type"),
            _cast_runner("on_3b").alias("runner_id"),
            (scored_away_1b3 & scored_away_2b3 & scored_away_3b3).cast(pl.Int8).alias("took_extra"),
        )
        .select("runner_id", "opp_type", "took_extra")
    )

    return pl.concat([first_to_third, second_to_home, tag_up], how="vertical_relaxed")


def mlb_baserunning_value(
    events: "pl.DataFrame",
    sprint_speed: "pl.DataFrame",
    *,
    speed_bin: float = 1.0,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-runner baserunning runs from extra-bases-taken above expected.

    Expected extra-base probability is an empirical rate by ``(opp_type,
    speed_bin)``; ``baserunning_runs = extra_bases_above_expected *
    RUN_VALUES["extra_base"]``.

    Args:
        events: Pitch-level frame passed to
            :func:`advancement_opportunities`.
        sprint_speed: A
            :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_sprint_speed`
            frame with ``runner_id`` (Utf8) and ``sprint_speed``.
        speed_bin: Bin width (ft/sec) for the sprint-speed bucket. Defaults
            to ``1.0``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per runner.

        | Column | Type | Description |
        |---|---|---|
        | runner_id | Utf8 | Runner MLBAM id |
        | opportunities | Int64 | Advancement opportunities faced |
        | extra_bases_above_expected | Float64 | Sum of (took_extra - expected rate) |
        | baserunning_runs | Float64 | extra_bases_above_expected x RUN_VALUES["extra_base"] |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_baserunning import mlb_baserunning_value
            baserunning = mlb_baserunning_value(pitches, sprint_speed)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant baserunning run-value leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    opps = advancement_opportunities(events)
    if opps.height == 0:
        out = pl.DataFrame(schema=_VALUE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    spd = sprint_speed.with_columns(pl.col("runner_id").cast(pl.Utf8))
    assert opps.schema["runner_id"] == spd.schema["runner_id"], "runner_id dtype mismatch before sprint-speed join"

    joined = opps.join(spd.select("runner_id", "sprint_speed"), on="runner_id", how="left").with_columns(
        (pl.col("sprint_speed") / speed_bin).floor().cast(pl.Int64, strict=False).alias("speed_b")
    )
    rate = joined.group_by(["opp_type", "speed_b"]).agg(pl.col("took_extra").mean().alias("expected_rate"))
    rv = RUN_VALUES["extra_base"]

    scored = joined.join(rate, on=["opp_type", "speed_b"], how="left").with_columns(
        (pl.col("took_extra") - pl.col("expected_rate").fill_null(0.5)).alias("extra_gain")
    )
    out = (
        scored.group_by("runner_id")
        .agg(pl.len().alias("opportunities"), pl.col("extra_gain").sum().alias("extra_bases_above_expected"))
        .with_columns((pl.col("extra_bases_above_expected") * rv).alias("baserunning_runs"))
        .sort("baserunning_runs", descending=True)
        .select("runner_id", "opportunities", "extra_bases_above_expected", "baserunning_runs")
    )
    return out.to_pandas() if return_as_pandas else out
