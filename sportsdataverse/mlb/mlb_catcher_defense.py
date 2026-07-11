"""Catcher blocking + throwing value (T6.3, model (2)).

Owns :func:`mlb_catcher_blocking` (dirt-pitch block model) and
:func:`mlb_catcher_throwing` (pop-time-based caught-stealing value). Both are
pure functions over already-loaded frames -- see
:mod:`sportsdataverse.mlb.mlb_run_values` for the shared run-value engine.

**Real-capture finding (documented deviation from a literal ``events``-column
read):** a real 2024-06 capture confirms Baseball Savant's per-pitch
``mlb_statcast_search`` ``events`` column carries **zero**
``wild_pitch``/``passed_ball`` values across 116k pitches (a genuine month
of MLB should have >100). Those events are narrated only as a trailing
clause in the terminal pitch's free-text ``des`` field (e.g. *"Mark Vientos
walks. Starling Marte to 3rd. Wild pitch by pitcher MacKenzie Gore."*),
attached to whichever batter's plate appearance the pitch belongs to -- not
tagged as their own ``events`` value. :func:`mlb_catcher_blocking` therefore
detects a WP/PB via a ``des`` regex, not the ``events`` column (``events``
stays a fallback when ``des`` is absent, e.g. older feeds). Because that
``des`` row's ``delta_run_exp`` bundles the *primary* batter outcome (the
walk) together with the WP/PB, it cannot isolate the WP/PB's own run value --
so ``blocking_runs``/``throwing_runs`` use the documented
:data:`sportsdataverse.mlb.mlb_run_values.RUN_VALUES` fallback constants
rather than :func:`sportsdataverse.mlb.mlb_run_values.event_run_value` on
these bundled rows.

See Also:
    * `baseballr`_ -- R sibling package for MLB sabermetrics.
    * Baseball Savant catcher blocking / catcher throwing leaderboards --
      concurrent-validity oracles
      (:func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_catcher_blocking`,
      :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_catcher_throwing`).

    .. _baseballr: https://baseballr.sportsdataverse.org
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Union

import polars as pl

from sportsdataverse.mlb.mlb_run_values import RUN_VALUES

if TYPE_CHECKING:
    import pandas as pd

_WP_PB_EVENTS = ["wild_pitch", "passed_ball"]
_WP_PB_DES_PATTERN = r"(?i)wild pitch|passed ball"

_BLOCKING_SCHEMA = {
    "catcher_id": pl.Utf8,
    "block_opps": pl.Int64,
    "blocks_above_expected": pl.Float64,
    "blocking_runs": pl.Float64,
}

_THROWING_SCHEMA = {
    "catcher_id": pl.Utf8,
    "attempts": pl.Int64,
    "cs_above_expected": pl.Float64,
    "throwing_runs": pl.Float64,
}


def _block_opportunities(pitches: "pl.DataFrame", *, dirt_bin_width: float) -> "pl.DataFrame":
    """Dirt-pitch block opportunities: a pitch below the zone with a runner on, or a WP/PB event."""
    on_base_cols = [c for c in ("on_1b", "on_2b", "on_3b") if c in pitches.columns]
    df = pitches.with_columns(
        ((pl.col("plate_z") - pl.col("sz_bot")) / (pl.col("sz_top") - pl.col("sz_bot"))).alias("pz_norm")
    )
    if "des" in df.columns:
        # Real capture: WP/PB show up only as a narrated clause in `des`,
        # never as their own `events` value (see module docstring).
        is_wp_pb = pl.col("des").str.contains(_WP_PB_DES_PATTERN).fill_null(False)
    elif "events" in df.columns:
        # is_in(...) on a null events value returns null in polars -- most
        # pitches don't terminate a PA and so carry a null events, which
        # must read as "not a WP/PB" (False), never propagate as null.
        is_wp_pb = pl.col("events").is_in(_WP_PB_EVENTS).fill_null(False)
    else:
        is_wp_pb = pl.lit(False)
    if on_base_cols:
        runner_on = pl.any_horizontal([pl.col(c).is_not_null() for c in on_base_cols])
    else:
        runner_on = pl.lit(False)
    dirt = pl.col("pz_norm") < 0
    opp = (dirt & runner_on) | is_wp_pb
    return df.filter(opp).with_columns(
        (~is_wp_pb).cast(pl.Int64).alias("is_blocked"),
        (pl.col("pz_norm") / dirt_bin_width).floor().cast(pl.Int64).alias("dirt_bin"),
    )


def mlb_catcher_blocking(
    pitches: "pl.DataFrame", *, dirt_bin_width: float = 0.2, return_as_pandas: bool = False
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-catcher blocking runs from a dirt-pitch block-probability model.

    A block opportunity is a pitch below the strike zone (``pz_norm < 0``)
    with a runner on base, or a pitch whose ``des`` narrates a wild
    pitch/passed ball (see module docstring for why ``des``, not
    ``events``). Expected block probability is the empirical block rate
    within the pitch's dirt-depth bin; ``blocking_runs =
    blocks_above_expected * RUN_VALUES["wp_pb"]`` (the documented fallback
    constant -- the narrating row's ``delta_run_exp`` bundles the primary
    batter outcome with the WP/PB and cannot isolate the latter's value).

    Args:
        pitches: Pitch-level frame with ``plate_z``/``sz_top``/``sz_bot``,
            ``fielder_2``, ``des`` (or ``events`` as a fallback), and (if
            present) ``on_1b``/``on_2b``/``on_3b``. MiLB feeds (e.g.
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search_minors`)
            run through the same function -- there is no Savant blocking
            leaderboard oracle for MiLB.
        dirt_bin_width: Bin width for the below-zone depth bucket. Defaults
            to ``0.2`` (zone-normalized units).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per catcher.

        | Column | Type | Description |
        |---|---|---|
        | catcher_id | Utf8 | Catcher MLBAM id (Savant ``fielder_2``) |
        | block_opps | Int64 | Dirt-pitch block opportunities faced |
        | blocks_above_expected | Float64 | Sum of (blocked - expected block rate) |
        | blocking_runs | Float64 | blocks_above_expected x RUN_VALUES["wp_pb"] |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_blocking
            blocking = mlb_catcher_blocking(pitches)

        Pipeline next step (one line)::

            blocking.filter(pl.col("block_opps") >= 50).sort("blocking_runs", descending=True)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant catcher blocking leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if pitches.height == 0:
        out = pl.DataFrame(schema=_BLOCKING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    opps = _block_opportunities(pitches, dirt_bin_width=dirt_bin_width)
    if opps.height == 0:
        out = pl.DataFrame(schema=_BLOCKING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    rate = opps.group_by("dirt_bin").agg(pl.col("is_blocked").mean().alias("expected_block_prob"))
    rv = RUN_VALUES["wp_pb"]
    scored = (
        opps.join(rate, on="dirt_bin", how="left")
        .with_columns(pl.col("fielder_2").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("catcher_id"))
        .with_columns((pl.col("is_blocked") - pl.col("expected_block_prob")).alias("block_gain"))
    )
    out = (
        scored.group_by("catcher_id")
        .agg(pl.len().alias("block_opps"), pl.col("block_gain").sum().alias("blocks_above_expected"))
        .with_columns((pl.col("blocks_above_expected") * rv).alias("blocking_runs"))
        .sort("blocking_runs", descending=True)
        .select("catcher_id", "block_opps", "blocks_above_expected", "blocking_runs")
    )
    return out.to_pandas() if return_as_pandas else out


def mlb_catcher_throwing(
    sb_attempts: "pl.DataFrame",
    poptime: "pl.DataFrame",
    *,
    pop_col: str = "pop_2b_sba",
    pop_bin_width: float = 0.05,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-catcher caught-stealing (throwing) value from a pop-time model.

    Expected caught-stealing probability is a monotone empirical function of
    catcher pop time (binned); ``throwing_runs = cs_above_expected *
    |RUN_VALUES["cs"] - RUN_VALUES["sb"]|`` (the documented fallback
    constants -- see :mod:`sportsdataverse.mlb.mlb_stolen_base` for why a
    real-capture attempt's ``delta_run_exp`` cannot isolate the steal's own
    run value from the bundled primary batter outcome it is narrated
    alongside).

    Args:
        sb_attempts: One row per stolen-base attempt (see
            :func:`sportsdataverse.mlb.mlb_stolen_base.sb_attempts_from_pitches`),
            with ``catcher_id`` (Utf8) and ``outcome`` (``"success"`` \\|
            ``"caught"``). MiLB feeds run through the same function -- there
            is no Savant throwing leaderboard oracle for MiLB.
        poptime: A :func:`sportsdataverse.mlb.mlb_statcast.mlb_statcast_leaderboard_poptime`
            frame with ``catcher_id`` (Utf8) and the pop-time column named
            by ``pop_col``.
        pop_col: Name of the pop-time column in ``poptime``. Defaults to
            ``"pop_2b_sba"`` (pop time to second on stolen-base attempts).
        pop_bin_width: Bin width (seconds) for the pop-time bucket. Defaults
            to ``0.05``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        pl.DataFrame: one row per catcher.

        | Column | Type | Description |
        |---|---|---|
        | catcher_id | Utf8 | Catcher MLBAM id |
        | attempts | Int64 | Stolen-base attempts caught behind the plate |
        | cs_above_expected | Float64 | Sum of (caught - expected CS rate) |
        | throwing_runs | Float64 | cs_above_expected x \\|RUN_VALUES["cs"] - RUN_VALUES["sb"]\\| |

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_catcher_defense import mlb_catcher_throwing
            throwing = mlb_catcher_throwing(sb_attempts, poptime)

    See Also:
        * `baseballr`_ -- R sibling package for MLB sabermetrics.
        * Baseball Savant catcher throwing leaderboard -- concurrent-validity oracle.

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    if sb_attempts.height == 0:
        out = pl.DataFrame(schema=_THROWING_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    att = sb_attempts.with_columns(
        pl.col("catcher_id").cast(pl.Utf8),
        (pl.col("outcome") == "caught").cast(pl.Int64).alias("is_cs"),
    )
    pop = poptime.with_columns(pl.col("catcher_id").cast(pl.Utf8))
    assert att.schema["catcher_id"] == pop.schema["catcher_id"], "catcher_id dtype mismatch before pop-time join"

    joined = att.join(pop.select("catcher_id", pop_col), on="catcher_id", how="left")
    joined = joined.with_columns((pl.col(pop_col) / pop_bin_width).floor().cast(pl.Int64).alias("pop_bin"))

    rate = joined.group_by("pop_bin").agg(pl.col("is_cs").mean().alias("expected_cs_prob"))
    rv = abs(RUN_VALUES["cs"] - RUN_VALUES["sb"])

    scored = joined.join(rate, on="pop_bin", how="left").with_columns(
        (pl.col("is_cs") - pl.col("expected_cs_prob").fill_null(0.5)).alias("cs_gain")
    )
    out = (
        scored.group_by("catcher_id")
        .agg(pl.len().alias("attempts"), pl.col("cs_gain").sum().alias("cs_above_expected"))
        .with_columns((pl.col("cs_above_expected") * rv).alias("throwing_runs"))
        .sort("throwing_runs", descending=True)
        .select("catcher_id", "attempts", "cs_above_expected", "throwing_runs")
    )
    return out.to_pandas() if return_as_pandas else out
