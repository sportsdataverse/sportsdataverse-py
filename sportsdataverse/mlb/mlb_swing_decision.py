"""② Swing-decision run value (SEAGER analog) -- from Savant's per-pitch
``delta_run_exp`` averaged over a zone x count decision surface.

We do NOT rebuild base-out run expectancy here (that is T6.4's
``mlb_run_expectancy_matrix`` / ``run_value``); this module averages Savant's
own per-pitch run value into RV(swing)/RV(take) surfaces, re-fit per pull.
Savant's ``mlb_statcast_leaderboard_swing_take`` total is used only as a
concurrent-validity oracle, never as a model input.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal, Optional, Union, overload

import polars as pl

from sportsdataverse.mlb.mlb_hitting_constants import SWING_DESCRIPTIONS, TAKE_DESCRIPTIONS
from sportsdataverse.mlb.mlb_statcast_extra import mlb_statcast_search

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import
    import pandas as pd

_SWING_DECISION_SCHEMA = {
    "batter": pl.Int64,
    "season": pl.Int64,
    "pitches": pl.Int64,
    "swing_take_runs": pl.Float64,
    "selective_agg": pl.Float64,
    "chase_rate": pl.Float64,
    "n_swings": pl.Int64,
}

_SURFACE_SCHEMA = {"zone": pl.Int64, "count": pl.Utf8, "decision": pl.Utf8, "n": pl.Int64, "rv": pl.Float64}

#: Savant "chase" attack zones (13/14 = waste, outside the strike zone entirely).
_CHASE_ZONES = {11, 12, 13, 14}


def _add_decision(pitches: pl.DataFrame) -> pl.DataFrame:
    """Label each pitch ``swing``/``take``/``null`` and add its ``{balls}-{strikes}`` count.

    Args:
        pitches: Raw Statcast pitch rows (``description``, ``balls``, ``strikes``).

    Returns:
        ``pitches`` with ``decision`` (Utf8, ``"swing"``/``"take"``/``null``)
        and ``count`` (Utf8, e.g. ``"1-1"``) appended.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_swing_decision import _add_decision
            _add_decision(pitches)
    """
    decision = (
        pl.when(pl.col("description").is_in(list(SWING_DESCRIPTIONS)))
        .then(pl.lit("swing"))
        .when(pl.col("description").is_in(list(TAKE_DESCRIPTIONS)))
        .then(pl.lit("take"))
        .otherwise(None)
    )
    count = pl.col("balls").cast(pl.Utf8) + pl.lit("-") + pl.col("strikes").cast(pl.Utf8)
    return pitches.with_columns(decision=decision, count=count)


def swing_take_surfaces(pitches_with_decision: pl.DataFrame) -> pl.DataFrame:
    """Build the RV(swing)/RV(take) zone x count decision surfaces.

    Args:
        pitches_with_decision: Output of :func:`_add_decision` -- must carry
            ``zone``, ``count``, ``decision``, ``delta_run_exp``.

    Returns:
        One row per (``zone``, ``count``, ``decision``): ``n`` (Int64), ``rv``
        (Float64, mean ``delta_run_exp``). Null ``zone``/``delta_run_exp``/
        ``decision`` rows are excluded. Empty input returns a zero-row frame
        with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_swing_decision import _add_decision, swing_take_surfaces
            surf = swing_take_surfaces(_add_decision(pitches))
    """
    valid = pitches_with_decision.filter(
        pl.col("zone").is_not_null() & pl.col("delta_run_exp").is_not_null() & pl.col("decision").is_not_null()
    )
    if valid.height == 0:
        return pl.DataFrame(schema=_SURFACE_SCHEMA)
    return (
        valid.with_columns(pl.col("zone").cast(pl.Int64))
        .group_by("zone", "count", "decision")
        .agg(pl.len().alias("n"), pl.col("delta_run_exp").mean().alias("rv"))
        .sort("zone", "count", "decision")
        .cast(_SURFACE_SCHEMA)
    )


def rv_lookup(pitches_with_decision: pl.DataFrame, surfaces: pl.DataFrame) -> pl.DataFrame:
    """Attach ``rv_swing``/``rv_take``/``rv_chosen``/``rv_optimal`` to each pitch.

    Args:
        pitches_with_decision: Output of :func:`_add_decision`.
        surfaces: Output of :func:`swing_take_surfaces`.

    Returns:
        ``pitches_with_decision`` with ``rv_swing``, ``rv_take``,
        ``rv_chosen`` (the RV of the decision actually made), and
        ``rv_optimal`` (``max(rv_swing, rv_take)``) appended.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_swing_decision import _add_decision, rv_lookup, swing_take_surfaces
            surf = swing_take_surfaces(_add_decision(pitches))
            rv_lookup(_add_decision(pitches), surf)
    """
    swing_pivot = surfaces.filter(pl.col("decision") == "swing").select(
        "zone", "count", pl.col("rv").alias("rv_swing"), pl.col("n").alias("n_swing")
    )
    take_pivot = surfaces.filter(pl.col("decision") == "take").select(
        "zone", "count", pl.col("rv").alias("rv_take"), pl.col("n").alias("n_take")
    )
    out = (
        pitches_with_decision.with_columns(pl.col("zone").cast(pl.Int64))
        .join(swing_pivot, on=["zone", "count"], how="left")
        .join(take_pivot, on=["zone", "count"], how="left")
    )
    rv_chosen = (
        pl.when(pl.col("decision") == "swing")
        .then(pl.col("rv_swing"))
        .when(pl.col("decision") == "take")
        .then(pl.col("rv_take"))
        .otherwise(None)
    )
    rv_optimal = pl.when(pl.col("rv_swing") >= pl.col("rv_take")).then(pl.col("rv_swing")).otherwise(pl.col("rv_take"))
    return out.with_columns(rv_chosen=rv_chosen, rv_optimal=rv_optimal)


@overload
def mlb_swing_decision(
    start_dt: str,
    end_dt: str,
    *,
    puller: Optional[Callable[..., pl.DataFrame]] = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def mlb_swing_decision(
    start_dt: str,
    end_dt: str,
    *,
    puller: Optional[Callable[..., pl.DataFrame]] = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def mlb_swing_decision(
    start_dt: str,
    end_dt: str,
    *,
    puller: Optional[Callable[..., pl.DataFrame]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Per player-season swing/take run value + selective-aggression (SEAGER analog).

    Pulls pitches via ``puller(start_dt, end_dt, player_type="batter")``,
    builds the RV(swing)/RV(take) zone x count surfaces (and the league
    swing-rate table) from the pull itself, then per batter:

    * ``swing_take_runs`` = sum of ``rv_chosen`` (the RV of the decision the
      batter actually made) over all their pitches.
    * ``selective_agg`` = sum of ``rv_chosen - rv_neutral``, where
      ``rv_neutral = swing_rate * rv_swing + (1 - swing_rate) * rv_take`` uses
      the **league** swing rate for that zone x count cell -- positive means
      the batter swings at hittable pitches and takes bad ones more than a
      league-average decision-maker would.
    * ``chase_rate`` = swings / pitches seen in the waste/chase zones
      (``zone in {11,12,13,14}``).

    Args:
        start_dt: Pull start date, ``YYYY-MM-DD``.
        end_dt: Pull end date, ``YYYY-MM-DD``.
        puller: Injectable Statcast search callable -- defaults to
            :func:`sportsdataverse.mlb.mlb_statcast_extra.mlb_statcast_search`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per (``batter``, ``season``): ``pitches``, ``swing_take_runs``,
        ``selective_agg``, ``chase_rate``, ``n_swings``. Empty pull returns a
        zero-row frame with the documented schema.

    Example:
        Quick start::

            from sportsdataverse.mlb.mlb_swing_decision import mlb_swing_decision

            df = mlb_swing_decision("2024-06-01", "2024-06-21")
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("selective_agg", descending=True).head()

        See Also:
            * `baseballr`_ -- Statcast helper functions (R)

        .. _baseballr: https://baseballr.sportsdataverse.org
    """
    puller = puller or mlb_statcast_search
    pitches = puller(start_dt, end_dt, player_type="batter")
    if pitches is None or pitches.height == 0:
        empty = pl.DataFrame(schema=_SWING_DECISION_SCHEMA)
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

    pitches = _add_decision(pitches)
    surfaces = swing_take_surfaces(pitches)

    # league swing rate per (zone, count) cell -- the "neutral" decision-maker baseline
    league_rate = (
        pitches.filter(pl.col("zone").is_not_null() & pl.col("decision").is_not_null())
        .with_columns(pl.col("zone").cast(pl.Int64))
        .group_by("zone", "count")
        .agg((pl.col("decision") == "swing").mean().alias("league_swing_rate"))
    )

    scored = rv_lookup(pitches, surfaces).join(league_rate, on=["zone", "count"], how="left")
    scored = scored.with_columns(
        (
            pl.col("league_swing_rate").fill_null(0.0) * pl.col("rv_swing")
            + (1.0 - pl.col("league_swing_rate").fill_null(0.0)) * pl.col("rv_take")
        ).alias("rv_neutral")
    )
    scored = scored.with_columns(
        (pl.col("rv_chosen") - pl.col("rv_neutral")).alias("_agg_term"),
        (pl.col("decision") == "swing").alias("_is_swing"),
        (pl.col("zone").cast(pl.Int64).is_in(list(_CHASE_ZONES)) & (pl.col("decision") == "swing")).alias(
            "_is_chase_swing"
        ),
        pl.col("zone").cast(pl.Int64).is_in(list(_CHASE_ZONES)).alias("_is_chase_pitch"),
    )

    result = (
        scored.group_by("batter", "season")
        .agg(
            pl.len().alias("pitches"),
            pl.col("rv_chosen").fill_null(0.0).sum().alias("swing_take_runs"),
            pl.col("_agg_term").fill_null(0.0).sum().alias("selective_agg"),
            pl.col("_is_swing").fill_null(False).sum().alias("n_swings"),
            pl.col("_is_chase_swing").fill_null(False).sum().alias("_chase_swings"),
            pl.col("_is_chase_pitch").fill_null(False).sum().alias("_chase_pitches"),
        )
        .with_columns(
            pl.when(pl.col("_chase_pitches") > 0)
            .then(pl.col("_chase_swings") / pl.col("_chase_pitches"))
            .otherwise(0.0)
            .alias("chase_rate")
        )
        .select("batter", "season", "pitches", "swing_take_runs", "selective_agg", "chase_rate", "n_swings")
        .cast(_SWING_DECISION_SCHEMA)
        .sort("batter", "season")
    )

    if return_as_pandas:
        return result.to_pandas()
    return result
