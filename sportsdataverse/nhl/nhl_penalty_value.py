"""NHL/PWHL penalty drawn/taken net value (T5.2 model 5).

Extracts penalty events from a parsed pbp frame and tallies each player's
penalties drawn vs. taken (minor/major split), converting the net into
expected goals via ``get_constants(league).pp_goal_value`` /
``major_penalty_value``. League-agnostic algorithm; league-specific values
come from :mod:`sportsdataverse.nhl.nhl_microstat_constants`.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value

        out = nhl_penalty_value(pbp)
        print(out.sort("net_penalty_value", descending=True).head())

See Also:
    * `nhl-api-py`_ -- Python NHL API client (companion data source).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.nhl.nhl_microstat_constants import get_constants

PENALTY_SCHEMA = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "committed_player_id": pl.Utf8,
    "drawn_player_id": pl.Utf8,
    "penalty_type_code": pl.Utf8,
    "is_minor": pl.Boolean,
    "is_major": pl.Boolean,
    "zone_code": pl.Utf8,
}

VALUE_SCHEMA = {
    "player_id": pl.Utf8,
    "penalties_drawn": pl.Int64,
    "penalties_taken": pl.Int64,
    "minors_drawn": pl.Int64,
    "minors_taken": pl.Int64,
    "majors_drawn": pl.Int64,
    "majors_taken": pl.Int64,
    "net_penalties": pl.Int64,
    "net_penalty_value": pl.Float64,
}

_MAJOR_CODES = {"MAJ", "MAJ-DBL", "MATCH"}


def extract_penalties(pbp: pl.DataFrame) -> pl.DataFrame:
    """Extract one row per penalty event from a parsed pbp frame.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract): ``type_desc_key``,
            ``committed_player_id``, ``drawn_player_id``,
            ``penalty_type_code``, ``zone_code``.

    Returns:
        One row per penalty: ``game_id``, ``season``,
        ``committed_player_id``, ``drawn_player_id``, ``penalty_type_code``,
        ``is_minor``, ``is_major``, ``zone_code``. Zero-row input (or a
        frame missing ``type_desc_key``) returns a zero-row frame with this
        schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_penalty_value import extract_penalties

            penalties = extract_penalties(pbp)
    """
    if pbp.height == 0 or "type_desc_key" not in pbp.columns:
        return pl.DataFrame(schema=PENALTY_SCHEMA)

    pen = pbp.filter(pl.col("type_desc_key") == "penalty")
    if pen.height == 0:
        return pl.DataFrame(schema=PENALTY_SCHEMA)

    out = pen.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("committed_player_id").cast(pl.Utf8),
        pl.col("drawn_player_id").cast(pl.Utf8),
        pl.col("penalty_type_code").cast(pl.Utf8),
        (pl.col("penalty_type_code") == "MIN").alias("is_minor"),
        pl.col("penalty_type_code").is_in(list(_MAJOR_CODES)).alias("is_major"),
        pl.col("zone_code").cast(pl.Utf8),
    )
    return out


@overload
def nhl_penalty_value(
    pbp: pl.DataFrame, *, league: str = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...
@overload
def nhl_penalty_value(pbp: pl.DataFrame, *, league: str = ..., return_as_pandas: Literal[True]) -> pd.DataFrame: ...
def nhl_penalty_value(
    pbp: pl.DataFrame,
    *,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-player net penalty drawn/taken value.

    Tallies each player's penalties drawn (``player_id == drawn_player_id``)
    and taken (``player_id == committed_player_id``), split minor/major, and
    converts the net into expected goals:
    ``net_penalty_value = (minors_drawn - minors_taken) * pp_goal_value +
    (majors_drawn - majors_taken) * major_penalty_value`` using
    ``get_constants(league)``.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        league: League key for :func:`~sportsdataverse.nhl.nhl_microstat_constants.get_constants`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-player frame: ``player_id``, ``penalties_drawn``,
        ``penalties_taken``, ``minors_drawn``, ``minors_taken``,
        ``majors_drawn``, ``majors_taken``, ``net_penalties``,
        ``net_penalty_value``. League-wide ``net_penalty_value`` sums to
        (approximately) zero by construction -- every penalty taken by one
        player is drawn by another. Zero-row input returns a zero-row frame
        with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value

            out = nhl_penalty_value(pbp)

        PWHL::

            out_pwhl = nhl_penalty_value(pwhl_pbp, league="pwhl")

    See Also:
        * `cfbfastR`_ -- shares the league-agnostic-algorithm /
          per-league-constants pattern for CFB.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    penalties = extract_penalties(pbp)
    if penalties.height == 0:
        empty = pl.DataFrame(schema=VALUE_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    taken = penalties.select(
        pl.col("committed_player_id").alias("player_id"),
        pl.col("is_minor").cast(pl.Int64).alias("minors_taken"),
        pl.col("is_major").cast(pl.Int64).alias("majors_taken"),
        pl.lit(0).alias("minors_drawn"),
        pl.lit(0).alias("majors_drawn"),
    ).filter(pl.col("player_id").is_not_null())
    drawn = penalties.select(
        pl.col("drawn_player_id").alias("player_id"),
        pl.lit(0).alias("minors_taken"),
        pl.lit(0).alias("majors_taken"),
        pl.col("is_minor").cast(pl.Int64).alias("minors_drawn"),
        pl.col("is_major").cast(pl.Int64).alias("majors_drawn"),
    ).filter(pl.col("player_id").is_not_null())

    combined = pl.concat([taken, drawn], how="vertical_relaxed")
    agg = combined.group_by("player_id").agg(
        pl.col("minors_taken").sum(),
        pl.col("majors_taken").sum(),
        pl.col("minors_drawn").sum(),
        pl.col("majors_drawn").sum(),
    )

    constants = get_constants(league)
    agg = agg.with_columns(
        (pl.col("minors_drawn") + pl.col("majors_drawn")).alias("penalties_drawn"),
        (pl.col("minors_taken") + pl.col("majors_taken")).alias("penalties_taken"),
    )
    agg = agg.with_columns(
        (pl.col("penalties_drawn") - pl.col("penalties_taken")).alias("net_penalties"),
        (
            (pl.col("minors_drawn") - pl.col("minors_taken")) * constants.pp_goal_value
            + (pl.col("majors_drawn") - pl.col("majors_taken")) * constants.major_penalty_value
        ).alias("net_penalty_value"),
    )
    out = agg.select(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("penalties_drawn").cast(pl.Int64),
        pl.col("penalties_taken").cast(pl.Int64),
        pl.col("minors_drawn").cast(pl.Int64),
        pl.col("minors_taken").cast(pl.Int64),
        pl.col("majors_drawn").cast(pl.Int64),
        pl.col("majors_taken").cast(pl.Int64),
        pl.col("net_penalties").cast(pl.Int64),
        pl.col("net_penalty_value").cast(pl.Float64),
    )
    return out.to_pandas() if return_as_pandas else out
