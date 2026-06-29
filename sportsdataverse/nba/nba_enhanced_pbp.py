"""Enhanced stats.nba.com v3 play-by-play."""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.nba import nba_pbp_constants as C


def enhanced_pbp_from_payload(payload: dict[str, Any], *, league_id: str = "00") -> pl.DataFrame:
    """Ingest and normalize v3 play-by-play actions.

    Flattens ``payload["game"]["actions"]``, snake-cases columns via
    ``underscore()``, parses ``seconds_remaining`` from ISO 8601 clock,
    and casts ID columns to their canonical dtypes.

    Event flags and ``order_index`` are added in Task 3.

    Args:
        payload: Raw ``playbyplayv3`` dict from stats.nba.com.
        league_id: League identifier (default ``"00"`` for NBA). Currently unused;
            reserved for G-League (``"20"``) and Summer League (``"15"``) support.

    Returns:
        Polars DataFrame with schema ``ENHANCED_PBP_SCHEMA``. Empty actions array
        returns a zero-row frame with the documented schema (never raises).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
            import json
            with open("playbyplayv3.json") as f:
                payload = json.load(f)
            df = enhanced_pbp_from_payload(payload)
            print(df.shape, df.schema["game_id"])

        Filter by clock time::

            row = df.filter(pl.col("clock") == "PT08M24.00S").head(1)
            print(row["seconds_remaining"][0])  # ~504.0 seconds

        See Also:
            * `nba_pbp_constants`_ -- schemas and helper functions
            * `Task 3 (event flags)`_ -- adds is_* event columns and order_index

        .. _nba_pbp_constants: sportsdataverse.nba.nba_pbp_constants
        .. _Task 3 (event flags): https://github.com/sportsdataverse/sportsdataverse-py/issues/???
    """
    actions = (((payload or {}).get("game") or {}).get("actions")) or []
    if not actions:
        return pl.DataFrame(schema=C.ENHANCED_PBP_SCHEMA)

    game_id = (payload.get("game") or {}).get("gameId") or payload.get("gameId")
    df = pl.DataFrame(actions, infer_schema_length=None).with_row_index("payload_position")

    # Snake-case columns; payload_position = raw actions[] order (sort tiebreak)
    df = df.rename({c: underscore(c) for c in df.columns})

    # Cast IDs and parse clock; add game_id
    df = df.with_columns(
        pl.lit(str(game_id)).alias("game_id"),
        pl.col("action_number").cast(pl.Int64),
        pl.col("period").cast(pl.Int64),
        pl.col("person_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        C.iso_clock_to_seconds(pl.col("clock")).alias("seconds_remaining"),
    )

    return df
