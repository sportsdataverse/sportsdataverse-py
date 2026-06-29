"""Enhanced stats.nba.com v3 play-by-play."""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.nba import nba_pbp_constants as C


def enhanced_pbp_from_payload(payload: dict[str, Any], *, league_id: str = "00") -> pl.DataFrame:
    """Ingest, normalize, classify, and order v3 play-by-play actions.

    Flattens ``payload["game"]["actions"]``, snake-cases columns via
    ``underscore()``, parses ``seconds_remaining`` from ISO 8601 clock,
    casts ID columns to their canonical dtypes, derives ``event_type``
    string slugs via ``ACTION_TYPE_EVENT``, adds boolean ``is_*`` event
    flag columns, and assigns a deterministic ``order_index`` via the
    fixture-verified sort rule:
    ``period asc → seconds_remaining desc → action_number asc → payload_position asc``.

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

    # --- Task 3: event classification, flags, and total order ---

    # 1. event_type string slug via ACTION_TYPE_EVENT mapping
    df = df.with_columns(
        pl.col("action_type").cast(pl.Utf8).replace_strict(C.ACTION_TYPE_EVENT, default="other").alias("event_type"),
    )

    # 2. Boolean event-flag columns
    et = pl.col("event_type")
    df = df.with_columns(
        (et == "made_shot").alias("is_made_shot"),
        (et == "missed_shot").alias("is_missed_shot"),
        (et == "free_throw").alias("is_free_throw"),
        (et == "rebound").alias("is_rebound"),
        (et == "turnover").alias("is_turnover"),
        (et == "foul").alias("is_foul"),
        (et == "substitution").alias("is_substitution"),
        (et == "jump_ball").alias("is_jump_ball"),
        (et == "timeout").alias("is_timeout"),
        (et == "period").alias("is_period"),
    )

    # 3. Fixture-verified v3 ordering rule (Task 0 ece5e30):
    #    period asc → seconds_remaining desc → action_number asc → payload_position asc.
    #    An event-type priority tiebreak was empirically DISPROVEN — adding one
    #    made agreement with pbpstats worse.  Never add one here.
    df = (
        df.sort(
            ["period", "seconds_remaining", "action_number", "payload_position"],
            descending=[False, True, False, False],
        )
        .with_row_index("order_index")
        .with_columns(pl.col("order_index").cast(pl.Int64))
        .drop("payload_position")
    )

    return df
