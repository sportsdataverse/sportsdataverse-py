"""Schedule-as-truth completeness — every scheduled final game must publish.

The highest-value live gate in the contract stack: reconcile a producer's
published game ids against the league schedule feed, treating the schedule
as ground truth for what SHOULD exist. A scheduled-and-completed game
missing from the published set is exactly the class of the NBA nine-season
silent-shortfall incident — caught at publish time here instead of at audit
time months later. Extra published ids that the schedule has never heard of
are reported too (warn-class: unknown provenance, not incompleteness).

:func:`schedule_frame_from_espn_scoreboard` parses the universal ESPN
scoreboard shape (identical across the league fixtures) into the schedule
frame; any other feed works as long as it yields a game-id column and a
completed flag.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional

import polars as pl


def schedule_frame_from_espn_scoreboard(payload: Dict[str, Any]) -> pl.DataFrame:
    """Parse an ESPN scoreboard payload into a schedule frame.

    Args:
        payload: Site v2 ``scoreboard`` dict (``events[]`` with ``id`` and
            ``status.type`` — the shape is identical across leagues).

    Returns:
        One row per event: ``game_id`` (Utf8), ``completed`` (Boolean),
        ``state`` (Utf8: pre/in/post), ``season`` (Int64, null when the
        event carries none). Empty payloads return the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.modeling.integrity import schedule_frame_from_espn_scoreboard
            schedule = schedule_frame_from_espn_scoreboard(scoreboard_payload)
            schedule.filter(pl.col("completed") == True)
    """
    rows: List[Dict[str, Any]] = []
    for event in payload.get("events") or []:
        status = (event.get("status") or {}).get("type") or {}
        season = (event.get("season") or {}).get("year")
        rows.append(
            {
                "game_id": str(event.get("id") or ""),
                "completed": bool(status.get("completed") or False),
                "state": str(status.get("state") or ""),
                "season": int(season) if season is not None else None,
            }
        )
    schema = {"game_id": pl.Utf8, "completed": pl.Boolean, "state": pl.Utf8, "season": pl.Int64}
    return pl.DataFrame(rows, schema=schema)


@dataclasses.dataclass(frozen=True)
class ScheduleCompleteness:
    """Outcome of a schedule-as-truth reconciliation.

    Attributes:
        n_scheduled_final: Scheduled games marked completed.
        n_published: Distinct published game ids.
        missing_ids: Scheduled-final ids ABSENT from the published set
            (the incident class; sorted).
        extra_ids: Published ids the schedule has never heard of (sorted;
            warn-class, does not affect ``ok``).
        coverage: Fraction of scheduled-final games published (1.0 when
            nothing is scheduled).
        by_partition: Per-partition missing counts when a partition key was
            given (``partition``, ``n_scheduled_final``, ``n_missing``).
    """

    n_scheduled_final: int
    n_published: int
    missing_ids: List[str]
    extra_ids: List[str]
    coverage: float
    by_partition: Optional[pl.DataFrame] = None

    @property
    def ok(self) -> bool:
        """True when every scheduled final game is published."""
        return not self.missing_ids

    def raise_if_incomplete(self) -> None:
        """Raise when scheduled final games are missing (producer-side block).

        Raises:
            ValueError: Listing the missing ids (capped at 25 in the message).
        """
        if self.missing_ids:
            shown = ", ".join(self.missing_ids[:25])
            more = f" (+{len(self.missing_ids) - 25} more)" if len(self.missing_ids) > 25 else ""
            raise ValueError(
                f"schedule completeness BLOCKED: {len(self.missing_ids)} scheduled final games "
                f"missing from the published set: {shown}{more}"
            )


def schedule_completeness(
    published: pl.DataFrame,
    schedule: pl.DataFrame,
    *,
    game_key: str = "game_id",
    completed_col: str = "completed",
    partition_key: Optional[str] = None,
) -> ScheduleCompleteness:
    """Reconcile published game ids against the schedule feed.

    Args:
        published: The producer's frame (any granularity — ids are uniqued).
        schedule: The schedule frame (see
            :func:`schedule_frame_from_espn_scoreboard`); when
            ``completed_col`` is absent every scheduled row counts as final.
        game_key: The game-id column, present in both frames.
        completed_col: Boolean column marking finished games on the schedule.
        partition_key: Optional schedule column (``season``) for a
            per-partition missing breakdown.

    Returns:
        The :class:`ScheduleCompleteness`.

    Raises:
        ValueError: On a missing ``game_key`` or a join-key dtype mismatch
            (fix the id dtype at the boundary, never paper over).

    Example:
        Producer-side gate after a season scrape::

            from sportsdataverse.modeling.integrity import (
                schedule_completeness, schedule_frame_from_espn_scoreboard,
            )
            schedule = schedule_frame_from_espn_scoreboard(scoreboard)
            schedule_completeness(published_pbp, schedule).raise_if_incomplete()
    """
    for frame, side in ((published, "published"), (schedule, "schedule")):
        if game_key not in frame.columns:
            raise ValueError(f"game_key {game_key!r} missing from the {side} frame")
    if published.schema[game_key] != schedule.schema[game_key]:
        raise ValueError(
            f"join-key dtype mismatch on {game_key!r}: published={published.schema[game_key]} "
            f"schedule={schedule.schema[game_key]}"
        )
    scheduled_final = (
        schedule.filter(pl.col(completed_col) == True)  # noqa: E712
        if completed_col in schedule.columns
        else schedule
    )
    published_ids = published.select(game_key).unique()
    missing = scheduled_final.select(game_key).unique().join(published_ids, on=game_key, how="anti")
    extra = published_ids.join(schedule.select(game_key).unique(), on=game_key, how="anti")
    n_final = scheduled_final.select(game_key).unique().height
    coverage = 1.0 if n_final == 0 else (n_final - missing.height) / n_final
    by_partition: Optional[pl.DataFrame] = None
    if partition_key is not None and partition_key in scheduled_final.columns:
        by_partition = (
            scheduled_final.unique(subset=[game_key])
            .join(published_ids.with_columns(pl.lit(True).alias("_published")), on=game_key, how="left")
            .group_by(partition_key, maintain_order=True)
            .agg(
                pl.len().alias("n_scheduled_final"),
                (pl.col("_published").is_null()).cast(pl.Int64).sum().alias("n_missing"),
            )
            .rename({partition_key: "partition"})
        )
    return ScheduleCompleteness(
        n_scheduled_final=n_final,
        n_published=published_ids.height,
        missing_ids=sorted(missing.get_column(game_key).cast(pl.Utf8).to_list()),
        extra_ids=sorted(extra.get_column(game_key).cast(pl.Utf8).to_list()),
        coverage=float(coverage),
        by_partition=by_partition,
    )
