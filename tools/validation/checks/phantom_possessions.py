"""Phantom possessions in the college possession table (``ncaa_mbb_possessions``).

Two events logged out of order (or twice) at one timestamp flip possession and flip it back, which
leaves a possession that lasts no time and holds nothing. A **phantom** is a possession that lasts
at most :data:`PHANTOM_MAX_SECONDS`, has no field-goal attempt and no points, is not its period's
last, and does not end in a turnover or steal (a quick turnover is real). Most phantoms end on a
defensive rebound or a substitution row.

A few are real, so the check judges the team-game count, not the row: on the men's 2025-26 season
(12,582 team-games) a team-game averages 0.80 phantoms, the 99th percentile is 4, and 29 team-games
(0.23%) have more than :data:`PHANTOM_TEAM_GAME_LIMIT`. Those are the games whose event order to
re-check; contest 6465160 (Howard 7, Hampton 4) is the worst, its play-by-play possessions 10 to 12
above the box estimate. The possession port is faithful to its source and stays unchanged: this
flags its input, it does not repair it.

Findings are WARN / ``needs_judgment``: a hurried end-of-game sequence can be real.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.validation.findings import CheckContext, Finding, Severity

#: A possession this short (seconds, ``last_event_time``) with nothing in it is a phantom candidate.
PHANTOM_MAX_SECONDS = 2
#: More phantoms than this in one team-game flags it (above the season's 99th percentile, 4).
PHANTOM_TEAM_GAME_LIMIT = 4
#: The datasets the limit was measured on: the women's table is not judged by the men's limit.
DATASETS = frozenset({"ncaa_mbb_possessions"})
#: The possession-table columns the check reads; a frame without them is skipped.
COLUMNS = (
    "contest_id",
    "period",
    "poss_num",
    "poss_team",
    "pts",
    "first_shot_type",
    "last_event_time",
    "last_event_type",
)
_REAL_QUICK_ENDS = ("Turnover", "Steal")


def phantom_counts(frame: pl.DataFrame) -> pl.DataFrame:
    """Phantom and total possessions per team-game.

    Args:
        frame: Possession rows with :data:`COLUMNS`.

    Returns:
        One row per ``(contest_id, poss_team)``: ``phantoms`` and ``possessions``.
    """
    # The period's last possession, taken over the whole frame: inside the team group a window would
    # give the team's own last possession of the period instead.
    last = pl.col("_period_last")
    phantom = (
        pl.col("first_shot_type").is_null()
        & (pl.col("pts").fill_null(0) == 0)
        # a possession of unknown length is not a phantom
        & (pl.col("last_event_time") <= PHANTOM_MAX_SECONDS).fill_null(False)
        & (pl.col("poss_num") != last)
        & (pl.col("last_event_type").is_in(_REAL_QUICK_ENDS).fill_null(False) == False)  # noqa: E712
    )
    return (
        frame.with_columns(pl.col("poss_num").max().over("contest_id", "period").alias("_period_last"))
        .group_by("contest_id", "poss_team")
        .agg(phantom.sum().cast(pl.Int64).alias("phantoms"), pl.len().cast(pl.Int64).alias("possessions"))
        .sort("contest_id", "poss_team")
    )


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Flag team-games with more than :data:`PHANTOM_TEAM_GAME_LIMIT` phantom possessions.

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation; skipped unless ``dataset`` is in :data:`DATASETS`
            and the frame has every :data:`COLUMNS` column.
        ctx: Check context supplying ``domain``.

    Returns:
        One WARN ``needs_judgment`` finding per flagged team-game, with its phantom count as
        ``actual`` / ``metric``; empty for any other frame.
    """
    if dataset not in DATASETS or frame.height == 0 or not set(COLUMNS) <= set(frame.columns):
        return []
    flagged = phantom_counts(frame).filter(pl.col("phantoms") > PHANTOM_TEAM_GAME_LIMIT)
    return [
        Finding(
            "phantom_possessions",
            Severity.WARN,
            ctx.domain,
            dataset,
            f"contest {row['contest_id']} {row['poss_team']}: {row['phantoms']} of {row['possessions']} possessions "
            f"last <= {PHANTOM_MAX_SECONDS} s with no shot and no points; re-check the event order",
            locator={"contest_id": row["contest_id"], "poss_team": row["poss_team"]},
            expected=PHANTOM_TEAM_GAME_LIMIT,
            actual=row["phantoms"],
            metric=float(row["phantoms"]),
            needs_judgment=True,
        )
        for row in flagged.iter_rows(named=True)
    ]
