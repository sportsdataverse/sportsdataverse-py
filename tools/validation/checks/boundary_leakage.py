from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Detect cross-group leakage on lag columns at group boundaries.

    Partitions the frame by ``ctx.group_key`` (frame assumed to be in play order
    within each group) and flags any ``ctx.lag_columns`` value that is non-null on
    a group's first row — a carried prior-group value (a cross-game leak).
    ``ctx.cumulative_columns`` is accepted but not yet inspected (follow-up).

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation (play-ordered within each group).
        ctx: Check context supplying ``group_key`` and ``lag_columns``.

    Returns:
        A list of Finding records; empty if ``group_key`` is absent or no
        ``lag_columns`` are declared.
    """
    findings: list[Finding] = []
    gk = ctx.group_key
    if gk not in frame.columns or not ctx.lag_columns:
        return findings

    indexed = frame.with_columns(_grp_idx=pl.int_range(pl.len()).over(gk))
    for col in ctx.lag_columns:
        if col not in frame.columns:
            continue
        n_leak = indexed.filter((pl.col("_grp_idx") == 0) & pl.col(col).is_not_null()).height
        if n_leak > 0:
            findings.append(
                Finding(
                    "boundary_leakage",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"lag column {col!r} is non-null on {n_leak} first-of-{gk} row(s) (cross-game leak)",
                    locator={"column": col, "group_key": gk},
                    metric=float(n_leak),
                )
            )
    return findings
