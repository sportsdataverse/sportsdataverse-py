from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Detect cross-group leakage on lag and cumulative columns at group boundaries.

    Partitions the frame by ``ctx.group_key`` (frame assumed to be in play order
    within each group) and performs two checks:

    1. **Lag columns** (``ctx.lag_columns``): flags any value that is non-null on
       a group's first row — a carried prior-group value (ERROR, cross-game leak).
    2. **Cumulative columns** (``ctx.cumulative_columns``): flags any group whose
       first value exceeds the prior group's last value — a non-reset that may
       indicate carried accumulation (WARN, ``needs_judgment=True``; reset
       semantics are column-specific and can false-positive).

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation (play-ordered within each group).
        ctx: Check context supplying ``group_key``, ``lag_columns``, and
            ``cumulative_columns``.

    Returns:
        A list of Finding records; empty if ``group_key`` is absent or neither
        ``lag_columns`` nor ``cumulative_columns`` are declared.
    """
    findings: list[Finding] = []
    gk = ctx.group_key
    if gk not in frame.columns or (not ctx.lag_columns and not ctx.cumulative_columns):
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

    # Cumulative columns: each group's first value should reset (<=) below the
    # prior group's last value; a first-of-group value exceeding the prior
    # group's last is a non-reset (possible carried accumulation) -> WARN.
    # This is a CROSS-GROUP check only (first-of-group vs prior-group-last); it
    # does not check intra-group monotonicity. A group whose last value is null
    # (an all-null column for that group) breaks the prior_last chain, so the
    # FOLLOWING group's comparison is skipped — unreachable for game_play_number
    # (games always have plays), relevant only for future null-bearing columns.
    for col in ctx.cumulative_columns:
        if col not in frame.columns:
            continue
        per_group = indexed.group_by(gk, maintain_order=True).agg(_first=pl.col(col).first(), _last=pl.col(col).last())
        prior_last = per_group.get_column("_last").shift(1)
        first = per_group.get_column("_first")
        n_nonreset = int(((prior_last.is_not_null()) & (first > prior_last)).sum())
        if n_nonreset > 0:
            findings.append(
                Finding(
                    "boundary_leakage",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"cumulative column {col!r} did not reset on {n_nonreset} {gk} boundary(ies) "
                    "(first-of-group exceeds prior group's last)",
                    locator={"column": col, "group_key": gk},
                    metric=float(n_nonreset),
                    needs_judgment=True,
                )
            )

    return findings
