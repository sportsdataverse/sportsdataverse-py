from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Run full-dataset anomaly checks over a frame.

    Emits an ERROR finding for duplicate rows on the join keys; WARN findings
    (routed to the anomaly-triage judgment agent) for columns whose null-rate
    exceeds ``null_rate_warn`` (default 0.5) and for numeric columns whose mean
    shifts beyond ``mean_shift_warn`` (default 0.10) versus the prior release.

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation.
        ctx: Check context supplying join_keys, thresholds, and the optional
            prior_frame (the previous release) for drift detection.

    Returns:
        A list of Finding records; empty if no anomaly is detected.
    """
    findings: list[Finding] = []
    height = frame.height or 1

    if ctx.join_keys:
        keys = list(ctx.join_keys)
        missing_keys = [k for k in keys if k not in frame.columns]
        if missing_keys:
            findings.append(
                Finding(
                    "sweep",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"join key(s) {missing_keys!r} absent from frame; duplicate check skipped",
                    locator={"missing_join_keys": missing_keys},
                )
            )
        else:
            n_dups = frame.height - frame.select(keys).unique().height
            if n_dups > 0:
                findings.append(
                    Finding(
                        "sweep",
                        Severity.ERROR,
                        ctx.domain,
                        dataset,
                        f"{n_dups} duplicate row(s) on join keys {keys}",
                        locator={"join_keys": keys},
                        metric=float(n_dups),
                    )
                )

    null_floor = ctx.thresholds.get("null_rate_warn", 0.5)
    null_counts = frame.null_count()
    for col in frame.columns:
        rate = null_counts.select(pl.col(col)).item() / height
        if rate > null_floor:
            findings.append(
                Finding(
                    "sweep",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{col!r} null-rate {rate:.3f} > {null_floor}",
                    locator={"column": col},
                    metric=rate,
                    needs_judgment=True,
                )
            )

    if ctx.prior_frame is not None:
        drift = ctx.thresholds.get("mean_shift_warn", 0.10)
        for col, dtype in frame.schema.items():
            if not dtype.is_numeric() or col not in ctx.prior_frame.columns:
                continue
            cur = frame.select(pl.col(col).mean()).item()
            pri = ctx.prior_frame.select(pl.col(col).mean()).item()
            if cur is None or pri is None:
                continue
            if pri == 0:
                # Prior mean is zero — use absolute shift instead of relative
                # to avoid permanently disabling drift detection for zero-baseline metrics.
                rel = abs(cur - pri)
            else:
                rel = abs(cur - pri) / abs(pri)
            if rel > drift:
                findings.append(
                    Finding(
                        "sweep",
                        Severity.WARN,
                        ctx.domain,
                        dataset,
                        f"{col!r} mean shifted {rel:.1%} vs prior release",
                        locator={"column": col},
                        metric=rel,
                        needs_judgment=True,
                    )
                )

    return findings
