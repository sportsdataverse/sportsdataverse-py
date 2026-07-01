from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Flag dead columns: entirely-null (or all-NaN) or zero-variance (single-valued) columns.

    A column that never varies is usually a producer that silently fails to
    populate it (e.g. an all-zero ``sacked`` count, an all-null ``fox_jersey``).
    Neither ``extraction`` (null-coverage) nor ``sweep`` (release-over-release
    mean-shift) catches a *long-standing* constant, and ``numeric_parity`` needs
    an oracle reference, so this single-frame check covers the gap.

    For each column NOT in ``ctx.expected_constant_columns``:

    - **all-null or all-NaN** -> WARN ``needs_judgment`` (``kind="all_null"``).
    - **zero-variance** (one distinct non-null/non-NaN value, incl. all-zero numeric) ->
      WARN ``needs_judgment`` (``kind="constant"``).

    An all-null (or all-NaN) column is reported once (as ``all_null``), never also as
    ``constant``. Judgment is routed to the ``anomaly-triage-reviewer``.

    For float columns, NaN is treated the same as null for deadness classification
    (polars treats NaN as a distinct non-null value, but an all-NaN column is
    equally dead/unpopulated as an all-null one).

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation.
        ctx: Check context supplying ``domain`` and ``expected_constant_columns``.

    Returns:
        A list of Finding records; empty for a zero-row frame or when every
        non-allowlisted column varies.
    """
    findings: list[Finding] = []
    n = frame.height
    if n == 0:
        return findings
    allow = set(ctx.expected_constant_columns)
    for col in frame.columns:
        if col in allow:
            continue
        s = frame.get_column(col)

        # Build a series with nulls dropped; for float columns also drop NaN so
        # that an all-NaN column is treated as dead (same as all-null).
        dropped = s.drop_nulls()
        if dropped.dtype.is_float():
            dropped = dropped.filter(dropped.is_not_nan())

        if dropped.len() == 0:
            # metric=1.0: all-null/NaN fraction (every row is null or NaN → dead)
            findings.append(
                Finding(
                    "constant_column",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"column {col!r} is entirely null/NaN across all {n} rows (dead/unpopulated?)",
                    locator={"column": col, "kind": "all_null"},
                    metric=1.0,
                    needs_judgment=True,
                )
            )
            continue

        if dropped.n_unique() == 1:
            # Reuse the already-materialized series — avoid a second drop_nulls() call.
            value = dropped.item(0)
            # metric=0.0: zero-variance proxy (variance is 0 for a constant column)
            findings.append(
                Finding(
                    "constant_column",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"column {col!r} is constant at {value!r} across all {dropped.len()} real "
                    "(non-null, non-NaN) rows (dead/stuck default?)",
                    locator={"column": col, "kind": "constant", "value": str(value)},
                    metric=0.0,
                    needs_judgment=True,
                )
            )
    return findings
