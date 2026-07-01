from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Flag dead columns: entirely-null or zero-variance (single-valued) columns.

    A column that never varies is usually a producer that silently fails to
    populate it (e.g. an all-zero ``sacked`` count, an all-null ``fox_jersey``).
    Neither ``extraction`` (null-coverage) nor ``sweep`` (release-over-release
    mean-shift) catches a *long-standing* constant, and ``numeric_parity`` needs
    an oracle reference, so this single-frame check covers the gap.

    For each column NOT in ``ctx.expected_constant_columns``:

    - **all-null** -> WARN ``needs_judgment`` (``kind="all_null"``).
    - **zero-variance** (one distinct non-null value, incl. all-zero numeric) ->
      WARN ``needs_judgment`` (``kind="constant"``).

    An all-null column is reported once (as ``all_null``), never also as
    ``constant``. Judgment is routed to the ``anomaly-triage-reviewer``.

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
        nulls = s.null_count()
        if nulls == n:
            findings.append(
                Finding(
                    "constant_column",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"column {col!r} is entirely null across all {n} rows (dead/unpopulated?)",
                    locator={"column": col, "kind": "all_null"},
                    metric=1.0,
                    needs_judgment=True,
                )
            )
            continue
        if s.drop_nulls().n_unique() == 1:
            value = s.drop_nulls().item(0)
            findings.append(
                Finding(
                    "constant_column",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"column {col!r} is constant at {value!r} across all {n - nulls} non-null rows "
                    "(dead/stuck default?)",
                    locator={"column": col, "kind": "constant", "value": str(value)},
                    metric=0.0,
                    needs_judgment=True,
                )
            )
    return findings
