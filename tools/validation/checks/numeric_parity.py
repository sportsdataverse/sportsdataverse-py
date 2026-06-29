from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_PROB_EPS = 1e-3


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Validate numeric columns against invariants, ranges, and the domain oracle.

    Emits ERROR findings for probability groups that do not sum to 1 (within
    1e-3) and for values outside their configured range; an INFO finding when an
    oracle is configured but supplies no reference frame; and WARN findings
    (routed to the parity-divergence judgment agent) when a column's correlation
    against the oracle falls below its threshold.

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation.
        ctx: Check context supplying prob_groups, range_constraints, join_keys,
            and the optional oracle.

    Returns:
        A list of Finding records; empty if every numeric check passes.
    """
    findings: list[Finding] = []

    for group in ctx.prob_groups:
        present = [c for c in group if c in frame.columns]
        if len(present) < 2:
            continue
        n_bad = frame.select(((pl.sum_horizontal(present) - 1.0).abs() > _PROB_EPS).sum()).item()
        if n_bad > 0:
            findings.append(
                Finding(
                    "numeric_parity",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"prob group {present} sum != 1 in {n_bad} row(s)",
                    locator={"columns": present},
                    metric=float(n_bad),
                )
            )

    for col, (lo, hi) in ctx.range_constraints.items():
        if col not in frame.columns:
            continue
        n_oob = frame.select(((pl.col(col) < lo) | (pl.col(col) > hi)).sum()).item()
        if n_oob > 0:
            findings.append(
                Finding(
                    "numeric_parity",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"{col!r} out of range [{lo},{hi}] in {n_oob} row(s)",
                    locator={"column": col, "range": [lo, hi]},
                    metric=float(n_oob),
                )
            )

    if ctx.oracle is not None and ctx.join_keys:
        keys = frame.select(list(ctx.join_keys))
        ref = ctx.oracle.reference_frame(dataset, keys)
        if ref is None:
            findings.append(
                Finding(
                    "numeric_parity",
                    Severity.INFO,
                    ctx.domain,
                    dataset,
                    f"oracle {ctx.oracle.domain!r} returned no reference frame; ran invariants/ranges only",
                )
            )
        else:
            joined = frame.join(ref, on=list(ctx.join_keys), how="inner", suffix="_oracle")
            for col, oracle_col in ctx.oracle.column_map.items():
                rcol = f"{oracle_col}_oracle" if oracle_col in frame.columns else oracle_col
                if col not in joined.columns or rcol not in joined.columns:
                    continue
                corr = joined.select(pl.corr(pl.col(col), pl.col(rcol))).item()
                floor = ctx.oracle.thresholds.get(col, 0.99)
                if corr is not None and corr < floor:
                    findings.append(
                        Finding(
                            "numeric_parity",
                            Severity.WARN,
                            ctx.domain,
                            dataset,
                            f"{col!r} corr {corr:.4f} < oracle floor {floor}",
                            locator={"column": col, "oracle_column": oracle_col},
                            metric=float(corr),
                            needs_judgment=True,
                        )
                    )
    return findings
