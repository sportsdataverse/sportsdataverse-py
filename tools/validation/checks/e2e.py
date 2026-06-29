from __future__ import annotations

import polars as pl

from tools.validation.findings import Finding, Severity


def run(
    dataset: str,
    upstream: pl.DataFrame,
    downstream: pl.DataFrame,
    join_keys: tuple[str, ...],
    domain: str,
) -> list[Finding]:
    """Validate coherence across a raw->data->published handoff.

    Compares an upstream frame to a downstream frame on the given join keys.
    Emits an ERROR finding for each join key whose dtype disagrees across the
    handoff, and an ERROR finding when downstream key groups are absent upstream
    (dropped/orphan games).

    Args:
        dataset: Dataset identifier recorded on each finding.
        upstream: The producing-side frame (e.g. raw or intermediate).
        downstream: The consuming-side frame (e.g. published).
        join_keys: Columns that must agree and join across the handoff.
        domain: Domain identifier recorded on each finding.

    Returns:
        A list of Finding records; empty if the handoff is coherent.
    """
    findings: list[Finding] = []
    keys = list(join_keys)

    for k in keys:
        if k in upstream.schema and k in downstream.schema:
            up_dtype = str(upstream.schema[k])
            dn_dtype = str(downstream.schema[k])
            if up_dtype != dn_dtype:
                findings.append(
                    Finding(
                        "e2e",
                        Severity.ERROR,
                        domain,
                        dataset,
                        f"join key {k!r} dtype disagreement across handoff",
                        locator={"join_key": k},
                        expected=up_dtype,
                        actual=dn_dtype,
                    )
                )

    dtype_disagreement_keys = {f.locator["join_key"] for f in findings if "join_key" in f.locator}
    safe_keys = [k for k in keys if k not in dtype_disagreement_keys]

    if safe_keys and all(k in upstream.columns and k in downstream.columns for k in safe_keys):
        orphans = downstream.join(upstream.select(safe_keys).unique(), on=safe_keys, how="anti")
        n_orphans = orphans.select(safe_keys).unique().height
        if n_orphans > 0:
            findings.append(
                Finding(
                    "e2e",
                    Severity.ERROR,
                    domain,
                    dataset,
                    f"{n_orphans} downstream key group(s) absent upstream",
                    locator={"join_keys": keys},
                    metric=float(n_orphans),
                )
            )

    return findings
