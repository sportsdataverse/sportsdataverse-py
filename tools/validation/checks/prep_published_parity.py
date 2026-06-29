from __future__ import annotations

import polars as pl

from tools.validation.findings import Finding, Severity


def run(
    dataset: str,
    prep: pl.DataFrame,
    published: pl.DataFrame,
    join_keys: tuple[str, ...],
    domain: str,
    tolerance: float = 1e-6,
) -> list[Finding]:
    """Validate that the published frame faithfully reflects the prep frame.

    Joins ``prep`` to ``published`` on ``join_keys``. Emits an ERROR for prep key
    groups absent from published (dropped rows), and for each shared numeric column
    whose value diverges beyond ``tolerance`` on a joined row.

    Args:
        dataset: Dataset identifier recorded on each finding.
        prep: The intermediate (producing-side) frame.
        published: The published frame.
        join_keys: Columns to join/compare on.
        domain: Domain identifier recorded on each finding.
        tolerance: Absolute tolerance for numeric divergence.

    Returns:
        A list of Finding records; empty when published faithfully reflects prep.
    """
    findings: list[Finding] = []
    keys = list(join_keys)

    dropped = prep.join(published.select(keys).unique(), on=keys, how="anti")
    n_dropped = dropped.select(keys).unique().height
    if n_dropped > 0:
        findings.append(
            Finding(
                "prep_published_parity",
                Severity.ERROR,
                domain,
                dataset,
                f"{n_dropped} prep key group(s) dropped from published",
                locator={"join_keys": keys},
                metric=float(n_dropped),
            )
        )

    joined = prep.join(published, on=keys, how="inner", suffix="_published")
    for col, dtype in prep.schema.items():
        if col in keys or col not in published.columns or not dtype.is_numeric():
            continue
        pcol = f"{col}_published"
        if pcol not in joined.columns:
            continue
        n_div = joined.filter((pl.col(col) - pl.col(pcol)).abs() > tolerance).height
        if n_div > 0:
            findings.append(
                Finding(
                    "prep_published_parity",
                    Severity.ERROR,
                    domain,
                    dataset,
                    f"{col!r} diverges between prep and published in {n_div} row(s)",
                    locator={"column": col},
                    metric=float(n_div),
                )
            )
    return findings
