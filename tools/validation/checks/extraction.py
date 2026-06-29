from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_TEXT_COL = "cleaned_text"


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    """Validate text-extraction coverage on a data frame.

    For each extracted column (suffix ``_player_name``), measures the fraction of
    rows that have non-empty ``cleaned_text`` yet a null extracted value. Emits an
    ERROR finding when a column is entirely null over rows with text, and a WARN
    finding (with sample rows, routed to the extraction-semantics judgment agent)
    when coverage falls below ``extraction_coverage_floor`` (default 0.95).

    Args:
        dataset: Dataset identifier recorded on each finding.
        frame: The data frame under validation.
        ctx: Check context supplying thresholds.

    Returns:
        A list of Finding records; empty when there is no text column, no
        extracted columns, or no rows with text.
    """
    findings: list[Finding] = []
    extracted = [c for c in frame.columns if c.endswith("_player_name")]
    if _TEXT_COL not in frame.columns or not extracted:
        return findings

    has_text = frame.filter(pl.col(_TEXT_COL).is_not_null() & (pl.col(_TEXT_COL).str.len_chars() > 0))
    n_text = has_text.height
    if n_text == 0:
        return findings

    floor = ctx.thresholds.get("extraction_coverage_floor", 0.95)
    for col in extracted:
        n_null = has_text.select(pl.col(col).is_null().sum()).item()
        coverage = 1.0 - (n_null / n_text)
        if coverage == 0.0:
            findings.append(
                Finding(
                    "extraction",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"extracted column {col!r} is 100% null over rows with text",
                    locator={"column": col},
                    metric=0.0,
                )
            )
        elif coverage < floor:
            sample = has_text.filter(pl.col(col).is_null()).select(_TEXT_COL).head(5).to_dicts()
            findings.append(
                Finding(
                    "extraction",
                    Severity.WARN,
                    ctx.domain,
                    dataset,
                    f"{col!r} extraction coverage {coverage:.3f} < {floor}",
                    locator={"column": col},
                    metric=coverage,
                    needs_judgment=True,
                    sample=sample,
                )
            )
    return findings
