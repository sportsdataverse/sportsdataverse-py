from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity

_TEXT_COL = "cleaned_text"


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
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
