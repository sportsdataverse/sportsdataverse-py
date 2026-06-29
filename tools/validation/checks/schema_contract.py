from __future__ import annotations

import polars as pl

from tools.validation.findings import CheckContext, Finding, Severity


def run(dataset: str, frame: pl.DataFrame, ctx: CheckContext) -> list[Finding]:
    findings: list[Finding] = []
    actual = {name: str(dtype) for name, dtype in frame.schema.items()}
    expected_cols, actual_cols = set(ctx.schema), set(actual)

    for col in sorted(expected_cols - actual_cols):
        findings.append(
            Finding(
                "schema_contract",
                Severity.ERROR,
                ctx.domain,
                dataset,
                f"missing column {col!r}",
                locator={"column": col},
                expected=ctx.schema[col],
                actual=None,
            )
        )
    for col in sorted(actual_cols - expected_cols):
        findings.append(
            Finding(
                "schema_contract",
                Severity.ERROR,
                ctx.domain,
                dataset,
                f"unexpected column {col!r}",
                locator={"column": col},
                expected=None,
                actual=actual[col],
            )
        )
    for col in sorted(expected_cols & actual_cols):
        if actual[col] != ctx.schema[col]:
            is_key = col in ctx.join_keys
            findings.append(
                Finding(
                    "schema_contract",
                    Severity.ERROR,
                    ctx.domain,
                    dataset,
                    f"dtype mismatch for {col!r}" + (" (JOIN KEY)" if is_key else ""),
                    locator={"column": col, "is_join_key": is_key},
                    expected=ctx.schema[col],
                    actual=actual[col],
                )
            )
    for col in ctx.required_columns:
        if col in actual_cols:
            n_null = frame.select(pl.col(col).is_null().sum()).item()
            if n_null > 0:
                findings.append(
                    Finding(
                        "schema_contract",
                        Severity.ERROR,
                        ctx.domain,
                        dataset,
                        f"{n_null} null(s) in required column {col!r}",
                        locator={"column": col},
                        metric=float(n_null),
                    )
                )
    return findings
