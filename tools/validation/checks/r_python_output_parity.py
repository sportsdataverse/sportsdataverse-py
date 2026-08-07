"""R <-> Python OUTPUT parity: do the two pipelines produce the same values?

The `-data` repos carry both an R chain and a Python build package for the same
datasets. ``tests/test_r_python_parity.py`` in those repos is the *contract*
gate — it proves the two sides declare the same datasets under the same stage
numbers. It deliberately does not open the data. This is the other half: run
both pipelines over the same season and compare the frames they emit.

**Neither side is authoritative.** This is the load-bearing difference from
``prep_published_parity``, which exists to prove a published frame faithfully
reflects its prep frame and is therefore asymmetric ("dropped from published").
Here a divergence means the two pipelines disagree and a human decides which is
right, so every message names both sides and neither is described as wrong.

Comparison is parquet-to-parquet in practice: both chains write
``{dataset}/{rds,parquet}/`` and would clobber each other at the same path, so
the R side is normally the artifact already on its release tag and the Python
side a fresh local build. Nothing here runs R — it takes two frames.

Findings are ordered cheapest-signal-first, because a failure at one level makes
the next level's output meaningless: join-key dtypes, then key sets, then the
column sets, then values. A dtype mismatch on a join key is reported as its own
ERROR rather than being allowed to surface as "every row dropped" — that
misdiagnosis is the whole reason the ID-dtype discipline exists.
"""

from __future__ import annotations

import polars as pl

from tools.validation.findings import Finding, Severity

#: Default columns excluded from value comparison — empty ON PURPOSE. Stamp
#: column names vary by repo and dataset (``built_at``, ``last_updated``, …), so
#: the caller names them; nothing is skipped implicitly. A silent default here
#: would hide a genuinely diverging column that happened to share a name.
_DEFAULT_IGNORE: tuple[str, ...] = ()

#: Suffix polars appends to the RIGHT frame's overlapping columns. ``r_frame`` is
#: the left side of the join, so a suffixed column holds the PYTHON value —
#: named accordingly so nobody reads the sides backwards.
_PY = "__py__"


def run(
    dataset: str,
    r_frame: pl.DataFrame,
    py_frame: pl.DataFrame,
    join_keys: tuple[str, ...],
    domain: str,
    *,
    tolerance: float = 1e-6,
    ignore_columns: tuple[str, ...] = _DEFAULT_IGNORE,
    sample_rows: int = 5,
) -> list[Finding]:
    """Compare the R-produced and Python-produced frames for one dataset.

    Args:
        dataset: Dataset identifier recorded on each finding.
        r_frame: The frame the R chain emitted.
        py_frame: The frame the Python build package emitted.
        join_keys: Columns identifying a row in both frames.
        domain: Domain identifier recorded on each finding.
        tolerance: Absolute tolerance for numeric divergence.
        ignore_columns: Columns excluded from value comparison (build stamps etc.).
        sample_rows: Max diverging rows to attach to a value finding.

    Returns:
        A list of Finding records; empty when the two pipelines agree.

    Raises:
        polars.exceptions.ColumnNotFoundError: If a name in ``ignore_columns``
            is absent from both frames is NOT raised — unknown names are simply
            inert. This function raises nothing of its own: every disagreement,
            including a structurally uncomparable pair, is reported as a Finding
            so a caller can chain without try/except.

    Example:
        Compare two frames::

            import polars as pl
            from tools.validation.checks import r_python_output_parity

            r = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.2]})
            py = pl.DataFrame({"game_id": [1, 2], "epa": [0.5, 0.9]})
            for f in r_python_output_parity.run("pbp", r, py, ("game_id",), "nfl"):
                print(f.severity.value, f.message)

        Exclude a build stamp from the value comparison::

            r_python_output_parity.run(
                "pbp", r, py, ("game_id",), "nfl", ignore_columns=("built_at",)
            )
    """
    findings: list[Finding] = []
    keys = list(join_keys)

    # ---- level 0: can we even compare? -----------------------------------
    if not keys:
        # polars rejects an empty `on=[]`, and a comparison with no notion of
        # row identity has nothing to say anyway.
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                "no join keys given — a row-level comparison needs at least one column "
                "that identifies a row in both frames",
                locator={"join_keys": []},
            )
        )
        return findings

    missing_r = [k for k in keys if k not in r_frame.columns]
    missing_py = [k for k in keys if k not in py_frame.columns]
    if missing_r or missing_py:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                f"join key(s) absent — R is missing {missing_r}, Python is missing {missing_py}",
                locator={"join_keys": keys},
            )
        )
        return findings  # nothing below can mean anything

    # A join-key dtype disagreement yields zero matches and would otherwise be
    # misread as "the two pipelines share no rows". Name it for what it is.
    key_dtype_clashes = [
        (k, str(r_frame.schema[k]), str(py_frame.schema[k])) for k in keys if r_frame.schema[k] != py_frame.schema[k]
    ]
    if key_dtype_clashes:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                "join-key dtype disagreement — the join would match nothing and any "
                "row-level result below would be meaningless: "
                + ", ".join(f"{k}: R={rd} python={pd}" for k, rd, pd in key_dtype_clashes),
                locator={"join_keys": keys},
                expected=[rd for _k, rd, _pd in key_dtype_clashes],
                actual=[pd for _k, _rd, pd in key_dtype_clashes],
            )
        )
        return findings

    # ---- level 1a: do the keys identify a row at all? ---------------------
    # Ahead of every row-set result below, because those are computed on
    # DEDUPLICATED keys: if a key repeats, the premise that a key group is a row
    # is already false, and both the shared/only counts and the level-4
    # denominator would describe something other than what they claim.
    r_keys = r_frame.select(keys).unique()
    py_keys = py_frame.select(keys).unique()
    dup_r = r_frame.height - r_keys.height
    dup_py = py_frame.height - py_keys.height
    if dup_r or dup_py:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                f"join keys do not identify a row — {dup_r} duplicate row(s) in R, "
                f"{dup_py} in Python. The join would fan out and every count below "
                "would be inflated, so the comparison was skipped. Widen the join "
                "keys until both sides are unique.",
                locator={"join_keys": keys},
                expected=0,
                actual=dup_r + dup_py,
                metric=float(dup_r + dup_py),
            )
        )
        return findings

    # ---- level 1b: same rows? --------------------------------------------
    only_r = r_keys.join(py_keys, on=keys, how="anti").height
    only_py = py_keys.join(r_keys, on=keys, how="anti").height
    shared = r_keys.join(py_keys, on=keys, how="semi").height

    if only_r or only_py:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                f"row sets differ — {only_r} key group(s) only in R, {only_py} only in Python "
                f"({shared} shared). Neither side is automatically right: decide which "
                "pipeline is correct, then fix the other.",
                locator={"join_keys": keys},
                expected=r_keys.height,
                actual=py_keys.height,
                metric=float(only_r + only_py),
                needs_judgment=True,
            )
        )

    if shared == 0:
        # Guard the guard: with no shared keys every per-column comparison below
        # is vacuously clean, which would read as "the pipelines agree".
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                "no shared key groups — the value comparison below would pass vacuously, so it was skipped entirely",
                locator={"join_keys": keys},
            )
        )
        return findings

    # ---- level 2: same columns? ------------------------------------------
    ignore = set(ignore_columns) | set(keys)
    r_cols = set(r_frame.columns) - ignore
    py_cols = set(py_frame.columns) - ignore
    if r_cols != py_cols:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.WARN,
                domain,
                dataset,
                f"column sets differ — only in R: {sorted(r_cols - py_cols)}; "
                f"only in Python: {sorted(py_cols - r_cols)}. A bundling difference "
                "can be legitimate; a renamed or dropped column is not.",
                locator={"r_only": sorted(r_cols - py_cols), "python_only": sorted(py_cols - r_cols)},
                needs_judgment=True,
            )
        )

    comparable = sorted(r_cols & py_cols)

    # ---- level 3: same dtypes on shared columns? -------------------------
    dtype_clashes = [
        (c, str(r_frame.schema[c]), str(py_frame.schema[c]))
        for c in comparable
        if r_frame.schema[c] != py_frame.schema[c]
    ]
    if dtype_clashes:
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.WARN,
                domain,
                dataset,
                f"{len(dtype_clashes)} shared column(s) differ in dtype: "
                + ", ".join(f"{c} (R={rd}, python={pd})" for c, rd, pd in dtype_clashes[:8])
                + ". Mixed-kind pairs (e.g. String vs Int64) are excluded from the "
                "value comparison below — polars cannot compare them, and this "
                "finding already names the divergence.",
                locator={"columns": [c for c, _rd, _pd in dtype_clashes]},
                needs_judgment=True,
            )
        )

    # ---- level 4: same values? -------------------------------------------
    joined = r_frame.join(py_frame, on=keys, how="inner", suffix=_PY)
    for col in comparable:
        pcol = f"{col}{_PY}"
        if pcol not in joined.columns:
            continue
        r_dtype, py_dtype = r_frame.schema[col], py_frame.schema[col]
        both_numeric = r_dtype.is_numeric() and py_dtype.is_numeric()
        if not both_numeric and r_dtype != py_dtype:
            # polars raises on a mixed-kind comparison (String vs Int64), which
            # would abort the whole run over one column. The level-3 WARN above
            # already reports the divergence, so skip rather than crash.
            continue

        r_expr, py_expr = pl.col(col), pl.col(pcol)
        null_mismatch = r_expr.is_null() != py_expr.is_null()
        if both_numeric:
            differs = ((r_expr - py_expr).abs() > tolerance) | null_mismatch
        else:
            differs = (r_expr != py_expr) | null_mismatch

        bad = joined.filter(differs)
        if bad.height == 0:
            continue
        findings.append(
            Finding(
                "r_python_output_parity",
                Severity.ERROR,
                domain,
                dataset,
                f"{col!r} disagrees between the R and Python pipelines in {bad.height} "
                f"of {joined.height} shared row(s)",
                locator={"column": col},
                metric=float(bad.height),
                needs_judgment=True,
                sample=bad.select([*keys, col, pcol]).head(sample_rows).to_dicts(),
            )
        )

    return findings
