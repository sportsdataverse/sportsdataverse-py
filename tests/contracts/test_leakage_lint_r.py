from __future__ import annotations

import csv
from pathlib import Path

from tests.conftest import skip_if_no_rscript
from tools.validation.findings import Severity
from tools.validation.lint import leakage_r

_FIXTURES = Path(__file__).parent / "fixtures" / "lint_r"


def _rows(name: str) -> list[dict[str, str]]:
    with open(_FIXTURES / name, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_analyze_flags_ungrouped_lag_and_cumsum():
    findings = leakage_r._analyze_parsedata(_rows("leaky.getparsedata.csv"), "leaky.R")
    assert len(findings) == 2
    assert all(f.severity is Severity.WARN and f.needs_judgment for f in findings)
    assert {f.locator["call"] for f in findings} == {"lag", "cumsum"}


def test_analyze_grouped_is_clean():
    assert leakage_r._analyze_parsedata(_rows("clean.getparsedata.csv"), "clean.R") == []


def test_iter_r_files_excludes_vendored():
    names = {p.name for p in leakage_r._iter_r_files(_FIXTURES)}
    assert {"leaky.R", "clean.R"} <= names
    assert "vendored.R" not in names  # lives under renv/


def test_missing_path_is_error():
    findings = leakage_r.run(str(_FIXTURES / "does_not_exist"))
    assert any(f.severity is Severity.ERROR for f in findings)


def test_no_rscript_returns_info(monkeypatch):
    monkeypatch.setattr(leakage_r, "rscript_path", lambda: None)
    findings = leakage_r.run(str(_FIXTURES))
    assert len(findings) == 1
    assert findings[0].severity is Severity.INFO
    assert "Rscript not found" in findings[0].message


@skip_if_no_rscript
def test_run_leaky_live_warns():
    findings = leakage_r.run(str(_FIXTURES / "leaky.R"))
    assert len(findings) == 2
    assert {f.locator["call"] for f in findings} == {"lag", "cumsum"}
    assert all(f.severity is Severity.WARN and f.needs_judgment for f in findings)


@skip_if_no_rscript
def test_run_clean_live_is_empty():
    assert leakage_r.run(str(_FIXTURES / "clean.R")) == []


@skip_if_no_rscript
def test_run_broken_live_warns_parse_error():
    findings = leakage_r.run(str(_FIXTURES / "broken.R"))
    assert findings and findings[0].severity is Severity.WARN
    assert "could not parse" in findings[0].message
