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


def test_analyze_finer_root_flags_ungrouped_lag_inside_function():
    findings = leakage_r._analyze_parsedata(_rows("fnwrap.getparsedata.csv"), "fnwrap.R")
    assert len(findings) == 1
    assert findings[0].locator["call"] == "lag"
    assert findings[0].locator["line"] == 8  # the ungrouped `b <- ... lag(w)`
    assert findings[0].severity is Severity.WARN and findings[0].needs_judgment


def test_analyze_inline_brace_in_grouped_pipe_is_clean():
    # lag inside an inline {} block within a grouped pipe must NOT be flagged.
    assert leakage_r._analyze_parsedata(_rows("inlinebrace.getparsedata.csv"), "inlinebrace.R") == []


@skip_if_no_rscript
def test_run_inlinebrace_live_is_empty():
    assert leakage_r.run(str(_FIXTURES / "inlinebrace.R")) == []


def test_analyze_lambda_wrapped_ungrouped_lag_is_flagged():
    findings = leakage_r._analyze_parsedata(_rows("lambdawrap.getparsedata.csv"), "lambdawrap.R")
    assert len(findings) == 1
    assert findings[0].locator["call"] == "lag"
    assert findings[0].locator["line"] == 8  # the ungrouped `b <- ... lag(w)`
    assert findings[0].severity is Severity.WARN and findings[0].needs_judgment


@skip_if_no_rscript
def test_run_lambdawrap_live_warns():
    findings = leakage_r.run(str(_FIXTURES / "lambdawrap.R"))
    assert len(findings) == 1 and findings[0].locator["call"] == "lag"


def test_parse_data_csv_decodes_rscript_output_as_utf8(monkeypatch):
    """getParseData emits UTF-8; _parse_data_csv must decode it as UTF-8, not the
    Windows cp1252 default (which UnicodeDecodeError's and silently drops the file)."""
    captured: dict = {}

    class _Proc:
        returncode = 0
        stdout = "id,parent,token,text\n"
        stderr = ""

    def _fake_run(_cmd, **kwargs):
        captured.update(kwargs)
        return _Proc()

    monkeypatch.setattr(leakage_r.subprocess, "run", _fake_run)
    out, err = leakage_r._parse_data_csv("rscript", Path("x.R"))
    assert captured.get("encoding") == "utf-8", "Rscript output must be decoded as UTF-8"
    assert out == _Proc.stdout and err == ""


@skip_if_no_rscript
def test_run_handles_utf8_source_file():
    """A UTF-8 R file (smart quotes / em-dash / accents) is linted, not silently
    dropped — regression for the cp1252 decode bug. The ungrouped lag is flagged."""
    findings = leakage_r.run(str(_FIXTURES / "utf8.R"))
    assert len(findings) == 1
    assert findings[0].locator["call"] == "lag"
    assert findings[0].severity is Severity.WARN and findings[0].needs_judgment
