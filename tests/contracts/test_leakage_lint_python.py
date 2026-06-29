from pathlib import Path

from tools.validation.findings import Severity
from tools.validation.lint import leakage_python

_FIXTURES = Path(__file__).parent / "fixtures" / "lint_python"


def test_ungrouped_shift_is_warn_needs_judgment():
    findings = leakage_python.run(str(_FIXTURES / "leaky.py"))
    assert len(findings) == 1
    assert findings[0].severity is Severity.WARN
    assert findings[0].needs_judgment is True
    assert findings[0].locator["call"] == "shift"


def test_grouped_shift_is_clean():
    assert leakage_python.run(str(_FIXTURES / "clean.py")) == []


def test_dir_scan_excludes_vendored_dirs():
    # scanning the whole fixtures dir finds the leak in leaky.py but NOT the
    # cumsum in vendored/site-packages/junk.py
    findings = leakage_python.run(str(_FIXTURES))
    files = {f.locator["file"] for f in findings}
    assert any("leaky.py" in f for f in files)
    assert not any("site-packages" in f for f in files)


def test_missing_path_is_error():
    findings = leakage_python.run(str(_FIXTURES / "does_not_exist"))
    assert any(f.severity is Severity.ERROR for f in findings)


def test_unparseable_file_is_warn():
    findings = leakage_python.run(str(_FIXTURES / "broken.py"))
    assert findings and findings[0].severity is Severity.WARN
    assert "could not parse" in findings[0].message
