"""The mutation runner itself can fail: a killable row passes, a test that cannot fail fails the run."""

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

_spec = importlib.util.spec_from_file_location(
    "run_mutants", Path(__file__).resolve().parents[2] / "tools" / "run_mutants.py"
)
rm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rm)

SOURCE = "def add(a, b):\n    return a + b\n"
TESTS = "from mod import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n\n\ndef test_always():\n    assert True\n"


def row(id_, test, expected="kill", snippet="return a + b"):
    return {
        "id": id_,
        "finding": "T-1",
        "file": "mod.py",
        "snippet": snippet,
        "mutant": "return a - b",
        "test": test,
        "expected": expected,
    }


@pytest.fixture
def tree(tmp_path, monkeypatch):
    (tmp_path / "mod.py").write_text(SOURCE, encoding="utf-8")
    (tmp_path / "test_mod.py").write_text(TESTS, encoding="utf-8")
    # the scratch tree is not a uv project: run pytest straight from this interpreter
    monkeypatch.setattr(rm, "PYTEST_CMD", [sys.executable, "-m", "pytest"])

    def run(rows, *extra):
        reg = tmp_path / "registry.yaml"
        reg.write_text(yaml.safe_dump(rows, sort_keys=False), encoding="utf-8")
        return rm.main(["--registry", str(reg), "--root", str(tmp_path), *extra])

    return tmp_path, run


def test_killable_row_is_killed_and_the_source_is_restored(tree, capsys):
    root, run = tree
    assert run([row("kill-me", "test_mod.py::test_add")]) == 0
    out = capsys.readouterr().out
    assert "KILLED" in out and "T-1: 1/1 mutants killed" in out and "-> OK" in out
    assert (root / "mod.py").read_text(encoding="utf-8") == SOURCE


def test_a_test_that_cannot_fail_fails_the_run(tree, capsys):
    root, run = tree
    assert run([row("cannot-fail", "test_mod.py::test_always")]) == 1
    out = capsys.readouterr().out
    assert "SURVIVED" in out and "[FAIL]" in out and "FAIL cannot-fail" in out
    assert (root / "mod.py").read_text(encoding="utf-8") == SOURCE


def test_survive_until_rows_are_reported_never_fatal(tree, capsys):
    _, run = tree
    rows = [
        row("documented-gap", "test_mod.py::test_always", expected={"survive_until": "9.9"}),
        row("fixed-early", "test_mod.py::test_add", expected={"survive_until": "9.9"}),
    ]
    assert run(rows) == 0
    out = capsys.readouterr().out
    assert "[expected (until 9.9)]" in out and "[killed early]" in out and "0 unexpected -> OK" in out


def test_only_selects_by_id_finding_or_group(tree, capsys):
    _, run = tree
    rows = [row("a", "test_mod.py::test_add"), {**row("b", "test_mod.py::test_always"), "group": "g"}]
    assert run(rows, "--only", "a") == 0
    assert "REGISTRY: 1 rows" in capsys.readouterr().out
    assert run(rows, "--only", "g") == 1
    assert "g: 0/1 mutants killed" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="no row has that id"):
        run(rows, "--only", "nope")


def test_dry_run_rejects_an_unmatched_snippet_and_a_line_keyed_row(tree, capsys):
    _, run = tree
    assert run([row("stale", "test_mod.py::test_add", snippet="return a * b")], "--dry-run") == 1
    assert "snippet occurs 0 times" in capsys.readouterr().out
    assert run([row("ok", "test_mod.py::test_add")], "--dry-run") == 0
    assert "DRY-RUN: 1/1 rows applicable" in capsys.readouterr().out
    with pytest.raises(SystemExit, match="keyed on line"):
        run([{**row("by-line", "test_mod.py::test_add"), "line": 2}], "--list")
