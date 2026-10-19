"""The mutation runner itself can fail: a killable row passes, a test that cannot fail fails the run."""

import importlib.util
import os
import py_compile
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
TESTS = (
    "from mod import add\n\n\n"
    "def test_add():\n    assert add(1, 2) == 3\n\n\n"
    "def test_always():\n    assert True\n\n\n"
    "def test_red():\n    assert add(1, 1) == 3\n"
)


def row(id_, test, expected="kill", snippet="return a + b", mutant="return a - b"):
    return {
        "id": id_,
        "finding": "T-1",
        "file": "mod.py",
        "snippet": snippet,
        "mutant": mutant,
        "test": test,
        "expected": expected,
    }


@pytest.fixture
def tree(tmp_path, monkeypatch):
    (tmp_path / "mod.py").write_bytes(SOURCE.encode("utf-8"))  # bytes: no CRLF translation on Windows
    (tmp_path / "test_mod.py").write_text(TESTS, encoding="utf-8")
    # pinned explicitly so these tests also run against an older runner that defaulted to `uv run`
    monkeypatch.setattr(rm, "PYTEST_CMD", [sys.executable, "-m", "pytest"])

    def run(rows, *extra):
        reg = tmp_path / "registry.yaml"
        reg.write_text(rows if isinstance(rows, str) else yaml.safe_dump(rows, sort_keys=False), encoding="utf-8")
        return rm.main(["--registry", str(reg), "--root", str(tmp_path), *extra])

    return tmp_path, run


def test_killable_row_is_killed_and_the_source_is_restored(tree, capsys):
    root, run = tree
    assert run([row("kill-me", "test_mod.py::test_add")]) == 0
    out = capsys.readouterr().out
    assert "KILLED" in out and "T-1: 1/1 mutants killed" in out and "-> OK" in out
    assert (root / "mod.py").read_bytes() == SOURCE.encode("utf-8")


def test_a_test_that_cannot_fail_fails_the_run(tree, capsys):
    root, run = tree
    assert run([row("cannot-fail", "test_mod.py::test_always")]) == 1
    out = capsys.readouterr().out
    assert "SURVIVED" in out and "[FAIL]" in out and "FAIL cannot-fail" in out
    assert (root / "mod.py").read_bytes() == SOURCE.encode("utf-8")


def test_survive_until_rows_are_reported_never_fatal(tree, capsys):
    _, run = tree
    rows = [
        row("documented-gap", "test_mod.py::test_always", expected={"survive_until": "9.9"}),
        row("fixed-early", "test_mod.py::test_add", expected={"survive_until": "9.9"}),
    ]
    assert run(rows) == 0
    out = capsys.readouterr().out
    assert "[expected (until 9.9)]" in out and "[killed early]" in out and "0 unexpected -> OK" in out


def test_an_orphaned_survive_until_row_is_fatal(tree, capsys):
    """A refactor that moves the snippet must not leave a documented gap silently green (R1)."""
    _, run = tree
    rows = [
        row("drifted", "test_mod.py::test_always", expected={"survive_until": "9.9"}, snippet="return a * b"),
        row("test-gone", "gone_test.py::test_x", expected={"survive_until": "9.9"}),
    ]
    assert run(rows) == 1
    out = capsys.readouterr().out
    assert "snippet occurs 0 times" in out and "test not found: gone_test.py" in out
    assert "2 errors, 2 unexpected -> FAIL drifted, test-gone" in out


def test_a_kill_row_needs_a_green_baseline_and_a_compiling_mutant(tree, capsys):
    """A red-before-the-mutant test or a SyntaxError mutant is an error, never a kill (R2)."""
    _, run = tree
    assert run([row("already-red", "test_mod.py::test_red")]) == 1
    assert "test is not green without the mutant" in capsys.readouterr().out
    assert run([row("syntax", "test_mod.py::test_add", mutant="return a +")]) == 1
    assert "mutant does not compile" in capsys.readouterr().out


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


def test_registry_rejects_identity_mutants_duplicate_ids_and_duplicate_keys(tree):
    _, run = tree
    with pytest.raises(SystemExit, match="mutant equals snippet"):
        run([row("same", "test_mod.py::test_add", mutant="return a + b")], "--list")
    with pytest.raises(SystemExit, match="duplicate id"):
        run([row("twice", "test_mod.py::test_add"), row("twice", "test_mod.py::test_always")], "--list")
    twice_expected = yaml.safe_dump([row("k", "test_mod.py::test_add")], sort_keys=False)
    twice_expected += "  expected:\n    survive_until: '9.9'\n"
    with pytest.raises(yaml.constructor.ConstructorError, match="duplicate key"):
        run(twice_expected, "--list")


def test_a_same_length_mutant_written_in_the_same_second_is_still_killed(tree, capsys, monkeypatch):
    """The green baseline must not leave bytecode the mutated run then imports (re-review F1).

    A ``.pyc`` is keyed on the source's integer-second mtime and its size; `return a - b` is as long
    as `return a + b`. Pinning the source mtime before every pytest run makes "both writes in the
    same second" deterministic instead of a timing coin flip.
    """
    root, run = tree
    real = rm._pytest
    pinned = 1_700_000_000

    def same_second(row_, root_):
        os.utime(root_ / row_["file"], (pinned, pinned))
        return real(row_, root_)

    monkeypatch.setattr(rm, "_pytest", same_second)
    assert run([row("same-second", "test_mod.py::test_add")]) == 0
    assert "KILLED" in capsys.readouterr().out
    assert not (root / "__pycache__").exists()  # the runner wrote no bytecode next to the source


def test_a_stale_pyc_from_an_earlier_run_is_never_imported(tree, capsys):
    """PYTHONDONTWRITEBYTECODE alone is not enough: an existing pyc must be ignored too (re-review F1)."""
    root, run = tree
    src = root / "mod.py"
    pyc = root / "__pycache__" / f"mod.{sys.implementation.cache_tag}.pyc"
    # an unchecked-hash pyc of the ORIGINAL source: CPython imports it without looking at the source
    py_compile.compile(str(src), cfile=str(pyc), invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
    assert run([row("stale-pyc", "test_mod.py::test_add")]) == 0
    assert "KILLED" in capsys.readouterr().out


def test_a_test_that_selects_nothing_is_an_error_not_a_survivor(tree, capsys):
    """A survive_until row whose `-k` stops matching must not stay green (re-review F2)."""
    _, run = tree
    rows = [row("nothing-selected", 'test_mod.py -k "no_such_test"', expected={"survive_until": "9.9"})]
    assert run(rows) == 1
    out = capsys.readouterr().out
    assert "no tests collected (pytest rc 5)" in out and "FAIL nothing-selected" in out


def test_utf8_output_from_the_child_pytest_is_classified_not_crashed_on(tree, capsys, monkeypatch):
    """The child pytest's UTF-8 output must not crash the runner under a cp1252 Windows locale.

    Decoded with the locale codec, the reader thread raised, `proc.stdout` came back None and the runner
    died before classifying the row. "\u00c1" (UTF-8 C3 81) is the trigger: 0x81 is one of the five bytes
    cp1252 leaves undefined, while "\u2713" and "\u00e9" only decode to mojibake.
    """
    root, run = tree
    (root / "test_utf8.py").write_text(
        'from mod import add\n\n\ndef test_add_utf8():\n    assert add(1, 2) == 3, "\u00c1 \u2713"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("PYTHONIOENCODING", "utf-8")  # the child writes UTF-8 whatever the platform locale
    assert run([row("utf8-output", "test_utf8.py::test_add_utf8")]) == 0
    out = capsys.readouterr().out
    assert "KILLED" in out and "T-1: 1/1 mutants killed" in out and "-> OK" in out
    assert (root / "mod.py").read_bytes() == SOURCE.encode("utf-8")


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
def test_shards_partition_the_real_registry_exactly(n):
    """CI runs `--shard i/N` per matrix job: every row must land in exactly one shard, none dropped."""
    rows = rm.load_registry(rm.REGISTRY)
    ids = [r["id"] for r in rows]
    shards = [[r["id"] for r in rm.shard(rows, f"{i}/{n}")] for i in range(1, n + 1)]
    flat = [x for s in shards for x in s]
    assert sorted(flat) == sorted(ids) and len(flat) == len(ids)
    assert max(map(len, shards)) - min(map(len, shards)) <= 1
    assert rm.shard(rows, None) == rows


@pytest.mark.parametrize("spec", ["0/4", "5/4", "1", "a/b", "1/0"])
def test_a_bad_shard_spec_is_rejected(spec):
    with pytest.raises(SystemExit, match="--shard"):
        rm.shard([{"id": "x"}], spec)


def test_shard_runs_only_its_slice(tree, capsys):
    _, run = tree
    rows = [row(f"r{i}", "test_mod.py::test_add") for i in range(3)]
    assert run(rows, "--shard", "2/2", "--list") == 0
    out = capsys.readouterr().out
    assert "REGISTRY: 1 rows" in out and "shard 2/2" in out and " r1 " in out and " r0 " not in out


def test_a_shard_that_selects_no_rows_fails():
    """A matrix wider than the registry must not yield a green job that ran nothing."""
    with pytest.raises(SystemExit, match="selects no rows"):
        rm.shard([{"id": "a"}, {"id": "b"}], "3/3")
