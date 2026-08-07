from __future__ import annotations

import polars as pl

from tools.validation import cli, registry
from tools.validation.findings import CheckContext

# `cli` imports `resolve` / `LINT_TARGETS` lazily, inside the functions that use
# them, so that `compare` does not drag in pyyaml (a dev-only dependency -- see
# test_cli_importable_without_yaml.py). That means there is no `cli.resolve` to
# patch; the owner is `registry`, and patching it there works precisely because
# the import happens at call time.


def test_run_dataset_aggregates_findings(monkeypatch):
    frame = pl.DataFrame({"game_id": ["1", "2"], "epa": [0.1, 0.2]})  # bad join-key dtype
    ctx = CheckContext(
        domain="nfl",
        dataset="nfl_pbp",
        schema={"game_id": "Int64", "epa": "Float64"},
        required_columns=("game_id",),
        join_keys=("game_id",),
    )
    monkeypatch.setattr(registry, "resolve", lambda dataset, release=None: (frame, ctx))
    out = cli.run_dataset("nfl_pbp")
    assert any(d["check"] == "schema_contract" and d["severity"] == "error" for d in out)


def test_main_returns_1_on_error(monkeypatch):
    monkeypatch.setattr(
        cli,
        "run_dataset",
        lambda dataset, release=None: [{"severity": "error", "check": "x", "dataset": "d", "message": "m"}],
    )
    assert cli.main(["run", "--dataset", "nfl_pbp", "--json"]) == 1


def test_main_returns_0_when_no_errors(monkeypatch):
    monkeypatch.setattr(
        cli,
        "run_dataset",
        lambda dataset, release=None: [{"severity": "warn", "check": "x", "dataset": "d", "message": "m"}],
    )
    assert cli.main(["run", "--dataset", "nfl_pbp", "--json"]) == 0


def test_lint_target_dispatches_python(monkeypatch):
    from tools.validation import cli
    from tools.validation.registry import LintTarget

    monkeypatch.setitem(
        registry.LINT_TARGETS,
        "t",
        LintTarget(name="t", path=str(_LEAKY_DIR()), language="python"),
    )
    out = cli.lint_target("t")
    assert any(d["check"] == "leakage_lint" and d["severity"] == "warn" for d in out)


def _LEAKY_DIR():
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / "lint_python" / "leaky.py"


def test_lint_target_unknown_raises_keyerror():
    import pytest

    from tools.validation import cli

    with pytest.raises(KeyError):
        cli.lint_target("nonexistent")


def test_lint_target_r_dispatches_to_r_linter(monkeypatch):
    from tools.validation import cli
    from tools.validation.lint import leakage_r
    from tools.validation.registry import LintTarget

    # no live R needed: force the graceful "Rscript absent" path and assert it
    # dispatched (INFO finding) rather than raising NotImplementedError.
    monkeypatch.setattr(leakage_r, "rscript_path", lambda: None)
    monkeypatch.setitem(registry.LINT_TARGETS, "rtgt", LintTarget(name="rtgt", path=".", language="r"))
    out = cli.lint_target("rtgt")
    assert any(d["severity"] == "info" for d in out)
    assert any(d["severity"] == "info" and d["message"] == "R lint skipped: Rscript not found" for d in out)
