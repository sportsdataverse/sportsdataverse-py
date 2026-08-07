import json

import polars as pl
import pytest

from tools.validation.checks import combo_drift
from tools.validation.findings import CheckContext, Severity


def _ctx(dataset="cfb_pbp", domain="cfb"):
    return CheckContext(domain=domain, dataset=dataset, schema={})


def _frame(rows):
    return pl.DataFrame(rows)


@pytest.fixture
def snap(tmp_path, monkeypatch):
    """Point the check at a tiny two-flag snapshot instead of the real one."""
    monkeypatch.setattr(combo_drift, "_SNAPSHOT_DIR", tmp_path)
    monkeypatch.setitem(combo_drift.SIGNATURES, "toy", ("type.text", ("rush", "pass")))
    known = combo_drift.build_snapshot(
        "toy",
        _frame(
            [
                {"type.text": "Rush", "rush": True, "pass": False, "season": 2024},
                {"type.text": "Pass Reception", "rush": False, "pass": True, "season": 2024},
            ]
        ),
    )
    (tmp_path / "toy.json").write_text(json.dumps(known), encoding="utf-8")
    return known


def test_known_combinations_produce_no_findings(snap):
    frame = _frame(
        [
            {"type.text": "Rush", "rush": True, "pass": False, "season": 2025},
            {"type.text": "Pass Reception", "rush": False, "pass": True, "season": 2025},
        ]
    )
    assert combo_drift.run("toy", frame, _ctx("toy")) == []


def test_new_combination_is_warn_with_decoded_sample(snap):
    # a play flagged BOTH rush and pass has never been seen -> drift
    frame = _frame([{"type.text": "Rush", "rush": True, "pass": True, "season": 2025}])
    findings = combo_drift.run("toy", frame, _ctx("toy"))
    assert len(findings) == 1
    f = findings[0]
    assert f.severity is Severity.WARN and f.needs_judgment
    assert f.metric == 1.0
    assert f.sample[0]["decoded"] == "Rush: rush, pass"


def test_new_play_type_is_drift_even_with_known_flags(snap):
    frame = _frame([{"type.text": "Jet Sweep Reverse", "rush": True, "pass": False, "season": 2025}])
    findings = combo_drift.run("toy", frame, _ctx("toy"))
    assert len(findings) == 1
    assert "Jet Sweep Reverse" in findings[0].sample[0]["decoded"]


def test_null_flags_fold_to_zero_not_a_new_combination(snap):
    """An era predating a flag ships nulls; that must not mint a new combo."""
    frame = _frame([{"type.text": "Rush", "rush": True, "pass": None, "season": 2005}])
    assert combo_drift.run("toy", frame, _ctx("toy")) == []


def test_missing_signature_column_reports_instead_of_false_drift(snap):
    """Comparing against a different flag alphabet would call everything new."""
    frame = _frame([{"type.text": "Rush", "rush": True, "season": 2025}])  # no `pass`
    findings = combo_drift.run("toy", frame, _ctx("toy"))
    assert len(findings) == 1
    assert "comparison skipped" in findings[0].message
    assert findings[0].locator["missing_columns"] == ["pass"]


def test_unregistered_dataset_and_missing_snapshot_skip(snap, tmp_path, monkeypatch):
    frame = _frame([{"type.text": "Rush", "rush": True, "pass": False}])
    assert combo_drift.run("not_registered", frame, _ctx("not_registered")) == []
    monkeypatch.setitem(combo_drift.SIGNATURES, "no_snap", ("type.text", ("rush",)))
    assert combo_drift.run("no_snap", frame, _ctx("no_snap")) == []


def test_committed_cfb_pbp_snapshot_is_loadable_and_populated():
    """The shipped snapshot must be real: the 22-season combination inventory."""
    snapshot = combo_drift.load_snapshot("cfb_pbp")
    assert snapshot is not None
    assert snapshot["type_column"] == "type.text"
    assert len(snapshot["combos"]) > 100
    assert snapshot["n_rows"] > 1_000_000
