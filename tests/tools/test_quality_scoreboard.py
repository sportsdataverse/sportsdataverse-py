"""Checks for quality_scoreboard.py: the ratchet must fail on a regression and pass on noise."""

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "quality_scoreboard", Path(__file__).resolve().parents[2] / "tools" / "quality" / "scoreboard.py"
)
qs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(qs)


def snap(**values):
    better = {"lint_likely_bugs": "lower", "coverage_handwritten_percent": "higher", "handwritten_lines": "info"}
    return {
        "commit": "abc1234",
        "measured_at": "2026-10-08T00:00:00+00:00",
        "metrics": {k: {"value": v, "better": better[k], "label": k} for k, v in values.items()},
    }


def test_lint_family_keeps_lookalike_codes_apart():
    assert qs.lint_family("C901") == "C901"
    assert qs.lint_family("C417") == "C4"
    assert qs.lint_family("RUF005") == "RUF"
    assert qs.lint_family("PLR0912") == "PLR0912"
    assert qs.lint_family("PLR2004") is None  # magic numbers are not tracked
    assert qs.lint_family("E501") is None


def test_noqa_for_a_rule_not_run_is_not_called_stale():
    assert qs.noqa_bucket("Unused `noqa` directive (non-enabled: `E712`)") == "NOQA_NOT_RUN"
    assert qs.noqa_bucket("Unused `noqa` directive (unused: `F401`)") == "NOQA_STALE"
    assert qs.noqa_bucket("Unused `noqa` directive (unused: `F401`; non-enabled: `E712`)") == "NOQA_STALE"
    assert qs.noqa_bucket("Unused `noqa` directive (unknown: `X999`)") == "NOQA_STALE"


def test_unchanged_snapshot_passes():
    base = snap(lint_likely_bugs=15, coverage_handwritten_percent=84.87, handwritten_lines=239581)
    assert qs.compare(base, base) == []


def test_more_lint_findings_is_a_regression():
    regressions = qs.compare(snap(lint_likely_bugs=15), snap(lint_likely_bugs=16))
    assert regressions == ["lint_likely_bugs: 15 -> 16"]


def test_coverage_drop_within_noise_passes_but_beyond_noise_fails():
    base = snap(coverage_handwritten_percent=84.87)
    assert qs.compare(base, snap(coverage_handwritten_percent=84.80)) == []
    assert qs.compare(base, snap(coverage_handwritten_percent=84.50)) == ["coverage_handwritten_percent: 84.87 -> 84.5"]


def test_info_metrics_and_unmeasured_metrics_never_fail():
    base = snap(handwritten_lines=1000, coverage_handwritten_percent=84.87)
    assert qs.compare(base, snap(handwritten_lines=5000)) == []


def test_cli_compares_two_saved_snapshots_without_measuring(tmp_path, monkeypatch, capsys):
    import json
    import sys

    base, loose = tmp_path / "base.json", tmp_path / "loose.json"
    base.write_text(json.dumps(snap(lint_likely_bugs=15)), encoding="utf-8")
    loose.write_text(json.dumps(snap(lint_likely_bugs=20)), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["scoreboard", "--compare", str(base), "--current", str(loose)])
    assert qs.main() == 1
    assert "SCOREBOARD FAILED" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["scoreboard", "--compare", str(base), "--current", str(base)])
    assert qs.main() == 0
    assert capsys.readouterr().out.rstrip().endswith("SCOREBOARD OK: no metric regressed")


def test_generated_marker_is_detected_only_in_the_header(tmp_path):
    pkg = tmp_path / "sportsdataverse"
    pkg.mkdir()
    (pkg / "made.py").write_text(f"# {qs.CODEGEN_MARKER}.\nx = 1\n", encoding="utf-8")
    (pkg / "mentions.py").write_text('"""Wraps the generated wrappers."""\n' + "\n" * 12 + f"# {qs.CODEGEN_MARKER}\n")
    (pkg / "nba_espn_ext.py").write_text("x = 1\n", encoding="utf-8")
    assert sorted(qs.handwritten_files(tmp_path)) == ["sportsdataverse/mentions.py"]
