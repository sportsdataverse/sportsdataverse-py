"""Checks for quality_scoreboard.py: the ratchet must fail on a regression and pass on noise."""

import importlib.util
import json
from pathlib import Path


_spec = importlib.util.spec_from_file_location(
    "quality_scoreboard", Path(__file__).resolve().parents[2] / "tools" / "quality_scoreboard.py"
)
qs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(qs)


def snap(**values):
    better = {
        "lint_likely_bugs": "lower",
        "coverage_handwritten_percent": "higher",
        "coverage_cfb_pbp_percent": "higher",
        "modules_never_run_by_tests": "lower",
        "handwritten_lines": "info",
    }
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


def test_hotspot_coverage_has_the_same_noise_tolerance_but_counts_stay_strict():
    base = snap(coverage_cfb_pbp_percent=92.62, modules_never_run_by_tests=0)
    assert qs.compare(base, snap(coverage_cfb_pbp_percent=92.55, modules_never_run_by_tests=0)) == []
    assert qs.compare(base, snap(coverage_cfb_pbp_percent=92.4, modules_never_run_by_tests=0)) == [
        "coverage_cfb_pbp_percent: 92.62 -> 92.4"
    ]
    assert qs.compare(base, snap(coverage_cfb_pbp_percent=92.62, modules_never_run_by_tests=1)) == [
        "modules_never_run_by_tests: 0 -> 1"
    ]


def _cov_entry(covered: int, total: int) -> dict:
    return {"summary": {"covered_lines": covered, "num_statements": total}}


def test_measure_coverage_normalises_paths_and_finds_never_run_modules_and_hotspots(tmp_path):
    report = tmp_path / "coverage.json"
    report.write_text(
        json.dumps(
            {
                "files": {
                    "sportsdataverse\\cfb\\cfb_pbp.py": _cov_entry(90, 100),  # Windows path from a local run
                    "sportsdataverse/dead.py": _cov_entry(0, 20),
                    "sportsdataverse/empty.py": _cov_entry(0, 0),  # no statements: not "never run"
                    "sportsdataverse/nba/nba_espn_ext.py": _cov_entry(0, 500),  # generated: not hand-written
                }
            }
        ),
        encoding="utf-8",
    )
    files = {rel: tmp_path / rel for rel in ["sportsdataverse/cfb/cfb_pbp.py", "sportsdataverse/dead.py"]}
    cov = qs.measure_coverage(report, files)
    assert cov == {
        "percent": 75.0,
        "never_run": ["sportsdataverse/dead.py"],
        "hotspots": {"sportsdataverse/cfb/cfb_pbp.py": 90.0},
    }


def test_measure_size_counts_spans_over_the_thresholds_and_reports_unparsable_files(tmp_path):
    long_fn = "def f():\n" + "    x = 1\n" * 100  # 101 lines: over 100, not over 200
    (tmp_path / "long.py").write_text(long_fn + "\n\ndef g():\n    pass\n", encoding="utf-8")
    (tmp_path / "broken.py").write_text("def (:\n", encoding="utf-8")
    size = qs.measure_size({"long.py": tmp_path / "long.py", "broken.py": tmp_path / "broken.py"})
    assert size["functions_over_100"] == 1
    assert size["functions_over_200"] == 0
    assert size["unparsable"] == ["broken.py"]
    assert size["longest_functions"][0] == "101 lines  long.py:1 f"


def test_info_metrics_never_fail_but_a_dropped_metric_does():
    base = snap(handwritten_lines=1000, coverage_handwritten_percent=84.87)
    assert qs.compare(base, snap(handwritten_lines=5000, coverage_handwritten_percent=84.87)) == []
    assert qs.compare(base, snap(handwritten_lines=5000)) == [
        "coverage_handwritten_percent: 84.87 -> not measured (a tracked metric cannot be dropped)"
    ]


def test_cli_compares_two_saved_snapshots_without_measuring(tmp_path, monkeypatch, capsys):
    import json
    import sys

    base, loose = tmp_path / "base.json", tmp_path / "loose.json"
    base.write_text(json.dumps(snap(lint_likely_bugs=15)), encoding="utf-8")
    loose.write_text(json.dumps(snap(lint_likely_bugs=20)), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["scoreboard", "--check", "--baseline", str(base), "--current", str(loose)])
    assert qs.main() == 1
    assert "SCOREBOARD FAILED" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["scoreboard", "--check", "--baseline", str(base), "--current", str(base)])
    assert qs.main() == 0
    assert capsys.readouterr().out.rstrip().endswith("SCOREBOARD OK: no metric regressed")


def test_generated_marker_is_detected_only_in_the_header(tmp_path):
    pkg = tmp_path / "sportsdataverse"
    pkg.mkdir()
    (pkg / "made.py").write_text(f"# {qs.CODEGEN_MARKER}.\nx = 1\n", encoding="utf-8")
    (pkg / "mentions.py").write_text('"""Wraps the generated wrappers."""\n' + "\n" * 12 + f"# {qs.CODEGEN_MARKER}\n")
    (pkg / "nba_espn_ext.py").write_text("x = 1\n", encoding="utf-8")
    assert sorted(qs.handwritten_files(tmp_path)) == ["sportsdataverse/mentions.py"]


# --- sync PRs: a metric fails only when it is worse than EVERY baseline (base + upstream) ---------
def _named(commit, **values):
    s = snap(**values)
    s["commit"] = commit
    return s


def test_a_worse_metric_the_upstream_baseline_allows_passes():
    base, upstream = _named("base", lint_likely_bugs=15), _named("upstream", lint_likely_bugs=20)
    assert qs.compare(base, snap(lint_likely_bugs=18), upstream) == []
    assert qs.compare(base, snap(lint_likely_bugs=20), upstream) == []


def test_without_an_upstream_baseline_the_same_metric_fails():
    """An ordinary task PR gets no upstream baseline from the workflow, so no allowance."""
    assert qs.compare(_named("base", lint_likely_bugs=15), snap(lint_likely_bugs=18)) == ["lint_likely_bugs: 15 -> 18"]


def test_a_metric_worse_than_both_baselines_fails_against_the_closer_one():
    base, upstream = _named("base", lint_likely_bugs=15), _named("upstream", lint_likely_bugs=20)
    assert qs.compare(base, snap(lint_likely_bugs=21), upstream) == [
        "lint_likely_bugs: 20 -> 21 (vs baseline upstream)"
    ]


def test_higher_is_better_metrics_use_their_own_direction():
    base = _named("base", coverage_handwritten_percent=85.0)
    upstream = _named("upstream", coverage_handwritten_percent=84.0)
    assert qs.compare(base, snap(coverage_handwritten_percent=84.0), upstream) == []
    assert qs.compare(base, snap(coverage_handwritten_percent=84.0)) == ["coverage_handwritten_percent: 85.0 -> 84.0"]
    assert qs.compare(base, snap(coverage_handwritten_percent=83.5), upstream) == [
        "coverage_handwritten_percent: 84.0 -> 83.5 (vs baseline upstream)"
    ]
    # an upstream that is STRICTER than the base takes nothing away: the base still allows 84.95
    assert (
        qs.compare(base, snap(coverage_handwritten_percent=84.95), _named("up", coverage_handwritten_percent=90.0))
        == []
    )


def test_an_upstream_that_does_not_track_a_metric_gives_no_allowance_for_it():
    base, upstream = _named("base", modules_never_run_by_tests=0), _named("upstream", lint_likely_bugs=99)
    assert qs.compare(base, snap(modules_never_run_by_tests=1), upstream) == [
        "modules_never_run_by_tests: 0 -> 1 (vs baseline base)"
    ]


def test_cli_takes_baseline_more_than_once(tmp_path, monkeypatch, capsys):
    import sys

    base, upstream, now = tmp_path / "base.json", tmp_path / "up.json", tmp_path / "now.json"
    base.write_text(json.dumps(_named("base", lint_likely_bugs=15)), encoding="utf-8")
    upstream.write_text(json.dumps(_named("upstream", lint_likely_bugs=20)), encoding="utf-8")
    now.write_text(json.dumps(snap(lint_likely_bugs=18)), encoding="utf-8")
    argv = ["scoreboard", "--check", "--baseline", str(base), "--current", str(now)]
    monkeypatch.setattr(sys, "argv", argv)
    assert qs.main() == 1
    capsys.readouterr()
    monkeypatch.setattr(sys, "argv", [*argv, "--baseline", str(upstream)])
    assert qs.main() == 0
    out = capsys.readouterr().out
    assert "| lint_likely_bugs | 15 (+3, WORSE) | 20 (-2, better) | 18 | allowed by upstream upstream |" in out


def test_with_two_baselines_each_row_shows_the_change_against_both(capsys):
    """What an upstream allowance absorbs stays visible: the base column still reads WORSE."""
    base = _named("devel-cfb", lint_likely_bugs=104, coverage_handwritten_percent=85.0)
    upstream = _named("devel-test-harness", lint_likely_bugs=112, coverage_handwritten_percent=84.0)
    capsys.readouterr()
    assert qs.compare(base, snap(lint_likely_bugs=112, coverage_handwritten_percent=85.0), upstream) == []
    out = capsys.readouterr().out
    assert "| Metric | Base | Upstream | Now | Status |" in out
    assert (
        "| lint_likely_bugs | 104 (+8, WORSE) | 112 (+0, same) | 112 | allowed by upstream devel-test-harness |" in out
    )
    assert "| coverage_handwritten_percent | 85.0 (+0.0, same) | 84.0 (+1.0, better) | 85.0 | same |" in out


def test_a_metric_the_base_tracks_cannot_be_dropped_even_if_the_upstream_lacks_it():
    base = _named("base", lint_likely_bugs=15, modules_never_run_by_tests=0)
    upstream = _named("upstream", lint_likely_bugs=15)
    assert qs.compare(base, snap(lint_likely_bugs=15), upstream) == [
        "modules_never_run_by_tests: 0 -> not measured (a tracked metric cannot be dropped)"
    ]
    # a metric only the upstream tracks is not the base's to keep
    assert qs.compare(upstream, snap(lint_likely_bugs=15), base) == []


def test_single_baseline_header_stays_on_one_line(capsys):
    qs.compare(_named("base", lint_likely_bugs=1), snap(lint_likely_bugs=1))
    assert capsys.readouterr().out.startswith("Baseline base (2026-10-08T00:00:00+00:00)  vs  now abc1234\n")
