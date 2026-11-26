"""The college shot-make fitter's committed reports: gated against the cell table, failed, nothing shipped.

``tools/models/mbb_shot_value/train_shot_make.py`` writes ``shot_make_{mbb,wbb}_holdout.json``. Their gate lines
must re-derive from their own season metrics with the NBA fitter's thresholds, every season must be scored by a
fit on earlier seasons only, and no college booster may be bundled while any gate fails.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "college_tsm", _ROOT / "tools" / "models" / "mbb_shot_value" / "train_shot_make.py"
)
tsm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tsm)


@pytest.mark.parametrize("league", sorted(tsm.LEAGUES))
def test_report_gates_re_derive_and_nothing_ships(league):
    cfg = tsm.LEAGUES[league]
    report = json.loads((tsm.HERE / f"shot_make_{cfg['lg']}_holdout.json").read_text(encoding="utf-8"))
    gated = [(s, m) for s, m in report["seasons"].items() if m["split"] != "monitor"]
    assert sorted(int(s) for s, _ in gated) == cfg["walk"]
    assert [f for s, m in gated for f in tsm.nba.gate(f"{m['split']} {s}", m)] == report["failed_gates"]
    for s, m in report["seasons"].items():
        assert max(m["train_seasons"]) < int(s)
        assert m["vs_zone_served_by_band"]["3PA"]["d_log_loss"] == 0  # threes keep the table value
    assert report["failed_gates"] and not report["owner_accepted"]
    assert not list((_ROOT / "sportsdataverse" / "mbb" / "models").glob("*_shot_make.ubj"))


def test_the_recorded_failures():
    """Observed 2026-11-26 (the fitter's docstring quotes them)."""
    mens = json.loads((tsm.HERE / "shot_make_mbb_holdout.json").read_text(encoding="utf-8"))["failed_gates"]
    womens = json.loads((tsm.HERE / "shot_make_wbb_holdout.json").read_text(encoding="utf-8"))["failed_gates"]
    assert mens == [
        "walk_forward 2025: gate 3 calibration slope 0.802",
        "holdout 2026: gate 2 calibration in the large -0.0136",
        "holdout 2026: gate 3 calibration slope 1.208",
    ]
    assert womens == ["walk_forward 2024: gate 3 calibration slope 0.876"]
