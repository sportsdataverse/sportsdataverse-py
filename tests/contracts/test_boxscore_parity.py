"""Per-season pbp-to-boxscore parity regression check.

Uses a tiny synthetic oracle + floor snapshot rather than the committed 488KB
one, so the tests are fast and independent of the published data.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from tools.validation.checks import boxscore_parity
from tools.validation.findings import CheckContext, Severity


def _ctx(dataset="toy", **kw):
    return CheckContext(domain="cfb", dataset=dataset, schema={}, **kw)


def _plays(rows):
    """Minimal pbp frame carrying every column the check requires."""
    base = {
        "game_id": 1,
        "season": 2024,
        "pos_team_id": 10,
        "def_pos_team_id": 20,
        "rush": False,
        "pass": False,
        "completion": False,
        "sack": False,
        "int": False,
        "fumble_lost": False,
        "penalty_flag": False,
        "penalty_yards_signed": None,
        "yds_rushed": None,
        "yds_receiving": None,
        "yds_sacked": None,
    }
    return pl.DataFrame([{**base, **r} for r in rows])


@pytest.fixture
def wired(tmp_path, monkeypatch):
    """Point the check at a synthetic oracle + floors in tmp_path."""
    monkeypatch.setattr(boxscore_parity, "_ORACLE_DIR", tmp_path)
    monkeypatch.setattr(boxscore_parity, "_FLOOR_DIR", tmp_path)

    def _install(oracle: pl.DataFrame, floors: dict):
        oracle.write_parquet(tmp_path / "toy_espn_team_box.parquet")
        (tmp_path / "toy.json").write_text(json.dumps({"floors": floors}), encoding="utf-8")

    return _install


def _oracle(completions: int):
    return pl.DataFrame([{"game_id": 1, "team_id": 10, "season": 2024, "espn_completions": completions}])


def test_matching_parity_produces_no_findings(wired):
    wired(_oracle(2), {"completions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}, {"pass": True, "completion": True}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_regression_below_floor_is_reported(wired):
    """Mutation: our count drops to 1 vs ESPN's 2 -> 0% exact vs a 100% floor."""
    wired(_oracle(2), {"completions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}, {"pass": True, "completion": False}])
    findings = boxscore_parity.run("toy", frame, _ctx())
    assert len(findings) == 1
    f = findings[0]
    assert f.severity is Severity.WARN and f.needs_judgment
    assert f.locator == {"stat": "completions", "season": 2024}
    assert f.expected == 100.0 and f.actual == 0.0


def test_drop_within_tolerance_does_not_fire(wired):
    """A floor recorded slightly above the current rate must not fire inside
    the tolerance band -- otherwise re-running on the same data self-trips."""
    wired(_oracle(2), {"completions": {"2024": 101.5}})  # 1.5pp above achievable
    frame = _plays([{"pass": True, "completion": True}, {"pass": True, "completion": True}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_tolerance_is_configurable_via_thresholds(wired):
    wired(_oracle(2), {"completions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}, {"pass": True, "completion": False}])
    # a 200pp tolerance can never fire
    ctx = _ctx(thresholds={"boxscore_parity_tolerance_pp": 200.0})
    assert boxscore_parity.run("toy", frame, _ctx()) != []
    assert boxscore_parity.run("toy", frame, ctx) == []


def test_missing_oracle_or_floors_skips(tmp_path, monkeypatch):
    monkeypatch.setattr(boxscore_parity, "_ORACLE_DIR", tmp_path)
    monkeypatch.setattr(boxscore_parity, "_FLOOR_DIR", tmp_path)
    frame = _plays([{"pass": True, "completion": True}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_missing_required_columns_skips(wired):
    wired(_oracle(1), {"completions": {"2024": 100.0}})
    assert boxscore_parity.run("toy", pl.DataFrame({"game_id": [1]}), _ctx()) == []


def test_stat_absent_from_floors_is_not_invented(wired):
    """A stat with no recorded floor must be ignored, not treated as floor 0."""
    wired(_oracle(2), {"interceptions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}, {"pass": True, "completion": False}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_penalties_are_charged_to_the_committing_team(wired):
    """Positive signed yardage means the offense gained -> the DEFENSE was
    flagged, so the penalty belongs to def_pos_team_id."""
    oracle = pl.DataFrame([{"game_id": 1, "team_id": 20, "season": 2024, "espn_penalties": 1}])
    wired(oracle, {"penalties": {"2024": 100.0}})
    frame = _plays([{"penalty_flag": True, "penalty_yards_signed": 15}])
    # team 20 (defense) is charged -> matches the oracle -> no finding
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_committed_cfb_pbp_floors_are_real():
    """The shipped snapshot must carry the measured 22-season floors."""
    path = boxscore_parity.floor_path("cfb_pbp")
    assert path.exists(), "cfb_pbp parity floors missing"
    payload = json.loads(path.read_text(encoding="utf-8"))
    floors = payload["floors"]
    assert len(floors) >= 8, "expected a floor per tracked stat"
    assert len(floors["completions"]) >= 20, "expected ~22 seasons of completion floors"
    assert boxscore_parity.oracle_path("cfb_pbp").exists(), "ESPN team-box oracle missing"


def test_uncastable_team_key_raises_instead_of_measuring_a_subset(wired):
    """ID-dtype discipline: a key that cannot become Int64 must fail loudly.

    `strict=False` would null it, the inner join would drop the row, and the
    measured parity rate would shift with no finding emitted -- the check would
    quietly grade itself on a subset of the data.
    """
    wired(_oracle(1), {"completions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}]).with_columns(pl.lit("not-an-id").alias("pos_team_id"))
    with pytest.raises(boxscore_parity.JoinKeyError, match="did not survive"):
        boxscore_parity.measure(frame, pl.read_parquet(boxscore_parity.oracle_path("toy")))


def test_non_int64_oracle_team_id_raises(wired):
    """The oracle side of the join key must be canonical Int64 too."""
    oracle = pl.DataFrame([{"game_id": 1, "team_id": "10", "season": 2024, "espn_completions": 1}])
    wired(oracle, {"completions": {"2024": 100.0}})
    frame = _plays([{"pass": True, "completion": True}])
    with pytest.raises(boxscore_parity.JoinKeyError, match="must be Int64"):
        boxscore_parity.measure(frame, pl.read_parquet(boxscore_parity.oracle_path("toy")))
