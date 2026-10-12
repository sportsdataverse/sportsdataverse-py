"""Per-season pbp-to-boxscore parity regression check.

Uses a tiny synthetic oracle + floor snapshot rather than the committed 488KB
one, so the tests are fast and independent of the published data.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from tools.validation.checks import boxscore_parity
from sportsdataverse.validation.findings import CheckContext, Severity


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
        "pos_fumble_lost": False,
        "def_fumble_lost": False,
        "penalty_flag": False,
        "penalty_declined": False,
        "penalty_offset": False,
        "penalty_no_play": False,
        "penalty_team_id": None,
        "penalty_yards_signed": None,
        "first_down_yards": False,
        "first_down_penalty": False,
        "text": "",
        "yds_rushed": None,
        "yds_receiving": None,
        "yds_sacked": None,
    }
    return pl.DataFrame([{**base, **r} for r in rows])


#: Team 20 has the ball once, so it gets an aggregate row of its own. Without
#: it, a stat charged to team 20 joins nothing and a test passes vacuously.
_TEAM_20_SNAP = {"pos_team_id": 20, "def_pos_team_id": 10}


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
    assert f.locator == {"stat": "completions", "season": 2024, "metric": "exact"}
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
    frame = _plays([{"penalty_flag": True, "penalty_yards_signed": 15}, _TEAM_20_SNAP])
    # team 20 (defense) is charged -> matches the oracle -> no finding
    assert boxscore_parity.run("toy", frame, _ctx()) == []
    # ...and charging the offense instead is caught
    assert boxscore_parity.run("toy", frame.with_columns(penalty_team_id=pl.lit(10)), _ctx()) != []


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


def _pen_oracle(team_id: int, penalties: int, yards: int | None = None):
    row = {"game_id": 1, "team_id": team_id, "season": 2024, "espn_penalties": penalties}
    if yards is not None:
        row["espn_penalty_yards"] = yards
    return pl.DataFrame([row])


def test_declined_and_offsetting_penalties_are_not_counted(wired):
    """ESPN's box counts accepted fouls only."""
    wired(_pen_oracle(20, 1), {"penalties": {"2024": 100.0}})
    frame = _plays(
        [
            {"penalty_flag": True, "penalty_yards_signed": 5},
            {"penalty_flag": True, "penalty_yards_signed": 5, "penalty_declined": True},
            {"penalty_flag": True, "penalty_yards_signed": 15, "penalty_offset": True},
            _TEAM_20_SNAP,
        ]
    )
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_penalty_team_id_wins_over_the_sign_rule(wired):
    """Pre-2014 text yardage is always positive, so the sign rule puts every foul
    on the defense; the parser's resolved ``penalty_team_id`` must win."""
    wired(_pen_oracle(10, 1), {"penalties": {"2024": 100.0}})
    frame = _plays([{"penalty_flag": True, "penalty_yards_signed": 5, "penalty_team_id": 10}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_intentional_grounding_counts_zero_penalty_yards(wired):
    """ESPN counts intentional grounding as a penalty with 0 yards (the loss is a play result)."""
    wired(_pen_oracle(10, 1, yards=0), {"penalty_yards": {"2024": 100.0}})
    frame = _plays(
        [
            {
                "penalty_flag": True,
                "penalty_yards_signed": -12,
                "penalty_team_id": 10,
                "text": "Smith pass incomplete, PENALTY Intentional Grounding 12 yards",
            }
        ]
    )
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def _fum_oracle(team_id: int, lost: int):
    return pl.DataFrame([{"game_id": 1, "team_id": team_id, "season": 2024, "espn_fumbles_lost": lost}])


def test_defensive_fumble_lost_is_charged_to_the_defense(wired):
    """A return fumble (``def_fumble_lost``) belongs to the defending team, not the offense."""
    wired(_fum_oracle(20, 1), {"fumbles_lost": {"2024": 100.0}})
    frame = _plays([{"def_fumble_lost": True}, _TEAM_20_SNAP])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_fumbles_on_nullified_plays_are_dropped(wired):
    wired(_fum_oracle(10, 0), {"fumbles_lost": {"2024": 100.0}})
    frame = _plays([{"pos_fumble_lost": True, "penalty_no_play": True}])
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_net_passing_oracle_is_total_minus_rushing(wired):
    """ESPN's pre-2012 netPassingYards adds sack yards back (O1); total - rushing is the judge."""
    oracle = pl.DataFrame(
        [
            {
                "game_id": 1,
                "team_id": 10,
                "season": 2010,
                "espn_net_passing_yards": 30,  # 20 passing + 10 sack yards added back
                "espn_total_yards": 15,
                "espn_rushing_yards": -5,
            }
        ]
    )
    wired(oracle, {"net_passing_yards": {"2010": 100.0}})
    frame = _plays(
        [
            {"season": 2010, "pass": True, "completion": True, "yds_receiving": 20},
            {"season": 2010, "pass": True, "sack": True, "yds_sacked": -10},
            {"season": 2010, "rush": True, "yds_rushed": 5},
        ]
    )
    assert boxscore_parity.run("toy", frame, _ctx()) == []


def test_exempt_seasons_are_not_measured(wired):
    """2004-05 box penalties are 0-0 (O6); 2013's feed has no sacks (O4)."""
    oracle = pl.DataFrame(
        [
            {"game_id": 1, "team_id": 10, "season": 2005, "espn_penalties": 0, "espn_rushing_attempts": 9},
            {"game_id": 2, "team_id": 10, "season": 2013, "espn_penalties": 0, "espn_rushing_attempts": 9},
        ]
    )
    frame = _plays(
        [
            {"season": 2005, "penalty_flag": True, "penalty_yards_signed": -5, "penalty_team_id": 10, "rush": True},
            {"game_id": 2, "season": 2013, "rush": True},
        ]
    )
    stats = {(r["stat"], r["season"]) for r in boxscore_parity.measure(frame, oracle).to_dicts()}
    assert ("penalties", 2005) not in stats and ("rushing_attempts", 2013) not in stats
    assert ("rushing_attempts", 2005) in stats and ("penalties", 2013) in stats


def test_complete_feed_and_within_tolerance_metrics(wired):
    """Game 1's feed is complete (attempts match) but its yardage is 3 off: exact fails,
    within-5 passes. Game 2 drops a carry, so it leaves the complete-feed subset."""
    oracle = pl.DataFrame(
        [
            {
                "game_id": g,
                "team_id": 10,
                "season": 2024,
                "espn_rushing_attempts": 1,
                "espn_pass_attempts": 0,
                "espn_rushing_yards": 10,
            }
            for g in (1, 2)
        ]
    )
    frame = _plays([{"game_id": 1, "rush": True, "yds_rushed": 7}, {"game_id": 2}])
    m = {r["stat"]: r for r in boxscore_parity.measure(frame, oracle).to_dicts() if r["season"] == 2024}
    assert m["rushing_yards"]["exact_pct"] == 0.0
    assert m["rushing_yards"]["within_5_pct"] == 50.0
    assert m["rushing_yards"]["n_complete"] == 1 and m["rushing_yards"]["complete_pct"] == 0.0
    assert m["rushing_attempts"]["within_5_pct"] is None  # counts have no tolerance band


def test_complete_feed_floor_regression_is_reported(wired):
    oracle = pl.DataFrame(
        [
            {
                "game_id": 1,
                "team_id": 10,
                "season": 2024,
                "espn_rushing_attempts": 1,
                "espn_pass_attempts": 0,
                "espn_rushing_yards": 10,
            }
        ]
    )
    wired(oracle, {"rushing_yards": {"2024": 0.0}})
    payload = {"floors": {"rushing_yards": {"2024": 0.0}}, "floors_complete": {"rushing_yards": {"2024": 100.0}}}
    boxscore_parity.floor_path("toy").write_text(json.dumps(payload), encoding="utf-8")
    frame = _plays([{"rush": True, "yds_rushed": 7}])
    (f,) = boxscore_parity.run("toy", frame, _ctx())
    assert f.locator == {"stat": "rushing_yards", "season": 2024, "metric": "complete_feed"}


def test_extract_oracle_parses_team_box(tmp_path):
    """The committed oracle is regenerated from ESPN's team box (the espn_cfb_team_box release)."""
    tb = pl.DataFrame(
        {
            "firstDowns": ["32", None],
            "thirdDownEff": ["5-7", None],
            "fourthDownEff": ["0-1", None],
            "totalYards": ["1,074", None],
            "netPassingYards": ["241", None],
            "completionAttempts": ["13/17", None],
            "rushingYards": ["833", None],
            "rushingAttempts": ["44", None],
            "totalPenaltiesYards": ["2-20", None],
            "turnovers": ["1", None],
            "fumblesLost": ["0", None],
            "interceptions": ["1", None],
            "team_id": [324, 2097],
            "home_away": ["away", "home"],
            "game_id": [401021659, 401021659],
            "season": [2018, 2018],
        }
    )
    tb.write_parquet(tmp_path / "team_box_2018.parquet")
    o = boxscore_parity.extract_oracle(str(tmp_path / "team_box_*.parquet"))
    assert o.height == 1  # the all-null row carries nothing to judge
    assert o.schema["game_id"] == o.schema["team_id"] == pl.Int64
    r = o.row(0, named=True)
    assert (r["espn_total_yards"], r["espn_completions"], r["espn_pass_attempts"]) == (1074, 13, 17)
    assert (r["espn_penalties"], r["espn_penalty_yards"]) == (2, 20)
    assert (r["espn_third_down_conv"], r["espn_third_down_att"], r["espn_fourth_down_att"]) == (5, 7, 1)


def test_first_downs_and_total_yards_are_measured(wired):
    oracle = pl.DataFrame(
        [
            {
                "game_id": 1,
                "team_id": 10,
                "season": 2024,
                "espn_first_downs": 2,
                "espn_total_yards": 25,
                "espn_rushing_yards": 5,
            }
        ]
    )
    frame = _plays(
        [
            {"rush": True, "yds_rushed": 5, "first_down_yards": True},
            {"pass": True, "completion": True, "yds_receiving": 20},
            {"penalty_flag": True, "penalty_yards_signed": 15, "first_down_penalty": True},
        ]
    )
    m = {r["stat"]: r["exact_pct"] for r in boxscore_parity.measure(frame, oracle).to_dicts()}
    assert m["first_downs"] == 100.0 and m["total_yards"] == 100.0
