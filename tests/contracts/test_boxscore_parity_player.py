"""Player-level pbp-to-boxscore parity (athlete-id join against ESPN's player box).

Synthetic frames and a tiny player-box parquet in tmp_path keep these fast and
independent of the published data.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from sportsdataverse.validation.findings import CheckContext, Severity
from tools.validation.checks import boxscore_parity_player as bpp


def _ctx(**kw):
    return CheckContext(domain="cfb", dataset="toy", schema={}, **kw)


def _plays(rows):
    base = {c: None for c in bpp._REQUIRED}
    base |= {"game_id": 1, "season": 2018}
    for flag in bpp._FLAGS:
        base[flag] = False
    return pl.DataFrame([{**base, **r} for r in rows], schema_overrides={c: pl.Int64 for c in bpp._ID_COLUMNS})


def _box(rows):
    """ESPN player-box rows: every stat a string, as ESPN ships it."""
    base = {"game_id": 1, "season": 2018, "team_id": 10, "athlete_name": "X"}
    return pl.DataFrame([{**base, **r} for r in rows])


@pytest.fixture
def wired(tmp_path, monkeypatch):
    """Point the check at a player box under a tmp data root and floors in tmp_path."""
    box_dir = tmp_path / "cfb" / "player_box" / "parquet"
    box_dir.mkdir(parents=True)
    monkeypatch.setenv("SDV_VALIDATION_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bpp, "_FLOOR_DIR", tmp_path)

    def _install(box: pl.DataFrame, floors: dict | None = None):
        box.write_parquet(box_dir / "player_box_2018.parquet")
        if floors is not None:
            (tmp_path / "toy_player.json").write_text(json.dumps(floors), encoding="utf-8")

    return _install


def test_sacks_count_as_the_passers_rushing(wired):
    """NCAA convention: a sack is a carry for the sacked passer, its yards are rushing yards."""
    frame = _plays(
        [
            {"rush": True, "rusher_player_id": 7, "yds_rushed": 12},
            {"sack": True, "passer_player_id": 7, "yds_sacked": -8},
        ]
    )
    lines = bpp.pbp_player_lines(frame).filter(pl.col("category") == "rushing").to_dicts()
    assert len(lines) == 1
    assert (lines[0]["athlete_id"], lines[0]["rushingAttempts"], lines[0]["rushingYards"]) == (7, 2, 4)


def test_measure_reports_exact_and_coverage_by_era(wired):
    wired(
        _box(
            [
                {"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"},
                {"category": "rushing", "athlete_id": 8, "rushingAttempts": "1", "rushingYards": "3"},
            ]
        )
    )
    # athlete 8's carry has no id in the pbp: an ESPN-only line lowers coverage, not exactness
    frame = _plays([{"rush": True, "rusher_player_id": 7, "yds_rushed": 12}, {"rush": True, "yds_rushed": 3}])
    m = {
        r["stat"]: r
        for r in bpp.measure(frame, bpp.espn_player_lines(bpp.espn_box_glob())).to_dicts()
        if r["category"] == "rushing"
    }
    assert m["rushingYards"]["era"] == "2014-19"
    assert m["rushingYards"]["exact_pct"] == 100.0 and m["rushingYards"]["coverage_pct"] == 50.0


def test_kicking_lines_without_field_goal_attempts_are_ignored(wired):
    """A kicker who only kicked extra points has a kicking row but no FG line to judge."""
    wired(_box([{"category": "kicking", "athlete_id": 3, "fieldGoalsMade/fieldGoalAttempts": "0/0"}]))
    m = bpp.measure(_plays([{"rush": True}]), bpp.espn_player_lines(bpp.espn_box_glob()))
    assert m.filter(pl.col("category") == "kicking").is_empty()


def test_regression_below_floor_is_reported(wired):
    wired(
        _box([{"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"}]),
        {"floors": {"rushing.rushingYards": {"2014-19": 100.0}}},
    )
    frame = _plays([{"rush": True, "rusher_player_id": 7, "yds_rushed": 11}])
    (f,) = bpp.run("toy", frame, _ctx())
    assert f.severity is Severity.WARN and f.needs_judgment
    assert f.locator == {"category": "rushing", "stat": "rushingYards", "era": "2014-19", "metric": "exact"}


def test_coverage_regression_is_reported(wired):
    """Ids vanishing from the pbp (garbled names, a lost roster join) show up as coverage."""
    wired(
        _box([{"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"}]),
        {"floors_coverage": {"rushing.rushingYards": {"2014-19": 100.0}}},
    )
    frame = _plays([{"rush": True, "yds_rushed": 12}])
    (f,) = bpp.run("toy", frame, _ctx())
    assert f.locator["metric"] == "coverage"


def test_matching_lines_produce_no_findings(wired):
    wired(
        _box([{"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"}]),
        {
            "floors": {"rushing.rushingYards": {"2014-19": 100.0}},
            "floors_coverage": {"rushing.rushingYards": {"2014-19": 100.0}},
        },
    )
    assert bpp.run("toy", _plays([{"rush": True, "rusher_player_id": 7, "yds_rushed": 12}]), _ctx()) == []


def test_missing_player_box_or_floors_skips(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_VALIDATION_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(bpp, "_FLOOR_DIR", tmp_path)
    assert bpp.run("toy", _plays([{"rush": True, "rusher_player_id": 7}]), _ctx()) == []


def test_athlete_id_dtype_mismatch_raises(wired):
    """ID-dtype discipline: a String athlete id on one side would join nothing."""
    wired(_box([{"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"}]))
    frame = _plays([{"rush": True, "rusher_player_id": 7}]).with_columns(pl.col("rusher_player_id").cast(pl.String))
    with pytest.raises(bpp.JoinKeyError):
        bpp.measure(frame, bpp.espn_player_lines(bpp.espn_box_glob()))


def test_committed_cfb_pbp_player_floors_are_real():
    payload = json.loads(bpp.floor_path("cfb_pbp").read_text(encoding="utf-8"))
    assert len(payload["floors"]) >= 15, "expected a floor per (category, stat)"
    assert set(payload["floors"]["passing.completions"]) == {"2004-06", "2007-13", "2014-19", "2020+"}


def test_newest_seasons_stay_in_the_check(wired):
    """The last era is open-ended: a 2027 game must not silently fall out of the measurement."""
    box = _box([{"category": "rushing", "athlete_id": 7, "rushingAttempts": "1", "rushingYards": "12"}])
    wired(box.with_columns(season=pl.lit(2027)))
    frame = _plays([{"season": 2027, "rush": True, "rusher_player_id": 7, "yds_rushed": 12}])
    m = bpp.measure(frame, bpp.espn_player_lines(bpp.espn_box_glob()))
    assert m.filter(pl.col("stat") == "rushingYards")["era"].to_list() == ["2020+"]
