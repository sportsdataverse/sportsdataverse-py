"""NFL advanced box (``create_box_score``) fixes from the 2026-10-08 percentile audit, on real ESPN data.

Fixtures (processed offline, no network):

* ``summary_401872922.json`` -- JAX (home, 30) vs IND (5): home-offense red-zone snaps, three
  JAX kneels, sacks, late downs and drives from both offenses.
* ``summary_401128075_trimmed.json.gz`` -- JAX @ IND, 2019 week 11: a returner's muffed punt
  recovered by the punting team (a special-teams fumble whose pos_team is the kicking team).
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nfl.nfl_pbp as mod

FIX = Path(__file__).parent / "fixtures"


def _process(game_id: int, name: str) -> tuple[pl.DataFrame, dict]:
    path = FIX / name
    if name.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            summary = json.load(fh)
    else:
        summary = json.loads(path.read_text())
    proc = mod.NFLPlayProcess(gameId=game_id, join_participants=False)
    proc.espn_nfl_pbp(summary=summary)
    game = proc.run_processing_pipeline()
    return proc.plays_frame, game["advBoxScore"]


@pytest.fixture(scope="module")
def jax_ind() -> tuple[pl.DataFrame, dict]:
    return _process(401872922, "summary_401872922.json")


def _t(name: str) -> pl.Expr:
    return (pl.col(name) == True).fill_null(False)  # noqa: E712


def _rows(box: dict, section: str, key: str = "pos_team") -> dict[int, dict]:
    return {int(r[key]): r for r in box[section]}


def test_red_zone_and_goal_to_go_read_yards_to_the_end_zone(jax_ind):
    # B2: start.yardLine is home-relative; the home offense's red zone was read from the wrong end
    plays, box = jax_ind
    snaps = plays.filter(_t("scrimmage_play"))
    home_rz = snaps.filter((pl.col("pos_team") == pl.col("homeTeamId")) & (pl.col("start.yardsToEndzone") <= 20))
    assert home_rz.height > 0
    assert (snaps["rz_play"] == (snaps["start.yardsToEndzone"] <= 20)).all()
    assert (snaps["goal_to_go"] == (snaps["start.distance"] == snaps["start.yardsToEndzone"])).all()
    situational = _rows(box, "situational")
    rz = snaps.filter(pl.col("start.yardsToEndzone") <= 20)
    for team, grp in rz.group_by("pos_team"):
        row = situational[int(team[0])]
        assert row["EPA_success_rz"] == grp["EPA_success"].sum()
        assert row["EPA_success_rate_rz"] == pytest.approx(grp["EPA_success"].mean(), abs=0.005)


def test_kneels_are_not_scrimmage_plays_and_leave_the_rush_rates(jax_ind):
    # B4: kneels are 0-or-worse rushes (stuffs, TFL havoc); the CFB box drops them via scrimmage_play
    plays, box = jax_ind
    kneels = plays.filter(_t("kneel_down"))
    assert kneels.height == 3
    assert not kneels["scrimmage_play"].any()
    rushes = plays.filter(_t("rush") & _t("scrimmage_play"))
    team = _rows(box, "team")[30]
    jax = rushes.filter(pl.col("pos_team") == 30)
    assert team["rushes"] == jax.height
    assert team["rushing_stuff"] == jax["stuffed_run"].sum()


def test_late_down_pass_and_rush_success_rates_use_their_own_denominators(jax_ind):
    # 3a: the pass and rush rates were both over every late down, so they summed to the overall rate
    _, box = jax_ind
    for row in box["situational"]:
        for kind in ("pass", "rush"):
            den = row[f"late_down_{kind}"]
            rate = row[f"EPA_success_late_down_{kind}_rate"]
            if den:
                assert rate == pytest.approx(row[f"EPA_success_late_down_{kind}"] / den)
            else:
                assert rate is None


def test_drive_metrics_take_one_row_per_drive(jax_ind):
    # 3b: drive-level columns repeat on every play; a play-weighted mean over-weights long drives
    plays, box = jax_ind
    drives = plays.filter(_t("scrimmage_play")).unique(["drive.id"], keep="first", maintain_order=True)
    offense = _rows(box, "drives")
    defense = _rows(box, "defensive", "def_pos_team")
    for team, grp in drives.group_by("pos_team"):
        row = offense[int(team[0])]
        assert row["drives"] == grp.height
        assert row["drive_total_gained_yards"] == grp["drive.yards"].sum()
        assert row["yards_per_drive"] == pytest.approx(grp["drive.yards"].mean(), abs=0.005)
        assert row["plays_per_drive"] == pytest.approx(grp["drive.offensivePlays"].mean(), abs=0.005)
    for team, grp in drives.group_by("def_pos_team"):
        stopped = 100 * grp["drive_stopped"].cast(pl.Float64).mean()
        assert defense[int(team[0])]["drive_stopped_rate"] == pytest.approx(stopped, abs=0.005)


def test_interception_drives_count_as_stopped(jax_ind):
    # ESPN writes an interception drive as "INT" / "INT TD"; the stop regex looked for "interception"
    plays, _ = jax_ind
    picked = plays.filter(pl.col("drive.result").is_in(["INT", "INT TD"]))
    assert picked.height > 0
    assert picked["drive_stopped"].all()


def test_fumbles_count_every_play_on_the_team_that_fumbled():
    # 3d: total_fumbles was scrimmage-only and keyed by pos_team (the KICKING team on a punt),
    # while fumbles_lost counts every play; a muffed punt was lost but never fumbled
    plays, box = _process(401128075, "summary_401128075_trimmed.json.gz")
    fumbles = plays.filter(_t("fumble_or_muff"))
    assert fumbles.filter(pl.col("fumbling_team") != pl.col("pos_team")).height > 0
    turnover = _rows(box, "turnover")
    for team, grp in fumbles.group_by("fumbling_team"):
        assert turnover[int(team[0])]["total_fumbles"] == grp.height
    for row in turnover.values():
        assert row["total_fumbles"] >= row["fumbles_lost_pbp"]
        assert row["fumbles_recovered"] == row["total_fumbles"] - row["fumbles_lost_pbp"]


def test_team_pass_box_carries_sack_yards_beside_sack_inclusive_passes(jax_ind):
    # G1: passes / yards_per_pass already count sacks (as 0 yards); sack_yards carries their yards
    plays, box = jax_ind
    dropbacks = plays.filter(_t("pass") & _t("scrimmage_play"))
    team = _rows(box, "team")
    for tid in (30, 5):
        mine = dropbacks.filter(pl.col("pos_team") == tid)
        sacks = mine.filter(_t("sack_vec"))
        assert sacks.height > 0
        assert team[tid]["passes"] == mine.height == mine["pass_attempt"].sum() + sacks.height
        assert team[tid]["sack_yards"] == sacks["yds_sacked"].sum()
        assert team[tid]["sack_yards"] < 0
    assert (team[30]["sack_yards"], team[5]["sack_yards"]) == (-11, -21)
