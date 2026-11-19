"""CFB box-score rushing follows the NCAA convention ESPN's box uses; the EPA columns do not move.

NCAA statistics charge a sack to the passer as a rush for the yards lost, and a kneel-down to
TEAM rushing. ``create_box_score``'s rusher lines (``Car`` / ``Yds`` / ``YPC``) and the team row's
``rushing_attempts`` / ``rushing_yards`` count them; the EPA / WPA / SR columns and the team
``rushes`` / ``rush_yards`` that the rush rates divide by stay on designed rushes.

Fixtures (``fixtures/box_convention/``, the trimmed form of ``fixtures/README.md``), captured
2026-11-19 from ESPN's site summary, 2026 week 12:

* 401856864 Utah @ Arizona -- Noah Fifita's 8 designed carries plus 4 sacks are ESPN's 12 for -16.
* 401856884 Florida @ Kentucky -- Aaron Philo's sack (-11) is in his 15 / 102; three kneels.
* 401856889 Alabama @ Vanderbilt -- Vanderbilt's 6 sacks of Jared Curtis.
"""

from __future__ import annotations

import functools
import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

pytestmark = pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")

FIX = Path(__file__).parent / "fixtures" / "box_convention"
GAMES = (401856864, 401856884, 401856889)


def _summary(gid: int) -> dict:
    with gzip.open(FIX / f"summary_{gid}_trimmed.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


@functools.cache
def _process(gid: int):
    summary = _summary(gid)
    proc = CFBPlayProcess(gameId=gid, join_participants=False)
    proc.espn_cfb_pbp(summary=summary)
    box = proc.run_processing_pipeline()["advBoxScore"]
    return gid, summary, proc.plays_frame, box


@pytest.fixture(params=GAMES)
def game(request):
    return _process(request.param)


def _espn_team(summary: dict) -> dict[int, dict[str, int]]:
    out = {}
    for team in summary["boxscore"]["teams"]:
        stats = {s["name"]: s["displayValue"] for s in team["statistics"]}
        out[int(team["team"]["id"])] = {
            "att": int(stats["rushingAttempts"]),
            "yds": int(stats["rushingYards"]),
        }
    return out


def test_team_rushing_attempts_match_espn(game):
    _gid, summary, _plays, box = game
    team = {int(r["pos_team"]): r for r in box["team"]}
    for tid, espn in _espn_team(summary).items():
        assert team[tid]["rushing_attempts"] == espn["att"], tid


def test_team_rushing_yards_match_espn():
    # Utah @ Arizona matches to the yard. The other two games keep play-level residuals the
    # convention does not cover: an intentional grounding filed as a Sack whose text states no
    # loss (ESPN charges -9), and a fumbled snap credited to a team-named rusher.
    _gid, summary, _plays, box = _process(401856864)
    team = {int(r["pos_team"]): r for r in box["team"]}
    for tid, espn in _espn_team(summary).items():
        assert team[tid]["rushing_yards"] == espn["yds"], tid


@pytest.mark.parametrize(
    ("gid", "name", "car", "yds"),
    [(401856864, "N.Fifita", 12, -16.0), (401856884, "A.Philo", 15, 102.0)],
)
def test_quarterback_line_counts_his_sacks(gid, name, car, yds):
    line = next(r for r in _process(gid)[3]["rush"] if r["rusher_player_name"] == name)
    assert (line["Car"], line["Yds"]) == (car, yds)
    assert line["YPC"] == pytest.approx(yds / car, abs=0.01)


def test_kneel_downs_are_team_rushes(game):
    _gid, _summary, plays, box = game
    kneels = plays.filter(pl.col("kneel_down") == True).group_by("pos_team").len()  # noqa: E712
    team_lines = {int(r["pos_team"]): r["Car"] for r in box["rush"] if r["rusher_player_name"] == "TEAM"}
    for tid, n in kneels.iter_rows():
        assert team_lines.get(int(tid), 0) >= n


def test_epa_columns_stay_on_designed_rushes(game):
    """Sacks and kneels change the counting columns only."""
    _gid, _summary, plays, box = game
    designed = plays.filter((pl.col("rush") == True) & (pl.col("scrimmage_play") == True))  # noqa: E712
    by_team = designed.group_by("pos_team").agg(n=pl.len(), epa=pl.col("EPA").sum())
    team = {int(r["pos_team"]): r for r in box["team"]}
    for tid, n, epa in by_team.iter_rows():
        row = team[int(tid)]
        assert row["rushes"] == n
        assert row["rushing_attempts"] > row["rushes"]
        assert row["EPA_rushing_overall"] == pytest.approx(epa, abs=0.06)
    by_rusher = designed.group_by("pos_team", "rusher_player_name").agg(epa=pl.col("EPA").sum())
    lines = {(int(r["pos_team"]), r["rusher_player_name"]): r for r in box["rush"]}
    for tid, name, epa in by_rusher.iter_rows():
        assert lines[(int(tid), name)]["EPA"] == pytest.approx(epa, abs=0.06)
