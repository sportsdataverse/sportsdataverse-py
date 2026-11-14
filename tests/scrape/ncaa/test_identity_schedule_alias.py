"""Page team names that the season's team list spells differently (ncaa-mbb-hoops-data#24, ncaa-wbb-hoops-data#23).

stats.ncaa.org game pages show a school's CURRENT short name even on old games, while each season's team
list keeps the name of that season. Real pair, MBB contest 5725861 (2024-12-28): the page says
"Boston College" vs "FDU"; the team list and the schedule say "Fairleigh Dickinson" (590625, ESPN 161).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from sportsdataverse.scrape.ncaa import identity

TEAMS = [
    {"team": "Boston College", "ncaa_team_id": "590530", "espn_team_id": "103", "division": "I"},
    {"team": "Fairleigh Dickinson", "ncaa_team_id": "590625", "espn_team_id": "161", "division": "I"},
]


def _repo(tmp_path: Path, schedule_rows: list) -> Path:
    (tmp_path / "mbb" / "teams" / "json").mkdir(parents=True)
    (tmp_path / "mbb" / "teams" / "json" / "2025.json").write_text(json.dumps(TEAMS))
    sched = tmp_path / "mbb" / "schedules" / "json" / "2025"
    sched.mkdir(parents=True)
    (sched / "590625.json").write_text(json.dumps(schedule_rows))
    return tmp_path


def _parsed(home: str, away: str) -> dict[str, Any]:
    return {
        "contest_id": "5725861",
        "pbp": [{"home": home, "away": away, "event_team": away}],
        "shots": [{"team_id": away, "shooter_id": None}, {"team_id": home, "shooter_id": None}],
    }


@pytest.fixture(autouse=True)
def _clear_caches() -> Any:
    for fn in (identity.load_team_index, identity.load_roster_index, identity.load_contest_sides):
        fn.cache_clear()
    yield
    for fn in (identity.load_team_index, identity.load_roster_index, identity.load_contest_sides):
        fn.cache_clear()


def test_a_renamed_side_gets_its_ids_from_the_schedule(tmp_path: Path) -> None:
    root = _repo(
        tmp_path,
        [
            {
                "contest_id": "5725861",
                "team_id": "590625",
                "team": "Fairleigh Dickinson",
                "home": "Boston College",
                "away": "Fairleigh Dickinson",
            },
        ],
    )
    out = identity.enrich_parsed(_parsed("Boston College", "FDU"), root=root, league="mbb", season=2025)
    fdu_shot, bc_shot = out["shots"]
    assert (fdu_shot["ncaa_team_id"], fdu_shot["espn_team_id"]) == ("590625", "161")
    assert (bc_shot["ncaa_team_id"], bc_shot["espn_team_id"]) == ("590530", "103")
    assert out["pbp"][0]["event_team_espn_team_id"] == "161"
    away = next(t for t in out["teams"] if t["side"] == "away")
    assert (away["team"], away["ncaa_team_id"], away["espn_team_id"]) == ("Fairleigh Dickinson", "590625", "161")


def test_no_alias_without_a_schedule_row(tmp_path: Path) -> None:
    root = _repo(tmp_path, [])
    out = identity.enrich_parsed(_parsed("Boston College", "FDU"), root=root, league="mbb", season=2025)
    assert out["shots"][0]["espn_team_id"] is None


def test_a_schedule_that_contradicts_the_other_side_is_not_used(tmp_path: Path) -> None:
    # The schedule puts Fairleigh Dickinson at HOME, but the page's home side is Boston College:
    # aliasing "FDU" to the schedule's away team (Boston College) would give it the wrong ids.
    root = _repo(
        tmp_path,
        [
            {
                "contest_id": "5725861",
                "team_id": "590625",
                "team": "Fairleigh Dickinson",
                "home": "Fairleigh Dickinson",
                "away": "Boston College",
            },
        ],
    )
    out = identity.enrich_parsed(_parsed("Boston College", "FDU"), root=root, league="mbb", season=2025)
    assert out["shots"][0]["espn_team_id"] is None
    assert out["shots"][1]["espn_team_id"] == "103"


def test_the_opponents_row_never_names_a_side(tmp_path: Path) -> None:
    # Boston College's own schedule row labels the away side with the page's short name "FDU";
    # only Fairleigh Dickinson's own row gives the team-list name. Read first, BC's row must not win.
    root = _repo(tmp_path, [])
    sched = tmp_path / "mbb" / "schedules" / "json" / "2025"
    (sched / "000000.json").write_text(
        json.dumps(
            [
                {
                    "contest_id": "5725861",
                    "team_id": "590530",
                    "team": "Boston College",
                    "home": "Boston College",
                    "away": "FDU",
                }
            ]
        )
    )
    (sched / "590625.json").write_text(
        json.dumps(
            [
                {
                    "contest_id": "5725861",
                    "team_id": "590625",
                    "team": "Fairleigh Dickinson",
                    "home": "Boston College",
                    "away": "Fairleigh Dickinson",
                }
            ]
        )
    )
    assert identity.load_contest_sides(str(root), "mbb", 2025)["5725861"] == ("Boston College", "Fairleigh Dickinson")
    out = identity.enrich_parsed(_parsed("Boston College", "FDU"), root=root, league="mbb", season=2025)
    assert out["shots"][0]["espn_team_id"] == "161"


def test_never_both_sides_on_one_team(tmp_path: Path) -> None:
    # Real case, contest 1606172 (2019): the page has LIU at home and UAlbany away, but UAlbany's own
    # row says UAlbany was home, and LIU has no row. "LIU" must not be aliased to UAlbany.
    (tmp_path / "mbb" / "teams" / "json").mkdir(parents=True)
    (tmp_path / "mbb" / "teams" / "json" / "2019.json").write_text(
        json.dumps([{"team": "UAlbany", "ncaa_team_id": "450468", "espn_team_id": "399"}])
    )
    sched = tmp_path / "mbb" / "schedules" / "json" / "2019"
    sched.mkdir(parents=True)
    (sched / "450468.json").write_text(
        json.dumps(
            [{"contest_id": "1606172", "team_id": "450468", "team": "UAlbany", "home": "UAlbany", "away": "LIU"}]
        )
    )
    parsed = {"contest_id": "1606172", "pbp": [{"home": "LIU", "away": "UAlbany"}], "shots": [{"team_id": "LIU"}]}
    out = identity.enrich_parsed(parsed, root=tmp_path, league="mbb", season=2019)
    assert out["shots"][0]["espn_team_id"] is None
    assert [t.get("ncaa_team_id") for t in out["teams"]] == [None, "450468"]
