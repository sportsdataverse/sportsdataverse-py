"""Offline tests for the cross-league player_core producer.

Fixtures are REAL core-v2 captures (see ``tests/fixtures/player_core/README.md``)
— the repo rule is that parsers are pinned against real payloads, never
hand-written ones. They deliberately span the coverage extremes, because
player_core field coverage is era-dependent by nature.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import helper_mbb_player_core
from sportsdataverse.nba import helper_nba_player_core
from sportsdataverse.nba.nba_player_core import _CORE_COLS, _ref_id
from sportsdataverse.wbb import helper_wbb_player_core
from sportsdataverse.wnba import helper_wnba_player_core

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "player_core"


def _load(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_nba_rich_record_projects_expected_values() -> None:
    df = helper_nba_player_core(_load("cap_player_core_nba_1966.json"), athlete_id=1966)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["athlete_id"] == 1966
    assert row["full_name"] == "LeBron James"
    assert row["height"] == 81.0 and row["display_height"] == "6' 9\""
    assert row["weight"] == 250.0
    assert row["jersey"] == "23"
    assert row["draft_year"] == 2003 and row["draft_round"] == 1 and row["draft_selection"] == 1
    assert row["headshot_href"].endswith("/1966.png")
    # Prep-to-pro: college is legitimately absent, NOT a parse failure.
    assert row["college_id"] is None


def test_athlete_id_is_int64_and_comes_from_the_caller() -> None:
    """athlete_id is the join key into player_box / player_season_stats.

    It is NOT in the payload body — the raw file's *name* is its only carrier —
    so the caller must supply it, and it must land as Int64 (never a
    float-origin "123.0" string).
    """
    payload = _load("cap_player_core_nba_1966.json")
    df = helper_nba_player_core(payload, athlete_id="1966")  # string in ...
    assert df.schema["athlete_id"] == pl.Int64  # ... Int64 out
    assert df.row(0, named=True)["athlete_id"] == 1966


def test_college_and_team_ids_are_parsed_from_refs_not_fetched() -> None:
    """team/college arrive as {"$ref": url} only. Hydrating them would triple
    the request count over the whole athlete universe, so the ids are parsed
    out of the URL. In college ball the team IS the college, hence the equality.
    """
    df = helper_mbb_player_core(_load("cap_player_core_mbb_4433176.json"), athlete_id=4433176)
    row = df.row(0, named=True)
    assert row["college_id"] == 153
    assert row["current_team_id"] == 153


@pytest.mark.parametrize(
    ("ref", "expected"),
    [
        ({"$ref": "http://sports.core.api.espn.com/v2/colleges/153?lang=en"}, 153),
        ({"$ref": "http://x/v2/sports/basketball/leagues/nba/seasons/2026/teams/13?lang=en"}, 13),
        ({"$ref": "http://x/v2/athletes/1966"}, None),  # neither colleges/ nor teams/
        ({}, None),
        (None, None),
        ("not-a-dict", None),
    ],
)
def test_ref_id_extraction(ref: object, expected: int | None) -> None:
    assert _ref_id(ref) == expected


def test_empty_payload_returns_empty_frame_without_raising() -> None:
    assert helper_nba_player_core({}, athlete_id=1).shape == (0, 0)
    assert helper_nba_player_core(None, athlete_id=1).shape == (0, 0)  # type: ignore[arg-type]


def test_sparse_record_still_carries_the_full_schema() -> None:
    """A 2000s-era athlete is missing most bio, but callers must still see a
    stable column set — coverage is era-dependent, the schema is not.
    """
    sparse = helper_wbb_player_core(_load("cap_player_core_wbb_9617.json"), athlete_id=9617)
    rich = helper_nba_player_core(_load("cap_player_core_nba_1966.json"), athlete_id=1966)
    assert sparse.columns == rich.columns == list(_CORE_COLS)
    assert sparse.schema == rich.schema
    assert sparse.height == 1
    # It really is sparse — otherwise this test proves nothing.
    assert sum(v is None for v in sparse.row(0)) > 0


def test_all_four_leagues_share_one_schema() -> None:
    """The core-v2 athlete resource is league-neutral; the siblings delegate to
    the NBA implementation. If a league ever forks, this fails first.
    """
    frames = [
        helper_nba_player_core(_load("cap_player_core_nba_1966.json"), athlete_id=1966),
        helper_mbb_player_core(_load("cap_player_core_mbb_4433176.json"), athlete_id=4433176),
        helper_wnba_player_core(_load("cap_player_core_wnba_1002.json"), athlete_id=1002),
        helper_wbb_player_core(_load("cap_player_core_wbb_9617.json"), athlete_id=9617),
    ]
    first = frames[0].schema
    for f in frames[1:]:
        assert f.schema == first
    # ... and they concat, which is what the season builder actually does.
    assert pl.concat(frames, how="vertical").height == 4
