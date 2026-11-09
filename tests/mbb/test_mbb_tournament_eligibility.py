"""The bundled NCAA-tournament ineligible list and how the season products use it.

Real cases (see sportsdataverse/mbb/data/ncaa_tournament_ineligible.md for every source):

* 2023 men's NEC: Merrimack, in its last reclassifying season, beat Fairleigh Dickinson 67-66 in
  the final; FDU took the auto-bid (``tests/fixtures/bb_college_season/mbb_schedule_2023_nec.parquet``).
* 2024 women's OVC: Southern Indiana won the final; UT Martin took the bid (the committed 2024 schedule).
* 2025: the NCAA cut the reclassification period to three years and the six 2022-23 movers were
  eligible from 2025-26 (Queens won the 2026 ASUN men's title and took the bid).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb import mbb_tournament_ineligible
from sportsdataverse.mbb.mbb_college_season import _conference_status, college_teams
from sportsdataverse.mbb.mbb_tournament_eligibility import INELIGIBLE_SCHEMA, LEAGUES, REASONS, _ineligible_team_ids

FIX = "tests/fixtures/bb_college_season"
MERRIMACK, FDU, SOUTHERN_INDIANA, UT_MARTIN, WEST_FLORIDA, QUEENS = "2771", "161", "88", "2630", "2697", "2511"


def test_the_bundled_list_is_well_formed():
    df = mbb_tournament_ineligible()
    assert dict(df.schema) == INELIGIBLE_SCHEMA
    assert df.select("season", "league", "team_id").is_unique().all()
    assert set(df["reason"]) <= REASONS and set(df["league"]) <= set(LEAGUES)
    assert set(df["confidence"]) <= {"high", "medium"}
    assert df["source_url"].str.starts_with("http").all()
    assert df["team_id"].str.contains(r"^\d+$").all()
    # every season 2010-2027 has rows; 2018 is the one season with no program reclassifying
    assert set(range(2010, 2028)) <= set(df["season"])
    rec = set(df.filter(pl.col("reason") == "reclassifying")["season"])
    assert set(range(2010, 2028)) - rec == {2018}


@pytest.mark.parametrize(
    ("season", "league", "team_id", "barred"),
    [
        (2027, "mens", WEST_FLORIDA, True),  # joined Division I for 2026-27
        (2027, "womens", WEST_FLORIDA, True),
        (2026, "mens", WEST_FLORIDA, False),  # still Division II
        (2023, "mens", MERRIMACK, True),
        (2024, "mens", MERRIMACK, False),  # first eligible season
        (2024, "womens", SOUTHERN_INDIANA, True),
        (2026, "mens", SOUTHERN_INDIANA, False),  # eligible a year early under the 2025 rule
        (2026, "mens", QUEENS, False),
        (2022, "mens", "91", True),  # Bellarmine won the 2022 ASUN final while reclassifying
        (2013, "mens", "41", True),  # UConn's APR ban
        (2022, "mens", "197", True),  # Oklahoma State's infractions ban
    ],
)
def test_known_cases(season, league, team_id, barred):
    assert (team_id in _ineligible_team_ids(season, league)) is barred


def test_bad_league_raises():
    with pytest.raises(ValueError, match="league"):
        mbb_tournament_ineligible(league="wnba")


@pytest.mark.parametrize(("lg", "league"), [("mbb", "mens"), ("wbb", "womens")])
@pytest.mark.parametrize("season", [2024, 2025])
def test_listed_ids_are_division_one_teams_that_season(lg, league, season):
    """The ESPN id mapping: every barred team is a Division I team on that season's ESPN schedule."""
    teams = college_teams(pl.read_parquet(f"{FIX}/{lg}_schedule_{season}.parquet"))
    barred = _ineligible_team_ids(season, league)
    assert barred and barred <= set(teams["team_id"])


def test_merrimack_2023_nec_title_hands_the_auto_bid_to_fdu():
    # the 2023 release has no TRNMNT type: the final is found by its note ("... Tournament - Final")
    sched = pl.read_parquet(f"{FIX}/mbb_schedule_2023_nec.parquet")
    barred = _ineligible_team_ids(2023, "mens")
    assert MERRIMACK in barred
    status = _conference_status(sched, college_teams(sched, barred)).row(0, named=True)
    assert status["winner_id"] == MERRIMACK and status["champion_id"] == FDU
    # without the list the reclassifying champion would keep the bid
    status = _conference_status(sched, college_teams(sched)).row(0, named=True)
    assert status["champion_id"] == MERRIMACK


def test_both_finalists_barred_leaves_the_bid_to_the_best_eligible_record():
    """Hypothetical bar on both 2023 NEC finalists: no finalist holds the bid, the title stays."""
    sched = pl.read_parquet(f"{FIX}/mbb_schedule_2023_nec.parquet")
    status = _conference_status(sched, college_teams(sched, {MERRIMACK, FDU})).row(0, named=True)
    assert status["winner_id"] == MERRIMACK and status["champion_id"] is None
