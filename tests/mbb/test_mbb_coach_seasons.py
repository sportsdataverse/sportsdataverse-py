"""BB-D8: college coach seasons on committed real captures (stats.ncaa.org coach cards, not KenPom).

Fixtures (``tests/fixtures/bb_coach_seasons/README.md``; none synthetic): two stats.ncaa.org roster
pages and their crosswalk rows from ``ncaa-mbb-hoops-raw``, every men's (2010-2026) and women's
(2011-2026) coach card parsed from the raw repos, BB-D22's season records (ESPN final scores) and
BB-D3's Big Ten 2024-25 possessions.

Gates are the observed values (never loosened to pass).
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.coach_seasons import COACH_SEASONS_SCHEMA
from sportsdataverse._common.team_season_rates import team_season_rates
from sportsdataverse.mbb.mbb_coach_seasons import (
    NCAA_COACHES_SCHEMA,
    coach_season_records,
    ncaa_team_coaches,
    parse_ncaa_team_coach,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
FIX = FIXTURES / "bb_coach_seasons"
RATES = FIXTURES / "bb_team_season_rates"
mod = importlib.import_module("sportsdataverse.mbb.mbb_coach_seasons")
tsr = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")

#: league -> (team-seasons 2014-2026, card record = ESPN W-L exactly, losses within one, other,
#: no card record to check, no ESPN record).
CHECK = {"mbb": (4625, 4104, 184, 42, 285, 10), "wbb": (4595, 3992, 270, 94, 222, 17)}


def test_parse_the_coach_card() -> None:
    page = (FIX / "ncaa_tree" / "mbb" / "rosters" / "html" / "2018" / "110939.html").read_text(encoding="utf-8")
    # John Groce's 10th season as a head coach (Ohio, Illinois, then Akron from 2017-18)
    assert parse_ncaa_team_coach(page) == {
        "coach_id": 33967,
        "coach": "John Groce",
        "career_seasons": 10,
        "career_wins": 196,
        "career_losses": 150,
    }
    assert parse_ncaa_team_coach("<html><body><div class='card-header'>Roster</div></body></html>") is None


def test_tree_reader_maps_espn_team_ids() -> None:
    out = ncaa_team_coaches(FIX / "ncaa_tree", [2017, 2018])  # 2017 has no crosswalk in the tree: skipped
    assert dict(out.schema) == NCAA_COACHES_SCHEMA
    assert out.select("season", "ncaa_team_id", "team_id", "team", "coach").rows() == [
        (2018, 110938, 2000, "Abilene Christian", "Joe Golding"),
        (2018, 110939, 2006, "Akron", "John Groce"),
    ]
    with pytest.raises(ValueError, match="league"):
        ncaa_team_coaches(FIX / "ncaa_tree", 2018, league="nba")


@pytest.mark.parametrize("lg", list(CHECK))
def test_card_record_against_espn_final_scores(lg: str) -> None:
    cards = coach_season_records(pl.read_parquet(FIX / f"ncaa_{lg}_coaches.parquet")).filter(
        pl.col("season").is_between(2014, 2026)
    )
    records = pl.read_parquet(FIXTURES / "bb_team_luck" / f"{lg}_season_records.parquet")
    assert cards.schema["team_id"] == records.schema["team_id"]
    j = cards.join(records, on=["season", "team_id"], how="left", validate="1:1")
    checked = pl.col("season_wins").is_not_null() & pl.col("games").is_not_null()
    exact = checked & (pl.col("season_wins") == pl.col("wins")) & (pl.col("season_losses") == pl.col("losses"))
    near = checked & ~exact & ((pl.col("season_losses") - pl.col("losses")).abs() <= 1)
    n, n_exact, n_near, n_other, n_uncounted, n_no_results = CHECK[lg]
    assert j.height == n
    assert j.filter(exact).height == n_exact
    assert j.filter(near).height == n_near
    assert j.filter(checked & ~exact & ~near).height == n_other
    assert j["season_wins"].null_count() == n_uncounted
    assert j.filter(pl.col("season_wins").is_not_null() & pl.col("games").is_null()).height == n_no_results
    # the card counts vacated wins out, never losses: Arizona 2016-17 is 0-5 on the card, 32-5 on the floor
    az = j.filter((pl.col("season") == 2017) & (pl.col("team") == "Arizona"))
    if lg == "mbb":
        assert az.select("season_wins", "season_losses", "wins", "losses").row(0) == (0, 5, 32, 5)


def test_card_record_chains_across_schools() -> None:
    cards = coach_season_records(pl.read_parquet(FIX / "ncaa_mbb_coaches.parquet"))
    groce = cards.filter((pl.col("coach_id") == 33967) & pl.col("season").is_in([2012, 2013])).sort("season")
    # Ohio 2011-12, then Illinois 2012-13 (23-13): the difference follows the coach, not the school
    assert groce["team"].to_list() == ["Ohio", "Illinois"]
    assert groce.select("season_wins", "season_losses").row(1) == (23, 13)


def _fixture(name: str):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(name)

    def load(seasons):  # type: ignore[no-untyped-def]
        assert list(seasons) == [2025]
        return frame

    return load


def test_mbb_coach_seasons_match_team_rates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tsr, "load_ncaa_mbb_possessions", _fixture(RATES / "ncaa_mbb_possessions_2025.parquet"))
    monkeypatch.setattr(tsr, "load_ncaa_mbb_team_box", _fixture(RATES / "ncaa_mbb_team_box_2025.parquet"))
    monkeypatch.setattr(tsr, "load_mbb_schedule", _fixture(FIX / "mbb_schedule_2025.parquet"))
    coaches = pl.read_parquet(FIX / "ncaa_mbb_coaches.parquet")
    out = mod.mbb_coach_seasons(2025, coaches)
    assert dict(out.schema) == COACH_SEASONS_SCHEMA
    assert (out["games"] == out["team_games"]).all()  # one card coach per team-season
    rated = out.filter(pl.col("pbp_games") > 0)
    rows = tsr.ncaa_possession_rate_rows(
        pl.read_parquet(RATES / "ncaa_mbb_possessions_2025.parquet"),
        pl.read_parquet(RATES / "mbb_schedule_2025.parquet"),
    )
    team = team_season_rates(
        rows,
        minutes_per_game=40,
        counts=tsr.ncaa_team_box_count_rows(pl.read_parquet(RATES / "ncaa_mbb_team_box_2025.parquet")),
    ).filter(split="all")
    joined = rated.join(team, on="team_id", suffix="_team", how="inner")
    assert joined.height == rated.height >= 18  # the Big Ten, each with its coach
    for m in ("off_rtg", "def_rtg", "net_rtg", "pace", "efg_pct", "tov_pct", "orb_pct"):
        assert joined[m].null_count() == joined[f"{m}_team"].null_count() == 0, m
        assert (joined[m] - joined[f"{m}_team"]).abs().max() < 1e-9, m
    assert (joined["poss"] == joined["poss_team"]).all()
    big_ten = set(joined["coach"])
    assert {"Dusty May", "Tom Izzo"} <= big_ten


def test_sample_against_sports_reference() -> None:
    # 16 team-seasons (10 men's, 6 women's) from Sports-Reference's school pages: coach and W-L
    sample = pl.read_parquet(FIX / "sports_reference_college_sample.parquet")
    checked = 0
    for lg in ("mbb", "wbb"):
        cards = pl.read_parquet(FIX / f"ncaa_{lg}_coaches.parquet").select("season", "team", "team_id", "coach")
        records = pl.read_parquet(FIXTURES / "bb_team_luck" / f"{lg}_season_records.parquet")
        j = (
            sample.filter(pl.col("league") == lg)
            .join(cards, on=["season", "team"], how="inner", suffix="_card")
            .join(records, on=["season", "team_id"], how="inner", validate="1:1")
        )
        assert (j["coach"] == j["coach_card"]).all()
        assert (j["w"] == j["wins"]).all() and (j["l"] == j["losses"]).all()
        checked += j.height
    assert checked == sample.height == 16


#: league -> (cards 2014-2026, of them a mid-season change: the card's season short of the team's).
STINTS = {"mbb": (4625, 37), "wbb": (4595, 29)}


@pytest.mark.parametrize("lg", list(STINTS))
def test_mid_season_change_is_left_uncredited(lg: str) -> None:
    records = pl.read_parquet(FIXTURES / "bb_team_luck" / f"{lg}_season_records.parquet")
    cards = pl.read_parquet(FIX / f"ncaa_{lg}_coaches.parquet")
    stints = mod.ncaa_coach_stints(cards, records).filter(pl.col("season").is_between(2014, 2026))
    n, changed = STINTS[lg]
    assert stints.height == n - changed
    assert stints["games"].null_count() == stints.height and set(stints["position"]) == {"any"}
    assert stints.select(pl.struct("season", "team_id").is_duplicated().any()).item() is False


def test_changed_season_card_figures_are_the_other_coachs() -> None:
    # Sports-Reference splits four of the changed seasons; each card's season figure is the coach
    # it does NOT name (Ole Miss 2017-18 names Andy Kennedy 1-4: Tony Madlock's games)
    changes = pl.read_parquet(FIX / "sports_reference_college_changes.parquet")
    cards = coach_season_records(pl.read_parquet(FIX / "ncaa_mbb_coaches.parquet"))
    j = cards.join(changes, on=["season", "team_id"], how="inner")
    assert j.height == 8
    other = j.filter(pl.col("coach") != pl.col("coach_right"))
    assert other.height == 4
    assert (other["season_wins"] == other["w"]).all() and (other["season_losses"] == other["l"]).all()
    stints = mod.ncaa_coach_stints(
        pl.read_parquet(FIX / "ncaa_mbb_coaches.parquet"),
        pl.read_parquet(FIXTURES / "bb_team_luck" / "mbb_season_records.parquet"),
    )
    assert stints.join(changes, on=["season", "team_id"], how="semi").height == 0
