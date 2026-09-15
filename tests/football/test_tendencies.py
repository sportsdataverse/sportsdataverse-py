"""Team / coach tendencies on the real NFL fixture game and a synthetic frame."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.football.play_participants import (
    athlete_lookup_from_summary,
    play_participants_from_items,
)
from sportsdataverse.football.tendencies import RATES, aggregate_tendencies, tendencies
from sportsdataverse.nfl import NFLPlayProcess

FIX = Path(__file__).parent.parent / "nfl" / "fixtures"
GAME_ID = 401872922


@pytest.fixture(scope="module")
def plays() -> pl.DataFrame:
    summary = json.loads((FIX / "summary_401872922.json").read_text())
    with gzip.open(FIX / "plays_401872922.json.gz", "rt", encoding="utf-8") as fh:
        items = json.load(fh)["items"]
    parts = play_participants_from_items(items, GAME_ID, athlete_lookup=athlete_lookup_from_summary(summary))
    proc = NFLPlayProcess(gameId=GAME_ID, participants=parts)
    proc.espn_nfl_pbp(summary=summary)
    out = proc.run_processing_pipeline()
    df = pl.from_dicts(out["plays"], infer_schema_length=None)
    return df.with_columns(season=pl.lit(2026))


def test_team_rows_from_one_game(plays):
    t = tendencies(plays, league="nfl")
    assert t.height == 2 and set(t["pos_team"].to_list()) == {5, 30}
    row = t.row(0, named=True)
    assert row["games"] == 1 and row["plays"] > 40 and row["drives"] >= 8
    # counts reconcile with their splits
    assert row["passes"] + row["rushes"] <= row["plays"]
    assert row["plays_leading"] + row["plays_tied"] + row["plays_trailing"] == row["plays"]
    assert row["plays_first_half"] + row["plays_second_half"] == row["plays"]
    assert abs(row["pass_rate"] - row["passes"] / row["plays"]) < 1e-12
    assert 0 <= row["pass_rate_neutral"] <= 1 and row["plays_neutral"] <= row["plays"]
    assert 15 < row["sec_per_play"] < 60 and row["pace_coverage"] == 1.0
    assert row["rz_trips"] <= row["so_trips"] <= row["drives"]
    assert row["scripted_drives"] + row["non_scripted_drives"] == row["drives"]
    assert row["scripted_drives"] <= 4


def test_fourth_down_columns(plays):
    t = tendencies(plays, league="nfl")
    for row in t.to_dicts():
        assert row["fourth_decisions"] >= row["fourth_went"] >= row["fourth_converted"]
        assert row["fourth_model_go"] + row["fourth_model_kick"] <= row["fourth_decisions"]
        assert row["fourth_went_when_go"] <= row["fourth_model_go"]
        assert row["fourth_agreed"] <= row["fourth_decisions"]
        assert row["fourth_wp_left"] >= 0
        if row["fourth_decisions"]:
            assert 0 <= row["go_rate"] <= 1 and 0 <= row["fourth_agreement_rate"] <= 1
    # timeouts and penalties on fourth down are not decisions
    fourth_rows = plays.filter(pl.col("start.down") == 4)
    assert t["fourth_decisions"].sum() < fourth_rows.height


def test_defense_twins_mirror_the_opponent(plays):
    t = tendencies(plays, league="nfl").sort("pos_team")
    a, b = t.row(0, named=True), t.row(1, named=True)
    assert a["def_plays"] == b["plays"] and b["def_plays"] == a["plays"]
    assert abs(a["def_epa_per_play"] - b["epa_per_play"]) < 1e-9
    assert a["def_rz_trips"] == b["rz_trips"]


def test_coach_grouping_and_career_aggregation(plays):
    coached = plays.with_columns(
        coach=pl.when(pl.col("pos_team") == 5).then(pl.lit("Coach A")).otherwise(pl.lit("Coach B")),
        def_coach=pl.when(pl.col("def_pos_team") == 5).then(pl.lit("Coach A")).otherwise(pl.lit("Coach B")),
    )
    c = tendencies(coached, league="nfl", group_cols=("season", "coach"), def_group_cols=("season", "def_coach"))
    assert set(c["coach"].to_list()) == {"Coach A", "Coach B"}
    t = tendencies(plays, league="nfl")
    assert (
        abs(c.filter(pl.col("coach") == "Coach A")["pass_rate"][0] - t.filter(pl.col("pos_team") == 5)["pass_rate"][0])
        < 1e-12
    )
    assert (
        abs(
            c.filter(pl.col("coach") == "Coach A")["def_success_rate"][0]
            - t.filter(pl.col("pos_team") == 5)["def_success_rate"][0]
        )
        < 1e-12
    )
    # a "career" of the same season twice doubles every count and keeps every rate
    two = c.with_columns(season=pl.lit(2025))
    career = aggregate_tendencies([c, two], keys=("coach",))
    row = career.filter(pl.col("coach") == "Coach A").row(0, named=True)
    one = c.filter(pl.col("coach") == "Coach A").row(0, named=True)
    assert row["seasons"] == 2 and row["first_season"] == 2025 and row["last_season"] == 2026
    assert row["plays"] == 2 * one["plays"] and row["fourth_decisions"] == 2 * one["fourth_decisions"]
    for rate, _, _ in RATES:
        if one.get(rate) is not None:
            assert abs(row[rate] - one[rate]) < 1e-9, rate
    assert abs(row["def_epa_per_play"] - one["def_epa_per_play"]) < 1e-9


def test_degrades_without_curve_or_clock(plays):
    t = tendencies(plays, league="nfl", third_down_curve=pl.DataFrame({"distance": [], "rate": []}))
    assert t["third_down_expected"].null_count() == t.height and t["third_down_over_expected"].null_count() == t.height
    no_clock = plays.drop("drive.timeElapsed.displayValue")
    t2 = tendencies(no_clock, league="nfl")
    assert (t2["pace_coverage"] == 0).all() and t2["sec_per_play"].null_count() == t2.height
    assert tendencies(pl.DataFrame(), league="nfl").height == 0
    assert aggregate_tendencies([]).height == 0
    with pytest.raises(ValueError, match="grouping columns"):
        tendencies(plays.drop("season"), league="nfl")
