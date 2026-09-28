"""Team / coach tendencies on real NFL / CFB fixture games and synthetic frames."""

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
from sportsdataverse.football.tendencies import RATES, SPLITS, aggregate_tendencies, tendencies
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
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
    c = tendencies(
        coached,
        league="nfl",
        group_cols=("season", "pos_team", "coach"),
        def_group_cols=("season", "def_pos_team", "def_coach"),
    )
    assert set(c["coach"].to_list()) == {"Coach A", "Coach B"}
    assert c.columns[:3] == ["season", "pos_team", "coach"] and "def_coach" not in c.columns
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
    # a career summed from seasons without a curve keeps "over expected" null, never the raw count
    career = aggregate_tendencies([t, t.with_columns(season=pl.lit(2025))], keys=("pos_team",))
    assert career["third_down_expected"].null_count() == career.height
    assert career["third_down_over_expected"].null_count() == career.height
    assert career["third_down_conversions"].sum() == 2 * t["third_down_conversions"].sum()
    with pytest.raises(ValueError, match="aggregation keys"):
        aggregate_tendencies([t], keys=("coach",))
    with pytest.raises(ValueError, match="aggregation keys"):  # missing in only one frame
        aggregate_tendencies([t.with_columns(coach=pl.lit("X")), t], keys=("coach",))
    with pytest.raises(ValueError, match="grouping columns"):
        tendencies(plays.drop("season"), league="nfl")


# ESPN spells the drive result out before 2014 (CFB) / 2005 (NFL); the offense's points
# must not depend on the era's spelling. Return TDs are the other team's points.
_DRIVE_RESULT_CASES = {
    "TD": 7.0,
    "RUSHING TD": 7.0,
    "PASSING TD": 7.0,
    "RUSHING TD TD": 7.0,
    "PASSING TD TD": 7.0,
    "RUSH TD": 7.0,
    "PASSRECEPTION TD": 7.0,
    "LATERAL TD": 7.0,
    "FG": 3.0,
    "FG GOOD": 3.0,
    "MADE FG": 3.0,
    "FIELDGOAL MADE FG": 3.0,
    "INT TD": 0.0,
    "INTERCEPTED PASS TD": 0.0,
    "PUNT RETURN TD": 0.0,
    "FUMBLE RETURN TD": 0.0,
    "BLOCKED PUNT TD": 0.0,
    "MISSED FG TD": 0.0,
    "PUNT": 0.0,
    "MISSED FG": 0.0,
    "FG MISSED": 0.0,
}


def test_drive_points_read_every_era_spelling():
    results = [*_DRIVE_RESULT_CASES, None]
    plays = pl.DataFrame(
        {
            "season": 2005,
            "game_id": 1,
            "pos_team": [f"T{i}" for i in range(len(results))],
            "def_pos_team": "OPP",
            "drive.id": [str(i) for i in range(len(results))],
            "drive.result": results,
            "scrimmage_play": True,
            "penalty_no_play": False,
        }
    )
    t = tendencies(plays, league="cfb", third_down_curve=pl.DataFrame({"distance": [], "rate": []}))
    got = dict(zip(t["pos_team"], t["drive_points"]))
    want = {f"T{i}": _DRIVE_RESULT_CASES.get(r, 0.0) for i, r in enumerate(results)}
    assert got == want
    assert t.filter(pl.col("pos_team") == "T0")["pts_per_drive"][0] == 7.0


def test_drive_points_on_a_2005_cfb_game():
    """Boston College @ BYU, 2005 (20-3): "PASSING TD" / "FG GOOD" drives score the final."""
    game_id = 252460252
    summary = json.loads((Path(__file__).parent.parent / "cfb" / "fixtures" / f"summary_{game_id}.json").read_text())
    proc = CFBPlayProcess(gameId=game_id)
    proc.espn_cfb_pbp(summary=summary)
    proc.run_processing_pipeline()
    t = tendencies(proc.plays_frame, league="cfb")
    points = dict(zip(t["pos_team"], t["drive_points"]))
    assert points == {103: 20.0, 252: 3.0}  # BC: 2 PASSING TD + 2 FG GOOD; BYU: 1 FG GOOD
    assert dict(zip(t["pos_team"], t["def_drive_points"])) == {103: 3.0, 252: 20.0}


# --- IF-1: every split carries EPA and success; field-zone / third-down-distance / one-score splits


def test_pre_existing_split_epa_is_unchanged(plays):
    """Pinned from origin/main 6426286aa, before ``_split`` generated the EPA columns."""
    t = tendencies(plays, league="nfl")
    got = {r["pos_team"]: (r["epa_early_down"], r["epa_per_play_neutral"]) for r in t.to_dicts()}
    want = {5: (-1.4621596468205098, -0.8858761638402939), 30: (13.574272631376516, 0.5800969863776118)}
    for team, (early, neutral) in want.items():
        assert abs(got[team][0] - early) < 1e-9 and abs(got[team][1] - neutral) < 1e-9, team


def test_every_split_carries_epa_and_success(plays):
    t = tendencies(plays, league="nfl")
    for row in t.to_dicts():
        assert abs(row["epa_leading"] + row["epa_tied"] + row["epa_trailing"] - row["epa"]) < 1e-9
        assert row["successes_leading"] + row["successes_tied"] + row["successes_trailing"] == row["successes"]
        assert row["plays_own_half"] + row["plays_opp_half"] == row["plays"]
        assert row["plays_d3_short"] + row["plays_d3_medium"] + row["plays_d3_long"] == row["plays_d3"]
        assert abs(row["epa_per_play_d1"] - row["epa_d1"] / row["plays_d1"]) < 1e-12
        assert row["plays_one_score"] <= row["plays"] and row["plays_red_zone"] <= row["plays"]
        for s in SPLITS:
            for rate, num in (("pass_rate", "passes"), ("epa_per_play", "epa"), ("success_rate", "successes")):
                assert (row[f"{rate}_{s}"] is None) == (row[f"plays_{s}"] == 0), (rate, s)
                assert f"{num}_{s}" in row
    rates = {r for r, _, _ in RATES}
    assert {f"{r}_{s}" for s in SPLITS for r in ("pass_rate", "epa_per_play", "success_rate")} <= rates
    # the names that shipped before the split families were generated
    assert {"pass_rate_d1", "pass_rate_neutral", "epa_per_play_early_down", "epa_per_play_neutral"} <= rates


def test_split_counts_recount_from_the_plays(plays):
    """Per team, straight from the fixture's own yards-to-end-zone and score-at-the-snap columns.

    ESPN's rz_play flag says team 30 had 1 red-zone snap; it read the absolute yard line.
    """
    snaps = plays.filter((pl.col("scrimmage_play") == True) & (pl.col("penalty_no_play") != True))
    ytg, score = pl.col("start.yardsToEndzone"), pl.col("pos_score_diff_start")
    recount = snaps.group_by("pos_team").agg(
        plays=pl.len(),
        red_zone=(ytg <= 20).sum(),
        opp_half=(ytg < 50).sum(),
        own_half=(ytg >= 50).sum(),
        one_score=(score.abs() <= 8).sum(),
        leading=(score > 0).sum(),
        tied=(score == 0).sum(),
        trailing=(score < 0).sum(),
    )
    pinned = {
        5: {
            "plays": 49,
            "red_zone": 7,
            "opp_half": 21,
            "own_half": 28,
            "one_score": 8,
            "leading": 0,
            "tied": 5,
            "trailing": 44,
        },
        30: {
            "plays": 56,
            "red_zone": 3,
            "opp_half": 26,
            "own_half": 30,
            "one_score": 17,
            "leading": 48,
            "tied": 8,
            "trailing": 0,
        },
    }
    assert {r.pop("pos_team"): r for r in recount.to_dicts()} == pinned
    for row in tendencies(plays, league="nfl").to_dicts():
        want = pinned[row["pos_team"]]
        assert {k: row["plays" if k == "plays" else f"plays_{k}"] for k in want} == want, row["pos_team"]


def test_split_boundaries_and_fallbacks():
    """One team per snap: red zone at 20, own half from 50, one score at 8, the score before the play."""
    n = 6
    plays = pl.DataFrame(
        {
            "season": 2025,
            "game_id": 1,
            "pos_team": [f"T{i}" for i in range(n)],
            "def_pos_team": "OPP",
            "scrimmage_play": True,
            "penalty_no_play": False,
            "start.yardsToEndzone": [20, 21, 49, 50, None, None],
            "rz_play": [False, True, False, False, True, False],  # read only when the distance is missing
            "pos_score_diff_start": [8, -8, 9, -9, None, 0],
            "pos_score_diff": [1, 1, 1, 1, 3, 7],  # the score AFTER the play: read only when the start is missing
        }
    )
    t = tendencies(plays, league="nfl", third_down_curve=pl.DataFrame({"distance": [], "rate": []}))
    t = t.sort("pos_team")
    splits = ("red_zone", "opp_half", "own_half", "one_score", "leading", "tied", "trailing")
    assert {s: t[f"plays_{s}"].to_list() for s in splits} == {
        "red_zone": [1, 0, 0, 0, 1, 0],
        "opp_half": [1, 1, 1, 0, 0, 0],
        "own_half": [0, 0, 0, 1, 0, 0],
        "one_score": [1, 1, 0, 0, 1, 1],
        "leading": [1, 0, 1, 0, 1, 0],
        "tied": [0, 0, 0, 0, 0, 1],
        "trailing": [0, 1, 0, 1, 0, 0],
    }


def test_play_level_def_twins_read_the_offense_side(plays):
    """``def_`` split columns keep the OFFENSE's situation; the perspective swap is the consumer's."""
    t = tendencies(plays, league="nfl")
    five, thirty = (t.filter(pl.col("pos_team") == k).row(0, named=True) for k in (5, 30))
    assert abs(five["def_epa_leading"] - thirty["epa_leading"]) < 1e-9
    assert abs(five["def_plays_opp_half"] - thirty["plays_opp_half"]) < 1e-9
    for s in SPLITS:
        for fam in ("plays", "passes", "epa", "successes"):
            assert abs(five[f"def_{fam}_{s}"] - thirty[f"{fam}_{s}"]) < 1e-9, (fam, s)


def test_split_counts_sum_into_careers(plays):
    t = tendencies(plays, league="nfl")
    two = aggregate_tendencies([t, t], keys=("pos_team",))
    for k in (5, 30):
        one, agg = (f.filter(pl.col("pos_team") == k).row(0, named=True) for f in (t, two))
        assert agg["plays_opp_half"] == 2 * one["plays_opp_half"]
        assert abs(agg["epa_per_play_opp_half"] - one["epa_per_play_opp_half"]) < 1e-9


# --- IF-1: optional game-context splits from Boolean ctx_* / def_ctx_* columns


def _with_context(plays: pl.DataFrame) -> pl.DataFrame:
    """Team 5 is the home side and the winner; team 30 is away and lost."""
    off, dfn = pl.col("pos_team") == 5, pl.col("def_pos_team") == 5
    return plays.with_columns(
        ctx_home=off,
        def_ctx_home=dfn,
        ctx_away=~off,
        def_ctx_away=~dfn,
        ctx_win=off,
        def_ctx_win=dfn,
    )


def test_game_context_splits(plays):
    t = tendencies(_with_context(plays), league="nfl")
    five, thirty = (t.filter(pl.col("pos_team") == k).row(0, named=True) for k in (5, 30))
    assert five["games_home"] == 1 and five["plays_home"] == five["plays"]
    assert thirty["games_home"] == 0 and thirty["plays_home"] == 0 and thirty["win_rate_home"] is None
    assert five["wins_home"] == 1 and five["win_rate_home"] == 1.0
    assert thirty["games_away"] == 1 and thirty["wins_away"] == 0 and thirty["win_rate_away"] == 0.0
    assert abs(five["epa_per_play_home"] - five["epa_per_play"]) < 1e-12
    # the defense reads the DEFENDING team's context: team 5 defended at home and won
    assert five["def_games_home"] == 1 and five["def_wins_home"] == 1 and five["def_plays_home"] == five["def_plays"]
    assert thirty["def_games_home"] == 0 and thirty["def_games_away"] == 1 and thirty["def_win_rate_away"] == 0.0
    # absent contexts emit nothing (no ctx_vs_ranked input, so no vs_ranked columns)
    assert not [c for c in t.columns if "vs_ranked" in c or "after_bye" in c]
    # careers sum the games / wins and re-rate them
    two = aggregate_tendencies([t, t], keys=("pos_team",)).filter(pl.col("pos_team") == 5).row(0, named=True)
    assert two["games_home"] == 2 and two["wins_home"] == 2 and two["win_rate_home"] == 1.0


def test_no_context_columns_emit_nothing_and_change_nothing(plays):
    base = tendencies(plays, league="nfl")
    assert not [c for c in base.columns if c.startswith(("games_", "wins_", "def_games_", "def_wins_"))]
    assert not [c for c in base.columns if "vs_ranked" in c or "win_rate" in c]
    ctx = tendencies(_with_context(plays), league="nfl")
    for c in base.columns:
        assert ctx[c].equals(base[c], check_dtypes=True), c


def test_a_null_context_is_not_true(plays):
    """ctx_home null on every third down: team 5 loses exactly those snaps from home and keeps the game."""
    third = pl.col("start.down") == 3
    ctx = _with_context(plays).with_columns(
        ctx_home=pl.when(third).then(None).otherwise(pl.col("ctx_home")),
        def_ctx_home=pl.when(third).then(None).otherwise(pl.col("def_ctx_home")),
    )
    t = tendencies(ctx, league="nfl")
    five, thirty = (t.filter(pl.col("pos_team") == k).row(0, named=True) for k in (5, 30))
    assert five["plays_d3"] > 0 and thirty["plays_d3"] > 0
    assert five["plays_home"] == five["plays"] - five["plays_d3"] and five["games_home"] == 1
    assert thirty["plays_home"] == 0 and thirty["games_home"] == 0
    assert five["def_plays_home"] == five["def_plays"] - five["def_plays_d3"] and thirty["def_games_home"] == 0


def test_an_all_null_boolean_context_is_allowed_a_null_dtype_is_not(plays):
    t = tendencies(plays.with_columns(ctx_home=pl.lit(None, dtype=pl.Boolean)), league="nfl")
    assert t["games_home"].to_list() == [0, 0] and t["plays_home"].to_list() == [0, 0]
    with pytest.raises(TypeError, match="ctx_home"):
        tendencies(plays.with_columns(ctx_home=pl.lit(None)), league="nfl")


@pytest.mark.parametrize("col", ["ctx_home", "def_ctx_win"])
def test_a_non_boolean_context_column_raises(plays, col):
    with pytest.raises(TypeError, match=col):
        tendencies(plays.with_columns(pl.lit("true").alias(col)), league="nfl")
