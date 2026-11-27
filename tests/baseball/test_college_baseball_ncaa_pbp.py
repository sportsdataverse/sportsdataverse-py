"""Offline structural tests for the stats.ncaa.org college-baseball pbp parser.

Parses committed real-game fixtures (never synthetic): three D1 games captured
2026-07-17. Asserts schema stability, the empty-input contract, full play-type
classification (0 unknown), decomposition correctness, and two strong end-to-end
invariants -- ``runs_scored`` sums to the final score, and ``scoring_runners``
has one entry per run.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp import (
    PBP_SCHEMA,
    decompose_college_baseball_plays,
    parse_college_baseball_ncaa_pbp,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "college_baseball_ncaa"
GAMES = sorted(FIX.glob("mba_pbp_*.html"))


def _parse(p: Path) -> pl.DataFrame:
    return parse_college_baseball_ncaa_pbp(p.read_text(encoding="utf-8"), contest_id=p.stem.split("_")[-1])


def test_fixtures_present() -> None:
    assert len(GAMES) >= 3, "need multiple captured games"


def test_returns_documented_schema() -> None:
    df = _parse(GAMES[0])
    assert df.columns == list(PBP_SCHEMA.keys())
    assert df.height > 50


def test_empty_input_is_zero_row_with_schema() -> None:
    df = parse_college_baseball_ncaa_pbp("")
    assert df.height == 0
    assert df.columns == list(PBP_SCHEMA.keys())


def test_zero_unknown_play_type_all_games() -> None:
    for p in GAMES:
        df = _parse(p)
        assert df.filter(pl.col("play_type") == "unknown").height == 0, p.name


def test_runs_scored_reconciles_to_final_score() -> None:
    # every run is counted exactly once: sum(runs_scored) == final away+home.
    for p in GAMES:
        df = _parse(p)
        final = (df.get_column("score_away").max() or 0) + (df.get_column("score_home").max() or 0)
        assert df.get_column("runs_scored").sum() == final, p.name


def test_scoring_runners_has_one_name_per_run() -> None:
    for p in GAMES:
        df = _parse(p)
        bad = df.filter(pl.col("scoring_runners").list.len() != pl.col("runs_scored"))
        assert bad.height == 0, p.name


def test_inning_and_half_and_teams() -> None:
    df = _parse(GAMES[0])
    assert df.get_column("inning").min() == 1
    assert set(df.get_column("inning_top_bot").unique().to_list()) <= {"top", "bot"}
    # batting/fielding are the two team names, never equal
    assert df.filter(pl.col("batting") == pl.col("fielding")).height == 0


def test_substitutions_have_no_batter() -> None:
    df = _parse(GAMES[0])
    subs = df.filter(pl.col("play_type") == "substitution")
    assert subs.height > 0
    assert subs.get_column("batter").is_null().all()
    # every play but a substitution or a "No play." row (``other``) resolves a batter
    non_sub = df.filter(pl.col("play_type").is_in(["substitution", "other"]) == False)  # noqa: E712
    assert non_sub.get_column("batter").is_not_null().all()


def test_hits_and_outs_and_counts() -> None:
    df = _parse(GAMES[0])
    hits = df.filter(pl.col("is_hit") == True)  # noqa: E712
    assert hits.height > 0
    assert set(hits.get_column("play_type").unique().to_list()) <= {"single", "double", "triple", "home_run"}
    # a pitch count implies both balls and strikes are set
    counted = df.filter(pl.col("count_balls").is_not_null())
    assert counted.get_column("count_strikes").is_not_null().all()
    assert counted.get_column("count_balls").is_between(0, 4).all()
    assert counted.get_column("count_strikes").is_between(0, 3).all()


def test_double_play_is_two_outs() -> None:
    dps = _parse(GAMES[0]).filter(pl.col("is_double_play") == True)  # noqa: E712
    if dps.height:  # DP present in this game
        assert (dps.get_column("outs_on_play") == 2).all()


def test_specific_decomposition() -> None:
    # contest 6357953: "Osoria, D. singled to center field, 2 RBI ...3a Ballinger, B
    # scored3a Brooks, M. scored." -> single, 2 RBI, 2 runs, both runners named.
    df = parse_college_baseball_ncaa_pbp(
        (FIX / "mba_pbp_6357953.html").read_text(encoding="utf-8"), contest_id="6357953"
    )
    hit = df.filter(pl.col("description").str.starts_with("Osoria, D. singled to center field, 2 RBI"))
    assert hit.height == 1
    row = hit.row(0, named=True)
    assert row["play_type"] == "single"
    assert row["rbi"] == 2
    assert row["runs_scored"] == 2
    assert row["fielded_position"] == "center field"
    assert set(row["scoring_runners"]) == {"Ballinger, B", "Brooks, M."}


def test_return_as_pandas() -> None:
    import pandas as pd

    df = parse_college_baseball_ncaa_pbp(GAMES[0].read_text(encoding="utf-8"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == list(PBP_SCHEMA.keys())


# --- decompose_college_baseball_plays (row-level entry point) --------------


def test_decompose_rows_matches_html_parse() -> None:
    """Legacy-shaped rows (combined ``score`` string) resolve into the same
    columns/values the HTML path produces -- the reconciliation contract for
    the R-era baseballr-data trees."""
    from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays

    rows = [
        {
            "inning": 1,
            "inning_top_bot": "top",
            "batting": "Texas A&M",
            "fielding": "Florida",
            "score": "0-0",
            "description": "Moss, J. singled to left field (1-2 KBFX).",
        },
        {
            "inning": 9,
            "inning_top_bot": "bot",
            "score": "3-2",
            "description": "Langford struck out swinging (2-2 FBKS).",
        },
    ]
    df = decompose_college_baseball_plays(rows)
    assert df.columns == list(PBP_SCHEMA.keys())
    assert df.get_column("play_number").to_list() == [1, 2]
    r0 = df.row(0, named=True)
    assert r0["play_type"] == "single" and r0["is_hit"] is True
    assert (r0["score_away"], r0["score_home"]) == (0, 0)
    assert r0["pitch_sequence"] == "KBFX"
    r1 = df.row(1, named=True)
    assert r1["play_type"] == "strikeout" and r1["strikeout_type"] == "swinging"
    assert (r1["score_away"], r1["score_home"]) == (3, 2)


def test_decompose_empty_is_zero_row_with_schema() -> None:
    from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays

    df = decompose_college_baseball_plays([])
    assert df.height == 0 and df.columns == list(PBP_SCHEMA.keys())


def test_count_columns_stay_integer_under_pandas() -> None:
    """B-L36: the documented integer count columns are nullable Int64 under pandas, not float64."""
    p = GAMES[0]
    pdf = parse_college_baseball_ncaa_pbp(
        p.read_text(encoding="utf-8"), contest_id=p.stem.split("_")[-1], return_as_pandas=True
    )
    assert pdf["count_balls"].isna().any() and pdf["count_balls"].notna().any()  # the capture has both
    assert {str(pdf[c].dtype) for c in ("count_balls", "count_strikes", "rbi", "inning")} == {"Int64"}


# Real descriptions from captured stats.ncaa.org pages (baseballr-data ncaa/raw/<season>), one per runner-clause
# format: run-on with no separator (2012-2021), colon (2016-2021), "3a"/";" (2022+), plus the clause shapes that
# used to drop a run. (description, runs_scored, scoring_runners, outs_on_play)
_RUNNER_CASES = [
    # 2016 contest 292583: clauses run on after the count; two runs, no separator anywhere
    (
        "PARRISH, M. singled to left field, RBI (0-2) TAYLOR, B. advanced to second HALLUM, C. scored.",
        1,
        ["HALLUM, C."],
        0,
    ),
    (
        "Adams, K. walked, RBI Van Asch, J. advanced to second Blaustein, D advanced to third Hogan, M. scored.",
        1,
        ["Hogan, M."],
        0,
    ),
    # 2016 / 2020: after a colon
    ("Nichting singled through the right side, RBI (2-2 SBSB): Sherer scored, unearned.", 1, ["Sherer"], 0),
    (
        "BROOKS, D. singled through the left side, RBI (0-0): MILLER-GREEN advanced to second: NIELSEN, P. scored.",
        1,
        ["NIELSEN, P."],
        0,
    ),
    # 2012 contest 755683: the second out of a double play
    ("BEGEMAN grounded into double play 2b to ss to 1b (0-1) DEEDS out on the play.", 0, [], 2),
    # a runner who moves up and then scores in one clause (2024)
    (
        "COZART doubled down the 1b line; HOGUE advanced to third, scored on a throwing error by 2b, unearned.",
        1,
        ["HOGUE"],
        0,
    ),
    # the runner leads the row (2024)
    ("Mihos, D. scored on a wild pitch.", 1, ["Mihos, D."], 0),
    # the batter comes all the way round (2013)
    ("Vitaletti tripled to center field, scored on a throwing error by cf, unearned.", 1, ["Vitaletti"], 0),
    # a comma before "scored" (2013)
    ("Gilbert, T. singled to right field, RBI (1-0 B) Dixon, B, scored.", 1, ["Dixon, B"], 0),
    # a one-letter first initial on a home run (2024)
    ("C Mcguire homered to left center, RBI (0-0).", 1, ["C Mcguire"], 0),
    # the page prints the runner's name where "scored" belongs (2019 contest 1654161, 2023 contest 2398659)
    (
        "Hayes, Carter singled to left field, RBI Verlin, Nate advanced to second Free, James scored.",
        1,
        ["Free, James"],
        0,
    ),
    (
        "Ryan, Tyler doubled down the lf line, RBI Hayes, Carter advanced to third Verlin, Nate Verlin, Nate.",
        1,
        ["Verlin, Nate"],
        0,
    ),
    (
        "Williams doubled down the rf line, 2 RBI; Hanson advanced to third; Chatfield scored; Atkinson Atkinson, unearned.",
        2,
        ["Chatfield", "Atkinson"],
        0,
    ),
    # ... or as the last item of the runner's own advance (2019 contest 1661692) or the batter's (2023 contest 2406783)
    (
        "Hunter Ruhstorfer singled to right field Dylan Watson advanced to second Kamrin Markwardt advanced to third"
        " on an error by rf, Kamrin Markwardt, unearned.",
        1,
        ["Kamrin Markwardt"],
        0,
    ),
    (
        "S. Mongelli stole second, advanced to third on a throwing error by c, S. Mongelli, unearned.",
        1,
        ["S. Mongelli"],
        0,
    ),
    # "advanced to home" is a run (2023 contest 2396369); thrown out at home is not
    (
        "HILLIER, Three grounded out to 2b, RBI; FOSTER, Cam advanced to second; TROYER, Camden advanced to third;"
        " KEETER, Nathan advanced to home on a fielder's choice.",
        1,
        ["KEETER, Nathan"],
        1,
    ),
    ("PICKENS doubled to right center (1-0) BIRKOFER advanced to home, out at home rf to ss to c.", 0, [], 1),
    # a real repeated given name or a commentary row is not a doubled-name run (2019 contest 1656406, 2023 contest
    # 2398026, 2018 contest 106623)
    ("Hague, Teddy pinch hit for Otaka, Dai Dai.", 0, [], 0),
    ("ARIBAL, Matt pinch ran for GAZDAR, Jon Jon.", 0, [], 0),
    ("Terrific sliding catch. Dah-nuh-nuh. Dah-nuh-nuh.", 0, [], 0),
]


@pytest.mark.parametrize(("desc", "runs", "runners", "outs"), _RUNNER_CASES)
def test_runner_clauses_in_every_page_format(desc: str, runs: int, runners: "list[str]", outs: int) -> None:
    row = decompose_college_baseball_plays([{"description": desc}]).row(0, named=True)
    assert (row["runs_scored"], row["scoring_runners"], row["outs_on_play"]) == (runs, runners, outs)


def test_run_on_batter_clause_stops_before_the_runners() -> None:
    """The batter clause used to swallow the runner text, so a fielder's choice read as the batter's out (2020)."""
    row = decompose_college_baseball_plays(
        [
            {
                "description": "Bicos reached on a fielders choice (0-0) Faria advanced to second Kanbara out at third p to 3b."
            }
        ]
    ).row(0, named=True)
    assert (row["play_type"], row["is_out"], row["outs_on_play"]) == ("fielders_choice", False, 1)
    assert row["runners_advanced"] == ["Faria->second"]


def test_bare_initial_names_and_no_play_rows_classify() -> None:
    rows = decompose_college_baseball_plays(
        [{"description": "BAINER L flied out to rf (2-1)."}, {"description": "No play."}]
    ).to_dicts()
    assert [(r["batter"], r["play_type"]) for r in rows] == [("BAINER L", "flyout"), (None, "other")]


def test_bare_initial_substitution_has_no_batter() -> None:
    """A real legacy-era row ("B Hughes to p for J Barroqueir.") must stay a substitution with no batter."""
    row = decompose_college_baseball_plays([{"description": "B Hughes to p for J Barroqueir."}]).row(0, named=True)
    assert (row["play_type"], row["batter"]) == ("substitution", None)


def test_runner_led_row_and_out_after_advance() -> None:
    """2020 contest 1829869 / 2012 contest 755683: the lead runner's own advance is recorded, and a runner
    who advances and is then put out counts the out."""
    lead, thrown_out = decompose_college_baseball_plays(
        [
            {"description": "Fogel, A. advanced to second Kaler, K. advanced to third on a wild pitch."},
            {
                "description": "PICKENS doubled to right center (1-0) BIRKOFER advanced to third, out at home rf to ss to c."
            },
        ]
    ).to_dicts()
    assert (lead["play_type"], lead["runners_advanced"]) == ("wild_pitch", ["Fogel, A.->second", "Kaler, K.->third"])
    assert (thrown_out["play_type"], thrown_out["outs_on_play"]) == ("double", 1)
