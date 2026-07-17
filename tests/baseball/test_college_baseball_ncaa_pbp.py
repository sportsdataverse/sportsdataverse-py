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

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp import (
    PBP_SCHEMA,
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
    # every non-substitution play resolves a batter
    non_sub = df.filter(pl.col("play_type") != "substitution")
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
