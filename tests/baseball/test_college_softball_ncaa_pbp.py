"""Offline tests for the stats.ncaa.org college-softball pbp parser.

``parse_college_softball_ncaa_pbp`` is a **by-reference re-export** of the
college-baseball parser: stats.ncaa.org softball play-by-play uses the identical
per-inning ``<table class="table">`` layout and play grammar, so one parser
serves both. These tests pin the re-export identity + schema and exercise the
shared parser through the softball entry point.

NOTE: a native WSB (softball) capture is a fast-follow. stats.ncaa.org softball
discovery via ``inst_team_list?sport_code=WSB`` returns a shell (WSB is the
confirmed softball code -- verified against the site's sport dropdown -- but its
team-list flow differs from baseball's), so the shared code path is exercised
here on a real baseball capture until a WSB game is captured.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_pbp import (
    parse_college_baseball_ncaa_pbp,
)
from sportsdataverse.baseball.college_softball.college_softball_ncaa_pbp import (
    PBP_SCHEMA,
    parse_college_softball_ncaa_pbp,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "college_baseball_ncaa"


def test_softball_parser_is_the_baseball_parser_by_reference() -> None:
    assert parse_college_softball_ncaa_pbp is parse_college_baseball_ncaa_pbp


def test_empty_input_is_zero_row_with_schema() -> None:
    df = parse_college_softball_ncaa_pbp("")
    assert df.height == 0
    assert df.columns == list(PBP_SCHEMA.keys())


def test_shared_parser_runs_through_softball_entrypoint() -> None:
    # exercises the same table-layout / grammar the softball page shares.
    p = FIX / "mba_pbp_6357953.html"
    df = parse_college_softball_ncaa_pbp(p.read_text(encoding="utf-8"), contest_id="6357953")
    assert df.columns == list(PBP_SCHEMA.keys())
    assert df.height > 50
    assert df.filter(pl.col("play_type") == "unknown").height == 0


def test_real_wsb_game_parses_and_reconciles() -> None:
    # A REAL softball capture (contest 6548848, Elon @ Saint Joseph's 2025-04-12),
    # discovered live via the scoreboard route. Softball differs from baseball --
    # last-name-only players, ';' clause separator, 'stole home' runs -- all handled
    # by the shared parser (verified end-to-end).
    df = parse_college_softball_ncaa_pbp(
        (FIX / "wsb_pbp_6548848.html").read_text(encoding="utf-8"), contest_id="6548848"
    )
    assert df.filter(pl.col("play_type") == "unknown").height == 0
    final = (df.get_column("score_away").max() or 0) + (df.get_column("score_home").max() or 0)
    assert df.get_column("runs_scored").sum() == final  # every run counted once
    assert (df.get_column("scoring_runners").list.len() == df.get_column("runs_scored")).all()
