"""Offline tests for the stats.ncaa.org college-softball box-score parsers.

These are a by-reference re-export of the college-baseball box parsers (softball
uses the identical markup). The tests pin the re-export identity + exercise the
shared parsers through the softball entry points. A native WSB capture is a
fast-follow (see the softball pbp test); the shared code path is validated here
on a real baseball capture.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    parse_college_baseball_ncaa_linescore,
    parse_college_baseball_ncaa_player_stats,
)
from sportsdataverse.baseball.college_softball.college_softball_ncaa_box import (
    LINESCORE_SCHEMA,
    parse_college_softball_ncaa_linescore,
    parse_college_softball_ncaa_player_stats,
    parse_college_softball_ncaa_situational_stats,
    parse_college_softball_ncaa_team_stats,
)

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "college_baseball_ncaa"


def test_softball_box_parsers_are_baseball_by_reference() -> None:
    assert parse_college_softball_ncaa_linescore is parse_college_baseball_ncaa_linescore
    assert parse_college_softball_ncaa_player_stats is parse_college_baseball_ncaa_player_stats


def test_all_softball_box_entrypoints_exist() -> None:
    for fn in (
        parse_college_softball_ncaa_linescore,
        parse_college_softball_ncaa_team_stats,
        parse_college_softball_ncaa_player_stats,
        parse_college_softball_ncaa_situational_stats,
    ):
        assert callable(fn)


def test_shared_linescore_runs_through_softball_entrypoint() -> None:
    html = (FIX / "bsb_box_score_6357953.html").read_text(encoding="utf-8")
    df = parse_college_softball_ncaa_linescore(html, contest_id="6357953")
    assert df.columns == list(LINESCORE_SCHEMA.keys())
    assert df.height > 0
    assert df.get_column("attendance").unique().to_list() == [284]


def test_empty_contracts() -> None:
    assert parse_college_softball_ncaa_player_stats("") == {}
    assert parse_college_softball_ncaa_linescore("").height == 0
    assert isinstance(parse_college_softball_ncaa_team_stats(""), pl.DataFrame)
