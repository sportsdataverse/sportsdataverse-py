"""Offline tests for the ESPN cricket parsers (payload-agnostic against captured fixtures)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

FIX = Path(__file__).parent / "fixtures" / "espn" / "cricket"


def _load(slug: str, host: str, name: str) -> dict:
    return json.loads((FIX / slug / host / f"{name}.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# parse_cricket_scoreboard
# ---------------------------------------------------------------------------


def test_parse_cricket_scoreboard_returns_one_row_per_match():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_scoreboard

    df = parse_cricket_scoreboard(_load("8048", "site-v2", "scoreboard"))
    assert isinstance(df, pl.DataFrame)
    expected = {"event_id", "date", "home_team", "away_team", "home_score", "away_score", "status"}
    assert expected <= set(df.columns), f"missing {expected - set(df.columns)}"
    assert df.height >= 1


def test_parse_cricket_scoreboard_empty_payload_zero_rows():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_scoreboard

    df = parse_cricket_scoreboard({})
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_cricket_scoreboard_pandas_flag():
    import pandas as pd
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_scoreboard

    out = parse_cricket_scoreboard(_load("8048", "site-v2", "scoreboard"), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


# ---------------------------------------------------------------------------
# parse_cricket_standings
# ---------------------------------------------------------------------------


def test_parse_cricket_standings_flattens_table_with_group_column():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_standings

    df = parse_cricket_standings(_load("8048", "site-v2", "standings"))
    assert isinstance(df, pl.DataFrame)
    assert {"team", "group"} <= set(df.columns)
    assert df.height >= 1


def test_parse_cricket_standings_has_stat_columns():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_standings

    df = parse_cricket_standings(_load("8048", "site-v2", "standings"))
    # IPL standings should have matches played / won / lost
    stat_cols = {c for c in df.columns if c not in ("team", "group", "team_id", "team_abbreviation")}
    assert len(stat_cols) >= 2


def test_parse_cricket_standings_empty_zero_rows():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_standings

    assert parse_cricket_standings({}).height == 0


# ---------------------------------------------------------------------------
# parse_cricket_summary
# ---------------------------------------------------------------------------


def test_parse_cricket_summary_dispatch_all_sections_returns_dict():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    out = parse_cricket_summary(_load("8048", "site-v2", "summary"))
    assert isinstance(out, dict)
    for sec in ("matchcards_batting", "matchcards_bowling", "matchcards_partnerships"):
        assert sec in out and isinstance(out[sec], pl.DataFrame), sec


def test_parse_cricket_summary_matchcards_batting_has_rows_and_columns():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    batting = parse_cricket_summary(_load("8048", "site-v2", "summary"), section="matchcards_batting")
    assert isinstance(batting, pl.DataFrame)
    assert {"player_name", "runs", "innings_number"} <= set(batting.columns)
    assert batting.height >= 1


def test_parse_cricket_summary_matchcards_bowling_has_rows_and_columns():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    bowling = parse_cricket_summary(_load("8048", "site-v2", "summary"), section="matchcards_bowling")
    assert isinstance(bowling, pl.DataFrame)
    assert {"player_name", "wickets", "overs"} <= set(bowling.columns)
    assert bowling.height >= 1


def test_parse_cricket_summary_partnerships_has_rows():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    partnerships = parse_cricket_summary(_load("8048", "site-v2", "summary"), section="matchcards_partnerships")
    assert isinstance(partnerships, pl.DataFrame)
    assert {"team_name", "partnership_runs"} <= set(partnerships.columns)
    assert partnerships.height >= 1


def test_parse_cricket_summary_rosters_has_rows():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    rosters = parse_cricket_summary(_load("8048", "site-v2", "summary"), section="rosters")
    assert isinstance(rosters, pl.DataFrame)
    assert {"athlete", "team_id", "home_away"} <= set(rosters.columns)
    assert rosters.height >= 11  # minimum XI per side


def test_parse_cricket_summary_unknown_section_returns_empty_frame():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    result = parse_cricket_summary(_load("8048", "site-v2", "summary"), section="nope")
    assert isinstance(result, pl.DataFrame) and result.height == 0


def test_parse_cricket_summary_empty_payload():
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    out = parse_cricket_summary({})
    assert isinstance(out, dict)
    assert out["matchcards_batting"].height == 0


def test_parse_cricket_summary_pandas_flag():
    import pandas as pd
    from sportsdataverse.cricket.cricket_espn_parsers import parse_cricket_summary

    batting = parse_cricket_summary(
        _load("8048", "site-v2", "summary"),
        section="matchcards_batting",
        return_as_pandas=True,
    )
    assert isinstance(batting, pd.DataFrame)
