"""Offline tests for the ESPN soccer parsers (payload-agnostic against captured fixtures)."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

FIX = Path(__file__).parent / "fixtures" / "espn" / "soccer"


def _load(slug: str, host: str, name: str) -> dict:
    return json.loads((FIX / slug / host / f"{name}.json").read_text(encoding="utf-8"))


def test_parse_soccer_scoreboard_returns_one_row_per_match():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    payload = _load("eng.1", "site-v2", "scoreboard")
    df = parse_soccer_scoreboard(payload)
    assert isinstance(df, pl.DataFrame)
    expected = {"event_id", "date", "home_team", "away_team", "home_score", "away_score", "status"}
    assert expected <= set(df.columns), f"missing {expected - set(df.columns)}"
    assert df.height == len(payload.get("events", []))


def test_parse_soccer_scoreboard_empty_payload_zero_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    df = parse_soccer_scoreboard({})
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_soccer_scoreboard_pandas_flag():
    import pandas as pd
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_scoreboard

    out = parse_soccer_scoreboard(_load("eng.1", "site-v2", "scoreboard"), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame)


def test_parse_soccer_standings_flattens_table_with_group_column():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    df = parse_soccer_standings(_load("eng.1", "site-v2", "standings"))
    assert isinstance(df, pl.DataFrame)
    assert {"team", "group"} <= set(df.columns)
    assert df.height >= 18  # 20 EPL clubs (>=18 tolerates a mid-season capture)
    assert any(c in df.columns for c in ("points", "rank", "games_played", "overall"))


def test_parse_soccer_standings_multi_group_mls_has_two_groups():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    df = parse_soccer_standings(_load("usa.1", "site-v2", "standings"))
    assert df["group"].n_unique() >= 2  # Eastern + Western conferences


def test_parse_soccer_standings_empty_zero_rows():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_standings

    assert parse_soccer_standings({}).height == 0


def test_parse_soccer_summary_dispatch_all_sections_returns_dict():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary(_load("eng.1", "site-v2", "summary"))
    assert isinstance(out, dict)
    for sec in ("header", "lineups", "key_events", "team_stats"):
        assert sec in out and isinstance(out[sec], pl.DataFrame), sec


def test_parse_soccer_summary_key_events_has_rows_and_columns():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    ke = parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="key_events")
    assert isinstance(ke, pl.DataFrame)
    assert {"type", "clock", "team_id"} <= set(ke.columns)
    assert ke.height >= 1


def test_parse_soccer_summary_lineups_one_row_per_player():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    lu = parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="lineups")
    assert {"athlete", "team_id", "starter"} <= set(lu.columns)
    assert lu.height >= 22


def test_parse_soccer_summary_unknown_section_returns_empty_frame():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    assert parse_soccer_summary(_load("eng.1", "site-v2", "summary"), section="nope").height == 0


def test_parse_soccer_summary_empty_payload():
    from sportsdataverse.soccer.soccer_espn_parsers import parse_soccer_summary

    out = parse_soccer_summary({})
    assert isinstance(out, dict) and out["key_events"].height == 0
