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
