"""Offline tests for the Fox Bifrost WBB/WNBA extensions (crosswalk prerequisites).

Run against committed real captures (see ``tests/fixtures/fox/README.md``) —
no network. The teams frame (``fox_team_id`` / ``fox_team_name`` /
``fox_section``) is the shape the R basketball crosswalks consume.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import sportsdataverse._fox_layout as fox_layout
from sportsdataverse._fox_layout import parse_teams

FIX = Path(__file__).parent / "fixtures" / "fox"

TEAMS_COLS = ["fox_team_id", "fox_team_name", "fox_section"]


def _load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def test_parse_teams_wnba_full_league():
    rows = parse_teams(_load("wnba_team_3_standings.json"))
    assert len(rows) >= 12  # league-wide directory from one standings payload
    ids = [r["fox_team_id"] for r in rows]
    assert len(ids) == len(set(ids)), "fox_team_id must be de-duplicated"
    assert {"Atlanta Dream", "Minnesota Lynx"}.issubset({r["fox_team_name"] for r in rows})
    assert all(set(r) == set(TEAMS_COLS) for r in rows)


def test_parse_teams_wcbk_conference_section():
    rows = parse_teams(_load("wcbk_team_11_standings.json"))
    assert len(rows) >= 10  # Big East from the UConn seed
    # fox_section prefers metadata.parameters.groupName -- the conference name
    assert {r["fox_section"] for r in rows} == {"Big East"}
    assert "Uconn Huskies" in {r["fox_team_name"] for r in rows}


def test_parse_teams_title_case_keeps_apostrophes():
    # ST. JOHN'S must not become "St. John'S" (str.title foot-gun)
    assert fox_layout._title_case("ST. JOHN'S RED STORM") == "St. John's Red Storm"


def test_fox_wbb_teams_offline(monkeypatch):
    from sportsdataverse.wbb import fox_wbb_teams

    payload = _load("wcbk_team_11_standings.json")
    monkeypatch.setattr(fox_layout, "_get", lambda url, **kw: payload)
    df = fox_wbb_teams("11")
    assert isinstance(df, pl.DataFrame)
    assert df.columns == TEAMS_COLS
    assert len(df) >= 10
    assert df.schema["fox_team_id"] == pl.Utf8
    raw = fox_wbb_teams("11", return_parsed=False)
    assert isinstance(raw, dict)


def test_fox_wnba_teams_offline(monkeypatch):
    from sportsdataverse.wnba import fox_wnba_teams

    payload = _load("wnba_team_3_standings.json")
    monkeypatch.setattr(fox_layout, "_get", lambda url, **kw: payload)
    df = fox_wnba_teams()
    assert isinstance(df, pl.DataFrame)
    assert df.columns == TEAMS_COLS
    assert len(df) >= 12


def test_fox_teams_empty_payload_keeps_schema(monkeypatch):
    from sportsdataverse.wnba import fox_wnba_teams

    monkeypatch.setattr(fox_layout, "_get", lambda url, **kw: {})
    df = fox_wnba_teams()
    assert df.columns == TEAMS_COLS
    assert len(df) == 0


def test_fox_wbb_teams_all_budget_and_dedupe(monkeypatch):
    import sportsdataverse.wbb.wbb_fox_ext as ext

    payload = _load("wcbk_team_11_standings.json")
    calls: list = []

    def fake_fox_get(path: str, **kw):
        calls.append(path)
        return payload

    monkeypatch.setattr(ext, "fox_get", fake_fox_get)
    df = ext.fox_wbb_teams_all(max_id=500, max_calls=3)
    assert len(calls) == 3, "max_calls must bound the fetch budget"
    assert df.columns == TEAMS_COLS
    assert df["fox_team_id"].n_unique() == len(df)
    # the walk starts at candidate id 1; every returned team id lands in `seen`
    # so later candidates matching them are skipped without spending a call
    assert calls[0] == "wcbk/team/1/standings"
    assert calls[1] == "wcbk/team/2/standings"


def test_fox_wbb_wnba_wrappers_registered():
    import sportsdataverse.wbb as wbb
    import sportsdataverse.wnba as wnba

    for fn in (
        "fox_wbb_pbp",
        "fox_wbb_boxscore",
        "fox_wbb_odds",
        "fox_wbb_team_roster",
        "fox_wbb_team_stats",
        "fox_wbb_team_gamelog",
        "fox_wbb_standings",
        "fox_wbb_league_leaders",
        "fox_wbb_teams",
        "fox_wbb_teams_all",
    ):
        assert callable(getattr(wbb, fn)), fn
    for fn in (
        "fox_wnba_pbp",
        "fox_wnba_boxscore",
        "fox_wnba_odds",
        "fox_wnba_team_roster",
        "fox_wnba_team_stats",
        "fox_wnba_team_gamelog",
        "fox_wnba_standings",
        "fox_wnba_league_leaders",
        "fox_wnba_teams",
    ):
        assert callable(getattr(wnba, fn)), fn
