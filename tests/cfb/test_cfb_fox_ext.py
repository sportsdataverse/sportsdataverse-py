"""``fox_cfb_pbp`` / CFB segment enumeration on real Fox captures (task 3.5: B-L54, B-L57, B-L58, B-L63)."""

from __future__ import annotations

import json
import pathlib

import pytest

import sportsdataverse.cfb.cfb_fox_ext as ext

HERE = pathlib.Path(__file__).resolve().parent
FOX = HERE / "fixtures" / "fox"


def _load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_fox_cfb_pbp_is_sorted_oldest_first(monkeypatch):
    """No CFB live poll is committed, so the NFL one stands in: the same Bifrost drive layout,
    served newest-first until FINAL (DET @ BUF, Fox 11052)."""
    raw = _load(HERE.parent / "nfl" / "fixtures" / "fox" / "fox_nfl_live_mid.json")
    monkeypatch.setattr(ext, "_fox_get", lambda *a, **k: raw)
    ids = [int(i) for i in ext.fox_cfb_pbp("11052")["play_id"].to_list()]
    assert ids[0] > 0 and ids == sorted(ids)
    assert ids[0] == min(int(p["id"]) for s in raw["pbp"]["sections"] for g in s["groups"] for p in g["plays"])


def test_fox_cfb_pbp_play_team_is_a_club_and_the_athlete_has_its_own_column(monkeypatch):
    """Kent State @ Florida State (Fox 41616): a play's entity link is the credited ATHLETE."""
    raw = _load(HERE / "fixtures" / "fox_cfb_event_41616_data.json")
    monkeypatch.setattr(ext, "_fox_get", lambda *a, **k: raw)
    df = ext.fox_cfb_pbp("41616")
    assert set(df["play_team"].to_list()) == {"KENT", "FSU"}
    first = next(p for s in raw["pbp"]["sections"] for g in s["groups"] for p in g["plays"] if p["id"] == "1")
    assert first["entityLink"]["contentType"] == "athlete"
    row = df.filter(df["play_id"] == "1").row(0, named=True)
    assert row["play_player"] == first["entityLink"]["title"]
    assert row["play_team"] != row["play_player"]


def test_the_bowls_segment_carries_every_cfp_game():
    """2025 postseason, captured 2026-10-19: all 11 CFP games are in the bowls segment, which is
    why a failed ``cfp`` group fetch may be dropped (after a retry and a warning)."""
    bowls = {r["game_id"] for r in ext._parse_fox_segment(_load(FOX / "fox_cfb_segment_2025_bowls.json"), "b")}
    cfp = {r["game_id"] for r in ext._parse_fox_segment(_load(FOX / "fox_cfb_segment_2025_cfp.json"), "c")}
    assert len(cfp) == 11
    assert cfp <= bowls


def _main_with(cfp_failures, exc=ConnectionError("fox down")):
    main = {"selectionGroupList": [{"selectionList": [{"id": "2025-1-1"}, {"id": "2025-bowls-2"}]}]}
    cfp_main = {"selectionGroupList": [{"selectionList": [{"id": "2025-cfp-2"}]}]}
    calls = []

    def fake_get(path, params=None, **k):
        if (params or {}).get("groupId") == "cfp":
            calls.append("cfp")
            if len(calls) <= cfp_failures:
                raise exc
            return cfp_main
        return main

    return fake_get, calls


def test_a_failed_cfp_group_fetch_is_retried_once(monkeypatch, recwarn):
    fake_get, calls = _main_with(cfp_failures=1)
    monkeypatch.setattr(ext, "_fox_get", fake_get)
    assert ext._fox_segment_ids(2025, "2") == ["2025-1-1", "2025-bowls-2", "2025-cfp-2"]
    assert calls == ["cfp", "cfp"]
    assert not recwarn.list


def test_a_cfp_group_that_fails_twice_warns(monkeypatch):
    fake_get, calls = _main_with(cfp_failures=2)
    monkeypatch.setattr(ext, "_fox_get", fake_get)
    with pytest.warns(UserWarning, match="CFP group enumeration failed twice"):
        assert ext._fox_segment_ids(2025, "2") == ["2025-1-1", "2025-bowls-2"]
    assert calls == ["cfp", "cfp"]


def test_a_successful_cfp_group_fetch_is_made_once(monkeypatch):
    fake_get, calls = _main_with(cfp_failures=0)
    monkeypatch.setattr(ext, "_fox_get", fake_get)
    assert ext._fox_segment_ids(2025, "2") == ["2025-1-1", "2025-bowls-2", "2025-cfp-2"]
    assert calls == ["cfp"]


def test_a_404_cfp_group_is_an_answer_not_retried_or_warned(monkeypatch, recwarn):
    from sportsdataverse.errors import NoDataError

    fake_get, calls = _main_with(cfp_failures=2, exc=NoDataError("404"))
    monkeypatch.setattr(ext, "_fox_get", fake_get)
    assert ext._fox_segment_ids(2025, "2") == ["2025-1-1", "2025-bowls-2"]
    assert calls == ["cfp"]
    assert not recwarn.list


def test_the_cfp_warning_points_at_the_caller(monkeypatch):
    fake_get, _ = _main_with(cfp_failures=2)
    monkeypatch.setattr(
        ext, "_fox_get", lambda path, params=None, **k: {} if "segment" in path else fake_get(path, params)
    )
    with pytest.warns(UserWarning, match="CFP group enumeration failed twice") as record:
        ext.fox_cfb_schedule(2025)
    assert record[0].filename == __file__
