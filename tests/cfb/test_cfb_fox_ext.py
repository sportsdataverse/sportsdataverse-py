"""``fox_cfb_pbp`` on real Fox captures (task 3.5: B-L57, B-L58, B-L63)."""

from __future__ import annotations

import json
import pathlib

import sportsdataverse.cfb.cfb_fox_ext as ext

HERE = pathlib.Path(__file__).resolve().parent


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
