"""``fox_nfl_pbp`` row order and club column on real Fox captures (task 3.5: B-L57, B-L58). Zero network."""

from __future__ import annotations

import json
import pathlib

import pytest

import sportsdataverse.nfl.nfl_fox_ext as ext

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures" / "fox"


def _load(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def live_poll(monkeypatch):
    """An unmodified live poll of DET @ BUF (Fox 11052): Fox serves it newest-first."""
    raw = _load("fox_nfl_live_mid.json")
    monkeypatch.setattr(ext, "fox_get", lambda *a, **k: raw)
    return raw


def test_fox_nfl_pbp_is_sorted_oldest_first_on_a_live_poll(live_poll):
    feed_order = [int(p["id"]) for s in live_poll["pbp"]["sections"] for g in s["groups"] for p in g["plays"]]
    assert feed_order[0] > feed_order[-1], "the capture must be newest-first for this test to mean anything"
    ids = [int(i) for i in ext.fox_nfl_pbp("11052")["play_id"].to_list()]
    assert ids == sorted(feed_order)


def test_fox_nfl_pbp_play_team_is_a_club(live_poll):
    df = ext.fox_nfl_pbp("11052")
    assert set(df["play_team"].to_list()) == {"DET", "BUF"}
