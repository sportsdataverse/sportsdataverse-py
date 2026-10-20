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


def test_fox_nfl_pbp_play_team_is_null_when_neither_side_has_a_uri(monkeypatch):
    """A header team with no uri and a drive link with no uri must not match on ``None``.

    The real ARI @ SEA final (Fox 10625) with the away team's ``uri`` and the first drive's
    ``contentUri`` removed: that drive's rows get no club, every other row keeps its own.
    """
    raw = _load("fox_nfl_401671775.json")
    del raw["header"]["leftTeam"]["uri"]
    first = raw["pbp"]["sections"][0]["groups"][0]
    first["entityLink"] = {}
    monkeypatch.setattr(ext, "fox_get", lambda *a, **k: raw)
    df = ext.fox_nfl_pbp("10625")
    in_first = df["drive_id"] == first["id"]
    assert df.filter(in_first)["play_team"].null_count() == in_first.sum() > 0
    assert set(df.filter(~in_first)["play_team"].drop_nulls().to_list()) <= {"SEA"}
