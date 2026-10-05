"""The league registry the docs site reads (sidebar, home grid, search contexts) covers every documented league."""

from __future__ import annotations

import json

from tools.codegen import generate


def test_every_documented_league_is_listed_once_under_a_known_sport():
    registry = json.loads(generate.render_leagues_json())
    assert [s["key"] for s in registry["sports"]] == ["football", "basketball", "hockey", "baseball", "soccer", "other"]
    prefixes = [lg["prefix"] for s in registry["sports"] for lg in s["leagues"]]
    assert sorted(prefixes) == sorted(generate._doc_leagues())
    assert len(prefixes) == len(set(prefixes))


def test_registry_sports_and_labels():
    registry = json.loads(generate.render_leagues_json())
    by_sport = {s["key"]: {lg["prefix"]: lg["label"] for lg in s["leagues"]} for s in registry["sports"]}
    assert by_sport["football"]["nfl"] == "NFL"
    assert by_sport["hockey"]["pwhl"] == "PWHL"
    assert "ahl" in by_sport["hockey"]
    assert by_sport["soccer"]["laliga"] == "LaLiga"
    assert by_sport["other"] == {
        "cricket": "Cricket",
        "odds": "Betting odds",
        "cbs": "CBS Sports",
        "yahoo": "Yahoo Sports",
    }
