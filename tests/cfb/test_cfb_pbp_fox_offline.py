"""Offline tests for the FoxSports-sourced CFB play processor backup.

Runs a captured Fox ``cfb/event/{id}/data`` payload (Kent State @ Florida State,
2025-09-20, FSU 66-10) through the adapter + the unmodified ``CFBPlayProcess``,
asserting the full EPA/WPA/box pipeline completes and the output is
game-consistent. No network: the Fox fetch is monkeypatched with the fixture.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp_fox import fox_cfb_play_process, fox_to_espn_summary

FIX = Path(__file__).parent / "fixtures"


def _load_fox(name="fox_cfb_event_41616_data.json"):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def test_adapter_produces_espn_summary_shape():
    summary = fox_to_espn_summary(_load_fox())
    assert set(summary).issuperset({"header", "drives"})
    comp = summary["header"]["competitions"][0]
    assert comp["playByPlaySource"] == "full"
    sides = {c["homeAway"] for c in comp["competitors"]}
    assert sides == {"home", "away"}
    drives = summary["drives"]["previous"]
    assert len(drives) > 10
    nplays = sum(len(d["plays"]) for d in drives)
    assert nplays > 150
    # a real play carries the load-bearing structured fields
    p = drives[0]["plays"][1]
    assert p["start"]["yardsToEndzone"] is not None
    assert p["type"]["text"] in {"Rush", "Pass Reception", "Kickoff"}
    assert ":" in p["clock"]["displayValue"]  # MM:SS


def test_archive_format_raises():
    fox = _load_fox()
    # strip modalPlay from every play -> looks like an archive-format game
    for s in fox["pbp"]["sections"]:
        for g in s["groups"]:
            for play in g["plays"]:
                play.pop("modalPlay", None)
    with pytest.raises(ValueError, match="archive-format"):
        fox_to_espn_summary(fox)


def _patch_fetch(monkeypatch, fox):
    # _fox_get returns the parsed JSON dict directly (no Response wrapper).
    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp_fox._fox_get", lambda *a, **k: fox)


def test_fox_cfb_pbp_full_pipeline(monkeypatch):
    _patch_fetch(monkeypatch, _load_fox())
    out = fox_cfb_play_process(41616)
    assert out["source"] == "fox"
    plays = out["plays"]
    assert len(plays) > 150
    df = pl.DataFrame(plays, infer_schema_length=None)
    # EPA + WPA computed for every play; box score built
    assert "EPA" in df.columns and df["EPA"].is_not_null().all()
    assert "wpa" in df.columns and df["wpa"].is_not_null().all()
    assert "advBoxScore" in out
    # down/distance/possession populated
    assert df["down"].drop_nulls().is_in([1, 2, 3, 4]).all()
    # game-consistent: the home blowout winner (FSU, id 3) has higher EPA/play
    epa = df.filter(pl.col("scrimmage_play")).group_by("pos_team").agg(pl.col("EPA").mean().alias("epa"))
    by = dict(zip(epa["pos_team"].to_list(), epa["epa"].to_list()))
    assert len(by) == 2  # both teams have possessions
    assert by[3] > by[67]  # FSU (winner) out-EPAs Kent State


def test_fox_cfb_pbp_raw_returns_summary(monkeypatch):
    _patch_fetch(monkeypatch, _load_fox())
    summary = fox_cfb_play_process(41616, raw=True)
    assert summary["source"] == "fox"
    assert "drives" in summary and "header" in summary
    assert "plays" not in summary  # not processed


def test_fox_cfb_pbp_cleaning_pipeline(monkeypatch):
    _patch_fetch(monkeypatch, _load_fox())
    out = fox_cfb_play_process(41616, process=False)
    assert out["source"] == "fox"
    assert len(out["plays"]) > 150
    # cleaning path stops before EPA
    df = pl.DataFrame(out["plays"], infer_schema_length=None)
    assert "EPA" not in df.columns
