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

# Processing a default-odds capture warns DefaultOddsWarning incidentally;
# tests/cfb/test_cfb_pbp_offline.py::test_default_odds_warn asserts it.
pytestmark = pytest.mark.filterwarnings("ignore::sportsdataverse.errors.DefaultOddsWarning")

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


def test_fox_without_odds_has_no_market_total(monkeypatch):
    """R895-6: with no odds the Fox path runs on the neutral pick'em line, whose 55.5 total is the
    default: ``overUnderAvailable`` False in the payload and on every play, not null (unknown)."""
    _patch_fetch(monkeypatch, _load_fox())
    out = fox_cfb_play_process(41616)
    assert out["gameSpreadAvailable"] is False
    assert out["overUnderAvailable"] is False
    flags = pl.DataFrame(out["plays"], infer_schema_length=None)["overUnderAvailable"]
    assert flags.unique().to_list() == [False]


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


# --------------------------------------------------------------------------------------
# Field position against Fox's own geometry (B-M37). ``yardStart`` is an ABSOLUTE 0-100 spot
# measured from the AWAY goal line (football/fox_common.py, the Game on Paper path), so the
# yards to the end zone are ``spot`` when the home team has the ball and ``100 - spot`` when
# the away team does. Possession is the drive group's team (``entityLink``), never the play.
# --------------------------------------------------------------------------------------

_SPECIAL = ("Kickoff", "Punt", "Field Goal", "Extra Point", "Two-Point", "Timeout", "End ", "Penalty")


# The two committed real captures: KENT @ FSU (full) and OKLA @ MICH (trimmed: the drive groups
# carry only ``entityLink.contentUri`` and the plays no team logo, so the side must come from it).
_CAPTURES = ("fox_cfb_event_41616_data.json", "fox/fox_cfb_401856679.json")


def _fox_team_id(entity):
    import re

    uri = entity.get("uri") or entity.get("contentUri") or (entity.get("entityLink") or {}).get("contentUri") or ""
    m = re.search(r"/teams/(\d+)", uri)
    return m.group(1) if m else None


def _snaps_by_side(name):
    from sportsdataverse.football.fox_common import _pos

    fox = _load_fox(name)
    head = fox["header"]
    sides = {_fox_team_id(head["leftTeam"]): "away", _fox_team_id(head["rightTeam"]): "home"}  # Fox: left = away
    emitted = [p for d in fox_to_espn_summary(fox)["drives"]["previous"] for p in d["plays"]]
    raw = []
    for section in fox["pbp"]["sections"]:
        for group in section.get("groups") or []:
            side = sides.get(_fox_team_id(group.get("entityLink") or {}))
            raw.extend((side, play) for play in group.get("plays") or [])
    assert len(raw) == len(emitted)  # the adapter emits one play per Fox play, in order
    out = {"home": [], "away": []}
    for (side, play), espn in zip(raw, emitted):
        events = ((play.get("modalPlay") or {}).get("play") or {}).get("events") or []
        spot = _pos(events[0].get("yardStart")) if events else None
        if side is None or spot is None or any(s in espn["type"]["text"] for s in _SPECIAL):
            continue
        want = spot if side == "home" else 100 - spot
        out[side].append((espn["text"], espn["start"]["yardsToEndzone"], want))
    return out


@pytest.mark.parametrize("name", _CAPTURES)
def test_home_snap_field_position_matches_fox_geometry(name):
    home = _snaps_by_side(name)["home"]
    assert len(home) > 40
    wrong = [(text, got, want) for text, got, want in home if got != want]
    assert not wrong, f"{len(wrong)} of {len(home)} home snaps mirrored, e.g. {wrong[:2]}"


@pytest.mark.parametrize("name", _CAPTURES)
def test_away_snap_field_position_matches_fox_geometry(name):
    away = _snaps_by_side(name)["away"]
    assert len(away) > 40
    wrong = [(text, got, want) for text, got, want in away if got != want]
    assert not wrong, f"{len(wrong)} of {len(away)} away snaps mirrored, e.g. {wrong[:2]}"


def test_no_coverage_final_header_is_not_a_completed_game(monkeypatch):
    """Fox answers an uncovered game with a FINAL header and ``pbp.sections == []`` (B-M6).

    The fetch succeeded and the answer is "nothing here", so the backup raises ``NoDataError``
    (never a completed 0-play game); the adapter alone warns and does not report it final.
    """
    from sportsdataverse.errors import NoDataError

    fox = _load_fox("fox/fox_cfb_no_coverage.json")
    assert fox["header"]["eventStatus"] == 3 and fox["pbp"]["sections"] == []  # the real shape
    _patch_fetch(monkeypatch, fox)
    with pytest.raises(NoDataError, match="no play-by-play"):
        fox_cfb_play_process(int(fox["header"]["id"]))
    with pytest.warns(UserWarning, match="no play-by-play"):
        summary = fox_to_espn_summary(fox)
    assert summary["drives"]["previous"] == []
    assert summary["header"]["competitions"][0]["status"]["type"]["completed"] is False


def test_backup_makes_no_espn_request(monkeypatch):
    """The ESPN-outage backup never reaches ESPN: no participants, roster or odds fetch (B-M28)."""
    import requests

    calls = []

    def _record(self, method, url, *a, **k):
        calls.append(url)
        raise requests.exceptions.ConnectionError("network blocked in this test")

    monkeypatch.setattr(requests.Session, "request", _record)
    # download() backs off between retries; skip the waits so a regression fails fast
    monkeypatch.setattr("sportsdataverse.dl_utils._retry_delay", lambda *a, **k: 0)
    _patch_fetch(monkeypatch, _load_fox())
    out = fox_cfb_play_process(41616)
    assert len(out["plays"]) > 150
    assert calls == []
