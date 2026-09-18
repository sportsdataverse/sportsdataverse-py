"""Dispatch entry: ESPN today, fall-through order and provenance for the adapters that come later."""

from __future__ import annotations

import copy
from types import SimpleNamespace

import polars as pl
import pytest

from sportsdataverse.football.sources import dispatch
from sportsdataverse.football.sources.dispatch import (
    SOURCE_ORDER,
    AdaptedGame,
    AllSourcesFailed,
    ProcessedGame,
    SourceUnavailable,
    _fallthrough_order,
    _process_game,
)

from .conftest import CFB_GAME_ID, NFL_GAME_ID


def test_espn_offline_dispatch_runs_the_real_pipeline(nfl_processed: ProcessedGame):
    prov = nfl_processed.provenance
    assert nfl_processed.game["source"] is prov
    assert prov["requested"] == prov["served"] == "espn" and prov["fallback"] is False
    assert [a["source"] for a in prov["attempts"]] == ["espn"] and prov["attempts"][0]["ok"]
    assert prov["playByPlaySource"] == "full" and prov["native_ids"] == {"espn_event_id": str(NFL_GAME_ID)}
    assert prov["contract"]["ok"] and prov["contract"]["gop_ok"]
    assert prov["lossy_columns"] == []
    assert prov["odds"] == {"source": "summary_pickcenter", "default": False, "from_idmap": False}
    assert nfl_processed.health == {"espn": "ok"}
    assert len(nfl_processed.game["plays"]) > 100
    assert isinstance(nfl_processed.plays_frame, pl.DataFrame)
    assert nfl_processed.plays_frame is nfl_processed.processor.plays_frame
    # the offline path never joins participants (no roster / plays fan-out)
    assert nfl_processed.processor.join_participants is False


def test_cfb_offline_dispatch(cfb_summary, no_network):
    out = _process_game("cfb", CFB_GAME_ID, payloads={"espn": cfb_summary})
    assert out.provenance["served"] == "espn" and out.provenance["contract"]["ok"]
    assert out.game["gameId"] == CFB_GAME_ID and len(out.game["plays"]) > 100
    assert set(out.game["advBoxScore"]) >= {"pass", "rush", "receiver", "team"}


@pytest.mark.parametrize(
    ("league", "source", "expected"),
    [
        ("nfl", "espn", ("espn", "shield", "cbs", "yahoo", "fox")),
        ("nfl", "shield", ("shield", "cbs", "yahoo", "fox", "espn")),
        ("nfl", "cbs", ("cbs", "shield", "yahoo", "fox", "espn")),
        ("cfb", "espn", ("espn", "cbs", "yahoo", "ncaa", "fox")),
        ("cfb", "ncaa", ("ncaa", "cbs", "yahoo", "fox", "espn")),
    ],
)
def test_fallthrough_order(league, source, expected):
    assert _fallthrough_order(league, source) == expected
    assert _fallthrough_order(league, source, fallthrough=False) == (source,)
    assert set(expected) == set(SOURCE_ORDER[league])


def test_unknown_league_or_source():
    with pytest.raises(ValueError):
        _fallthrough_order("nhl", "espn")
    with pytest.raises(ValueError):
        _fallthrough_order("nfl", "ncaa")


@pytest.fixture
def fast_processor(monkeypatch):
    """Stub the pipeline so fall-through tests do not pay 8 s per attempt; the real run is covered above."""
    calls = []

    def _stub(league, espn_id, adapted):
        calls.append(adapted)
        frame = pl.DataFrame({"id": [1]})
        return SimpleNamespace(plays_frame=frame), {"plays": [{"id": 1}], "advBoxScore": {}}

    monkeypatch.setattr(dispatch, "_run_processor", _stub)
    return calls


def test_alternate_requested_falls_through_to_espn(nfl_summary, fast_processor, monkeypatch):
    def _shield_down(league, espn_id, ctx):
        raise SourceUnavailable("shield down")

    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "shield"), _shield_down)
    out = _process_game("nfl", NFL_GAME_ID, source="shield", payloads={"espn": nfl_summary})

    prov = out.provenance
    assert prov["requested"] == "shield" and prov["served"] == "espn" and prov["fallback"] is True
    assert [(a["source"], a["ok"]) for a in prov["attempts"]] == [
        ("shield", False),
        ("cbs", False),
        ("yahoo", False),
        ("fox", False),
        ("espn", True),
    ]
    assert out.health["shield"] == "SourceUnavailable: shield down"
    assert out.health["cbs"] == out.health["yahoo"] == out.health["fox"] == "not implemented"
    assert fast_processor[0].summary is nfl_summary  # ESPN consumed its injected payload


def test_contract_failure_is_a_fall_through_reason(nfl_summary, fast_processor, monkeypatch):
    gutted = copy.deepcopy(nfl_summary)
    for d in gutted["drives"]["previous"]:
        for p in d["plays"]:
            del p["statYardage"]

    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "cbs"), lambda league, espn_id, ctx: AdaptedGame(summary=gutted))
    out = _process_game("nfl", NFL_GAME_ID, source="cbs", payloads={"espn": nfl_summary})

    assert out.provenance["served"] == "espn"
    assert out.health["cbs"].startswith("contract: missing=['plays[].statYardage']")
    assert len(fast_processor) == 1  # the gutted summary never reached the processor


def test_alternate_adapter_provenance(nfl_summary, fast_processor, monkeypatch):
    adapted = copy.deepcopy(nfl_summary)
    adapted["header"]["competitions"][0]["playByPlaySource"] = "shield"
    seen = {}

    def _fake_shield(league, espn_id, ctx):
        seen["ctx"] = ctx
        return AdaptedGame(summary=adapted, native_ids={"shield_game_id": "abc"}, notes=["open drive synthesized"])

    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "shield"), _fake_shield)
    row = {"espn_event_id": str(NFL_GAME_ID), "shield_game_id": "abc"}
    out = _process_game("nfl", NFL_GAME_ID, source="shield", idmap_row=row, payloads={"shield": {"raw": 1}})

    prov = out.provenance
    assert prov["served"] == "shield" and prov["fallback"] is False
    assert prov["playByPlaySource"] == "shield" and prov["native_ids"] == {"shield_game_id": "abc"}
    assert prov["notes"] == ["open drive synthesized"] and prov["lossy_columns"] == []
    assert seen["ctx"].idmap_row is row and seen["ctx"].payload == {"raw": 1}


def test_processor_exception_falls_through(nfl_summary, monkeypatch):
    def _explode(league, espn_id, adapted):
        raise RuntimeError("boom")

    monkeypatch.setattr(dispatch, "_run_processor", _explode)
    with pytest.raises(AllSourcesFailed) as ei:
        _process_game("nfl", NFL_GAME_ID, payloads={"espn": nfl_summary})
    errs = {a.source: a.error for a in ei.value.attempts}
    assert errs["espn"] == "processor: RuntimeError: boom"
    # shield IS registered now, so it fails on its own terms (unmapped id, no payload) rather
    # than "not implemented"; cbs / yahoo / fox are still unregistered slots
    assert "SourceUnavailable" in errs["shield"] and len(errs) == 5
    assert [errs[s] for s in ("cbs", "yahoo", "fox")] == ["not implemented"] * 3


def test_no_fallthrough_raises_with_one_attempt():
    with pytest.raises(AllSourcesFailed) as ei:
        _process_game("cfb", CFB_GAME_ID, source="ncaa", fallthrough=False)
    assert [(a.source, a.error) for a in ei.value.attempts] == [("ncaa", "not implemented")]


def test_stored_closing_line_becomes_odds_override(nfl_summary, fast_processor):
    row = {
        "espn_event_id": str(NFL_GAME_ID),
        "spread_line": 8.5,
        "total_line": 40.5,
        "odds_source": "nflverse_schedule",
    }
    out = _process_game("nfl", NFL_GAME_ID, payloads={"espn": nfl_summary}, idmap_row=row)
    assert fast_processor[0].odds_override == {
        "gameSpread": 8.5,
        "overUnder": 40.5,
        "homeFavorite": True,
        "gameSpreadAvailable": True,
    }
    assert out.provenance["odds"]["from_idmap"] is True
    # an explicit odds_override wins over the stored line
    fast_processor.clear()
    explicit = {"gameSpread": 3.0, "overUnder": 44.0, "homeFavorite": False, "gameSpreadAvailable": True}
    _process_game("nfl", NFL_GAME_ID, payloads={"espn": nfl_summary}, idmap_row=row, odds_override=explicit)
    assert fast_processor[0].odds_override == explicit


def test_default_odds_are_flagged_in_provenance(nfl_summary, no_network):
    stripped = copy.deepcopy(nfl_summary)
    stripped["pickcenter"] = []
    out = _process_game("nfl", NFL_GAME_ID, payloads={"espn": stripped})
    assert out.provenance["odds"] == {"source": "default", "default": True, "from_idmap": False}


def test_adapter_returning_the_wrong_type_falls_through(nfl_summary, fast_processor, monkeypatch):
    """A misbehaving adapter is a fall-through reason, not an AttributeError out of dispatch.

    GOP maps :class:`AllSourcesFailed` to its 404; an exception escaping ``_process_game``
    is a 500 on the game page instead.
    """
    monkeypatch.setitem(dispatch._ADAPTERS, ("nfl", "shield"), lambda league, espn_id, ctx: None)
    out = _process_game("nfl", NFL_GAME_ID, source="shield", payloads={"espn": nfl_summary})
    assert out.provenance["served"] == "espn"
    assert out.health["shield"] == "SourceUnavailable: adapter returned NoneType, expected AdaptedGame"
