import json
from pathlib import Path

import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


def _load_summary(name="summary_401628455.json"):
    return json.loads((FIX / name).read_text())


def test_raw_allowlist_includes_injuries_and_gamenotes(monkeypatch):
    summary = _load_summary()
    summary["injuries"] = [{"team": {"id": "333"}, "injuries": []}]
    summary["gameNotes"] = [{"type": "note", "headline": "Week 1"}]

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    raw = CFBPlayProcess(gameId=401628455, raw=True).espn_cfb_pbp()
    assert "injuries" in raw and raw["injuries"], "injuries dropped by raw allowlist"
    assert "gameNotes" in raw and raw["gameNotes"], "gameNotes dropped by raw allowlist"


def test_odds_source_tag_summary_path():
    proc = CFBPlayProcess(gameId=401628455)
    pbp_txt = {
        "pickcenter": [
            {"provider": {"id": "58"}, "spread": -7.5, "overUnder": 52.5, "homeTeamOdds": {"favorite": True}},
            {"provider": {"id": "1002"}, "spread": -7.0, "overUnder": 52.0, "homeTeamOdds": {"favorite": True}},
        ],
    }
    proc._CFBPlayProcess__helper_cfb_pickcenter(pbp_txt)
    assert proc.odds_source == "summary_pickcenter"
    assert proc.gameSpreadAvailable is True


def test_injected_odds_bypasses_network(monkeypatch):
    proc = CFBPlayProcess(
        gameId=401628455,
        odds_override={"gameSpread": -10.5, "overUnder": 60.0, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    # If the override path regressed into the live cascade, this would raise.
    # The method uses dunder-both-sides naming so no Python name mangling applies;
    # patch the class to cover all instances.
    monkeypatch.setattr(
        CFBPlayProcess,
        "__helper__espn_cfb_odds_information__",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("live odds endpoint must not be called")),
    )
    proc._CFBPlayProcess__helper_cfb_pickcenter({"pickcenter": []})
    assert proc.gameSpread == -10.5
    assert proc.overUnder == 60.0
    assert proc.odds_source == "injected"


def test_join_participants_constructor_arg():
    """``join_participants`` is accepted as a constructor arg, not only as a
    post-construction attribute. ``CFBPlayProcess(gameId=..., join_participants=False)``
    selects the fetch-free fast path (skips the participants join + roster fetch)
    at construction. Previously the kwarg was swallowed by ``**kwargs`` and the
    pipeline read it via ``getattr(self, "join_participants", True)``, so passing
    it to the constructor silently no-op'd."""
    assert CFBPlayProcess(gameId=1).join_participants is True  # default: lookups on
    assert CFBPlayProcess(gameId=1, join_participants=False).join_participants is False
    assert CFBPlayProcess(gameId=1, join_participants=True).join_participants is True


def test_odds_override_validation():
    with pytest.raises(ValueError):
        CFBPlayProcess(gameId=1, odds_override={"gameSpread": -3.5})  # missing keys
    with pytest.raises(ValueError):
        CFBPlayProcess(gameId=1, odds_override=[1, 2, 3])  # not a dict
