import json
from pathlib import Path
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


def test_injected_odds_bypasses_network():
    proc = CFBPlayProcess(
        gameId=401628455,
        odds_override={"gameSpread": -10.5, "overUnder": 60.0, "homeFavorite": True, "gameSpreadAvailable": True},
    )
    # Empty pickcenter would normally cascade to the LIVE core-odds endpoint.
    # With an override present, the helper must short-circuit and never look at pickcenter
    # or hit the network.
    proc._CFBPlayProcess__helper_cfb_pickcenter({"pickcenter": []})
    assert proc.gameSpread == -10.5
    assert proc.overUnder == 60.0
    assert proc.homeFavorite is True
    assert proc.gameSpreadAvailable is True
    assert proc.odds_source == "injected"
