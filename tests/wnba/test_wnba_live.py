import json
from pathlib import Path

from sportsdataverse.nba import nba_live as nba_live_mod
from sportsdataverse.wnba.wnba_live import wnba_live_boxscore, wnba_live_pbp

FIX = Path(__file__).parent / "fixtures" / "wnba_live"
PBP_PAYLOAD = json.loads((FIX / "playbyplay_1022600097.json").read_text())
BOX_PAYLOAD = json.loads((FIX / "boxscore_1022600097.json").read_text())


def _fake_transport(status, text):
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen["url"], seen["headers"] = url, headers
        return status, text

    return transport, seen


def test_wnba_live_pbp_hits_wnba_host_and_parses_fixture(monkeypatch):
    transport, seen = _fake_transport(200, json.dumps(PBP_PAYLOAD))
    monkeypatch.setattr(nba_live_mod, "_curl_transport", transport)
    df = wnba_live_pbp("1022600097")
    assert "cdn.wnba.com" in seen["url"]
    assert "cdn.nba.com" not in seen["url"]
    assert seen["headers"]["Origin"] == "https://www.wnba.com"
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])


def test_wnba_live_boxscore_hits_wnba_host_and_parses_fixture(monkeypatch):
    transport, seen = _fake_transport(200, json.dumps(BOX_PAYLOAD))
    monkeypatch.setattr(nba_live_mod, "_curl_transport", transport)
    result = wnba_live_boxscore("1022600097")
    assert "cdn.wnba.com" in seen["url"]
    officials = result["officials"]
    assert officials.height >= 3
    assert officials.schema["person_id"].__str__() == "Int64"
