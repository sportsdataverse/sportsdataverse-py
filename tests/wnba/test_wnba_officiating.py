import json
from pathlib import Path


from sportsdataverse.nba import nba_officiating as mod_nba
from sportsdataverse.wnba.wnba_officiating import wnba_referee_assignments

FIX = Path(__file__).parent.parent / "nba" / "fixtures" / "official_nba"


class _Resp:
    def __init__(self, status, body, ctype):
        self.status_code, self.text, self.headers = status, body, {"Content-Type": ctype}
        self.content = body.encode()

    def json(self):
        return json.loads(self.text)


def test_wnba_referee_assignments_contract(monkeypatch):
    """Test that wnba_referee_assignments returns WNBA games with correct league tag."""
    payload = json.loads((FIX / "referee_assignments_2026-06-13.json").read_text(encoding="utf-8"))

    def fake_official_get(url, *, params=None, proxy=None):
        return _Resp(200, json.dumps(payload), "application/json")

    monkeypatch.setattr(mod_nba, "_official_get", fake_official_get)
    result = wnba_referee_assignments("2026-06-13")
    officials = result["officials"]
    assert (officials["league"] == "wnba").all()
    assert officials["game_id"].n_unique() == 4
