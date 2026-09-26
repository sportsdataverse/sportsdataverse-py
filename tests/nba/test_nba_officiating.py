from pathlib import Path

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_officiating as mod

FIX = Path(__file__).parent / "fixtures" / "official_nba"


class _Resp:
    def __init__(self, status, body, ctype):
        self.status_code, self.text, self.headers = status, body, {"Content-Type": ctype}
        self.content = body.encode()

    def json(self):
        import json

        return json.loads(self.text)


def _patch(monkeypatch, resp):
    seen = {}

    def fake_download(url, **kw):
        seen.update(kw, url=url)
        return resp

    monkeypatch.setattr(mod, "download", fake_download)
    return seen


def test_browser_ua_and_no_403_retry(monkeypatch):
    seen = _patch(monkeypatch, _Resp(200, "{}", "application/json"))
    mod._official_get("https://official.nba.com/l2m/json/0042500405.json")
    assert "Mozilla/5.0" in seen["headers"]["User-Agent"]
    assert 403 not in seen["retry_statuses"]


def test_s3_xml_403_is_no_data(monkeypatch):
    body = (FIX / "l2m_json_0022500002_no_report_s3_403.xml").read_text()
    _patch(monkeypatch, _Resp(403, body, "application/xml"))
    with pytest.raises(NoDataError):
        mod._official_get("https://official.nba.com/l2m/json/0022500002.json")


def test_akamai_html_403_is_fetch_error(monkeypatch):
    body = (FIX / "akamai_403_blocked_ua.html").read_text()
    _patch(monkeypatch, _Resp(403, body, "text/html"))
    with pytest.raises(AssetFetchError):
        mod._official_get("https://official.nba.com/l2m/json/0042500405.json")


def test_errors_are_disjoint():
    assert not issubclass(NoDataError, AssetFetchError)
    assert not issubclass(AssetFetchError, NoDataError)
