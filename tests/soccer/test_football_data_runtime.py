"""Football-Data.co.uk runtime: CSV bodies come back as text, not {}."""

import json

import pytest

from sportsdataverse.soccer import football_data_runtime as rt


class _Resp:
    """The attributes _codegen_runtime's body helpers actually touch."""

    def __init__(self, ctype, text):
        self.status_code = 200
        self.headers = {"content-type": ctype}
        self.text = text
        self.content = text.encode()
        self.url = "https://www.football-data.co.uk/fixtures.csv"

    def json(self):
        return json.loads(self.text)


def _resp(ctype, text):
    return _Resp(ctype, text)


def test_csv_body_returns_text(monkeypatch):
    csv = "Div,Date,HomeTeam\nE0,16/08/2025,Liverpool\n"
    monkeypatch.setattr(rt, "download", lambda **kw: _resp("text/csv", csv))
    assert rt._get("https://www.football-data.co.uk/fixtures.csv") == csv


def test_plain_text_body_returns_text(monkeypatch):
    notes = "Div = League Division\n"
    monkeypatch.setattr(rt, "download", lambda **kw: _resp("text/plain", notes))
    assert rt._get("https://www.football-data.co.uk/notes.txt") == notes


def test_json_body_still_parses(monkeypatch):
    monkeypatch.setattr(rt, "download", lambda **kw: _resp("application/json", '{"ok": true}'))
    assert rt._get("https://www.football-data.co.uk/x.json") == {"ok": True}


def test_none_params_are_dropped(monkeypatch):
    seen = {}

    def fake(**kw):
        seen.update(kw)
        return _resp("text/csv", "Div\nE0\n")

    monkeypatch.setattr(rt, "download", fake)
    rt._get("https://www.football-data.co.uk/fixtures.csv", {"a": 1, "b": None})
    assert seen["params"] == {"a": 1}


def test_an_html_body_is_a_failed_fetch_not_data(monkeypatch):
    """A 200 error page must not reach the CSV parser.

    football-data.co.uk is a static archive on shared hosting, so the realistic failure is a
    200 interstitial rather than a 404. Passed through as text it parses to a 2x1 frame whose
    one column is ``<!doctype html>`` -- non-empty, no exception, no warning.
    """
    from sportsdataverse.errors import AssetFetchError

    html = "<!DOCTYPE html>\n<html><head><title>404 Not Found</title></head><body>nope</body></html>"
    monkeypatch.setattr(rt, "download", lambda **kw: _resp("text/html; charset=UTF-8", html))
    with pytest.raises(AssetFetchError):
        rt._get("https://www.football-data.co.uk/mmz4281/9999/ZZ.csv")
