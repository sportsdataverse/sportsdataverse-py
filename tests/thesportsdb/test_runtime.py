"""TheSportsDB runtime: the API key is a path segment, not a query parameter."""

from sportsdataverse.thesportsdb import thesportsdb_runtime as rt

URL = "https://www.thesportsdb.com/api/v1/json/{key}/all_sports.php"


def _capture(monkeypatch):
    seen = {}

    def fake(url, params=None, **kw):
        seen["url"] = url
        return {}

    monkeypatch.setattr(rt._rt, "_get", fake)
    return seen


def test_default_key_is_the_free_test_key(monkeypatch):
    monkeypatch.delenv("THESPORTSDB_API_KEY", raising=False)
    seen = _capture(monkeypatch)
    rt._get(URL)
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"


def test_env_key_replaces_the_placeholder(monkeypatch):
    monkeypatch.setenv("THESPORTSDB_API_KEY", "abc123")
    seen = _capture(monkeypatch)
    rt._get(URL)
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/abc123/all_sports.php"


def test_a_url_without_the_placeholder_is_untouched(monkeypatch):
    monkeypatch.setenv("THESPORTSDB_API_KEY", "abc123")
    seen = _capture(monkeypatch)
    rt._get("https://www.thesportsdb.com/api/v1/json/3/all_sports.php")
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"


def test_an_empty_env_key_falls_back_to_the_free_key(monkeypatch):
    monkeypatch.setenv("THESPORTSDB_API_KEY", "")
    seen = _capture(monkeypatch)
    rt._get(URL)
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"
