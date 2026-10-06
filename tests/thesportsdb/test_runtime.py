"""TheSportsDB runtime: the API key is a path segment, not a query parameter."""

import pytest

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


def test_the_free_key_segment_is_swapped_for_the_callers_key(monkeypatch):
    """The endpoint YAML pins the free key in its host so the docs links work, so the
    runtime must recognise that segment as well as a literal ``{key}`` placeholder."""
    monkeypatch.setenv("THESPORTSDB_API_KEY", "abc123")
    seen = _capture(monkeypatch)
    rt._get("https://www.thesportsdb.com/api/v1/json/3/all_sports.php")
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/abc123/all_sports.php"


def test_a_url_carrying_some_other_key_is_left_alone(monkeypatch):
    """Only the free-key segment and the placeholder are rewritten; a hand-built URL with
    its own key is the caller's business."""
    monkeypatch.setenv("THESPORTSDB_API_KEY", "abc123")
    seen = _capture(monkeypatch)
    rt._get("https://www.thesportsdb.com/api/v1/json/someoneelseskey/all_sports.php")
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/someoneelseskey/all_sports.php"


def test_the_free_key_survives_when_no_env_key_is_set(monkeypatch):
    monkeypatch.delenv("THESPORTSDB_API_KEY", raising=False)
    seen = _capture(monkeypatch)
    rt._get("https://www.thesportsdb.com/api/v1/json/3/all_sports.php")
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"


def test_an_empty_env_key_falls_back_to_the_free_key(monkeypatch):
    monkeypatch.setenv("THESPORTSDB_API_KEY", "")
    seen = _capture(monkeypatch)
    rt._get(URL)
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"


def test_a_whitespace_env_key_falls_back_to_the_free_key(monkeypatch):
    """A key pasted with a trailing newline would otherwise build /json/%20/ and burn the retry budget."""
    monkeypatch.setenv("THESPORTSDB_API_KEY", "  \n")
    seen = _capture(monkeypatch)
    rt._get(URL)
    assert seen["url"] == "https://www.thesportsdb.com/api/v1/json/3/all_sports.php"


def test_a_browser_user_agent_is_sent(monkeypatch):
    """A bare python-requests UA is a routine 403 trigger, and a 403 is retried 15x first."""
    seen = {}

    def fake(url, params=None, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(rt._rt, "_get", fake)
    rt._get(URL)
    assert "User-Agent" in seen.get("headers", {})
    assert "sportsdataverse" in seen["headers"]["User-Agent"]


def test_a_caller_supplied_header_wins(monkeypatch):
    seen = {}

    def fake(url, params=None, **kw):
        seen.update(kw)
        return {}

    monkeypatch.setattr(rt._rt, "_get", fake)
    rt._get(URL, headers={"User-Agent": "mine"})
    assert seen["headers"]["User-Agent"] == "mine"


def test_a_404_raises_the_package_vocabulary_through_this_runtime(monkeypatch):
    """`_VOCAB_GETTERS` only appends Raises: lines to the generated docstrings -- nothing
    verifies the claim. This exercises the real shared body helpers, not a stubbed _get."""
    import json as _json

    from sportsdataverse import _codegen_runtime as crt
    from sportsdataverse.errors import NoDataError

    class _Resp:
        status_code = 404
        headers = {"content-type": "application/json"}
        text = '{"error": "not found"}'
        url = "https://www.thesportsdb.com/api/v1/json/3/lookupevent.php"

        def json(self):
            return _json.loads(self.text)

    monkeypatch.setattr(crt, "download", lambda **kw: _Resp())
    with pytest.raises(NoDataError):
        rt._get("https://www.thesportsdb.com/api/v1/json/{key}/lookupevent.php", {"id": "0"})
