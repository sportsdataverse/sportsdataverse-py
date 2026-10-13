"""The shared international basketball runtime spaces requests to one host at least MIN_INTERVAL apart."""

from __future__ import annotations

import pytest

from sportsdataverse.basketball import basketball_runtime as rt
from sportsdataverse.errors import AssetFetchError


@pytest.fixture
def clock(monkeypatch):
    """A fake monotonic clock: ``sleep`` advances it, so the test never waits."""
    state = {"now": 1000.0, "slept": []}
    monkeypatch.setattr(rt.time, "monotonic", lambda: state["now"])

    def sleep(s):
        state["slept"].append(s)
        state["now"] += s

    monkeypatch.setattr(rt.time, "sleep", sleep)
    monkeypatch.setattr(rt, "_last", {})
    return state


def test_second_call_to_a_host_waits_the_full_interval(clock):
    rt._polite_wait("https://fibalivestats.example/data/games")
    rt._polite_wait("https://fibalivestats.example/data/players")
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]


def test_other_hosts_do_not_wait(clock):
    rt._polite_wait("https://fibalivestats.example/data/games")
    rt._polite_wait("https://embed-api.example/v1/x")
    assert clock["slept"] == []


def test_no_wait_once_the_interval_has_passed(clock):
    rt._polite_wait("https://fibalivestats.example/a")
    clock["now"] += rt.MIN_INTERVAL
    rt._polite_wait("https://fibalivestats.example/b")
    assert clock["slept"] == []


def test_get_delegates_to_the_shared_getter_over_the_polite_session(monkeypatch):
    seen = {}
    monkeypatch.setattr(rt._rt, "_get", lambda url, params=None, **kw: seen.update(url=url, params=params, **kw) or {})
    rt._get("https://fibalivestats.example/data/games", {"season": 2026}, timeout=5)
    assert seen["url"] == "https://fibalivestats.example/data/games"
    assert seen["session"] is rt.SESSION


class _Resp:
    def __init__(self, status, body=b"{}"):
        self.status_code = status
        self.content = body
        self.text = body.decode()
        self.headers = {"Content-Type": "application/json"}
        self.url = "https://fibalivestats.example/data/games"
        self.reason = "x"

    def json(self):
        import json

        return json.loads(self.content)


def test_every_session_request_waits_its_turn(clock, monkeypatch):
    monkeypatch.setattr(rt.requests.Session, "request", lambda self, method, url, *a, **k: _Resp(200))
    rt.SESSION.get("https://fibalivestats.example/data/games")
    rt.SESSION.get("https://fibalivestats.example/data/players")
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]


def test_download_retries_are_throttled_too(clock, monkeypatch):
    """A retry inside ``download()`` goes through the session, so it is spaced and recorded."""
    answers = iter([_Resp(503), _Resp(200)])
    sent = []

    def fake(self, method, url, *a, **k):
        sent.append(clock["now"])
        return next(answers)

    monkeypatch.setattr(rt.requests.Session, "request", fake)
    assert rt._get("https://fibalivestats.example/data/games") == {}
    assert len(sent) == 2
    assert sent[1] - sent[0] >= rt.MIN_INTERVAL  # the retry waited for the host's slot
    assert rt._last["fibalivestats.example"] == pytest.approx(sent[1])  # and is the host's last request


def test_proxy_from_env_reads_the_named_variable_and_is_none_when_unset(monkeypatch):
    monkeypatch.delenv("SDV_TEST_PROXY", raising=False)
    assert rt.proxy_from_env("SDV_TEST_PROXY") is None
    monkeypatch.setenv("SDV_TEST_PROXY", "  ")
    assert rt.proxy_from_env("SDV_TEST_PROXY") is None
    monkeypatch.setenv("SDV_TEST_PROXY", "http://user:secret@proxy.example:8080")
    assert rt.proxy_from_env("SDV_TEST_PROXY") == {"http": "http://user:secret@proxy.example:8080"} | {
        "https": "http://user:secret@proxy.example:8080"
    }


def test_get_through_a_proxy_passes_it_and_never_names_the_url_in_an_error(monkeypatch):
    url = "http://user:secret@proxy.example:8080"
    monkeypatch.setenv("SDV_TEST_PROXY", url)
    seen = {}

    def boom(dl, target, **kw):
        seen.update(kw)
        raise AssetFetchError(f"cannot reach {url}") from ConnectionError(url)

    monkeypatch.setattr(rt._rt, "_download_json", boom)
    with pytest.raises(AssetFetchError) as err:
        rt._get("https://fibalivestats.example/data/1/data.json", {"a": None, "b": 1}, proxy_env="SDV_TEST_PROXY")
    assert seen["proxy"] == {"http": url, "https": url}
    assert seen["params"] == {"b": 1} and seen["session"] is rt.SESSION
    assert "secret" not in str(err.value) and "SDV_TEST_PROXY" in str(err.value) and "ConnectionError" in str(err.value)


def test_get_without_the_proxy_variable_set_goes_direct(monkeypatch):
    monkeypatch.delenv("SDV_TEST_PROXY", raising=False)
    seen = {}
    monkeypatch.setattr(rt._rt, "_get", lambda url, params=None, **kw: seen.update(kw) or {})
    rt._get("https://fibalivestats.example/data/1/data.json", proxy_env="SDV_TEST_PROXY")
    assert "proxy" not in seen and seen["session"] is rt.SESSION
