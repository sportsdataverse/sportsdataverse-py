"""The shared European hockey runtime spaces requests to one host at least MIN_INTERVAL apart."""

from __future__ import annotations

import pytest

from sportsdataverse.hockey import euro_hockey_runtime as rt


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
    rt._polite_wait("https://liiga.fi/api/v2/games")
    rt._polite_wait("https://liiga.fi/api/v2/players")
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]


def test_other_hosts_do_not_wait(clock):
    rt._polite_wait("https://liiga.fi/api/v2/games")
    rt._polite_wait("https://www.shl.se/api/x")
    assert clock["slept"] == []


def test_no_wait_once_the_interval_has_passed(clock):
    rt._polite_wait("https://liiga.fi/a")
    clock["now"] += rt.MIN_INTERVAL
    rt._polite_wait("https://liiga.fi/b")
    assert clock["slept"] == []


def test_get_throttles_then_delegates_to_the_shared_getter(clock, monkeypatch):
    seen = {}
    monkeypatch.setattr(rt._rt, "_get", lambda url, params=None, **kw: seen.update(url=url, params=params, **kw) or {})
    rt._get("https://liiga.fi/api/v2/games", {"season": 2026}, timeout=5)
    rt._get("https://liiga.fi/api/v2/games")
    assert seen["url"] == "https://liiga.fi/api/v2/games"
    assert clock["slept"] == [pytest.approx(rt.MIN_INTERVAL)]
