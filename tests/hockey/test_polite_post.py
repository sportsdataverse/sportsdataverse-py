"""Every attempt (retries and re-primes included) starts >= ``MIN_INTERVAL`` after the host's last ANSWER.

Owner ruling 2026-10-18: the gap is measured from the end of one response to the start of the next
request. The family tests stub ``_polite_wait`` and check it was *called*; here the real throttle runs
on a fake monotonic clock, and every fake request takes :data:`SLOW` seconds, so a path that spaced
request starts (or skipped the throttle) would show a gap under ``MIN_INTERVAL``. No network.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from sportsdataverse.hockey import euro_hockey_runtime as rt
from sportsdataverse.hockey.ha import ha_runtime as HA
from sportsdataverse.hockey.khl import khl_runtime as KHL

SID = "0123456789abcdef0123456789abcdef"
FIXTURES = Path(__file__).parents[1] / "fixtures" / "khl_rest"
OK = json.loads((FIXTURES / "game__header.json").read_text(encoding="utf-8"))
SLOW = 0.8  # seconds every fake request takes: an answer that slow used to let the next one go 0.2 s later


class _Resp:
    def __init__(self, status: int = 200, body: Any = None, text: str | None = None) -> None:
        self.status_code, self.headers = status, {}
        self.text = text if text is not None else json.dumps(body)

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Dict[str, float]:
    """A fake monotonic clock shared by ``_polite_wait`` and every backoff ``sleep``."""
    state = {"now": 1000.0}
    monkeypatch.setattr(rt.time, "monotonic", lambda: state["now"])  # one ``time`` module for every runtime
    monkeypatch.setattr(rt.time, "sleep", lambda s: state.__setitem__("now", state["now"] + s))
    monkeypatch.setattr(rt, "_last", {})
    monkeypatch.setattr(rt, "_host_locks", {})
    monkeypatch.setattr(KHL, "_sessions", {})
    return state


def _slow(state: Dict[str, float], seen: List[Tuple[float, float]], answer: Any) -> Any:
    """One fake request: note its start, take :data:`SLOW` seconds, note its end, answer (or raise)."""
    start = state["now"]
    state["now"] += SLOW
    seen.append((start, state["now"]))
    if isinstance(answer, BaseException):
        raise answer
    return answer


def _gaps(spans: List[Tuple[float, float]]) -> List[float]:
    """End of each request -> start of the next."""
    return [round(b[0] - a[1], 6) for a, b in zip(spans, spans[1:])]


def test_khl_every_request_is_spaced_prime_re_prime_retry_and_next_call(clock, monkeypatch):
    seen: List[Tuple[float, float]] = []
    posts: List[Any] = [_Resp(307, text=""), _Resp(503, text="busy"), OSError("reset"), _Resp(200, OK), _Resp(200, OK)]

    class Http:
        def get(self, url: str, timeout: Any = None) -> _Resp:
            return _slow(clock, seen, _Resp(text=f'"bitrix_sessid":"{SID}"'))

        def post(self, url: str, **kw: Any) -> _Resp:
            return _slow(clock, seen, posts.pop(0))

    monkeypatch.setattr(KHL, "_new_http", lambda: Http())
    route = "https://www.khl.ru/rest/game/header/"
    form = {"page": "resume", "values[gameid]": "898236", "values[tournament]": "1369"}
    KHL._get(route, form)  # GET prime, POST 307, GET re-prime, POST 503, POST dead connection, POST 200
    KHL._get(route, form)  # POST 200 on the reused session
    assert len(seen) == 7 and not posts
    assert min(_gaps(seen)) >= rt.MIN_INTERVAL  # after the 307, the 503 and the exception alike


def test_ha_post_retries_are_spaced(clock, monkeypatch):
    seen: List[Tuple[float, float]] = []
    answers: List[Any] = [_Resp(503, text=""), OSError("reset"), _Resp(200, {"ok": 1})]

    class Session:
        def post(self, url: str, **kw: Any) -> _Resp:
            return _slow(clock, seen, answers.pop(0))

    monkeypatch.setattr(HA, "_SESSION", Session())
    resp = HA._post("https://hockeyallsvenskan.se/api/x", json={})
    answers.append(_Resp(200, {}))
    HA._post("https://hockeyallsvenskan.se/api/y", json={})  # the next call waits for the last answer too
    assert resp.status_code == 200 and len(seen) == 4
    assert min(_gaps(seen)) >= rt.MIN_INTERVAL


def test_the_gap_runs_from_the_end_of_a_slow_answer(clock):
    seen: List[Tuple[float, float]] = []
    for path in ("a", "b", "c"):
        with rt._polite(f"https://www.khl.ru/{path}"):
            _slow(clock, seen, None)
    assert _gaps(seen) == [rt.MIN_INTERVAL, rt.MIN_INTERVAL]  # not MIN_INTERVAL - SLOW


def test_an_exception_still_records_the_finish(clock):
    seen: List[Tuple[float, float]] = []
    with pytest.raises(OSError):
        with rt._polite("https://www.khl.ru/a"):
            _slow(clock, seen, OSError("reset"))
    with rt._polite("https://www.khl.ru/b"):
        _slow(clock, seen, None)
    assert _gaps(seen) == [rt.MIN_INTERVAL]


def test_the_polite_session_and_get_text_transport_space_from_the_answer(clock, monkeypatch):
    seen: List[Tuple[float, float]] = []
    monkeypatch.setattr(rt.requests.Session, "request", lambda self, m, url, *a, **k: _slow(clock, seen, _Resp()))
    rt.SESSION.get("https://liiga.fi/a")
    page = _Resp(text="<html>x</html>")
    page.headers, page.url, page.content = {"content-type": "text/html"}, "https://liiga.fi/b", b"<html>x</html>"
    rt.get_text("https://liiga.fi/b", transport=lambda url: _slow(clock, seen, page))
    rt.SESSION.get("https://liiga.fi/c")
    assert len(seen) == 3 and min(_gaps(seen)) >= rt.MIN_INTERVAL


def test_two_threads_on_one_host_never_overlap_and_keep_the_gap(clock):
    seen: List[Tuple[float, float]] = []

    def worker(path: str) -> None:
        for i in range(3):
            with rt._polite(f"https://www.khl.ru/{path}{i}"):
                _slow(clock, seen, None)

    threads = [threading.Thread(target=worker, args=(p,)) for p in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(5)
    spans = sorted(seen)
    assert len(spans) == 6 and min(_gaps(spans)) >= rt.MIN_INTERVAL


def test_other_hosts_are_not_held_up_by_a_request_in_flight(clock):
    started, release, done = threading.Event(), threading.Event(), threading.Event()

    def slow_khl() -> None:
        with rt._polite("https://www.khl.ru/a"):
            started.set()
            release.wait(5)

    t = threading.Thread(target=slow_khl)
    t.start()
    assert started.wait(5)
    with rt._polite("https://liiga.fi/a"):  # khl.ru is mid-request; liiga.fi goes at once
        done.set()
    release.set()
    t.join(5)
    assert done.is_set()
