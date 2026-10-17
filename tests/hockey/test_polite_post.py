"""Every POST attempt (retries and re-primes included) waits its turn on the real throttle.

The family tests stub ``_polite_wait`` and check it was *called*; here the real one runs on a fake
monotonic clock, so a path that skipped it would show two requests to a host less than
``MIN_INTERVAL`` apart. No network, no sleeping.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from sportsdataverse.hockey import euro_hockey_runtime as rt
from sportsdataverse.hockey.ha import ha_runtime as HA
from sportsdataverse.hockey.khl import khl_runtime as KHL

SID = "0123456789abcdef0123456789abcdef"
FIXTURES = Path(__file__).parents[1] / "fixtures" / "khl_rest"
OK = json.loads((FIXTURES / "game__header.json").read_text(encoding="utf-8"))


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
    monkeypatch.setattr(KHL, "_sessions", {})
    return state


def _gaps(times: List[float]) -> List[float]:
    return [round(b - a, 6) for a, b in zip(times, times[1:])]


def test_khl_every_request_is_spaced_prime_re_prime_retry_and_next_call(clock, monkeypatch):
    now = clock
    seen: List[float] = []
    posts = [_Resp(307, text=""), _Resp(503, text="busy"), _Resp(200, OK), _Resp(200, OK)]

    class Http:
        def get(self, url: str, timeout: Any = None) -> _Resp:
            seen.append(now["now"])
            return _Resp(text=f'"bitrix_sessid":"{SID}"')

        def post(self, url: str, **kw: Any) -> _Resp:
            seen.append(now["now"])
            return posts.pop(0)

    monkeypatch.setattr(KHL, "_new_http", lambda: Http())
    route = "https://www.khl.ru/rest/game/header/"
    form = {"page": "resume", "values[gameid]": "898236", "values[tournament]": "1369"}
    KHL._get(route, form)  # GET prime, POST 307, GET re-prime, POST 503, POST 200
    KHL._get(route, form)  # POST 200 on the reused session
    assert len(seen) == 6 and not posts
    assert min(_gaps(seen)) >= rt.MIN_INTERVAL


def test_ha_post_retries_are_spaced(clock, monkeypatch):
    now = clock
    seen: List[float] = []
    answers = [_Resp(503, text=""), _Resp(502, text=""), _Resp(200, {"ok": 1})]

    class Session:
        def post(self, url: str, **kw: Any) -> _Resp:
            seen.append(now["now"])
            return answers.pop(0)

    monkeypatch.setattr(HA, "_SESSION", Session())
    resp = HA._post("https://hockeyallsvenskan.se/api/x", json={})
    assert resp.status_code == 200 and len(seen) == 3
    assert min(_gaps(seen)) >= rt.MIN_INTERVAL


def test_the_clock_fixture_really_enforces_spacing(clock):
    t = clock
    rt._polite_wait("https://www.khl.ru/a")
    first = t["now"]
    rt._polite_wait("https://www.khl.ru/b")
    assert t["now"] - first >= rt.MIN_INTERVAL
