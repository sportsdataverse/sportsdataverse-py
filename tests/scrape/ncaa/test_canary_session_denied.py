"""The canary's own browser transports stop at an Akamai session denial (#1039 follow-up).

Both used the size-only unsolved rule, so a ~400-byte 403 "Access Denied" spent every
solve attempt on a refused session. They now drop the session and raise
``_SessionDeniedError``, which ``classify_error`` scores as CHALLENGE, like the
production fetcher.
"""

from pathlib import Path

import pytest

from sportsdataverse.mbb.mbb_ncaa_fetch import _SessionDeniedError
from sportsdataverse.scrape.ncaa.canary import (
    CHALLENGE,
    _ManagedCdpTransport,
    _PatchrightTransport,
    classify_error,
)

_DENIED = (
    Path(__file__).resolve().parents[2] / "fixtures" / "ncaa" / "capture" / "access_denied_box_score_385627.html"
).read_text(encoding="utf-8")
_URL = "https://stats.ncaa.org/contests/1/box_score"


class _DeniedPage:
    """Every in-page fetch answers the real 403 denial page; counts fetches."""

    def __init__(self) -> None:
        self.fetches = 0

    def goto(self, url: str, **kw: object) -> None:
        pass

    def wait_for_timeout(self, ms: int) -> None:
        pass

    def evaluate(self, js: str, url: str) -> dict:
        self.fetches += 1
        return {"status": 403, "text": _DENIED}


@pytest.mark.parametrize("cls", [_PatchrightTransport, _ManagedCdpTransport])
def test_canary_transport_stops_at_a_session_denial(cls: type, monkeypatch: pytest.MonkeyPatch) -> None:
    t = cls.__new__(cls)  # skip __init__: it would start a browser
    t.challenge_wait_ms, t.nav_timeout_ms, t.solve_attempts = 0, 1000, 3
    page = _DeniedPage()
    t._page = page
    closed: "list[bool]" = []
    monkeypatch.setattr(t, "_ensure", lambda *a: None)
    monkeypatch.setattr(t, "close", lambda: closed.append(True))

    with pytest.raises(_SessionDeniedError) as exc:
        t(_URL, {}, {})

    assert page.fetches == 1  # not solve_attempts
    assert closed == [True]  # the next call starts a fresh session
    assert classify_error(exc.value) == CHALLENGE
