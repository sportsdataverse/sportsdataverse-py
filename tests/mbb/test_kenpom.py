"""KenPom transport: a dead session's login page is a failed fetch, not data (A-M19, task 4.4).

``tests/fixtures/kenpom/summary_logged_out.html`` is what kenpom.com answers a member page
with when the session cookie is missing, expired, or replaced by a second login (KenPom keeps
one live session per account): HTTP 200, the login form, no table. Before task 4.4 the
logged-out check ran only inside ``login()``, so ``get_html`` handed this page to
``parse_kenpom_page`` and a caller got ``{}`` with no exception, from a cached session that
kept failing for its whole 30-minute life.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import pytest
import requests

from sportsdataverse import _subscription_http as sh
from sportsdataverse.errors import AssetFetchError
from sportsdataverse.mbb import kenpom_runtime

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "kenpom"
_LOGGED_OUT = (_FIX / "summary_logged_out.html").read_text(encoding="utf-8")
_MEMBER_PAGE = (_FIX / "ratings_2025.trim.html").read_text(encoding="utf-8")


class _LoginSession:
    """A ``requests.Session`` stand-in whose login round trip succeeds."""

    def __init__(self) -> None:
        self.headers: dict = {}
        self.proxies: dict = {}

    def _answer(self, text: str) -> requests.Response:
        resp = requests.Response()
        resp.status_code = 200
        resp._content = text.encode("utf-8")
        resp.encoding = "utf-8"
        return resp

    def get(self, url, **kwargs):
        return self._answer('<form action="handlers/login_handler.php"><input name="email"></form>')

    def post(self, url, data=None, headers=None, **kwargs):
        return self._answer("<html><body>Welcome back</body></html>")


@pytest.fixture
def logins(monkeypatch) -> List[_LoginSession]:
    """Every session a login created; credentials from the environment, cache cleared."""
    created: List[_LoginSession] = []

    def _new() -> _LoginSession:
        created.append(_LoginSession())
        return created[-1]

    monkeypatch.setenv("KENPOM_EMAIL", "u@example.com")
    monkeypatch.setenv("KENPOM_PW", "fake-pw")
    monkeypatch.setattr("sportsdataverse._subscription_http.requests.Session", _new)
    sh.clear_session_cache()
    yield created
    sh.clear_session_cache()


def _serve(monkeypatch, body: str) -> None:
    def download(url, **kwargs):
        resp = requests.Response()
        resp.status_code = 200
        resp._content = body.encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = url
        return resp

    monkeypatch.setattr("sportsdataverse._subscription_http.download", download)


def test_the_captured_logged_out_page_is_the_login_form() -> None:
    assert sh._looks_logged_out(_LOGGED_OUT, kenpom_runtime.KENPOM)
    assert kenpom_runtime.parse_kenpom_page(_LOGGED_OUT) == {}  # what callers got before 4.4


def test_logged_out_page_raises_instead_of_parsing(monkeypatch, logins) -> None:
    _serve(monkeypatch, _LOGGED_OUT)
    with pytest.raises(AssetFetchError, match=r"KenPom answered kenpom.com/summary.php with its logged-out page"):
        kenpom_runtime._get("https://kenpom.com/summary.php", {"y": 2025})


def test_logged_out_page_drops_the_cached_session(monkeypatch, logins) -> None:
    _serve(monkeypatch, _LOGGED_OUT)
    with pytest.raises(AssetFetchError):
        kenpom_runtime._get("https://kenpom.com/summary.php", {"y": 2025})
    assert sh._sessions == {}
    _serve(monkeypatch, _MEMBER_PAGE)
    kenpom_runtime._get("https://kenpom.com/summary.php", {"y": 2025})
    assert len(logins) == 2  # the dead session was not reused: the second call logged in again


def test_member_page_is_not_mistaken_for_logged_out(monkeypatch, logins) -> None:
    _serve(monkeypatch, _MEMBER_PAGE)
    assert kenpom_runtime._get("https://kenpom.com/index.php", {"y": 2025}) == _MEMBER_PAGE
    assert len(logins) == 1 and len(sh._sessions) == 1  # the live session stays cached
