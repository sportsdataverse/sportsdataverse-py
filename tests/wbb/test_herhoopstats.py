"""Her Hoop Stats: the free-tier teaser is a failed fetch, and a page without a roster has none.

``tests/fixtures/herhoopstats/team_single_seasons_2025_logged_out.html`` is the real page a
logged-out client gets for ``herhoopstats_teams(min_season=2025)``: a 5-row teaser with the full
schema and no login form (A-M19: the subscriber answer the same day was 362 rows). Only its
analytics ``dataLayer`` (``'subscribed': 0``) marks it. It carries no player column, so it is also
what ``herhoopstats_team_roster`` used to answer with its tallest table (B-GB-24).
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import polars as pl
import pytest
import requests

from sportsdataverse import _subscription_http as sh
from sportsdataverse.errors import AssetFetchError
from sportsdataverse.mbb.kenpom_runtime import KENPOM
from sportsdataverse.wbb import herhoopstats as hhs

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "herhoopstats"
_TEASER = (_FIX / "team_single_seasons_2025_logged_out.html").read_text(encoding="utf-8")
_ROSTER_PAGE = (_FIX / "team_roster.trim.html").read_text(encoding="utf-8")
_LOGIN_FORM = '<form action="/accounts/login/"><input name="email"><input type="password" name="password"></form>'


def _response(text: str) -> requests.Response:
    resp = requests.Response()
    resp.status_code = 200
    resp._content = text.encode("utf-8")
    resp.encoding = "utf-8"
    return resp


class _LoginSession:
    """A ``requests.Session`` stand-in; the credentials POST answers ``post_body``."""

    def __init__(self, post_body: str = "<html><body>Welcome back</body></html>") -> None:
        self.headers: dict = {}
        self.proxies: dict = {}
        self._post_body = post_body

    def get(self, url, **kwargs):
        return _response(_LOGIN_FORM)

    def post(self, url, data=None, headers=None, **kwargs):
        return _response(self._post_body)


@pytest.fixture
def logins(monkeypatch) -> List[_LoginSession]:
    created: List[_LoginSession] = []

    def _new() -> _LoginSession:
        created.append(_LoginSession())
        return created[-1]

    monkeypatch.setenv("HERHOOPSTATS_EMAIL", "u@example.com")
    monkeypatch.setenv("HERHOOPSTATS_PW", "fake-pw")
    monkeypatch.setattr("sportsdataverse._subscription_http.requests.Session", _new)
    sh.clear_session_cache()
    yield created
    sh.clear_session_cache()


def _serve(monkeypatch, body: str) -> None:
    """Every member page answers ``body``; the login form GET answers the form."""

    def download(url, **kwargs):
        return _response(_LOGIN_FORM if url == hhs.HERHOOPSTATS.login_url else body)

    monkeypatch.setattr("sportsdataverse._subscription_http.download", download)


def test_the_captured_free_tier_page_is_marked_not_subscribed() -> None:
    assert sh._looks_logged_out(_TEASER, hhs.HERHOOPSTATS)
    assert not sh._looks_logged_out(_TEASER, KENPOM)  # no login form: the form check alone misses it
    assert not sh._looks_logged_out(_ROSTER_PAGE, hhs.HERHOOPSTATS)  # a subscriber page passes
    assert max(len(t) for t in sh.html_tables(_TEASER, min_rows=1).values()) == 5  # what callers got


def test_free_tier_page_raises_instead_of_a_teaser_frame(monkeypatch, logins) -> None:
    _serve(monkeypatch, _TEASER)
    with pytest.raises(AssetFetchError, match="Her Hoop Stats answered .* free-tier teaser"):
        hhs.herhoopstats_teams(min_season=2025)
    assert sh._sessions == {}  # the dead session is dropped


def test_free_tier_answer_to_the_login_is_rejected(monkeypatch) -> None:
    # A login that lands on the teaser (no live subscription) is not a working session.
    with pytest.raises(RuntimeError, match="rejected the supplied credentials"):
        sh.login(hhs.HERHOOPSTATS, "u@example.com", "fake-pw", session=_LoginSession(_TEASER), use_cache=False)


def test_team_page_without_a_roster_is_a_zero_row_roster(monkeypatch) -> None:
    monkeypatch.setattr(hhs, "_page", lambda *a, **k: _TEASER)
    frame = hhs.herhoopstats_team_roster("/stats/ncaa/team/x/")
    assert frame.shape == (0, 14)
    assert dict(frame.schema) == hhs._ROSTER_SCHEMA


def test_roster_schema_is_what_a_real_roster_page_parses_to(monkeypatch) -> None:
    monkeypatch.setattr(hhs, "_page", lambda *a, **k: _ROSTER_PAGE)
    frame = hhs.herhoopstats_team_roster("/stats/ncaa/team/x/")
    assert frame.height == 3
    assert dict(frame.schema) == hhs._ROSTER_SCHEMA
    assert isinstance(frame, pl.DataFrame)
