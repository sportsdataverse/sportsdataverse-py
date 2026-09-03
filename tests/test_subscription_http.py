"""Offline tests for the subscription-scraping layer (KenPom + Her Hoop Stats).

No network and no credentials: the login flow is driven through an injected
``requests.Session`` double, and the table parsing runs on inline HTML shaped
like the real pages (a two-row ``<thead>`` with unlabelled rank columns, which
is the whole reason hoopR carries ~44 hardcoded header vectors).
"""

from __future__ import annotations

import pytest

from sportsdataverse._html_tables import _clean_name, _dedupe_headers, html_tables
from sportsdataverse._subscription_http import (
    clear_session_cache,
    login,
    resolve_credentials,
    resolve_proxy,
)
from sportsdataverse.mbb.kenpom_runtime import KENPOM, parse_kenpom_page
from sportsdataverse.wbb.herhoopstats import HERHOOPSTATS

# A KenPom-shaped ratings table: grouped two-row header, and each metric followed
# by an UNLABELLED rank cell -- the exact layout rvest flattens into garbage.
KENPOM_HTML = """
<html><body>
<table id="ratings-table">
  <thead>
    <tr><th></th><th></th><th colspan="2">AdjO</th><th colspan="2">AdjD</th></tr>
    <tr><th>Rk</th><th>Team</th><th>AdjO</th><th></th><th>AdjD</th><th></th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>Duke</td><td>128.1</td><td>1</td><td>91.2</td><td>4</td></tr>
    <tr><td>2</td><td>Houston</td><td>124.0</td><td>5</td><td>84.9</td><td>1</td></tr>
  </tbody>
</table>
<table id="nav"><tr><th>x</th></tr><tr><td>1</td></tr></table>
</body></html>
"""

LOGIN_FORM = (
    '<form action="handlers/login_handler.php">'
    '<input name="email"><input type="password" name="password">'
    '<input type="hidden" name="csrfmiddlewaretoken" value="TOK123">'
    "</form>"
)
LOGGED_IN = "<html><body>Welcome back</body></html>"


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    """Stands in for requests.Session; records the login round trip."""

    def __init__(self, post_body: str = LOGGED_IN) -> None:
        self.headers: dict = {}
        self.proxies: dict = {}
        self._post_body = post_body
        self.posted: dict = {}

    def get(self, url, **kwargs):
        return FakeResponse(LOGIN_FORM)

    def post(self, url, data=None, headers=None, **kwargs):
        self.posted = {"url": url, "data": dict(data or {}), "headers": dict(headers or {})}
        return FakeResponse(self._post_body)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in (
        "KENPOM_EMAIL",
        "KENPOM_PW",
        "KP_USER",
        "KP_PW",
        "SDV_PY_KENPOM_EMAIL",
        "SDV_PY_KENPOM_PW",
        "SDV_PY_KENPOM_PROXY",
        "HERHOOPSTATS_EMAIL",
        "HERHOOPSTATS_PW",
        "SDV_PY_HERHOOPSTATS_EMAIL",
        "SDV_PY_HERHOOPSTATS_PW",
        "SDV_PY_HERHOOPSTATS_PROXY",
        "SDV_PY_PROXY",
    ):
        monkeypatch.delenv(var, raising=False)
    clear_session_cache()
    yield
    clear_session_cache()


# --- header flattening: the replacement for hoopR's 44 header_cols vectors ---


def test_dedupe_headers_names_rank_columns():
    assert _dedupe_headers(["team", "adj_o", "", "adj_d", "adj_d"]) == [
        "team",
        "adj_o",
        "adj_o_rk",
        "adj_d",
        "adj_d_rk",
    ]


def test_symbol_only_headers_are_not_mistaken_for_rank_columns():
    # A header that strips to "" is treated as an unlabelled rank cell, so "#" must
    # become a word BEFORE the non-alnum strip or it silently aliases the column
    # before it ("team", "#") -> ("team", "team_rk").
    cleaned = [_clean_name(h) for h in ["Team", "#", "PTS", "+/-", "eFG%"]]
    assert cleaned == ["team", "number", "pts", "plus_minus", "e_fg_pct"]
    assert _dedupe_headers(cleaned) == cleaned


def test_dedupe_headers_never_collides():
    out = _dedupe_headers(["x", "x", "x", ""])
    assert len(set(out)) == len(out)


def test_html_tables_flattens_kenpom_two_row_header():
    frames = html_tables(KENPOM_HTML, min_rows=2)
    assert list(frames) == ["ratings_table"]  # the 1-row nav table is dropped
    df = frames["ratings_table"]
    assert df.columns == ["rk", "team", "adj_o", "adj_o_rk", "adj_d", "adj_d_rk"]
    assert df.shape == (2, 6)
    assert df["team"].to_list() == ["Duke", "Houston"]


def test_parse_kenpom_page_returns_dict_of_frames():
    frames = parse_kenpom_page(KENPOM_HTML)
    assert set(frames) == {"ratings_table"}


def test_html_tables_empty_page_is_empty_not_an_error():
    assert html_tables("<html><body>no tables</body></html>") == {}


# --- credentials -------------------------------------------------------------


def test_credentials_missing_raises_with_instructions():
    with pytest.raises(RuntimeError, match="KENPOM_EMAIL"):
        resolve_credentials(KENPOM)


def test_credentials_from_hoopr_env_names(monkeypatch):
    monkeypatch.setenv("KP_USER", "r@example.com")
    monkeypatch.setenv("KP_PW", "secret")
    assert resolve_credentials(KENPOM) == ("r@example.com", "secret")


def test_explicit_credentials_beat_environment(monkeypatch):
    monkeypatch.setenv("KENPOM_EMAIL", "env@example.com")
    monkeypatch.setenv("KENPOM_PW", "envpw")
    assert resolve_credentials(KENPOM, "arg@example.com", "argpw") == ("arg@example.com", "argpw")


# --- proxy -------------------------------------------------------------------


def test_proxy_precedence(monkeypatch):
    assert resolve_proxy(KENPOM) is None
    monkeypatch.setenv("SDV_PY_PROXY", "http://global:1")
    assert resolve_proxy(KENPOM) == {"http": "http://global:1", "https": "http://global:1"}
    monkeypatch.setenv("SDV_PY_KENPOM_PROXY", "http://site:2")
    assert resolve_proxy(KENPOM)["https"] == "http://site:2"
    assert resolve_proxy(KENPOM, "http://arg:3")["https"] == "http://arg:3"


def test_proxy_dict_passes_through():
    supplied = {"https": "http://only-https:8080"}
    assert resolve_proxy(HERHOOPSTATS, supplied) == supplied


# --- login -------------------------------------------------------------------


def test_login_posts_to_the_form_action_and_binds_the_proxy():
    sess = FakeSession()
    out = login(KENPOM, "u@example.com", "pw", proxy="http://p:9", session=sess)
    assert out is sess
    assert sess.posted["url"] == "https://kenpom.com/handlers/login_handler.php"
    assert sess.posted["data"]["email"] == "u@example.com"
    assert sess.posted["data"]["submit"] == "Login"
    assert sess.proxies == {"http": "http://p:9", "https": "http://p:9"}


def test_login_echoes_the_django_csrf_token():
    sess = FakeSession()
    login(HERHOOPSTATS, "u@example.com", "pw", session=sess)
    assert sess.posted["data"]["csrfmiddlewaretoken"] == "TOK123"
    assert sess.posted["headers"]["Referer"] == HERHOOPSTATS.login_url


def test_rejected_login_raises_instead_of_scraping_the_logged_out_page():
    # A bad password on both sites returns HTTP 200 with the login form again.
    sess = FakeSession(post_body=LOGIN_FORM)
    with pytest.raises(RuntimeError, match="rejected the supplied credentials"):
        login(KENPOM, "u@example.com", "wrong", session=sess)


def test_session_is_cached_per_credentials(monkeypatch):
    monkeypatch.setenv("KENPOM_EMAIL", "u@example.com")
    monkeypatch.setenv("KENPOM_PW", "pw")
    created = []

    def _fake_session():
        sess = FakeSession()
        created.append(sess)
        return sess

    monkeypatch.setattr("sportsdataverse._subscription_http.requests.Session", _fake_session)
    first = login(KENPOM)
    second = login(KENPOM)
    assert first is second
    assert len(created) == 1  # the second call did not log in again
