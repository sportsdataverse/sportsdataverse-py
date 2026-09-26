"""Tests for the NCAA cache-first proxy fetch layer (Phase 5f, Task 5f.1).

Fully offline -- every test injects a fake transport (or an explicit
``proxy_pool``) and never touches the network. The two binding directives
(proxy-only, cache-first) are asserted structurally, not merely assumed:
see :func:`test_no_proxy_configured_raises` and
:func:`test_cache_hit_makes_zero_transport_calls`.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
from pathlib import Path
from typing import Callable, Iterator, Optional

import pytest

from sportsdataverse.mbb.mbb_ncaa_fetch import (
    NcaaFetchConfig,
    _TermsGateError,
    NcaaFetcher,
    cached_path,
    get_config,
    is_cached,
    load_proxybonanza_pool,
    playwright_transport,
    reset_config,
    update_config,
)


class FakeTransport:
    """Records every ``(url, proxies, headers)`` call; replays scripted responses."""

    def __init__(self, responses: "list[tuple[int, str]]") -> None:
        self.responses = list(responses)
        self.calls: "list[tuple[str, dict, dict]]" = []

    def __call__(self, url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        self.calls.append((url, dict(proxies), dict(headers)))
        if not self.responses:
            raise AssertionError("FakeTransport exhausted -- unexpected extra call")
        return self.responses.pop(0)


def _cfg(
    tmp_path: Path, transport: Optional[FakeTransport] = None, proxy_url: Optional[str] = "http://u:p@1.1.1.1:1"
) -> NcaaFetchConfig:
    return NcaaFetchConfig(cache_dir=tmp_path, proxy_url=proxy_url, transport=transport)


_POOL = ["http://u:p@10.0.0.1:1", "http://u:p@10.0.0.2:1", "http://u:p@10.0.0.3:1"]
# Stand-in for a real game page. Must exceed _MIN_CONTENT_BYTES (1KB) -- real
# stats.ncaa.org pages run 100KB+, and the browser transport uses size to spot
# the marker-less thin stub, so a 700-byte "clean" fixture would be unrealistic
# AND would read as unsolved.
_CLEAN = "<html><body><table>" + "<tr><td>xxxxxxxxxx</td></tr>" * 100 + "</table></body></html>"
_BAN = "<html><body>Access Denied</body></html>"


def _proxies_used(transport: FakeTransport) -> "list[str]":
    return [c[1].get("http") for c in transport.calls]


# --- proxy rotation: IPs are a consumable budget (bans are permanent) ------


def test_rotate_every_retires_a_proxy_while_it_is_still_healthy(tmp_path: Path) -> None:
    """PROACTIVE rotation -- the whole point. Waiting for a ban is too late:
    stats.ncaa.org's per-IP bans never lift, so a healthy IP must be retired on
    a fetch budget, not on failure. One IP absorbing a backfill is what burned
    31.14.9.13 permanently."""
    transport = FakeTransport([(200, _CLEAN)] * 6)
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotate_every=2, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    for i in range(6):
        fetcher.fetch_html(f"contests/{i}/play_by_play")

    # 2 fetches per proxy, cycling the pool -- never 6 on one IP.
    assert _proxies_used(transport) == [_POOL[0], _POOL[0], _POOL[1], _POOL[1], _POOL[2], _POOL[2]]


def test_rotate_every_zero_disables_proactive_rotation(tmp_path: Path) -> None:
    transport = FakeTransport([(200, _CLEAN)] * 3)
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotate_every=0, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    for i in range(3):
        fetcher.fetch_html(f"contests/{i}/play_by_play")

    assert _proxies_used(transport) == [_POOL[0]] * 3  # legacy behavior, opt-out


def test_banned_proxy_is_retired_and_never_reused(tmp_path: Path) -> None:
    """A burned IP must never be handed out again -- rotation that cycles back
    into a known-dead proxy just re-earns the 403."""
    # p0 bans; the retry lands clean on p1. Then 2 more fetches must avoid p0.
    transport = FakeTransport([(403, _BAN), (200, _CLEAN), (200, _CLEAN), (200, _CLEAN)])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotate_every=0, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    for i in range(3):
        fetcher.fetch_html(f"contests/{i}/play_by_play")

    used = _proxies_used(transport)
    assert used[0] == _POOL[0]  # tried once
    assert _POOL[0] not in used[1:]  # then retired for good
    assert _POOL[0] in fetcher._dead


def test_all_proxies_banned_raises_a_clear_error(tmp_path: Path) -> None:
    transport = FakeTransport([(403, _BAN)] * 8)
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    with pytest.raises(RuntimeError, match="every proxy in the pool is banned"):
        fetcher.fetch_html("contests/1/play_by_play")


def test_missing_patchright_is_not_treated_as_a_transport_failure(tmp_path: Path) -> None:
    """A missing optional dependency must not be rotated on.

    patchright is imported lazily inside the browser transport, so its
    ImportError arrives through the same call as a 403. Rotating on it burned
    the entire pool -- one backoff sleep per proxy -- and then reported the real
    cause under "NCAA fetch failed after rotating proxies", which reads as a ban
    and sends the reader hunting for an IP problem that does not exist. It cost a
    live cron a night: the install error was invisible under a proxy headline.
    """

    class _NoPatchright:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
            self.calls += 1
            raise ImportError("The NCAA browser transport requires patchright")

    transport = _NoPatchright()
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    # The install instruction is the error -- not a RuntimeError about proxies.
    with pytest.raises(ImportError, match="requires patchright"):
        fetcher.fetch_html("contests/1/play_by_play")

    # And the pool is untouched: rotation cannot install a package.
    assert transport.calls == 1
    assert fetcher._dead == set()


def test_rotate_every_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.mbb.mbb_ncaa_fetch import _from_env

    monkeypatch.setenv("SDV_PY_NCAA_ROTATE_EVERY", "25")
    assert _from_env().rotate_every == 25
    monkeypatch.setenv("SDV_PY_NCAA_ROTATE_EVERY", "abc")
    assert _from_env().rotate_every == 200  # invalid -> default


# --- the bm-verify challenge shell is a THIRD response class ---------------

# Shape 1 -- the NAVIGATION interstitial (what curl_cffi sees, ~2.3KB + markers).
_CHALLENGE = (
    '<!DOCTYPE html><html><head><meta http-equiv="refresh" content="0">'
    '<script>window._abck="x";bm-verify</script></head><body></body></html>'
)
# Shape 2 -- the THIN in-page-fetch stub Akamai returns when _abck is invalid.
# Captured live from stats.ncaa.org: exactly this, 15 bytes, no markers at all.
_STUB = "NCAA Statistics"


def test_unsolved_challenge_is_not_a_ban_and_not_content() -> None:
    """The classification hole behind the outage: an unsolved challenge is 200
    with NO ban marker, so _ban_check calls it 'clean' and the fetch layer
    returned it as a successful fetch."""
    from sportsdataverse.mbb.mbb_ncaa_fetch import _ban_check, _is_challenge

    assert _ban_check(_CHALLENGE) == "clean"  # ban-check genuinely cannot see it
    assert _is_challenge(_CHALLENGE) is True  # ...which is why this exists
    assert _is_challenge(_CLEAN) is False  # real content is not a challenge
    assert _is_challenge(_BAN) is False  # a ban is not a challenge
    # Size guard: a huge page mentioning _abck is content, not a shell.
    assert _is_challenge("<html>" + "x" * 30000 + "_abck</html>") is False


def test_thin_xhr_stub_is_detected_as_unsolved() -> None:
    """The shape that actually broke us. Akamai answers an in-page fetch that
    carries an invalid _abck with a 15-byte body -- no 'bm-verify', no '_abck',
    no ban text. Nothing marker-based can see it, so the BROWSER check catches
    it by size (no real stats.ncaa.org page is under 1KB).

    The size rule lives ONLY in the browser check: _is_challenge judges
    responses from every transport, where a small body may be legitimate --
    putting a size floor there broke 18 unrelated tests."""
    from sportsdataverse.mbb.mbb_ncaa_fetch import (
        _ban_check,
        _browser_response_unsolved,
        _is_challenge,
    )

    assert len(_STUB) == 15
    assert _ban_check(_STUB) == "clean"  # invisible to ban-check
    assert "bm-verify" not in _STUB.lower() and "_abck" not in _STUB.lower()
    assert _is_challenge(_STUB) is False  # marker-based: genuinely cannot see it
    assert _browser_response_unsolved(_STUB) is True  # size-based: caught here
    assert _browser_response_unsolved("") is True
    # Both shapes converge for the browser; real content passes both.
    assert _browser_response_unsolved(_CHALLENGE) is True
    assert _browser_response_unsolved(_CLEAN) is False


def test_challenge_rotates_the_proxy_and_never_returns_the_shell(tmp_path: Path) -> None:
    """A challenge must rotate to a fresh IP (fresh browser = fresh solve) and
    must NOT be handed back to the caller as if it were a page."""
    transport = FakeTransport([(200, _CHALLENGE), (200, _CLEAN)])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    got = fetcher.fetch_html("contests/1/play_by_play")

    assert got == _CLEAN  # never the shell
    assert _proxies_used(transport) == [_POOL[0], _POOL[1]]  # rotated on the challenge


def test_challenge_does_not_retire_the_proxy(tmp_path: Path) -> None:
    """A challenge is NOT a ban -- the IP may be fine. Retiring on a challenge
    would throw away healthy IPs (they are a scarce, subnet-shared resource)."""
    transport = FakeTransport([(200, _CHALLENGE), (200, _CLEAN)])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    fetcher.fetch_html("contests/1/play_by_play")

    assert fetcher._dead == set()  # vs a 403, which DOES retire the proxy


def test_challenge_does_not_count_toward_rotate_every(tmp_path: Path) -> None:
    """Only real fetches age a proxy's budget; shells must not."""
    transport = FakeTransport([(200, _CHALLENGE), (200, _CLEAN), (200, _CLEAN)])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotate_every=2, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    fetcher.fetch_html("contests/1/play_by_play")  # challenge -> rotate to p1, then clean on p1
    assert fetcher._since_rotate == 1  # the shell did NOT increment the budget
    fetcher.fetch_html("contests/2/play_by_play")
    assert fetcher._since_rotate == 0  # 2 real fetches on p1 -> rotated


# --- the solve must be PROVEN by the fetch, never assumed ------------------


class _FakePage:
    """Stands in for a Playwright page: scripted fetch bodies, records solves."""

    def __init__(self, bodies: "list[str]") -> None:
        self.bodies = list(bodies)
        self.gotos = 0

    def goto(self, url: str, **kw: object) -> None:
        self.gotos += 1

    def wait_for_timeout(self, ms: int) -> None:
        pass

    def content(self) -> str:
        return "<html>ok</html>"  # interstitial looks cleared

    def evaluate(self, js: str, url: str) -> dict:
        return {"status": 200, "text": self.bodies.pop(0)}


def _transport_with(page: _FakePage) -> object:
    t = playwright_transport(challenge_wait_ms=0, solve_attempts=2)
    t._page = page
    t._current_proxy = _POOL[0]
    t._challenge_solved = False
    return t


def test_failed_solve_is_retried_not_latched() -> None:
    """The latch bug: _challenge_solved was set True after a BLIND wait, so a
    solve that never actually passed was treated as success -- and every later
    fetch returned an unsolved response forever (1485 in one live run).
    The fetch itself must be the proof."""
    page = _FakePage([_STUB, _CLEAN])  # two failed solves, then it takes
    t = _transport_with(page)

    status, text = t("https://stats.ncaa.org/contests/1/play_by_play", {"http": _POOL[0]}, {})

    assert text == _CLEAN  # never returned a stub as if it were a page
    assert page.gotos == 2  # re-solved on the unsolved response


def test_solve_gives_up_after_solve_attempts_so_the_fetcher_can_rotate() -> None:
    """Bounded: after solve_attempts the transport RAISES, which the fetch
    layer's rotate-on-transport-error path turns into a fresh proxy (= fresh
    browser + a genuinely fresh sensor run). It must never hand the stub back
    as if it were a page -- that is what made the fetcher think it succeeded."""
    page = _FakePage([_STUB, _STUB])
    t = _transport_with(page)

    with pytest.raises(RuntimeError, match="bm-verify not passed after 2 attempts"):
        t("https://stats.ncaa.org/contests/1/play_by_play", {"http": _POOL[0]}, {})

    assert page.gotos == 2  # exactly solve_attempts sensor runs, then stop


def test_unsolvable_proxy_is_rotated_away_from_not_returned(tmp_path: Path) -> None:
    """End-to-end at the fetch layer: a transport that cannot solve raises, the
    fetcher rotates to the next proxy, and the caller gets real content."""
    calls: "list[str]" = []

    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        px = proxies.get("http")
        calls.append(px)
        if px == _POOL[0]:
            raise RuntimeError("bm-verify not passed after 3 attempts (15-byte response)")
        return (200, _CLEAN)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)

    assert fetcher.fetch_html("contests/1/play_by_play") == _CLEAN
    assert calls == [_POOL[0], _POOL[1]]  # rotated off the unsolvable proxy
    assert fetcher._dead == set()  # unsolved != banned -- the IP is not retired


class _DroppingPage(_FakePage):
    """evaluate() raises the scripted errors first, then serves bodies."""

    def __init__(self, errors: "list[str]", bodies: "list[str]") -> None:
        super().__init__(bodies)
        self.errors = list(errors)
        self.evaluates = 0

    def evaluate(self, js: str, url: str) -> dict:
        self.evaluates += 1
        if self.errors:
            raise RuntimeError(self.errors.pop(0))
        return super().evaluate(js, url)


_DROPPED = "Error: Page.evaluate: TypeError: Failed to fetch"


def test_dropped_in_page_fetch_is_retried_on_the_same_browser() -> None:
    """A dropped request used to rotate: a browser relaunch plus a fresh Terms
    acceptance spent from the site-wide budget, for a network blip."""
    page = _DroppingPage([_DROPPED], [_CLEAN])
    t = _transport_with(page)
    t._challenge_solved = True

    assert t("https://stats.ncaa.org/contests/1/play_by_play", {"http": _POOL[0]}, {}) == (200, _CLEAN)
    assert page.evaluates == 2 and page.gotos == 0  # retried in place, no navigation


@pytest.mark.parametrize(
    "errors",
    [[_DROPPED, _DROPPED], ["Error: Page.evaluate: Target page, context or browser has been closed"]],
    ids=["dropped-twice", "not-a-dropped-request"],
)
def test_in_page_fetch_raises_so_the_fetcher_can_rotate(errors: "list[str]") -> None:
    page = _DroppingPage(errors, [_CLEAN])
    t = _transport_with(page)
    t._challenge_solved = True

    with pytest.raises(RuntimeError, match=errors[-1].split(": ", 1)[1]):
        t("https://stats.ncaa.org/contests/1/play_by_play", {"http": _POOL[0]}, {})
    assert page.evaluates == len(errors)  # one retry for a drop, none for anything else


def test_twice_dropped_fetch_rotates_to_the_next_proxy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The real transport inside the real fetcher: a double drop must stay an
    ordinary transport error (rotated), never a Terms error (which is not)."""
    pages = {_POOL[0]: _DroppingPage([_DROPPED, _DROPPED], []), _POOL[1]: _DroppingPage([], [_CLEAN])}
    t = playwright_transport(challenge_wait_ms=0, solve_attempts=2)

    def ensure_page(proxies: dict) -> None:  # one fake page per proxy; no browser launch
        t._page, t._current_proxy, t._challenge_solved = pages[proxies["http"]], proxies["http"], True

    monkeypatch.setattr(t, "_ensure_page", ensure_page)
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=t, rotation_backoff=0.0)

    assert NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("contests/1/play_by_play") == _CLEAN
    assert [pages[p].evaluates for p in _POOL[:2]] == [2, 1]  # retried once, then rotated


def test_rotation_logs_the_transport_error_without_credentials(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        if proxies.get("http") == _POOL[0]:
            raise RuntimeError("net::ERR_TUNNEL_CONNECTION_FAILED via http://u:secret@10.0.0.1:1")
        return (200, _CLEAN)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    with caplog.at_level("DEBUG", logger="sportsdataverse.mbb.mbb_ncaa_fetch"):
        assert NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("contests/1/play_by_play") == _CLEAN
    logged = [r.getMessage() for r in caplog.records if "transport error on" in r.getMessage()]
    assert len(logged) == 1 and "RuntimeError: net::ERR_TUNNEL_CONNECTION_FAILED" in logged[0]
    assert "secret" not in caplog.text  # neither the proxy nor the message leaks credentials


def test_solved_session_does_not_re_solve_on_every_fetch() -> None:
    """Once genuinely solved, the token is reused -- no needless navigation."""
    page = _FakePage([_CLEAN, _CLEAN])
    t = _transport_with(page)

    t("https://stats.ncaa.org/contests/1/play_by_play", {"http": _POOL[0]}, {})
    t("https://stats.ncaa.org/contests/2/play_by_play", {"http": _POOL[0]}, {})

    assert page.gotos == 1  # solved once, reused


# --- the stats.ncaa.org Terms gate (2026-09-22) is accepted, never kept -----

# ~19 KB live; padded past _MIN_CONTENT_BYTES so it reads as "solved" like the real one.
# The form controls are the live markup (stats.ncaa.org gate page, captured
# 2026-09-25) and the fake page counts selectors against it, so _raw_fetch's
# selectors are checked against the real ids rather than a list in the test.
_GATE = (
    '<html><body><form action="/stats_terms" accept-charset="UTF-8" method="post">'
    '<input type="hidden" name="nonce" id="nonce" value="n1" />'
    '<input type="checkbox" name="terms_accepted" id="terms_accepted" value="t1" class="stats-terms-checkbox" />'
    '<button type="submit" class="stats-access-button" id="stats-access-button" disabled>'
    "Continue to Statistics</button></form>" + "<p>Continue to NCAA Statistics?</p>" * 60 + "</body></html>"
)


class _Landing:
    """The response the Continue click redirects to."""

    def __init__(self, url: str, body: str) -> None:
        self.url, self.status, self._body = url, 200, body

    def text(self) -> str:
        return self._body


class _NavInfo:
    def __init__(self, value: _Landing) -> None:
        self.value = value


class _GatedPage(_FakePage):
    """A _FakePage whose Terms gate clears only through the UI: dwell, tick, click."""

    def __init__(self, bodies: "list[str]", landing: _Landing) -> None:
        super().__init__(bodies)
        self.ui: "list[str]" = []
        self.landing = landing

    gate = _GATE

    def content(self) -> str:
        return self.gate  # navigation lands on /stats_terms

    def wait_for_timeout(self, ms: int) -> None:
        self.ui.append(f"wait {ms}")

    def check(self, selector: str) -> None:
        self.ui.append(selector)

    def click(self, selector: str) -> None:
        self.ui.append(selector)

    def locator(self, selector: str) -> "_Count":
        # count id matches in the markup, as the browser would
        return _Count(self.gate.count(f'id="{selector.removeprefix("#")}"'))

    @contextlib.contextmanager
    def expect_navigation(self, **kw: object) -> "Iterator[_NavInfo]":
        yield _NavInfo(self.landing)


class _Count:
    def __init__(self, n: int) -> None:
        self.n = n

    def count(self) -> int:
        return self.n


_URL = "https://stats.ncaa.org/teams/614563"
_ACCEPTED = _Landing(_URL, _CLEAN)  # 302 back to the page asked for
_REFUSED = _Landing("https://stats.ncaa.org/stats_terms", _GATE)  # the gate, re-rendered


def test_terms_gate_is_accepted_and_the_redirect_body_is_returned() -> None:
    page = _GatedPage([_GATE], _ACCEPTED)  # one in-page fetch only: a refetch would pop an empty list
    status, text = _transport_with(page)(_URL, {"http": _POOL[0]}, {})

    assert (status, text) == (200, _CLEAN)
    # dwell first (an immediate submit is rejected live), then tick, then Continue
    assert page.ui[-3:] == ["wait 10000", "#terms_accepted", "#stats-access-button"]


def test_redirect_elsewhere_falls_back_to_an_in_page_fetch() -> None:
    page = _GatedPage([_GATE, _CLEAN], _Landing("https://stats.ncaa.org/", _CLEAN))
    assert _transport_with(page)(_URL, {"http": _POOL[0]}, {})[1] == _CLEAN


def test_terms_gate_surviving_acceptance_raises() -> None:
    page = _GatedPage([_GATE, _GATE], _REFUSED)
    with pytest.raises(RuntimeError, match="terms gate not accepted"):
        _transport_with(page)(_URL, {"http": _POOL[0]}, {})


class _NoForm(_GatedPage):
    """The gate with its controls gone -- a changed form, or a proxy-truncated page."""

    gate = _GATE.replace('id="terms_accepted"', 'id="terms_agreed"')


def _pinned(page: _GatedPage, proxies: dict) -> object:
    t = _transport_with(page)
    t._current_proxy = proxies.get("http")  # pin the fake page to this proxy: no real relaunch
    return t


def test_changed_terms_form_fails_after_three_proxies_not_the_whole_pool(tmp_path: Path) -> None:
    calls: "list[str]" = []
    pages: "list[_GatedPage]" = []

    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        calls.append(proxies.get("http"))
        pages.append(_NoForm([_GATE], _ACCEPTED))
        return _pinned(pages[-1], proxies)(url, proxies, headers)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    with pytest.raises(_TermsGateError, match=r"terms acceptance failed.*\(on 3 proxies\)"):
        NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563")
    assert len(calls) == 3  # not len(_POOL) + 2
    assert not any("#stats-access-button" in p.ui for p in pages)  # never touched the form


def test_truncated_gate_on_one_proxy_rotates(tmp_path: Path) -> None:
    calls: "list[str]" = []

    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        calls.append(proxies.get("http"))
        page = _NoForm([_GATE], _ACCEPTED) if len(calls) == 1 else _GatedPage([_GATE], _ACCEPTED)
        return _pinned(page, proxies)(url, proxies, headers)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    assert NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563") == _CLEAN
    assert len(calls) == 2


class _TunnelFailure(_GatedPage):
    def click(self, selector: str) -> None:
        raise RuntimeError("Page.click: net::ERR_TUNNEL_CONNECTION_FAILED")


def test_network_error_during_acceptance_is_a_transport_error_not_a_gate_error() -> None:
    with pytest.raises(RuntimeError, match="ERR_TUNNEL_CONNECTION_FAILED") as exc:
        _transport_with(_TunnelFailure([_GATE], _ACCEPTED))(_URL, {"http": _POOL[0]}, {})
    assert not isinstance(exc.value, _TermsGateError)


def test_network_error_during_acceptance_rotates_to_the_next_proxy(tmp_path: Path) -> None:
    calls: "list[str]" = []

    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        calls.append(proxies.get("http"))
        page = _TunnelFailure([_GATE], _ACCEPTED) if len(calls) == 1 else _GatedPage([_GATE], _ACCEPTED)
        return _pinned(page, proxies)(url, proxies, headers)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    assert NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563") == _CLEAN
    assert len(calls) == 2  # rotated past the failing proxy instead of dying


def test_acceptance_failures_are_counted_per_fetch_not_per_process(tmp_path: Path) -> None:
    calls: "list[str]" = []

    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        calls.append(proxies.get("http"))
        page = _GatedPage([_GATE], _ACCEPTED) if len(calls) % 3 == 0 else _NoForm([_GATE], _ACCEPTED)
        return _pinned(page, proxies)(url, proxies, headers)

    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=_POOL)
    for _ in range(3):  # 2 failures per fetch -- 6 in all, never 3 within one fetch
        assert fetcher.fetch_html("teams/614563", force=True) == _CLEAN
    assert len(calls) == 9


def test_failed_call_forces_a_fresh_navigation_next_time() -> None:
    # single-proxy pools reuse the page: after a failure it may sit on chrome-error://
    t = _transport_with(_TunnelFailure([_GATE], _ACCEPTED))
    with pytest.raises(RuntimeError, match="ERR_TUNNEL_CONNECTION_FAILED"):
        t(_URL, {"http": _POOL[0]}, {})
    assert t._challenge_solved is False


@pytest.mark.parametrize(
    "gate",
    [
        _GATE.replace('action="/stats_terms"', 'action="https://stats.ncaa.org/stats_terms/accept"'),
        _GATE.replace('action="/stats_terms"', 'action="/terms"'),  # only the checkbox id is left
    ],
)
def test_a_gate_with_a_changed_action_is_still_never_cached(tmp_path: Path, gate: str) -> None:
    transport = FakeTransport([(200, gate)])
    with pytest.raises(RuntimeError, match="terms gate needs the browser transport"):
        NcaaFetcher(_cfg(tmp_path, transport)).fetch_html("teams/614563")
    assert not cached_path("teams/614563", cache_dir=tmp_path).exists()


def _refusing_transport(calls: "list[str]", clean_after: int = 10**9) -> "Callable[..., tuple[int, str]]":
    def transport(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        calls.append(proxies.get("http"))
        if len(calls) > clean_after:
            return (200, _CLEAN)
        return _transport_with(_GatedPage([_GATE, _GATE], _REFUSED))(url, proxies, headers)

    return transport


def test_terms_gate_fails_fast_instead_of_rotating_the_pool(tmp_path: Path) -> None:
    calls: "list[str]" = []
    cfg = NcaaFetchConfig(
        cache_dir=tmp_path, transport=_refusing_transport(calls), rotation_backoff=0.0, terms_retries=0
    )
    with pytest.raises(RuntimeError, match="terms gate not accepted"):
        NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563")
    assert len(calls) == 1  # one proxy tried, not the whole pool


def test_refused_terms_gate_backs_off_and_retries(tmp_path: Path) -> None:
    calls: "list[str]" = []
    cfg = NcaaFetchConfig(
        cache_dir=tmp_path, transport=_refusing_transport(calls, clean_after=1), terms_backoff=0.0, terms_retries=2
    )
    assert NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563") == _CLEAN
    assert len(calls) == 2  # refused once, backed off, then through


def test_terms_gate_retries_are_bounded(tmp_path: Path) -> None:
    calls: "list[str]" = []
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=_refusing_transport(calls), terms_backoff=0.0, terms_retries=2)
    with pytest.raises(RuntimeError, match="terms gate not accepted"):
        NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563")
    assert len(calls) == 3  # first try + 2 retries


def test_terms_backoff_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.mbb.mbb_ncaa_fetch import _from_env

    monkeypatch.setenv("SDV_PY_NCAA_TERMS_BACKOFF", "90")
    monkeypatch.setenv("SDV_PY_NCAA_TERMS_RETRIES", "5")
    assert (_from_env().terms_backoff, _from_env().terms_retries) == (90.0, 5)
    monkeypatch.setenv("SDV_PY_NCAA_TERMS_BACKOFF", "inf")
    monkeypatch.setenv("SDV_PY_NCAA_TERMS_RETRIES", "x")
    assert (_from_env().terms_backoff, _from_env().terms_retries) == (300.0, 3)  # invalid -> default


def test_terms_gate_from_a_non_browser_transport_is_never_cached_or_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sleeps: "list[float]" = []
    monkeypatch.setattr("sportsdataverse.mbb.mbb_ncaa_fetch.time.sleep", sleeps.append)
    transport = FakeTransport([(200, _GATE)])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))  # default terms_retries / terms_backoff

    with pytest.raises(RuntimeError, match="terms gate needs the browser transport"):
        fetcher.fetch_html("teams/614563")
    assert not cached_path("teams/614563", cache_dir=tmp_path).exists()
    assert len(transport.calls) == 1
    assert sleeps == []  # unrecoverable: no backoff


def test_negative_terms_settings_are_clamped(tmp_path: Path) -> None:
    calls: "list[str]" = []
    cfg = NcaaFetchConfig(
        cache_dir=tmp_path, transport=_refusing_transport(calls), terms_backoff=-5.0, terms_retries=-1
    )
    with pytest.raises(RuntimeError, match="terms gate not accepted"):  # not ValueError / UnboundLocalError
        NcaaFetcher(cfg, proxy_pool=_POOL).fetch_html("teams/614563")
    assert len(calls) == 1


def test_cached_terms_gate_is_a_miss(tmp_path: Path) -> None:
    cache_file = cached_path("teams/614563", cache_dir=tmp_path)
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(_GATE, encoding="utf-8")
    transport = FakeTransport([(200, _CLEAN)])

    assert NcaaFetcher(_cfg(tmp_path, transport)).fetch_html("teams/614563") == _CLEAN
    assert len(transport.calls) == 1
    assert cache_file.read_text(encoding="utf-8") == _CLEAN  # healed in place


# --- the browser transport must honor a rotation (it is pinned at launch) ---


class _StopBeforeLaunch(Exception):
    pass


def test_browser_transport_relaunches_context_when_the_proxy_rotates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Playwright binds the proxy at LAUNCH. A rotation must relaunch the CONTEXT
    (not silently reuse proxy A -- the bug that burned one IP and stalled the
    backfill at 38%). It closes only the context, NOT the driver: stop/starting the
    driver per rotation crashed patchright (EPIPE)."""
    t = playwright_transport()
    t._page = object()  # pretend a live context on proxy A
    t._current_proxy = _POOL[0]

    def _boom() -> None:
        raise _StopBeforeLaunch  # stand in for the (real) context relaunch

    monkeypatch.setattr(t, "_close_context", _boom)

    # Same proxy -> reuse, no relaunch.
    t._ensure_page({"http": _POOL[0], "https": _POOL[0]})

    # Different proxy -> MUST close the CONTEXT + relaunch, not reuse proxy A.
    with pytest.raises(_StopBeforeLaunch):
        t._ensure_page({"http": _POOL[1], "https": _POOL[1]})


def test_rotation_uses_close_context_never_full_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """The EPIPE fix: a rotation relaunches only the CONTEXT (``_close_context``);
    it must NEVER call ``close()`` (which stops the Playwright driver). Stop/starting
    the driver per rotation is what crashed patchright during a backfill."""
    t = playwright_transport(relaunch_backoff=0.0)
    t._page = object()
    t._current_proxy = _POOL[0]
    calls: "list[str]" = []
    monkeypatch.setattr(t, "close", lambda: calls.append("close"))

    def _ctx() -> None:
        calls.append("_close_context")
        raise _StopBeforeLaunch  # stop before the real (browser-launching) relaunch

    monkeypatch.setattr(t, "_close_context", _ctx)
    with pytest.raises(_StopBeforeLaunch):
        t._ensure_page({"http": _POOL[1], "https": _POOL[1]})
    assert calls == ["_close_context"]  # context closed; full close()/driver-stop NOT called


# --- the wbb shim must inherit all of the above, by reference ---------------


def test_wbb_fetch_shim_is_the_same_implementation() -> None:
    """wbb_ncaa_fetch re-exports the mbb core BY REFERENCE, so every fix here
    reaches WBB with no duplication. Pin it: a copy-paste fork would silently
    strand WBB on the old, IP-burning behavior."""
    from sportsdataverse.mbb import mbb_ncaa_fetch as m
    from sportsdataverse.wbb import wbb_ncaa_fetch as w

    assert w.NcaaFetcher is m.NcaaFetcher
    assert w.playwright_transport is m.playwright_transport
    assert w.NcaaFetchConfig is m.NcaaFetchConfig
    assert w.get_config() is m.get_config()  # one process-wide config singleton


# --- binding directive 1: no direct-fetch mode -----------------------------


def test_no_proxy_configured_raises(tmp_path: Path) -> None:
    cfg = NcaaFetchConfig(cache_dir=tmp_path)  # no proxy_url, no proxybonanza pair
    fetcher = NcaaFetcher(cfg)
    with pytest.raises(RuntimeError, match="proxy"):
        fetcher.fetch_html("contests/1/play_by_play")


# --- binding directive 2: cache-first / fetch-once --------------------------


def test_cache_hit_makes_zero_transport_calls(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "<html>hi</html>")])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    first = fetcher.fetch_html("contests/123/play_by_play")
    assert first == "<html>hi</html>"
    assert len(transport.calls) == 1
    assert cached_path("contests/123/play_by_play", cache_dir=tmp_path).exists()

    second = fetcher.fetch_html("contests/123/play_by_play")
    assert second == first
    assert len(transport.calls) == 1  # no new transport call on cache hit


def test_force_refetch_overwrites_cache(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "v1"), (200, "v2")])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    assert fetcher.fetch_html("contests/1/play_by_play") == "v1"
    assert fetcher.fetch_html("contests/1/play_by_play", force=True) == "v2"
    assert len(transport.calls) == 2
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == "v2"


def test_is_cached_helper(tmp_path: Path) -> None:
    assert not is_cached("contests/1/play_by_play", cache_dir=tmp_path)
    transport = FakeTransport([(200, "x")])
    NcaaFetcher(_cfg(tmp_path, transport)).fetch_html("contests/1/play_by_play")
    assert is_cached("contests/1/play_by_play", cache_dir=tmp_path)


# --- host guard --------------------------------------------------------------


def test_other_host_url_rejected(tmp_path: Path) -> None:
    fetcher = NcaaFetcher(_cfg(tmp_path))
    with pytest.raises(ValueError, match="stats.ncaa.org"):
        fetcher.fetch_html("https://example.com/contests/1/play_by_play")


# --- ban-marker rotation -----------------------------------------------------


def test_ban_marker_response_rotates_proxy(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "Access Denied - captcha"), (200, "<html>ok</html>")])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1", "http://u:p@2.2.2.2:2"])

    text = fetcher.fetch_html("contests/1/play_by_play")

    assert text == "<html>ok</html>"
    assert len(transport.calls) == 2
    proxies_seen = {c[1]["http"] for c in transport.calls}
    assert proxies_seen == {"http://u:p@1.1.1.1:1", "http://u:p@2.2.2.2:2"}
    # the ban-page response is never the one written to cache
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == "<html>ok</html>"


def test_all_proxies_exhausted_raises(tmp_path: Path) -> None:
    transport = FakeTransport([(403, "forbidden"), (403, "forbidden"), (403, "forbidden")])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, max_retries=0, rotation_backoff=0.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])
    with pytest.raises(RuntimeError, match="rotating proxies"):
        fetcher.fetch_html("contests/1/play_by_play")


# --- rotation backoff ---------------------------------------------------------


def test_rotation_backoff_sleeps_between_retries_not_before_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Backoff paces the retry path only -- no sleep before the first attempt."""
    transport = FakeTransport(
        [(200, "Access Denied - captcha"), (200, "Access Denied - captcha"), (200, "<html>ok</html>")]
    )
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=0.5)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1", "http://u:p@2.2.2.2:2", "http://u:p@3.3.3.3:3"])

    sleeps: "list[float]" = []
    monkeypatch.setattr("sportsdataverse.mbb.mbb_ncaa_fetch.time.sleep", sleeps.append)

    text = fetcher.fetch_html("contests/1/play_by_play")

    assert text == "<html>ok</html>"
    assert sleeps == [0.5, 0.5]


def test_rotation_backoff_no_sleep_on_clean_first_hit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    transport = FakeTransport([(200, "<html>ok</html>")])
    cfg = NcaaFetchConfig(cache_dir=tmp_path, transport=transport, rotation_backoff=1.0)
    fetcher = NcaaFetcher(cfg, proxy_pool=["http://u:p@1.1.1.1:1"])

    sleeps: "list[float]" = []
    monkeypatch.setattr("sportsdataverse.mbb.mbb_ncaa_fetch.time.sleep", sleeps.append)

    fetcher.fetch_html("contests/1/play_by_play")

    assert sleeps == []


def test_rotation_backoff_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.mbb.mbb_ncaa_fetch import _from_env

    monkeypatch.setenv("SDV_PY_NCAA_ROTATION_BACKOFF", "2.5")
    assert _from_env().rotation_backoff == 2.5

    monkeypatch.setenv("SDV_PY_NCAA_ROTATION_BACKOFF", "abc")
    assert _from_env().rotation_backoff == 1.0

    # Non-finite values would reach time.sleep() and raise -- keep the default.
    for bad in ("inf", "-inf", "nan"):
        monkeypatch.setenv("SDV_PY_NCAA_ROTATION_BACKOFF", bad)
        assert _from_env().rotation_backoff == 1.0


# --- redaction ---------------------------------------------------------------


def test_repr_redacts_secrets() -> None:
    cfg = NcaaFetchConfig(
        proxy_url="http://secretlogin:secretpass@5.6.7.8:8080",
        proxybonanza_key="SUPERSECRETKEY",
        proxybonanza_pkg="pkg123",
    )
    r = repr(cfg)
    assert "secretlogin" not in r
    assert "secretpass" not in r
    assert "SUPERSECRETKEY" not in r
    assert str(cfg) == r
    assert "pkg123" in r  # package id is not a secret


# --- pool loader ---------------------------------------------------------------


def test_load_proxybonanza_pool_parses_canned_response() -> None:
    canned = json.dumps(
        {
            "data": {
                "login": "fakeuser",
                "password": "fakepass",
                "ippacks": [
                    {"ip": "9.9.9.1", "port_http": 8080},
                    {"ip": "9.9.9.2", "port_http": 8081},
                ],
            }
        }
    )

    def transport(url: str, headers: dict) -> "tuple[int, str]":
        assert url == "https://api.proxybonanza.com/v1/userpackages/pkg123.json"
        assert headers["Authorization"] == "fake-api-key"
        return 200, canned

    pool = load_proxybonanza_pool("fake-api-key", "pkg123", transport=transport)

    assert pool == [
        "http://fakeuser:fakepass@9.9.9.1:8080",
        "http://fakeuser:fakepass@9.9.9.2:8081",
    ]


def test_load_proxybonanza_pool_raises_on_error_status() -> None:
    def transport(url: str, headers: dict) -> "tuple[int, str]":
        return 401, "unauthorized"

    with pytest.raises(RuntimeError, match="status=401"):
        load_proxybonanza_pool("bad-key", "pkg123", transport=transport)


# --- path builders -------------------------------------------------------------


def test_path_builders_modern_and_legacy(tmp_path: Path) -> None:
    transport = FakeTransport([(200, "a")] * 6)
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    fetcher.fetch_game_pbp("4690813")
    fetcher.fetch_game_pbp("4690813", legacy=True)
    fetcher.fetch_game_box("4690813", period=2)
    fetcher.fetch_game_box("4690813", period=2, legacy=True)
    fetcher.fetch_team_roster("391", "2024")
    fetcher.fetch_team_schedule("391")

    urls = [c[0] for c in transport.calls]
    assert urls == [
        "https://stats.ncaa.org/contests/4690813/play_by_play",
        "https://stats.ncaa.org/game/play_by_play/4690813",
        "https://stats.ncaa.org/contests/4690813/box_score?period_no=2",
        "https://stats.ncaa.org/game/box_score/4690813?period_no=2",
        "https://stats.ncaa.org/teams/391/roster/2024",
        "https://stats.ncaa.org/teams/391/game_by_game",
    ]


# --- cache-key query-string sanitization ---------------------------------------


def test_query_string_cache_key_sanitization(tmp_path: Path) -> None:
    p1 = cached_path("contests/1/box_score?period_no=1", cache_dir=tmp_path)
    p2 = cached_path("contests/1/box_score?period_no=2", cache_dir=tmp_path)

    assert p1 != p2
    assert p1.parent == p2.parent
    assert p1.name == "box_score__period_no=1.html"
    assert p2.name == "box_score__period_no=2.html"
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).name == "play_by_play.html"


def test_query_string_unsafe_chars_sanitized(tmp_path: Path) -> None:
    p = cached_path("contests/1/box_score?a=1&b=hi there", cache_dir=tmp_path)
    assert p.name == "box_score__a=1&b=hi_there.html"


# --- UTF-8 round-trip ------------------------------------------------------


def test_utf8_roundtrip(tmp_path: Path) -> None:
    text = "Über Team – café"
    transport = FakeTransport([(200, text)])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))

    out = fetcher.fetch_html("contests/1/play_by_play")

    assert out == text
    assert cached_path("contests/1/play_by_play", cache_dir=tmp_path).read_text(encoding="utf-8") == text


# --- config surface ----------------------------------------------------------


def test_update_config_then_reset() -> None:
    reset_config()
    default_timeout = get_config().timeout
    update_config(timeout=999)
    assert get_config().timeout == 999
    reset_config()
    assert get_config().timeout == default_timeout


def test_update_config_unknown_key_raises() -> None:
    with pytest.raises(ValueError, match="Unknown config key"):
        update_config(not_a_real_field=1)
    reset_config()


def test_env_vars_populate_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SDV_PY_NCAA_CACHE_DIR", str(tmp_path))
    monkeypatch.setenv("SDV_PY_NCAA_PROXY_URL", "http://envuser:envpass@1.2.3.4:8080")
    monkeypatch.setenv("SDV_PY_PROXYBONANZA_KEY", "envkey")
    monkeypatch.setenv("SDV_PY_PROXYBONANZA_PKG", "envpkg")

    cfg = reset_config()

    assert cfg.cache_dir == tmp_path
    assert cfg.proxy_url == "http://envuser:envpass@1.2.3.4:8080"
    assert cfg.proxybonanza_key == "envkey"
    assert cfg.proxybonanza_pkg == "envpkg"

    monkeypatch.undo()
    reset_config()


# --- browser transport (suggested game-detail scraping method) --------------


def test_custom_transport_needs_no_proxy_pool(tmp_path: Path) -> None:
    """The no-direct-fetch guard is scoped to the default curl transport; an
    injected (browser / unblocker) transport runs pool-less and gets ``{}``
    for proxies."""
    transport = FakeTransport([(200, "<html>ok</html>")])
    fetcher = NcaaFetcher(NcaaFetchConfig(cache_dir=tmp_path, transport=transport))  # no proxy
    assert fetcher.fetch_html("contests/1/play_by_play") == "<html>ok</html>"
    assert len(transport.calls) == 1
    assert transport.calls[0][1] == {}  # empty pool -> direct, no proxy dict


def test_playwright_transport_shape() -> None:
    """The factory returns a callable, closeable, context-managed transport --
    all without importing Playwright (lazy until first fetch)."""
    t = playwright_transport()
    assert callable(t)
    with t as t2:
        assert t2 is t
    t.close()  # idempotent, browser never launched


def test_playwright_transport_missing_dep_raises() -> None:
    """patchright is not a hard dep; first use without it raises a clear hint."""
    if importlib.util.find_spec("patchright") is not None:
        pytest.skip("patchright installed -- ImportError path not exercised")
    t = playwright_transport()
    with pytest.raises(ImportError, match="[Pp]atchright"):
        t("https://stats.ncaa.org/contests/1/play_by_play", {}, {})


def test_with_browser_wires_transport_pool_less(tmp_path: Path) -> None:
    fetcher = NcaaFetcher.with_browser(NcaaFetchConfig(cache_dir=tmp_path))
    assert callable(fetcher.config.transport)
    assert hasattr(fetcher.config.transport, "close")
    assert fetcher._pool == []  # browser path needs no proxy pool
    fetcher.__exit__()  # closes the (never-launched) transport without error


def test_ban_check_ignores_blocked_shot_in_real_content(tmp_path: Path) -> None:
    """A real game page says "layup blocked" (blocked shots); the WAF-specific
    ban markers must NOT false-positive on it (regression: bare "blocked")."""
    real = "<html><body><table><tr><td>Bezhanishvili 2pt layup blocked missed</td></tr></table></body></html>"
    transport = FakeTransport([(200, real)])
    fetcher = NcaaFetcher(_cfg(tmp_path, transport))
    assert fetcher.fetch_html("contests/1613299/play_by_play") == real
    assert len(transport.calls) == 1  # accepted first response, no rotation


def test_sweep_orphan_profiles_reaps_only_dead_or_stale_unmarked(tmp_path):
    """Profiles stranded by killed runs are reaped; a live owner's profile never is."""
    import os
    import subprocess
    import sys
    import time

    from sportsdataverse.mbb import mbb_ncaa_fetch as f

    dead = subprocess.Popen([sys.executable, "-c", "pass"])
    dead.wait()

    def profile(name, owner=None, age_s=0):
        """An ncaa_pw_* dir, optionally owner-marked, with its mtime pushed back ``age_s``."""
        d = tmp_path / f"ncaa_pw_{name}"
        d.mkdir()
        if owner is not None:
            (d / f._OWNER_FILE).write_text(str(owner))
        stamp = time.time() - age_s
        os.utime(d, (stamp, stamp))
        return d

    live = profile("live", owner=os.getpid())
    orphan = profile("orphan", owner=dead.pid)
    old_unmarked = profile("old", age_s=2 * 86_400)
    new_unmarked = profile("new")
    unrelated = tmp_path / "patchright_udd_x"
    unrelated.mkdir()

    # Windows never probes owners (os.kill(pid, 0) is CTRL_C_EVENT there), so a
    # marked orphan is kept rather than reaped; everything else is the same.
    on_windows = os.name == "nt"
    assert f._sweep_orphan_profiles(str(tmp_path)) == (1 if on_windows else 2)
    assert live.exists() and new_unmarked.exists() and unrelated.exists()
    assert orphan.exists() is on_windows and not old_unmarked.exists()
