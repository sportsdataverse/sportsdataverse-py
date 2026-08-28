from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from sportsdataverse.dl_utils import _MAX_RETRY_AFTER, _parse_retry_after, _retry_delay, download
from sportsdataverse.errors import NoESPNDataError


class TestRetryDelay:
    """Offline tests for the retry backoff helper (no network)."""

    def test_exponential_backoff_capped(self):
        # base=0.5: 0.5, 1, 2, 4, then capped at 4. jitter=False for the exact curve.
        assert _retry_delay(None, 0, jitter=False) == 0.5
        assert _retry_delay(None, 1, jitter=False) == 1.0
        assert _retry_delay(None, 3, jitter=False) == 4.0
        assert _retry_delay(None, 10, jitter=False) == 4.0  # capped

    def test_jitter_stays_within_half_to_full(self):
        # Default jitter spreads the exponential fallback over 50-100% of the
        # computed delay, so a thundering herd de-syncs. 50 draws stay in-band
        # and at least one differs from the deterministic value.
        vals = [_retry_delay(None, 3) for _ in range(50)]  # base curve = 4.0
        assert all(2.0 <= v <= 4.0 for v in vals)
        assert any(v != 4.0 for v in vals)

    def test_jitter_never_touches_retry_after(self):
        # The server-dictated Retry-After path is exact even with jitter on.
        resp = types.SimpleNamespace(headers={"Retry-After": "7"})
        assert all(_retry_delay(resp, 0) == 7.0 for _ in range(20))

    def test_honors_numeric_retry_after_header(self):
        resp = types.SimpleNamespace(headers={"Retry-After": "7"})
        assert _retry_delay(resp, 0) == 7.0  # server's ask beats the exponential 0.5

    def test_honors_long_retry_after_up_to_cap(self):
        # >16s legitimate waits are honored now (not clipped to the old cap*4).
        resp = types.SimpleNamespace(headers={"Retry-After": "45"})
        assert _retry_delay(resp, 0) == 45.0

    def test_retry_after_bounded_by_max(self):
        # An outsized value is clamped so a stray header can't park us for minutes.
        resp = types.SimpleNamespace(headers={"Retry-After": "100000"})
        assert _retry_delay(resp, 0) == _MAX_RETRY_AFTER

    def test_honors_http_date_retry_after(self):
        # RFC 7231 also allows an HTTP-date; we convert it to seconds-from-now.
        future = datetime.now(timezone.utc) + timedelta(seconds=30)
        resp = types.SimpleNamespace(headers={"Retry-After": format_datetime(future, usegmt=True)})
        delay = _retry_delay(resp, 0)
        assert 25 <= delay <= 30  # ~30s, with slack for elapsed test time

    def test_http_date_in_past_is_zero(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        assert _parse_retry_after(format_datetime(past, usegmt=True)) == 0.0

    def test_negative_retry_after_is_clamped_to_zero(self):
        # RFC 7231 disallows negatives; clamp to 0 instead of returning -5.0,
        # which would crash time.sleep(-5.0) and take down the retry loop.
        resp = types.SimpleNamespace(headers={"Retry-After": "-5"})
        assert _retry_delay(resp, 0) == 0.0
        assert _parse_retry_after("-5") == 0.0

    def test_bad_retry_after_falls_back_to_backoff(self):
        resp = types.SimpleNamespace(headers={"Retry-After": "soon"})
        assert _retry_delay(resp, 2, jitter=False) == 2.0  # unparseable -> exponential


class TestDownload:
    # Tests that the function can download a valid URL with default parameters
    def test_download_valid_url_default_params(self):
        url = "https://www.google.com"
        response = download(url)
        assert response.status_code == 200

    # Tests that the function can download a valid URL with custom parameters
    def test_download_valid_url_custom_params(self):
        url = "https://jsonplaceholder.typicode.com/posts"
        params = {"userId": 1}
        response = download(url, params=params)
        assert response.status_code == 200

    # Tests that the function can download a valid URL with custom headers
    def test_download_valid_url_custom_headers(self):
        url = "https://jsonplaceholder.typicode.com/posts"
        headers = {"User-Agent": "Mozilla/5.0"}
        response = download(url, headers=headers)
        assert response.status_code == 200

    # Tests that the function can download a valid URL with a proxy
    # def test_download_valid_url_with_proxy(self):
    #     url = "https://jsonplaceholder.typicode.com/posts"
    #     proxy = {"https": "https://localhost:8080"}
    #     response = download(url, proxy=proxy)
    #     assert response.status_code == 200

    # Tests that the function can download a valid URL with a very short timeout
    def test_download_valid_url_with_short_timeout(self):
        # `num_retries=0` so the test deterministically asserts the
        # exception propagates instead of waiting on the default
        # 16-attempt retry loop where a warmed connection pool can let
        # one of the retries succeed and the test fails with "DID NOT
        # RAISE".
        #
        # A fresh `requests.Session()` (not `download()`'s default shared
        # module-level session) is required here too: the two preceding
        # tests in this class already hit this same host through the
        # shared session, so a keep-alive connection is warm by the time
        # this test runs. Reusing it lets the request complete inside the
        # 1ms budget on a fast CI network (observed on windows-latest),
        # so the deadline never actually gets exercised. A cold session
        # forces a real DNS+TCP(+TLS) handshake within the deadline.
        #
        # Accept either Timeout or ConnectionError: at a 1ms deadline the
        # kernel can surface ENETUNREACH / ECONNRESET before requests has
        # a chance to raise its own Timeout, depending on which stage of
        # the TCP handshake the deadline fires in. Both outcomes prove
        # the bounded-deadline behavior we care about — the call did not
        # block past the budget.
        url = "https://jsonplaceholder.typicode.com/posts"
        timeout = 0.001
        with pytest.raises((requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            download(url, timeout=timeout, num_retries=0, session=requests.Session())

    # Tests that the function handles an invalid URL
    def test_download_invalid_url(self):
        # Same `num_retries=0` rationale as above — DNS failures are
        # already immediate, but we want the test to assert the
        # exception bubbles up regardless of retry budget.
        url = "https://thisisnotavalidurl.com"
        with pytest.raises(requests.exceptions.RequestException):
            download(url, num_retries=0)

    def test_download_does_not_retry_404(self, monkeypatch):
        """A 404 (NoESPNDataError) is a definitive "no data" answer — download must
        NOT burn the retry budget on it. Exactly one HTTP attempt even when
        num_retries is large (offline; session.get is stubbed to a 404)."""
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            calls["n"] += 1
            return types.SimpleNamespace(status_code=404, url=url, headers={})

        monkeypatch.setattr(requests.Session, "get", fake_get)
        with pytest.raises(NoESPNDataError):
            download("https://sports.core.api.espn.com/v2/_no_such_/roster", num_retries=15)
        assert calls["n"] == 1  # not 16

    def test_download_retries_transient_status_then_succeeds(self, monkeypatch):
        """A 503/429 comes back as a normal Response (requests doesn't raise on
        it) — download must retry the transient status and return the eventual
        200. Offline: session.get is stubbed and time.sleep is a no-op."""
        import sportsdataverse.cache as _cache

        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
        monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
        seq = [503, 503, 200]
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            code = seq[calls["n"]]
            calls["n"] += 1
            return types.SimpleNamespace(status_code=code, url=url, reason="x", headers={}, json=lambda: {})

        monkeypatch.setattr(requests.Session, "get", fake_get)
        resp = download("https://site.api.espn.com/x", num_retries=5)
        assert resp.status_code == 200
        assert calls["n"] == 3  # 503, 503, 200

    def test_download_returns_last_response_when_retry_budget_exhausted(self, monkeypatch):
        """A persistent 429 must NOT raise — once the budget is spent, return the
        429 Response so callers can key on ``.status_code`` (backward-compatible
        with the pre-retry behavior of returning whatever came back)."""
        import sportsdataverse.cache as _cache

        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
        monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            calls["n"] += 1
            return types.SimpleNamespace(
                status_code=429, url=url, reason="Too Many Requests", headers={}, json=lambda: {}
            )

        monkeypatch.setattr(requests.Session, "get", fake_get)
        resp = download("https://site.api.espn.com/y", num_retries=2)
        assert resp.status_code == 429
        assert calls["n"] == 3  # 1 initial + 2 retries

    def test_download_interleaved_conn_error_then_status_returns_response(self, monkeypatch):
        """A connection error on an early attempt sets ``last_exc``; if the FINAL
        attempt is a retryable status, the loop must still RETURN that response
        rather than fall through to ``raise last_exc`` and surface the stale
        connection exception. Regression for the interleaved conn-error + status
        retry path (status_budget == attempts-1)."""
        import sportsdataverse.cache as _cache

        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
        monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
        seq = ["boom", 503, 503]  # conn error, then persistent 503
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            item = seq[calls["n"]]
            calls["n"] += 1
            if item == "boom":
                raise requests.exceptions.ConnectionError("boom")
            return types.SimpleNamespace(status_code=item, url=url, reason="x", headers={}, json=lambda: {})

        monkeypatch.setattr(requests.Session, "get", fake_get)
        resp = download("https://site.api.espn.com/w", num_retries=2)
        assert resp.status_code == 503  # returned, NOT raised as the stale ConnectionError
        assert calls["n"] == 3

    def test_download_does_not_retry_non_retryable_status(self, monkeypatch):
        """A 401 (not in the retry set) is a definitive answer — one attempt, no
        retry, even with a large budget."""
        import sportsdataverse.cache as _cache

        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            calls["n"] += 1
            return types.SimpleNamespace(status_code=401, url=url, reason="Unauthorized", headers={}, json=lambda: {})

        monkeypatch.setattr(requests.Session, "get", fake_get)
        resp = download("https://site.api.espn.com/z", num_retries=5)
        assert resp.status_code == 401
        assert calls["n"] == 1

    def test_download_status_retries_capped_below_num_retries(self, monkeypatch):
        """A persistent 429 with a large connection budget (num_retries=15) is
        capped at _MAX_STATUS_RETRIES (4) status retries — 5 requests total, not
        16 — so we don't hammer a host already signalling back-off."""
        import sportsdataverse.cache as _cache

        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "off")
        monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)
        calls = {"n": 0}

        def fake_get(self, url, **kwargs):
            calls["n"] += 1
            return types.SimpleNamespace(
                status_code=429, url=url, reason="Too Many Requests", headers={}, json=lambda: {}
            )

        monkeypatch.setattr(requests.Session, "get", fake_get)
        resp = download("https://site.api.espn.com/capped", num_retries=15)
        assert resp.status_code == 429
        assert calls["n"] == 5  # 1 initial + 4 capped status retries

    def test_download_does_not_cache_non_2xx(self, monkeypatch):
        """A retryable status returned after the budget is spent must NOT be
        written to the cache; a 2xx still is."""
        import sportsdataverse.cache as _cache

        writes = []
        monkeypatch.setattr(_cache, "get_cache_mode", lambda: "memory")
        monkeypatch.setattr(_cache, "cache_get", lambda *a, **k: None)
        monkeypatch.setattr(_cache, "cache_set", lambda url, params, body, ttl=None: writes.append(url))
        monkeypatch.setattr("sportsdataverse.dl_utils.time.sleep", lambda *a, **k: None)

        def _resp(code):
            def fake_get(self, url, **kwargs):
                return types.SimpleNamespace(status_code=code, url=url, reason="x", headers={}, json=lambda: {})

            return fake_get

        monkeypatch.setattr(requests.Session, "get", _resp(503))
        download("https://site.api.espn.com/nocache", num_retries=1)
        assert writes == []  # 503 body never cached

        monkeypatch.setattr(requests.Session, "get", _resp(200))
        download("https://site.api.espn.com/docache", num_retries=1)
        assert writes == ["https://site.api.espn.com/docache"]  # 200 cached


# ---------------------------------------------------------------------------
# Env-tunable retry budget
#
# The library defaults (15 retries / 30s) suit a scraper that must not lose a
# game, but they are wrong for a test suite: one unreachable host parks a single
# call for 15 x 30s plus backoff, which is how CI runs reached 60-100 minutes.
# These lock the contract that makes CI able to bound it WITHOUT changing what
# ordinary callers get.
# ---------------------------------------------------------------------------


def test_retry_budget_defaults_are_unchanged(monkeypatch):
    """No env set -> the historical 15 / 30 defaults, exactly as before."""
    from sportsdataverse import dl_utils as d

    monkeypatch.delenv(d._ENV_RETRIES, raising=False)
    monkeypatch.delenv(d._ENV_TIMEOUT, raising=False)
    assert d._env_int(d._ENV_RETRIES, d._DEFAULT_RETRIES) == 15
    assert d._env_int(d._ENV_TIMEOUT, d._DEFAULT_TIMEOUT) == 30


def test_retry_budget_env_override(monkeypatch):
    from sportsdataverse import dl_utils as d

    monkeypatch.setenv(d._ENV_RETRIES, "3")
    monkeypatch.setenv(d._ENV_TIMEOUT, "10")
    assert d._env_int(d._ENV_RETRIES, d._DEFAULT_RETRIES) == 3
    assert d._env_int(d._ENV_TIMEOUT, d._DEFAULT_TIMEOUT) == 10


@pytest.mark.parametrize("bad", ["", "abc", "0", "-5", "3.5"])
def test_malformed_env_falls_back_rather_than_raising(monkeypatch, bad):
    """A typo in an env var must not take the process down.

    This runs on EVERY request, so raising here would turn a harmless
    misconfiguration into a total outage. Non-positive values are ignored too --
    zero retries with a zero timeout would make every call fail instantly.
    """
    from sportsdataverse import dl_utils as d

    monkeypatch.setenv(d._ENV_RETRIES, bad)
    assert d._env_int(d._ENV_RETRIES, d._DEFAULT_RETRIES) == 15


def test_explicit_kwarg_beats_the_environment(monkeypatch):
    """An explicit ``num_retries=`` must win, or callers lose control in CI.

    ``cfb_fourth_down`` and ``ep_wp`` both pass ``num_retries=5`` deliberately;
    the env is a floor for callers that cannot pass kwargs, not an override of
    those that can.
    """
    from sportsdataverse import dl_utils as d

    seen = {}

    class _Resp:
        status_code = 200
        content = b"{}"
        url = "https://example.invalid/x"
        headers: dict = {}

        def json(self):
            return {}

    def fake_get(url, **kw):
        seen["timeout"] = kw.get("timeout")
        return _Resp()

    monkeypatch.setenv(d._ENV_TIMEOUT, "99")
    session = types.SimpleNamespace(get=fake_get)
    d.download("https://example.invalid/x", session=session, timeout=7)
    assert seen["timeout"] == 7, "explicit timeout kwarg was overridden by the environment"
