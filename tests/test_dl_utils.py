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
        # base=0.5: 0.5, 1, 2, 4, then capped at 4.
        assert _retry_delay(None, 0) == 0.5
        assert _retry_delay(None, 1) == 1.0
        assert _retry_delay(None, 3) == 4.0
        assert _retry_delay(None, 10) == 4.0  # capped

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
        assert _retry_delay(resp, 2) == 2.0  # unparseable -> exponential


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
        # Accept either Timeout or ConnectionError: at a 1ms deadline the
        # kernel can surface ENETUNREACH / ECONNRESET before requests has
        # a chance to raise its own Timeout, depending on which stage of
        # the TCP handshake the deadline fires in. Both outcomes prove
        # the bounded-deadline behavior we care about — the call did not
        # block past the budget.
        url = "https://jsonplaceholder.typicode.com/posts"
        timeout = 0.001
        with pytest.raises((requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
            download(url, timeout=timeout, num_retries=0)

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
