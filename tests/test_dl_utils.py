from __future__ import annotations

import pytest
import requests

from sportsdataverse.dl_utils import download


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
