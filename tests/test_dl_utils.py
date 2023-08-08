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
        url = "https://jsonplaceholder.typicode.com/posts"
        timeout = 0.001
        with pytest.raises(requests.exceptions.Timeout):
            download(url, timeout=timeout)

    # Tests that the function handles an invalid URL
    def test_download_invalid_url(self):
        url = "https://thisisnotavalidurl.com"
        with pytest.raises(requests.exceptions.RequestException):
            download(url)
