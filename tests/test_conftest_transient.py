"""Which exceptions make a live test skip instead of fail (tests/conftest.py)."""

from __future__ import annotations

from urllib.error import HTTPError

import pytest
import requests

from sportsdataverse.errors import AssetFetchError
from tests.conftest import _is_transient


def _wrapped(cause: BaseException) -> AssetFetchError:
    try:
        raise cause
    except BaseException as e:  # noqa: BLE001
        try:
            raise AssetFetchError("fetch failed") from e
        except AssetFetchError as outer:
            return outer


def _requests_http_error(status: int) -> requests.HTTPError:
    resp = requests.Response()
    resp.status_code = status
    return requests.HTTPError(f"{status} Error", response=resp)


def _suppressed_timeout() -> AssertionError:
    """``raise ... from None`` inside ``except Timeout:``: the timeout is not the failure."""
    try:
        try:
            raise requests.exceptions.ReadTimeout("read timed out")
        except requests.exceptions.ReadTimeout:
            raise AssertionError("wrong column") from None
    except AssertionError as e:
        return e


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ConnectionError("reset"),
        TimeoutError(),
        HTTPError("u", 503, "Service Unavailable", None, None),
        HTTPError("u", 429, "Too Many Requests", None, None),
        _wrapped(requests.exceptions.ReadTimeout("read timed out")),
        _requests_http_error(503),
        _requests_http_error(429),
        AssetFetchError("api.example.test /v1/x answered HTTP 503: upstream unavailable"),
        AssetFetchError("release asset fetch failed with HTTP 502: https://x/y.parquet"),
    ],
)
def test_upstream_trouble_is_transient(exc):
    assert _is_transient(exc)


@pytest.mark.parametrize(
    "exc",
    [
        ValueError("schema drift"),
        AssertionError("wrong column"),
        HTTPError("u", 404, "Not Found", None, None),
        AssetFetchError("403 Forbidden"),
        AssetFetchError("api.example.test /v1/x answered HTTP 403: entitlement"),
        _requests_http_error(404),
        _suppressed_timeout(),
    ],
)
def test_real_failures_are_not(exc):
    assert not _is_transient(exc)
