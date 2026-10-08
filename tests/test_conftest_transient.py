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


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ConnectionError("reset"),
        TimeoutError(),
        HTTPError("u", 503, "Service Unavailable", None, None),
        HTTPError("u", 429, "Too Many Requests", None, None),
        _wrapped(requests.exceptions.ReadTimeout("read timed out")),
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
    ],
)
def test_real_failures_are_not(exc):
    assert not _is_transient(exc)
