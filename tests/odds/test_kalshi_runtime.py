"""Kalshi second-host failover: only a failed fetch (connection error, 5xx) moves to external-api.

The body served is the real /exchange/status capture; the transport is replaced so nothing hits the network.
"""

import json
import logging
import pathlib

import pytest
import requests
import yaml

from sportsdataverse import _codegen_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.odds import kalshi_exchange_status, kalshi_runtime
from sportsdataverse.odds.kalshi_runtime import FALLBACK_HOST, PRIMARY_HOST

STATUS = json.loads(
    (pathlib.Path(__file__).parent.parent / "fixtures" / "kalshi" / "exchange__status.json").read_text(encoding="utf-8")
)


class _Resp:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body) if body is not None else ""

    def json(self):
        if self._body is None:
            raise ValueError("no body")
        return self._body


class _Calls(list):
    """URLs requested, in order; ``sent`` keeps each call's full kwargs."""

    def __init__(self):
        super().__init__()
        self.sent = []


@pytest.fixture
def transport(monkeypatch):
    """Patch the shared ``download``: ``answers[host]`` is a response or an exception per host."""
    calls, answers = _Calls(), {}

    def fake(**kwargs):
        url = kwargs["url"]
        calls.append(url)
        calls.sent.append(kwargs)
        answer = answers[PRIMARY_HOST if url.startswith(PRIMARY_HOST) else FALLBACK_HOST]
        if isinstance(answer, BaseException):
            raise answer
        return answer

    monkeypatch.setattr(rt, "download", fake)
    return calls, answers


def test_wrappers_use_the_failover_getter():
    assert kalshi_exchange_status.__globals__["_get"] is kalshi_runtime._get


def test_healthy_primary_is_the_only_call(transport):
    calls, answers = transport
    answers[PRIMARY_HOST] = _Resp(200, STATUS)
    assert kalshi_exchange_status(return_parsed=False) == STATUS
    assert calls == [f"{PRIMARY_HOST}/exchange/status"]


@pytest.mark.parametrize(
    "failure",
    [requests.ConnectionError("dns"), requests.Timeout("slow"), _Resp(502), _Resp(503)],
    ids=["connection", "timeout", "502", "503"],
)
def test_connection_error_or_5xx_moves_to_the_second_host(transport, failure, caplog):
    calls, answers = transport
    answers[PRIMARY_HOST], answers[FALLBACK_HOST] = failure, _Resp(200, STATUS)
    with caplog.at_level(logging.WARNING, logger=kalshi_runtime.__name__):
        assert kalshi_exchange_status(return_parsed=False) == STATUS
    assert calls == [f"{PRIMARY_HOST}/exchange/status", f"{FALLBACK_HOST}/exchange/status"]
    assert "retrying on https://external-api.kalshi.com" in caplog.text


@pytest.mark.parametrize(
    ("status", "error"),
    [
        (400, ValueError),
        (422, ValueError),
        (401, AssetFetchError),
        (403, AssetFetchError),
        (408, AssetFetchError),
        (429, AssetFetchError),
    ],
)
def test_a_4xx_never_fails_over(transport, status, error):
    calls, answers = transport
    answers[PRIMARY_HOST] = _Resp(status, {"error": "no"})
    with pytest.raises(error):
        kalshi_exchange_status(return_parsed=False)
    assert calls == [f"{PRIMARY_HOST}/exchange/status"]


def test_a_404_never_fails_over(transport):
    calls, answers = transport
    answers[PRIMARY_HOST] = NoDataError("unknown ticker")  # download raises this itself on a 404
    with pytest.raises(NoDataError):
        kalshi_exchange_status(return_parsed=False)
    assert calls == [f"{PRIMARY_HOST}/exchange/status"]


def test_both_hosts_down_is_a_failed_fetch(transport, caplog):
    calls, answers = transport
    answers[PRIMARY_HOST], answers[FALLBACK_HOST] = _Resp(500), requests.ConnectionError("down")
    with caplog.at_level(logging.WARNING, logger=kalshi_runtime.__name__), pytest.raises(AssetFetchError):
        kalshi_exchange_status(return_parsed=False)
    assert len(calls) == 2


def test_query_and_path_carry_over(transport):
    calls, answers = transport
    answers[PRIMARY_HOST], answers[FALLBACK_HOST] = _Resp(504), _Resp(200, {"markets": []})
    kalshi_runtime._get(f"{PRIMARY_HOST}/markets/KXNFLGAME-26NOV26CHIDET-DET/orderbook", params={"depth": "3"})
    assert calls[-1] == f"{FALLBACK_HOST}/markets/KXNFLGAME-26NOV26CHIDET-DET/orderbook"
    first, second = calls.sent[-2:]
    assert second["params"] == first["params"] == {"depth": "3"}
    assert {k: v for k, v in second.items() if k != "url"} == {k: v for k, v in first.items() if k != "url"}


def test_a_malformed_request_does_not_fail_over(transport):
    calls, answers = transport
    answers[PRIMARY_HOST] = requests.exceptions.InvalidURL("bad")
    with pytest.raises(AssetFetchError):
        kalshi_exchange_status(return_parsed=False)
    assert len(calls) == 1


def test_primary_host_is_the_generated_family_host():
    spec = yaml.safe_load(
        (pathlib.Path(__file__).parents[2] / "tools/codegen/endpoints/kalshi.yaml").read_text("utf-8")
    )
    assert spec["host"] == PRIMARY_HOST  # a drift here would silently switch the failover off


def test_a_caller_transport_is_honoured_and_still_fails_over():
    seen = []

    def mine(**kwargs):
        seen.append(kwargs["url"])
        return _Resp(503) if kwargs["url"].startswith(PRIMARY_HOST) else _Resp(200, STATUS)

    assert kalshi_exchange_status(return_parsed=False, transport=mine) == STATUS
    assert seen == [f"{PRIMARY_HOST}/exchange/status", f"{FALLBACK_HOST}/exchange/status"]
