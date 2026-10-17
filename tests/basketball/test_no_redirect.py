"""Credential-bearing families never follow a redirect: the header must not leave the host.

``cebl`` (X-Api-Key), ``bnxt`` (X-Authorization) and ``scb`` (x-api-key / x-api-secret) send their requests
through ``basketball_runtime.NOREDIRECT_SESSION``. Each test mounts an adapter that answers every request with a
3xx to another host and checks that the 3xx is a fetch failure and that the other host is never requested.
"""

from __future__ import annotations

from typing import Any, List

import pytest
import requests
from requests.adapters import BaseAdapter

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.bnxt import bnxt_runtime as bnxt
from sportsdataverse.basketball.cebl import cebl_runtime as cebl
from sportsdataverse.basketball.scb import scb_runtime as scb
from sportsdataverse.errors import AssetFetchError

ELSEWHERE = "https://elsewhere.example/steal"


class _Redirector(BaseAdapter):
    """Answer every request with a 302 to ELSEWHERE and remember what was asked."""

    def __init__(self) -> None:
        super().__init__()
        self.seen: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        self.seen.append(request)
        resp = requests.Response()
        resp.status_code = 302
        resp.headers["Location"] = ELSEWHERE
        resp.url = request.url or ""
        resp.request = request
        resp._content = b""
        return resp

    def close(self) -> None:  # pragma: no cover - nothing to release
        pass


@pytest.fixture
def redirecting(monkeypatch):
    monkeypatch.setattr(bb, "_polite_wait", lambda *a, **k: None)
    sess = bb.NoRedirectSession()
    adapter = _Redirector()
    sess.mount("https://", adapter)
    return sess, adapter


def _only_on_host(adapter: _Redirector, *hosts: str) -> None:
    assert adapter.seen, "the scripted adapter saw no request"
    for req in adapter.seen:
        assert req.url.split("/")[2] in hosts, req.url


def test_the_production_sessions_do_not_follow_redirects():
    assert isinstance(cebl.SESSION, bb.NoRedirectSession)
    assert isinstance(bnxt.SESSION, bb.NoRedirectSession)
    assert scb.NOREDIRECT_SESSION is bb.NOREDIRECT_SESSION
    assert not isinstance(bb.SESSION, bb.NoRedirectSession)


def test_cebl_does_not_follow_a_redirect(redirecting, monkeypatch):
    sess, adapter = redirecting
    monkeypatch.setattr(cebl, "SESSION", sess)
    monkeypatch.setitem(cebl._key, "value", None)
    with pytest.raises(AssetFetchError, match="302"):
        cebl._call("/v1/games")
    _only_on_host(adapter, "api.data.cebl.ca", "cebl-stats-hub.web.app")


def test_bnxt_does_not_follow_a_redirect(redirecting, monkeypatch):
    sess, adapter = redirecting
    monkeypatch.setattr(bnxt, "SESSION", sess)
    monkeypatch.setitem(bnxt._key, "value", None)
    with pytest.raises(AssetFetchError, match="302"):
        bnxt._call("/wp-json/sportspress/v2/teams")
    _only_on_host(adapter, bnxt.API.split("/")[2], bnxt.SITE.split("/")[2])


def test_scb_does_not_follow_a_redirect(redirecting, monkeypatch):
    sess, adapter = redirecting
    monkeypatch.setattr(scb, "NOREDIRECT_SESSION", sess)
    monkeypatch.setattr(scb, "_headers", lambda refresh=False: {"x-api-key": "publicWebUser", "x-api-secret": "s"})
    with pytest.raises(AssetFetchError):
        scb._get(scb.HOSTS["bbl"] + "/seasons")
    _only_on_host(adapter, "api.basketball-bundesliga.de")
