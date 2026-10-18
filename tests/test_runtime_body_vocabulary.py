"""A 2xx body that is not data is a failed fetch, and is never cached (CLAUDE.md "Error vocabulary").

The chokepoint body rules of ``_codegen_runtime._json_data`` / ``_text_body`` and
``errors._error_envelope_code`` / ``_string_route`` / ``_is_data_body`` (the ``dl_utils.download``
cache gate). They live here, not in ``tests/codegen``, because the coverage job ignores that
directory (R880-7); the helpers come from ``tests/codegen/test_runtime_errors.py``, which keeps
the per-runtime vocabulary matrix.
"""

from __future__ import annotations

import json
from typing import Any, Dict

import pytest

from sportsdataverse._codegen_runtime import _get
from sportsdataverse.errors import AssetFetchError
from tests.codegen.test_runtime_errors import FIX, _body, _serve


@pytest.mark.parametrize("body", ["null", "42", '"Too many requests"', "true"])
def test_a_2xx_scalar_body_is_a_failed_fetch(monkeypatch, body):
    # B-GB-16: a JSON scalar is no payload any parser reads; _json_body returned it as data.
    _serve(monkeypatch, 200, body)
    with pytest.raises(AssetFetchError, match="not an object or array"):
        _get("https://example.test/x")


@pytest.mark.parametrize(("code", "err"), [(400, ValueError), (429, AssetFetchError), (503, AssetFetchError)])
def test_a_2xx_error_envelope_is_classified_by_its_code(monkeypatch, code, err):
    # A-M13: ESPN's own envelope (espn_site_roster_400.json) served with HTTP 200 was data.
    envelope = {**json.loads(_body("espn_site_roster_400.json")), "code": code}
    _serve(monkeypatch, 200, json.dumps(envelope))
    with pytest.raises(err, match="a 2xx error envelope"):
        _get("https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/99999/roster")


def test_a_code_field_beside_data_is_not_an_error_envelope(monkeypatch):
    _serve(monkeypatch, 200, '{"code": 500, "message": "x", "items": []}')
    assert _get("https://example.test/x") == {"code": 500, "message": "x", "items": []}


def test_a_business_code_is_not_an_error_envelope(monkeypatch):
    # R880-5: only an HTTP-shaped code (400-599) is an error envelope; 1001 is a provider's own code.
    _serve(monkeypatch, 200, '{"code": 1001, "message": "ok"}')
    assert _get("https://example.test/x") == {"code": 1001, "message": "ok"}


_LAST_CHANGE = "https://api.openligadb.de/getlastchangedate/bl1/2025/1"


def test_openligadb_last_change_date_keeps_its_bare_iso_string(monkeypatch):
    # R880-1: /getlastchangedate answers a quoted ISO string by design (openligadb.yaml); the scalar
    # refusal (B-GB-16) made the wrapper always raise. Real capture, served through the real _get.
    from sportsdataverse.soccer import openligadb_last_change_date

    body = (FIX.parent / "openligadb" / "getlastchangedate__league__season__group.json").read_text(encoding="utf-8")
    _serve(monkeypatch, 200, body)
    assert _get(_LAST_CHANGE) == "2025-08-24T19:25:06.787"
    assert openligadb_last_change_date("bl1", "2025", "1", return_parsed=False) == "2025-08-24T19:25:06.787"
    assert openligadb_last_change_date("bl1", "2025", "1").height == 1


@pytest.mark.parametrize(
    ("url", "body"),
    [
        ("https://api.openligadb.de/getavailableleagues", '"2025-08-24T19:25:06.787"'),
        # R880-6a: the allowance is OpenLigaDB's host AND path, not the path on any host.
        ("https://example.test/getlastchangedate/bl1/2025/1", '"2025-08-24T19:25:06.787"'),
        (_LAST_CHANGE, "null"),
        (_LAST_CHANGE, "42"),
    ],
)
def test_the_string_allowance_is_that_route_only(monkeypatch, url, body):
    # Another route answering a string, or that route answering null / a number, is still a failed fetch.
    _serve(monkeypatch, 200, body)
    with pytest.raises(AssetFetchError, match="not an object or array"):
        _get(url)


def _memory_cache(monkeypatch) -> Dict[str, Any]:
    """A fresh in-memory response cache for one test (download() imports these per call)."""
    import sportsdataverse.cache as _cache

    store: Dict[str, Any] = {}
    monkeypatch.setattr(_cache, "get_cache_mode", lambda: "memory")
    monkeypatch.setattr(_cache, "cache_get", lambda url, params, ttl=None: store.get(url))
    monkeypatch.setattr(
        _cache,
        "cache_set",
        lambda url, params, body, ttl=None: store.__setitem__(url, body),
    )
    return store


@pytest.mark.parametrize("body", ['{"code": 503, "message": "Service Unavailable"}', "42", "null", '"x"'])
def test_a_refused_2xx_body_is_never_cached(monkeypatch, body):
    # R880-4: a 2xx error envelope or scalar was cached, so a transient failure re-raised for the whole TTL.
    # '"x"' (R880-6b): a JSON string is refused too unless its route is one of errors._STRING_ROUTES.
    calls = _serve(monkeypatch, 200, body)
    store = _memory_cache(monkeypatch)
    for _ in range(2):
        with pytest.raises(AssetFetchError):
            _get("https://example.test/x")
    assert len(calls) == 2 and store == {}


def test_data_and_the_allowed_string_are_still_cached(monkeypatch):
    calls = _serve(monkeypatch, 200, '"2025-08-24T19:25:06.787"')
    store = _memory_cache(monkeypatch)
    assert _get(_LAST_CHANGE) == _get(_LAST_CHANGE) == "2025-08-24T19:25:06.787"
    assert len(calls) == 1 and list(store.values()) == ["2025-08-24T19:25:06.787"]


@pytest.mark.parametrize("prefix", ["\n", "﻿", " \r\n\t"])
def test_a_page_behind_whitespace_or_a_bom_is_still_a_page(monkeypatch, prefix):
    # R880-3: the HTML check skips leading whitespace and a BOM before looking for "<".
    from sportsdataverse.mbb import torvik_runtime as rt

    _serve(
        monkeypatch,
        200,
        prefix + "<!DOCTYPE html><html>Just a moment...</html>",
        "text/html",
    )
    with pytest.raises(AssetFetchError, match="a page, not data"):
        rt._get("https://barttorvik.com/2025_team_results.csv")
