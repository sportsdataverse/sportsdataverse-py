"""Truth-table tests for the proxy-layer outcome classifier.

Pure bookkeeping — no HTTP. The categories are the quarantine contract: only
``transport_err`` / ``blocked`` / ``blank`` may count against a proxy;
``server_err`` and ``notfound`` reached the host and must never quarantine.
"""

from __future__ import annotations

from sportsdataverse.scrape.stats.proxy import _QUARANTINE_CATS, classify, redact


def test_error_wins_over_status() -> None:
    assert classify(None, "", "timed out") == "transport_err"
    assert classify(200, "body", "conn reset") == "transport_err"


def test_ok_requires_a_body() -> None:
    assert classify(200, '{"resultSets": []}', None) == "ok"
    assert classify(200, "", None) == "blank"
    assert classify(200, "   \n", None) == "blank"


def test_notfound_is_400_and_404() -> None:
    assert classify(400, "", None) == "notfound"
    assert classify(404, "nope", None) == "notfound"


def test_server_err_is_5xx_and_never_quarantines() -> None:
    for status in (500, 502, 503, 599):
        assert classify(status, "", None) == "server_err"
    assert "server_err" not in _QUARANTINE_CATS
    assert "notfound" not in _QUARANTINE_CATS


def test_everything_else_is_blocked() -> None:
    assert classify(403, "", None) == "blocked"
    assert classify(429, "", None) == "blocked"
    assert classify(None, "", None) == "blocked"


def test_redact_strips_credentials() -> None:
    out = redact("http://user:secret@1.2.3.4:8080")
    assert "secret" not in out
    assert "1.2.3.4" in out
