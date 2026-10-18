"""``errors._redact_secrets``: the credential shapes the first redaction pass missed (A-L65).

Every credential below is a fake. The shapes the earlier pass already covered are tested in
``tests/test_credential_redaction.py``.
"""

from __future__ import annotations

import time

import pytest

from sportsdataverse.errors import AssetFetchError, _redact_secrets


@pytest.mark.parametrize(
    "text, expected",
    [
        # userinfo: the proxy form download() documents and mbb_ncaa_fetch builds
        ("http://user:FAKEpw123@proxy.example:8080", "http://user:REDACTED@proxy.example:8080"),
        ("proxy https://u:p@10.0.0.1:3128/ failed", "proxy https://u:REDACTED@10.0.0.1:3128/ failed"),
        ("?appkey=FAKEAPP123&x=1", "?appkey=REDACTED&x=1"),
        ("?app_key=fake1", "?app_key=REDACTED"),
        ("?access_key=FAKEAK&b=2", "?access_key=REDACTED&b=2"),
        ("?subscription-key=FAKESUB", "?subscription-key=REDACTED"),
        ("Ocp-Apim-Subscription-Key: FAKESUB", "Ocp-Apim-Subscription-Key: REDACTED"),
        ("{'x-api-key': 'FAKE1234'}", "{'x-api-key': 'REDACTED'}"),
        ("?sig=fakesig&x=1", "?sig=REDACTED&x=1"),
        ("?signature=FAKESIGV", "?signature=REDACTED"),
        ("?auth=fakeauth", "?auth=REDACTED"),
        # a header echoed in a message: the bearer token, not only the word "Bearer"
        ("Authorization: Bearer fake.jwt.tok", "Authorization: REDACTED REDACTED"),
        ('{"Authorization": "Bearer fake.jwt.tok"}', '{"Authorization": "REDACTED"}'),
        ("sent bearer fake.jwt.tok", "sent bearer REDACTED"),
        # R897-3: Basic auth, an unquoted Cookie header line (every cookie on it), pass=
        ("Authorization: Basic ZmFrZTpmYWtlcHc=", "Authorization: REDACTED REDACTED"),
        ("Cookie: session=FAKEsess1; csrftoken=FAKEcsrf2", "Cookie: REDACTED"),
        ("{'Cookie': 'session=FAKEsess1; other=FAKE2'}", "{'Cookie': 'REDACTED'}"),
        ("?user=me&pass=FAKEpass1", "?user=me&pass=REDACTED"),
    ],
)
def test_redact_secrets_covers_userinfo_and_the_other_credential_names(text, expected):
    assert _redact_secrets(text) == expected
    assert _redact_secrets(expected) == expected  # idempotent


@pytest.mark.parametrize(
    "text",
    [
        "design=1&signal=2&author=me&authority=x&keyboard=q&assign=3",
        "http://host:8080/path?season=2025",  # a port is not a password
        "mailto:a@b.example",
        "https://x.example/@handle",
        "the flag bearer and the ball bearers",  # a short word after "bearer" is not a token
        "basic stats; passport=US&bypass=1&passes=12",
    ],
)
def test_redact_secrets_leaves_lookalikes_alone(text):
    assert _redact_secrets(text) == text


def test_redact_error_message_carries_no_proxy_password():
    err = AssetFetchError("proxy http://scraper:FAKEpw123@203.0.113.7:8080 refused the connection")
    assert "FAKEpw123" not in str(err)
    assert "203.0.113.7:8080" in str(err)


_ADVERSARIAL = {  # ~100 KB each
    "userinfo_starts": "://x:" * 25_000,
    "userinfo_no_at": "://" + "a:" * 50_000,
    "bearers": "bearer " * 15_000,
    "auth_pairs": "auth=" * 20_000,
    "sig_names": "sig" * 34_000,
    "cookie_lines": "cookie: a\n" * 10_000,
    "basics": "basic " * 16_000,
}


@pytest.mark.parametrize("shape", _ADVERSARIAL)
def test_redact_secrets_new_shapes_stay_linear(shape):
    start = time.perf_counter()
    _redact_secrets(_ADVERSARIAL[shape])
    # A quadratic scan of ~100 KB takes many seconds; 0.5 s leaves a slow CI runner ~30x headroom
    # over the ~15 ms these take locally, so the bound cannot flake yet still catches a blow-up.
    assert time.perf_counter() - start < 0.5


def test_redact_exception_drops_every_credential_header():
    """A failed request's ``request.headers`` lose ``X-Api-Key`` / ``x-api-secret`` /
    ``X-Authorization`` (the basketball runtimes' keys), not only Authorization and Cookie."""
    import requests

    from sportsdataverse.dl_utils import _redact_exception

    headers = {
        "X-Api-Key": "FAKE-key",
        "x-api-secret": "FAKE-secret",
        "X-Authorization": "FAKE-auth",
        "Authorization": "Bearer FAKE-bearer-token",
        "Accept": "application/json",
    }
    req = requests.Request("GET", "https://x.example/feed", headers=headers).prepare()
    err = requests.ConnectionError("connection refused", request=req)
    _redact_exception(err)
    assert dict(err.request.headers) == {"Accept": "application/json"}
