"""``errors._redact_secrets``: the credential shapes the first redaction pass missed (A-L65).

Every credential below is a fake. The shapes the earlier pass already covered are tested in
``tests/test_credential_redaction.py``.
"""

from __future__ import annotations

import pytest

from sportsdataverse.errors import AssetFetchError, _redact_secrets
from tests.test_credential_redaction import assert_redaction_linear


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


_ADVERSARIAL = {  # each builds a string of about n characters
    "userinfo_starts": lambda n: "://x:" * (n // 5),
    "userinfo_no_at": lambda n: "://" + "a:" * (n // 2),
    "bearers": lambda n: "bearer " * (n // 7),
    "auth_pairs": lambda n: "auth=" * (n // 5),
    "sig_names": lambda n: "sig" * (n // 3),
    "cookie_lines": lambda n: "cookie: a\n" * (n // 10),
    "basics": lambda n: "basic " * (n // 6),
}


@pytest.mark.parametrize("shape", _ADVERSARIAL)
def test_redact_secrets_new_shapes_stay_linear(shape):
    assert_redaction_linear(_ADVERSARIAL[shape])


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


def test_object_rows_drops_null_rows_and_rejects_other_non_objects():
    """WB-1: the row check pandas 3 makes itself, run on every pandas. An NA-like row (JSON null, NaN,
    pd.NA) is no data: it is dropped (4.13), and never hides a bad row after it (R941-1)."""
    import pandas as pd

    from sportsdataverse.errors import SchemaDriftWarning, _first_row, _object_rows

    for na in (None, float("nan"), pd.NA):
        assert _object_rows([{"a": 1}, na]) == [{"a": 1}]
        assert _object_rows([na, {}, na]) == [{}]
        assert _object_rows([na, na]) == []
        rows = [{"a": 1}, na, {"b": 2}, {"a": 1}, {"c": 3}]
        assert _object_rows(rows) == [{"a": 1}, {"b": 2}, {"a": 1}, {"c": 3}]  # order kept, no dedup (R953-3)
        assert _first_row([na, na, {"b": 2}, {"a": 1}]) == {"b": 2}
        assert _first_row([na]) is None
    for bad in ("x", 1, [1]):
        with pytest.raises(TypeError, match=f"a row is {type(bad).__name__}, not an object"):
            _object_rows([{"a": 1}, bad])
    with pytest.raises(TypeError, match="a row is str, not an object"):
        _object_rows([None, "x"])
    with pytest.raises(TypeError, match="a row is list, not an object"):  # pd.isna([None]) is not a scalar NA (R941-9)
        _object_rows([[None]])
    # 4.14: one rule for the #941 sites and the provider flatteners. None is no rows; a non-list is drift.
    assert _object_rows(None) == [] and _object_rows(None, "p: k") == []
    for bad in (5, "x", {"a": 1}, {}, ""):
        with pytest.raises(TypeError, match=f"rows is {type(bad).__name__}, not a list"):
            _object_rows(bad)
    with pytest.warns(SchemaDriftWarning, match=r"^p: k: unexpected payload shape \(TypeError: a row is str") as w:
        assert _object_rows([None, {"a": 1}, "x"], "p: k", stacklevel=2) is None
    assert w[0].filename == __file__  # stacklevel counts from the helper: 2 is its caller (the default 3, the parser's)
    assert _object_rows([None, {"a": 1}], "p: k") == [{"a": 1}]
