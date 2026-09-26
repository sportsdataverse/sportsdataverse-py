"""Offline tests for the PFF Developer API runtime (``sportsdataverse.nfl.pff_api_runtime``)."""

import json

import pytest

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.nfl import pff_api_runtime as rt

URL = "https://api.pff.com/v1/leagues"


def _err(code, status_detail=None):
    return json.dumps(
        {"error": {"code": code, "message": f"{code} happened", "request_id": "rid-1", "details": status_detail}}
    )


class Recorder:
    def __init__(self, status=200, text='{"leagues": []}'):
        self.status, self.text, self.calls = status, text, []

    def __call__(self, url, params, headers):
        self.calls.append((url, params, headers))
        return self.status, self.text


@pytest.fixture(autouse=True)
def _no_env_key(monkeypatch):
    for name in rt._KEY_ENV:
        monkeypatch.delenv(name, raising=False)


def test_explicit_key_sends_bearer_and_drops_none_params():
    t = Recorder()
    assert rt._get(URL, {"league": "nfl", "week": None}, api_key="ak_x", transport=t) == {"leagues": []}
    url, params, headers = t.calls[0]
    assert params == {"league": "nfl"}
    assert headers["Authorization"] == "Bearer ak_x"


def test_key_precedence(monkeypatch):
    monkeypatch.setenv("PFF_API_KEY", "ak_pff")
    t = Recorder()
    rt._get(URL, transport=t)
    assert t.calls[-1][2]["Authorization"] == "Bearer ak_pff"
    monkeypatch.setenv("SDV_PY_PFF_API_KEY", "ak_sdv")
    rt._get(URL, transport=t)
    assert t.calls[-1][2]["Authorization"] == "Bearer ak_sdv"
    rt._get(URL, api_key="ak_arg", transport=t)
    assert t.calls[-1][2]["Authorization"] == "Bearer ak_arg"
    rt._get(URL, headers={"Authorization": "Bearer ak_hdr"}, api_key="ak_arg", transport=t)
    assert t.calls[-1][2]["Authorization"] == "Bearer ak_hdr"


def test_no_key_raises_before_any_request():
    t = Recorder()
    with pytest.raises(RuntimeError, match="PFF_API_KEY"):
        rt._get(URL, transport=t)
    assert t.calls == []


@pytest.mark.parametrize("text", ["", "   ", "not json", "[1, 2]"])
def test_unusable_200_body_is_an_unknown_answer_not_an_empty_one(text):
    with pytest.raises(AssetFetchError, match="non-object body"):
        rt._get(URL, api_key="k", transport=Recorder(200, text))


def test_gateway_error_with_a_string_error_field_still_maps():
    with pytest.raises(AssetFetchError, match="invalid_token"):
        rt._get(URL, api_key="k", transport=Recorder(401, '{"error": "invalid_token"}'))


def test_explicit_key_is_stripped():
    t = Recorder()
    rt._get(URL, api_key=" ak_x\n", transport=t)
    assert t.calls[0][2]["Authorization"] == "Bearer ak_x"


@pytest.mark.parametrize("status", [400, 422])
def test_rejected_request_is_value_error(status):
    with pytest.raises(ValueError, match="invalid_parameter: invalid_parameter happened"):
        rt._get(URL, api_key="k", transport=Recorder(status, _err("invalid_parameter")))


@pytest.mark.parametrize(
    "status,code,detail",
    [
        (401, "unauthorized", {"reason": "revoked"}),
        (403, "forbidden", {"upstream_status": 403}),
        (429, "rate_limited", {"scope": "read"}),
        (502, "upstream_error", None),
        (504, "upstream_timeout", None),
    ],
)
def test_refused_or_failed_fetch_is_asset_fetch_error_not_empty(status, code, detail):
    with pytest.raises(AssetFetchError) as exc:
        rt._get(URL, api_key="k", transport=Recorder(status, _err(code, detail)))
    msg = str(exc.value)
    assert f"HTTP {status}" in msg and code in msg and "rid-1" in msg


def test_default_transport_never_retries_a_403(monkeypatch):
    seen = {}

    class Resp:
        status_code, text = 200, '{"ok": true}'

    def fake_download(**kw):
        seen.update(kw)
        return Resp()

    monkeypatch.setattr(rt, "download", fake_download)
    assert rt._get(URL, api_key="k") == {"ok": True}
    assert 403 not in seen["retry_statuses"] and {429, 502, 503, 504} <= set(seen["retry_statuses"])
    # the package cache keys on URL+params, not the key: PFF bodies must never go through it
    assert seen["cache_ttl"].total_seconds() == 0


RESTRICTED = json.dumps({"passing_summary": [{"player_id": 1}], "restricted": ["grades_pass", "grades_offense"]})


def test_withheld_columns_warn_by_default_and_return_the_partial_body(monkeypatch):
    monkeypatch.delenv("SDV_PY_PFF_STRICT", raising=False)
    with pytest.warns(UserWarning, match="withheld columns.*grades_pass, grades_offense"):
        body = rt._get(URL, api_key="k", transport=Recorder(200, RESTRICTED))
    assert body["passing_summary"] == [{"player_id": 1}]


def test_withheld_columns_raise_when_strict_by_kwarg_or_env(monkeypatch):
    monkeypatch.delenv("SDV_PY_PFF_STRICT", raising=False)
    with pytest.raises(AssetFetchError, match="withheld columns"):
        rt._get(URL, api_key="k", transport=Recorder(200, RESTRICTED), strict=True)
    monkeypatch.setenv("SDV_PY_PFF_STRICT", "1")
    with pytest.raises(AssetFetchError, match="withheld columns"):
        rt._get(URL, api_key="k", transport=Recorder(200, RESTRICTED))
    # an explicit strict=False beats the environment
    with pytest.warns(UserWarning):
        rt._get(URL, api_key="k", transport=Recorder(200, RESTRICTED), strict=False)


def test_empty_restricted_list_is_silent(recwarn):
    rt._get(URL, api_key="k", transport=Recorder(200, json.dumps({"leagues": [], "restricted": []})))
    assert not [w for w in recwarn if "withheld" in str(w.message)]


def test_generated_wrapper_forwards_strict(monkeypatch):
    from sportsdataverse.nfl import pff_api

    monkeypatch.setattr(rt, "_default_transport", Recorder(200, RESTRICTED))
    with pytest.raises(AssetFetchError, match="withheld"):
        pff_api.pff_api_facet_passing_summary(league="ufl", season="2025", api_key="k", strict=True)
