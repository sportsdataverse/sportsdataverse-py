from unittest.mock import patch

import pytest

from sportsdataverse._codegen_runtime import _csv, _get
from sportsdataverse.errors import AssetFetchError


def test_csv_joins_iterables_and_passes_scalars():
    assert _csv([1, 2, 3]) == "1,2,3"
    assert _csv(("a", "b")) == "a,b"
    assert _csv("x") == "x"
    assert _csv(None) is None


def test_get_strips_none_params_and_returns_json():
    class FakeResp:
        def json(self):
            return {"ok": True}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        out = _get("https://example.test/x", params={"a": 1, "b": None})
    assert out == {"ok": True}
    assert dl.call_args.kwargs["params"] == {"a": 1}


@pytest.mark.parametrize("kind", ["data", "json"])
def test_get_sends_a_body_through_download_and_strips_none_fields(kind):
    class FakeResp:
        def json(self):
            return {"ok": True}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        out = _get("https://example.test/x", params={"page": 1, "q": None}, **{kind: {"a": 1, "b": None}})
    assert out == {"ok": True}
    assert dl.call_args.kwargs["params"] == {"page": 1}
    assert dl.call_args.kwargs[kind] == {"a": 1}
    assert ("json" if kind == "data" else "data") not in dl.call_args.kwargs  # a GET call stays byte-identical


def test_get_raises_on_download_none():
    # No response is a failed fetch, never an empty payload (was ``{}``).
    with patch("sportsdataverse._codegen_runtime.download", return_value=None):
        with pytest.raises(AssetFetchError, match="no response"):
            _get("https://example.test/x")


def test_get_refetches_a_truncated_json_body_but_not_an_html_one(monkeypatch):
    import sportsdataverse._codegen_runtime as rt

    class Resp:
        status_code = 200

        def __init__(self, text):
            self.text = text

        def json(self):
            import json

            return json.loads(self.text)

    monkeypatch.setattr(rt, "_TRUNCATED_JSON_DELAY", 0)
    bodies = iter(['{"games": {"a": 1', '{"games": {"a": 1}}'])
    with patch("sportsdataverse._codegen_runtime.download", side_effect=lambda **k: Resp(next(bodies))) as dl:
        assert _get("https://example.test/x") == {"games": {"a": 1}}
    assert dl.call_count == 2

    with patch("sportsdataverse._codegen_runtime.download", return_value=Resp("<html>terms</html>")) as dl:
        with pytest.raises(AssetFetchError, match="non-JSON body"):
            _get("https://example.test/x")
    assert dl.call_count == 1  # an HTML 200 is an answer, not a cut-off body
