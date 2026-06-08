from unittest.mock import patch

from sportsdataverse._codegen_runtime import _csv, _get


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


def test_get_returns_empty_dict_on_download_none():
    with patch("sportsdataverse._codegen_runtime.download", return_value=None):
        assert _get("https://example.test/x") == {}
