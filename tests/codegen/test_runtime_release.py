"""404-safe release-parquet helper + season-list normalization (generated loaders).

``_read_release_parquet`` reads the asset DIRECTLY with Arrow and uses
:func:`sportsdataverse.dl_utils.download` only to *classify* a failure. These tests
lock both halves of that split:

* the success path must not touch the HTTP gateway at all -- buffering the bytes
  through it measured +33% peak RSS and +68% wall on a 59 MB asset (issue #397),
  so ``test_success_path_never_calls_the_transport`` is a deliberate regression
  guard, not an implementation detail;
* a failed read must be classified by what the SERVER says, not by a substring of
  the reader's exception message -- a genuinely missing asset returns ``None``, a
  403 raises ``AssetFetchError``, and a real parse error is re-raised untouched.
"""

import io
from unittest.mock import patch

import polars as pl
import pytest

from sportsdataverse import _codegen_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError


class _Resp:
    """Minimal stand-in for the ``requests.Response`` ``download`` returns."""

    def __init__(self, content: bytes = b"", status_code: int = 200):
        self.content = content
        self.status_code = status_code


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def test_as_season_list_normalizes():
    assert rt._as_season_list(2024) == [2024]
    assert rt._as_season_list(range(2022, 2024)) == [2022, 2023]
    assert rt._as_season_list([2021, 2022]) == [2021, 2022]
    assert rt._as_season_list("2024") == [2024]


def test_read_release_parquet_returns_df_on_success():
    frame = pl.DataFrame({"a": [1, 2, 3]})
    with patch.object(rt.pl, "read_parquet", return_value=frame):
        out = rt._read_release_parquet("https://x/ok.parquet")
    assert out is not None and out.shape == (3, 1)


def test_success_path_never_calls_the_transport():
    """The bytes must NOT be routed through ``download``.

    Fetch-then-parse co-resides the compressed asset with the decoded frame; Arrow's
    direct read does not. This asserts the fast path stays fast -- if someone later
    reroutes the read through the gateway "for consistency", this fails first.
    """
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return _Resp(b"")

    with patch.object(rt.pl, "read_parquet", return_value=pl.DataFrame({"a": [1]})):
        with patch.object(rt, "download", side_effect=counting):
            rt._read_release_parquet("https://x/ok.parquet")

    assert calls["n"] == 0, f"success path made {calls['n']} gateway call(s), expected 0"


def test_read_release_parquet_returns_none_on_404():
    """A missing asset is the gateway's typed 404, so the season is skipped."""
    with patch.object(rt.pl, "read_parquet", side_effect=OSError("arrow could not open")):
        with patch.object(rt, "download", side_effect=NoDataError("404")):
            assert rt._read_release_parquet("https://x/missing.parquet") is None


def test_read_release_parquet_raises_on_non_200():
    """A 403 that outlived the retry budget must NOT be masked as "no data"."""
    with patch.object(rt.pl, "read_parquet", side_effect=OSError("arrow could not open")):
        with patch.object(rt, "download", return_value=_Resp(b"", status_code=403)):
            with pytest.raises(AssetFetchError, match="403"):
                rt._read_release_parquet("https://x/forbidden.parquet")


def test_readable_asset_reraises_the_parse_error():
    """Asset fetches fine but won't parse -> the real error surfaces, not ``None``."""

    class _Corrupt(Exception):
        pass

    with patch.object(rt.pl, "read_parquet", side_effect=_Corrupt("invalid parquet footer")):
        with patch.object(rt, "download", return_value=_Resp(b"bytes", status_code=200)):
            with pytest.raises(_Corrupt, match="footer"):
                rt._read_release_parquet("https://x/corrupt.parquet")


def test_keyboard_interrupt_is_not_swallowed_into_a_fetch():
    """``PanicException`` handling must not turn Ctrl-C into an HTTP request."""
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return _Resp(b"")

    with patch.object(rt.pl, "read_parquet", side_effect=KeyboardInterrupt()):
        with patch.object(rt, "download", side_effect=counting):
            with pytest.raises(KeyboardInterrupt):
                rt._read_release_parquet("https://x/interrupted.parquet")

    assert calls["n"] == 0


def test_panic_falls_back_to_metadata_stripped_read():
    """R-producer ``arrow.r.vctrs`` metadata panics polars' FFI import; recover."""
    payload = _parquet_bytes(pl.DataFrame({"a": [1]}))

    class _Panic(BaseException):
        pass

    _Panic.__name__ = "PanicException"

    with patch.object(rt.pl, "read_parquet", side_effect=_Panic("arrow-ffi metadata panic")):
        with patch.object(rt, "download", return_value=_Resp(payload)):
            out = rt._read_release_parquet("https://x/rvctrs.parquet")

    assert out is not None and out.shape == (1, 1)


def test_failed_fetch_is_not_a_missing_asset():
    """``AssetFetchError`` and ``NoDataError`` must stay distinguishable.

    Both subclass ``SportsDataverseError``, but only the latter means "skip this
    season" -- collapsing them would turn a rate-limited fetch into an empty frame.
    """
    assert not issubclass(AssetFetchError, NoDataError)
    assert not issubclass(NoDataError, AssetFetchError)


# --------------------------------------------------------------------------
# raise-vs-skip: two semantics, one implementation
#
# `_fetch_release_parquet` raises on a missing asset; `_read_release_parquet`
# returns None. Hand-written loaders (nfl_loaders, cfb_loaders_extra) use the
# raising form because a missing NFL season is an error there, while the 226
# generated call sites use the wrapper because a season gap is routine. Both must
# classify failures identically -- only "absent" is allowed to differ.
# --------------------------------------------------------------------------


def test_fetch_raises_where_read_skips():
    """The same missing asset: one raises, the other returns ``None``."""
    with patch.object(rt.pl, "read_parquet", side_effect=OSError("arrow could not open")):
        with patch.object(rt, "download", side_effect=NoDataError("404")):
            with pytest.raises(NoDataError):
                rt._fetch_release_parquet("https://x/missing.parquet")
            assert rt._read_release_parquet("https://x/missing.parquet") is None


def test_both_variants_surface_a_failed_fetch():
    """A 403 must NOT be softened by either variant.

    The wrapper only converts "absent" to ``None``. If it also swallowed
    ``AssetFetchError``, a rate-limited season would silently become an empty
    frame -- the exact failure the split exists to prevent.
    """
    for fn in (rt._fetch_release_parquet, rt._read_release_parquet):
        with patch.object(rt.pl, "read_parquet", side_effect=OSError("arrow could not open")):
            with patch.object(rt, "download", return_value=_Resp(b"", status_code=403)):
                with pytest.raises(AssetFetchError, match="403"):
                    fn("https://x/forbidden.parquet")


def test_fetch_success_path_never_calls_the_transport():
    """The raising variant keeps the direct-read fast path too."""
    calls = {"n": 0}

    def counting(*a, **k):
        calls["n"] += 1
        return _Resp(b"")

    with patch.object(rt.pl, "read_parquet", return_value=pl.DataFrame({"a": [1]})):
        with patch.object(rt, "download", side_effect=counting):
            rt._fetch_release_parquet("https://x/ok.parquet")

    assert calls["n"] == 0
