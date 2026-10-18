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


@pytest.mark.parametrize("read", [rt._fetch_release_parquet, rt._read_release_parquet])
def test_release_read_goes_through_pyarrow(read):
    """A-M10: polars' own reader asks the release CDN for a suffix range, which it answers 501.

    Removing ``use_pyarrow=True`` brings that back on every release loader under polars 2.0,
    so the kwarg itself is the contract (CLAUDE.md "Polars version").
    """
    with patch.object(rt.pl, "read_parquet", return_value=pl.DataFrame({"a": [1]})) as rp:
        read("https://github.com/o/r/releases/download/t/x.parquet")
    rp.assert_called_once_with("https://github.com/o/r/releases/download/t/x.parquet", use_pyarrow=True)


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


def test_panic_refetch_that_fails_raises_rather_than_parsing_the_error_body():
    """The metadata fallback issues its OWN request, which can fail on its own.

    The first read reached the asset (that is why it panicked), but the refetch can
    still come back 403 or 5xx. Handing that body to ``pq.read_table`` would surface
    a parquet parse error for what is really a failed fetch -- the same confusion
    ``AssetFetchError`` exists to prevent, just one layer down.
    """

    class _Panic(BaseException):
        pass

    _Panic.__name__ = "PanicException"

    with patch.object(rt.pl, "read_parquet", side_effect=_Panic("arrow-ffi metadata panic")):
        with patch.object(rt, "download", return_value=_Resp(b"<html>rate limited</html>", status_code=403)):
            with pytest.raises(AssetFetchError, match="403"):
                rt._fetch_release_parquet("https://x/rvctrs.parquet")


def test_panic_refetch_404_is_still_a_missing_asset():
    """A refetch 404 keeps its "absent" meaning through both entry points."""

    class _Panic(BaseException):
        pass

    _Panic.__name__ = "PanicException"

    with patch.object(rt.pl, "read_parquet", side_effect=_Panic("arrow-ffi metadata panic")):
        with patch.object(rt, "download", side_effect=NoDataError("404")):
            with pytest.raises(NoDataError):
                rt._fetch_release_parquet("https://x/gone.parquet")
            assert rt._read_release_parquet("https://x/gone.parquet") is None


# --------------------------------------------------------------------------
# Task 4.3: every failure classified at the release-read boundary, against a
# real local HTTP server (real sockets, real status lines, committed bytes).
# --------------------------------------------------------------------------

import http.server  # noqa: E402
import socket  # noqa: E402
import threading  # noqa: E402
from pathlib import Path  # noqa: E402

from sportsdataverse import cache  # noqa: E402
from sportsdataverse.dl_utils import _SHARED_SESSION  # noqa: E402
from sportsdataverse.nfl import clear_cache, nfl_loaders, reset_config, update_config  # noqa: E402

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_loaders"
_CSV = (_FIXTURES / "db_playerids_slice.csv").read_bytes()  # real DynastyProcess rows
_PARQUET = (_FIXTURES / "games_2023_2024_head3.parquet").read_bytes()  # real nflverse schedule rows


class _Handler(http.server.BaseHTTPRequestHandler):
    """Routes by path; ``hits`` counts requests per path, ``ranges`` records each Range header."""

    hits: dict = {}
    ranges: list = []

    def log_message(self, *args):
        pass

    def _send(self, status, body=b"", ctype="text/plain", headers=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        n = self.hits[self.path] = self.hits.get(self.path, 0) + 1
        rng = self.headers.get("Range")
        if rng:
            self.ranges.append((self.path, rng))
        csv, octet = "text/csv; charset=utf-8", "application/octet-stream"
        if self.path == "/ok.csv" or (self.path == "/flaky.csv" and n > 2):
            self._send(200, _CSV, csv)
        elif self.path in ("/missing.csv", "/missing.parquet"):
            self._send(404, b"Not Found")
        elif self.path == "/bad.csv":
            self._send(400, b"bad request")
        elif self.path in ("/limited.csv", "/flaky.csv"):
            self._send(429, b"slow down", headers=[("Retry-After", "0")])
        elif self.path == "/page.csv":
            self._send(
                200,
                b"<!DOCTYPE html><html><title>Just a moment...</title></html>",
                "text/html",
            )
        elif self.path == "/flaky.parquet" and rng == "bytes=0-0":
            self._send(
                206,
                _PARQUET[:1],
                octet,
                [("Content-Range", f"bytes 0-0/{len(_PARQUET)}")],
            )
        elif self.path == "/flaky.parquet" and n == 1:
            self._send(502, b"bad gateway")  # the direct read fails in transit
        elif self.path in ("/flaky.parquet", "/asset.parquet"):
            self._send(200, _PARQUET, octet)
        else:
            self._send(500, b"unrouted")


@pytest.fixture()
def server():
    """A local HTTP server, with the HTTP response cache and the nfl loader cache off."""
    _Handler.hits, _Handler.ranges = {}, []
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    mode = cache.get_cache_mode()
    cache.set_cache_mode("off")
    update_config(cache_mode="off")
    clear_cache()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()
    _SHARED_SESSION.close()  # drop the pooled sockets to the closed server (else a ResourceWarning later)
    cache.set_cache_mode(mode)
    reset_config()


@pytest.fixture()
def dead_url(monkeypatch):
    """A URL on a port nothing listens on, with one retry (the env floor: 0 reads as the default 15)."""
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "1")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    return f"http://127.0.0.1:{port}/asset"


# A-M9: the classification GET sat outside _transport_errors, so a dead connection leaked
# requests.ConnectionError from every release loader although Raises: names AssetFetchError.
@pytest.mark.parametrize("read", ["_fetch_release_parquet", "_read_release_parquet", "_fetch_csv"])
def test_connection_error_is_asset_fetch_error(read, dead_url):
    with pytest.raises(AssetFetchError, match="fetch failed after retries: ConnectionError"):
        getattr(rt, read)(dead_url + ".parquet")


def test_connection_error_in_the_panic_refetch_is_asset_fetch_error(dead_url):
    class _Panic(BaseException):
        pass

    _Panic.__name__ = "PanicException"
    with patch.object(rt.pl, "read_parquet", side_effect=_Panic("arrow-ffi metadata panic")):
        with pytest.raises(AssetFetchError, match="ConnectionError"):
            rt._fetch_release_parquet(dead_url + ".parquet")


@pytest.mark.parametrize(
    ("loader", "attr", "kwargs"),
    [
        ("load_nfl_schedule", "NFL_TEAM_SCHEDULE_URL", {"seasons": [2024]}),
        ("load_nfl_teams", "NFL_TEAM_LOGO_URL", {}),
        ("load_nfl_ff_playerids", "NFL_FF_PLAYERIDS_URL", {}),
    ],
)
def test_connection_error_reaches_the_loader_as_asset_fetch_error(loader, attr, kwargs, server, dead_url, monkeypatch):
    monkeypatch.setattr(nfl_loaders, attr, dead_url)
    with pytest.raises(AssetFetchError):
        getattr(nfl_loaders, loader)(**kwargs)


# A-M11: on polars 2.0 pl.read_csv(url) raised a bare OSError for every HTTP failure, so the
# 429 backoff never ran and a missing file was neither NoDataError nor AssetFetchError.
def _ff_playerids(server, monkeypatch, path):
    monkeypatch.setattr(nfl_loaders, "NFL_FF_PLAYERIDS_URL", server + path)
    return nfl_loaders.load_nfl_ff_playerids()


def test_csv_vocab_reads_the_real_slice_with_pinned_ids(server, monkeypatch):
    df = _ff_playerids(server, monkeypatch, "/ok.csv")
    assert df.height == 6 and df.schema["mfl_id"] == pl.Utf8
    assert "0156" in df["mfl_id"].to_list()


def test_csv_vocab_404_is_no_data(server, monkeypatch):
    with pytest.raises(NoDataError):
        _ff_playerids(server, monkeypatch, "/missing.csv")


def test_csv_vocab_400_is_value_error(server, monkeypatch):
    with pytest.raises(ValueError, match="400"):
        _ff_playerids(server, monkeypatch, "/bad.csv")


def test_csv_vocab_rate_limit_backs_off_then_reads(server, monkeypatch):
    df = _ff_playerids(server, monkeypatch, "/flaky.csv")
    assert df.height == 6 and _Handler.hits["/flaky.csv"] == 3


def test_csv_vocab_persistent_rate_limit_is_asset_fetch_error(server, monkeypatch):
    with pytest.raises(AssetFetchError, match="429"):
        _ff_playerids(server, monkeypatch, "/limited.csv")
    assert _Handler.hits["/limited.csv"] > 1  # retried before giving up


def test_csv_vocab_challenge_page_is_asset_fetch_error(server, monkeypatch):
    with pytest.raises(AssetFetchError, match="page, not data"):
        _ff_playerids(server, monkeypatch, "/page.csv")


def test_csv_vocab_teams_404_is_no_data(server, monkeypatch):
    monkeypatch.setattr(nfl_loaders, "NFL_TEAM_LOGO_URL", server + "/missing.csv")
    with pytest.raises(NoDataError):
        nfl_loaders.load_nfl_teams()


# A-N10 + B-L74: the classification asks for one byte, and a reachable asset whose first
# read failed in transit is read again instead of re-raising the reader's error.
def test_transient_read_failure_is_classified_by_one_byte_then_read_again(server):
    out = rt._fetch_release_parquet(server + "/flaky.parquet")
    assert out.height == 6 and out["game_id"][0] == "2023_01_DET_KC"
    assert _Handler.ranges == [("/flaky.parquet", "bytes=0-0")]
    assert _Handler.hits["/flaky.parquet"] == 3  # failed read, 1-byte probe, good read


def test_missing_asset_probe_is_no_data(server):
    with pytest.raises(NoDataError):
        rt._fetch_release_parquet(server + "/missing.parquet")
    assert rt._read_release_parquet(server + "/missing.parquet") is None


# A-L29: no_espn_data decoded every 2xx body as JSON, charset-scanning a binary asset first.
def test_binary_body_is_never_decoded_as_json(server, monkeypatch):
    import requests

    from sportsdataverse.dl_utils import download

    calls = {"n": 0}
    real_json = requests.Response.json

    def counting(self, *a, **k):
        calls["n"] += 1
        return real_json(self, *a, **k)

    monkeypatch.setattr(requests.Response, "json", counting)
    resp = download(server + "/asset.parquet")
    assert resp.status_code == 200 and resp.content == _PARQUET
    assert calls["n"] == 0


# B-GB-14: seasons=[] returned every season; None raised TypeError before the fetch.
def _games() -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(_PARQUET))


def test_seasons_empty_schedule_is_zero_rows_with_the_schema(server, monkeypatch):
    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", lambda url: _games())
    out = nfl_loaders.load_nfl_schedule([])
    assert out.height == 0 and out.schema == _games().schema


def test_seasons_none_schedule_is_every_season(server, monkeypatch):
    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", lambda url: _games())
    assert nfl_loaders.load_nfl_schedule(None).height == 6
    assert nfl_loaders.load_nfl_schedule([2024]).height == 3


def _participation(season: int) -> pl.DataFrame:
    return pl.read_parquet(_FIXTURES / f"pbp_participation_{2016 if season < 2023 else 2023}_head3.parquet")


def test_seasons_empty_per_season_loader_reads_nothing(server, monkeypatch):
    urls: list = []
    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", lambda url: urls.append(url) or _games())
    assert nfl_loaders.load_nfl_pbp_participation([]).height == 0
    assert nfl_loaders.load_nfl_ratings_weekly([]).height == 0  # pl.concat([]) raised here
    assert urls == []


def test_seasons_none_per_season_loader_reads_floor_to_current(server, monkeypatch):
    urls: list = []

    def fetch(url):
        urls.append(url)
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        if season == 2024:  # the current season, not built yet
            raise NoDataError("404")
        return _participation(season)

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fetch)
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2024)
    out = nfl_loaders.load_nfl_pbp_participation(None)
    assert len(urls) == 9 and urls[0].endswith("_2016.parquet")  # 2016..2024
    assert out.height == 8 * 3 and out.schema["play_id"] == pl.Float64
    with pytest.raises(NoDataError):  # a named season with no asset still raises
        nfl_loaders.load_nfl_pbp_participation([2024])


def test_seasons_none_gap_before_the_current_season_raises(server, monkeypatch):
    def fetch(url):
        if url.endswith("_2019.parquet"):
            raise NoDataError("404")
        return _participation(2016)

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fetch)
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2024)
    with pytest.raises(NoDataError):
        nfl_loaders.load_nfl_pbp_participation(None)
