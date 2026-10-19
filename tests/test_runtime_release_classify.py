"""Release and CSV reads classify every failure (Task 4.3), against a real local HTTP server.

Real sockets, real status lines and committed real bytes: a DynastyProcess CSV slice and an
nflverse schedule slice (``tests/fixtures/nfl_loaders/README.md``). Kept at the top level, not
under ``tests/codegen/``, so the coverage jobs (which skip that directory) count them.

* A-M9: a refused connection is ``AssetFetchError``, never ``requests.ConnectionError``.
* A-M11: CSV reads go through the HTTP gateway and its vocabulary.
* A-N10 / B-L74: a failed read is classified by a one-byte probe and read again when the
  asset is reachable; the second read is classified too; a parse error is not refetched.
* A-L29: a binary body is never decoded as JSON.
* B-GB-14: the NFL loaders follow nflreadpy: ``None`` = current season, ``True`` = every
  season, ``[]`` = an empty frame and no request.
"""

import http.server
import io
import socket
import threading
import urllib.error
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pytest

from sportsdataverse import _codegen_runtime as rt
from sportsdataverse import cache
from sportsdataverse.dl_utils import _SHARED_SESSION
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nfl import clear_cache, nfl_loaders, reset_config, update_config

_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "nfl_loaders"
_CSV = (_FIXTURES / "db_playerids_slice.csv").read_bytes()  # real DynastyProcess rows
_PARQUET = (_FIXTURES / "games_2023_2024_head3.parquet").read_bytes()  # real nflverse schedule rows
_OCTET = "application/octet-stream"


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

    def _parquet(self, n, rng, *, probe=206, second=200, second_body=_PARQUET):
        """First direct read 502 (in transit), then the probe, then the second direct read."""
        if rng == "bytes=0-0":
            if probe == 206:
                self._send(206, _PARQUET[:1], _OCTET, [("Content-Range", f"bytes 0-0/{len(_PARQUET)}")])
            else:
                self._send(probe, b"", _OCTET)
        elif n == 1:
            self._send(502, b"bad gateway")
        else:
            self._send(second, second_body if second == 200 else b"no", _OCTET)

    def do_GET(self):  # noqa: N802
        n = self.hits[self.path] = self.hits.get(self.path, 0) + 1
        rng = self.headers.get("Range")
        if rng:
            self.ranges.append((self.path, rng))
        csv = "text/csv; charset=utf-8"
        if self.path == "/ok.csv" or (self.path == "/flaky.csv" and n > 2):
            self._send(200, _CSV, csv)
        elif self.path in ("/missing.csv", "/missing.parquet"):
            self._send(404, b"Not Found")
        elif self.path == "/bad.csv":
            self._send(400, b"bad request")
        elif self.path in ("/limited.csv", "/flaky.csv"):
            self._send(429, b"slow down", headers=[("Retry-After", "0")])
        elif self.path == "/page.csv":
            self._send(200, b"<!DOCTYPE html><html><title>Just a moment...</title></html>", "text/html")
        elif self.path == "/flaky.parquet":
            self._parquet(n, rng)
        elif self.path == "/second-429.parquet":
            self._parquet(n, rng, second=429)
        elif self.path == "/second-404.parquet":
            self._parquet(n, rng, second=404)
        elif self.path == "/zero-byte.parquet":
            self._parquet(n, rng, probe=416, second_body=b"")
        elif self.path == "/corrupt.parquet":
            self._send(200, b"PAR1 this is not a parquet file PAR1", _OCTET)
        elif self.path == "/asset.parquet":
            self._send(200, _PARQUET, _OCTET)
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


# A-M9: the classification GET sat outside _transport_errors, so a refused connection leaked
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


# R898-4: the second read is classified like the first (and its error response closed).
def test_second_read_rate_limit_is_asset_fetch_error(server):
    with pytest.raises(AssetFetchError, match="HTTP 429"):
        rt._fetch_release_parquet(server + "/second-429.parquet")
    assert _Handler.hits["/second-429.parquet"] == 3


def test_second_read_404_is_no_data(server):
    with pytest.raises(NoDataError):
        rt._fetch_release_parquet(server + "/second-404.parquet")
    assert rt._read_release_parquet(server + "/second-404.parquet") is None


# R898-5: a body that arrived whole but does not parse is the asset's own error: no probe, no refetch.
def test_parse_error_is_not_refetched(server):
    with pytest.raises(pa.ArrowInvalid):
        rt._fetch_release_parquet(server + "/corrupt.parquet")
    assert _Handler.hits["/corrupt.parquet"] == 1 and _Handler.ranges == []


# R898-6: a zero-byte asset answers the probe 416; it is reachable, so the second read's own
# parse error surfaces, not an AssetFetchError.
def test_zero_byte_asset_416_probe_is_reachable(server):
    with pytest.raises(pa.ArrowInvalid):
        rt._fetch_release_parquet(server + "/zero-byte.parquet")
    assert _Handler.ranges == [("/zero-byte.parquet", "bytes=0-0")]
    assert _Handler.hits["/zero-byte.parquet"] == 3


# R898-1 / A-M10: the second read keeps use_pyarrow=True as well (polars 2.0's own reader asks
# the release CDN for a suffix range, which it answers 501).
def test_second_read_goes_through_pyarrow():
    frame = pl.read_parquet(io.BytesIO(_PARQUET))

    class _Resp:
        status_code = 206

    with patch.object(rt.pl, "read_parquet", side_effect=[urllib.error.URLError("reset"), frame]) as rp:
        with patch.object(rt, "download", return_value=_Resp()):
            assert rt._fetch_release_parquet("https://x/flaky.parquet").height == 6
    assert rp.call_count == 2
    assert all(call.kwargs == {"use_pyarrow": True} for call in rp.call_args_list)


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


# B-GB-14 (owner ruling R898-2, nflreadpy): seasons=[] returned every season and None raised
# TypeError. Now [] is an empty frame with no request, None the current season, True every season.
def _games() -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(_PARQUET))


@pytest.fixture()
def schedule_asset(monkeypatch):
    urls: list = []
    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", lambda url: urls.append(url) or _games())
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2024)
    return urls


def test_seasons_empty_schedule_makes_no_request(server, schedule_asset):
    out = nfl_loaders.load_nfl_schedule([])
    assert out.height == 0 and schedule_asset == []


def test_seasons_none_schedule_is_the_current_season(server, schedule_asset):
    out = nfl_loaders.load_nfl_schedule(None)
    assert out.height == 3 and set(out["season"].to_list()) == {2024}


def test_seasons_true_schedule_is_every_season(server, schedule_asset):
    assert nfl_loaders.load_nfl_schedule(True).height == 6
    assert nfl_loaders.load_nfl_schedule([2023]).height == 3


def _participation(season: int) -> pl.DataFrame:
    return pl.read_parquet(_FIXTURES / f"pbp_participation_{2016 if season < 2023 else 2023}_head3.parquet")


def test_seasons_empty_per_season_loader_reads_nothing(server, schedule_asset):
    assert nfl_loaders.load_nfl_pbp_participation([]).height == 0
    assert nfl_loaders.load_nfl_ratings_weekly([]).height == 0  # pl.concat([]) raised here
    assert schedule_asset == []


@pytest.fixture()
def participation_asset(monkeypatch):
    """Per-season participation fixtures; the current season (2024) is not built yet."""
    urls: list = []

    def fetch(url):
        urls.append(url)
        season = int(url.rsplit("_", 1)[1].split(".")[0])
        if season == 2024:
            raise NoDataError("404")
        return _participation(season)

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fetch)
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2024)
    return urls


def test_seasons_none_per_season_loader_is_the_current_season(server, participation_asset, monkeypatch):
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2023)
    out = nfl_loaders.load_nfl_pbp_participation(None)
    assert [u.rsplit("_", 1)[1] for u in participation_asset] == ["2023.parquet"]
    assert out.height == 3


def test_seasons_true_per_season_loader_reads_floor_to_current(server, participation_asset):
    out = nfl_loaders.load_nfl_pbp_participation(True)
    assert len(participation_asset) == 9 and participation_asset[0].endswith("_2016.parquet")  # 2016..2024
    assert out.height == 8 * 3 and out.schema["play_id"] == pl.Float64
    with pytest.raises(NoDataError):  # a named season with no asset still raises
        nfl_loaders.load_nfl_pbp_participation([2024])


def test_seasons_true_gap_before_the_current_season_raises(server, monkeypatch):
    def fetch(url):
        if url.endswith("_2019.parquet"):
            raise NoDataError("404")
        return _participation(2016)

    monkeypatch.setattr(nfl_loaders, "_fetch_release_parquet", fetch)
    monkeypatch.setattr(nfl_loaders, "get_current_nfl_season", lambda: 2024)
    with pytest.raises(NoDataError):
        nfl_loaders.load_nfl_pbp_participation(True)
