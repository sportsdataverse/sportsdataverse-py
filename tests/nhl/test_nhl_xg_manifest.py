"""Offline tests: the xG booster cache is checked against the nhl_xg_models release manifest."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nhl.nhl_xg import ensure_xg_models, load_xg_models

# the module, not the package's re-exported nhl_xg() function of the same name
nhl_xg = sys.modules["sportsdataverse.nhl.nhl_xg"]

MODELS = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact" / "xg_models"
BOOSTERS = ("xg_model_5v5.json", "xg_model_st.json")
MANIFEST = "xg_model_meta.json"


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


class _Resp:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        pass


@pytest.fixture
def release(monkeypatch, tmp_path):
    """A fake release serving new boosters + their manifest; the cache is ``tmp_path``."""
    monkeypatch.setenv("NHL_XG_MODEL_DIR", str(tmp_path))
    files = {fn: b"new " + fn.encode() for fn in BOOSTERS}
    pins = {fn: _sha(b) for fn, b in files.items()} | {"feats_5v5.txt": "0" * 64, "feats_st.txt": "1" * 64}
    served = {**files, MANIFEST: json.dumps({"xg_model_ps": 0.31, "version": "test", "files": pins}).encode()}
    calls: list[str] = []

    def fake_download(url, **kwargs):
        # ensure_xg_models goes through dl_utils.download (shared retries), never response-cached
        assert kwargs.get("cache_ttl") == 0, kwargs
        fn = url.rsplit("/", 1)[-1]
        calls.append(fn)
        if fn not in served:
            raise NoDataError(f"404 {url}")
        return _Resp(served[fn])

    monkeypatch.setattr(nhl_xg, "download", fake_download)
    return SimpleNamespace(dir=tmp_path, files=files, served=served, calls=calls)


def _offline(monkeypatch) -> None:
    def fail(url, **kwargs):  # what download re-raises once its retry budget is spent
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(nhl_xg, "download", fail)


def test_cache_hit_fetches_only_the_manifest(release):
    for fn, b in release.files.items():
        (release.dir / fn).write_bytes(b)
    assert ensure_xg_models() == release.dir
    assert release.calls == [MANIFEST]
    assert json.loads((release.dir / MANIFEST).read_text())["version"] == "test"


def test_stale_cached_booster_is_replaced(release):
    (release.dir / "xg_model_5v5.json").write_bytes(b"2026-04 booster")
    (release.dir / "xg_model_st.json").write_bytes(release.files["xg_model_st.json"])
    ensure_xg_models()
    assert release.calls == [MANIFEST, "xg_model_5v5.json"]
    assert (release.dir / "xg_model_5v5.json").read_bytes() == release.files["xg_model_5v5.json"]


def test_download_not_matching_the_manifest_is_refused(release):
    (release.dir / "xg_model_5v5.json").write_bytes(b"2026-04 booster")
    release.served["xg_model_5v5.json"] = b"half-uploaded"
    with pytest.raises(AssetFetchError, match="does not match"):
        ensure_xg_models()
    assert (release.dir / "xg_model_5v5.json").read_bytes() == b"2026-04 booster"
    # nothing committed: neither the new manifest nor a temp file
    assert sorted(p.name for p in release.dir.iterdir()) == ["xg_model_5v5.json"]


def test_a_refresh_that_fails_part_way_keeps_the_previous_set_usable_offline(release, monkeypatch):
    """Manifest A + boosters A cached; release B's second booster fails its hash. The cache must
    still hold the complete A set (manifest written last), so the next offline load works."""
    ensure_xg_models()  # set A
    before = {p.name: p.read_bytes() for p in release.dir.iterdir()}
    new = {fn: b"B " + fn.encode() for fn in BOOSTERS}
    pins = {fn: _sha(b) for fn, b in new.items()} | {"feats_5v5.txt": "2" * 64, "feats_st.txt": "3" * 64}
    release.served.update({"xg_model_5v5.json": new["xg_model_5v5.json"], "xg_model_st.json": b"truncated"})
    release.served[MANIFEST] = json.dumps({"xg_model_ps": 0.32, "version": "B", "files": pins}).encode()
    with pytest.raises(AssetFetchError, match="does not match"):
        ensure_xg_models()
    assert {p.name: p.read_bytes() for p in release.dir.iterdir()} == before
    _offline(monkeypatch)
    assert ensure_xg_models() == release.dir


def test_a_release_without_a_manifest_falls_back_to_the_cache(release):
    ensure_xg_models()
    del release.served[MANIFEST]  # a 404 is NoDataError from download
    assert ensure_xg_models() == release.dir


def test_offline_falls_back_to_the_cached_manifest(release, monkeypatch):
    ensure_xg_models()  # online: caches manifest + boosters
    _offline(monkeypatch)
    assert ensure_xg_models() == release.dir
    (release.dir / "xg_model_st.json").write_bytes(b"stale")  # offline cannot repair a stale file
    with pytest.raises(requests.ConnectionError):
        ensure_xg_models()


def test_offline_without_a_cached_manifest_raises(release, monkeypatch):
    _offline(monkeypatch)
    with pytest.raises(AssetFetchError, match="manifest"):
        ensure_xg_models()


def test_old_unpinned_manifest_is_rejected(release, monkeypatch):
    (release.dir / MANIFEST).write_text('{"xg_model_ps": 0.3202197}')  # the pre-2026-11 sidecar
    _offline(monkeypatch)
    with pytest.raises(ValueError, match="expected format"):
        ensure_xg_models()


def test_explicit_model_dir_is_network_free(monkeypatch):
    _offline(monkeypatch)
    assert ensure_xg_models(MODELS) == MODELS
    assert load_xg_models(MODELS)["ps"] == pytest.approx(0.3202197)


def test_a_write_that_fails_mid_commit_keeps_the_previous_set(release, monkeypatch):
    """Release B verifies, but writing its second file fails (a full disk): the cached set A
    must be untouched and still load offline -- nothing is moved until every file is written."""
    import os

    ensure_xg_models()  # set A
    before = {p.name: p.read_bytes() for p in release.dir.iterdir()}
    new = {fn: b"B " + fn.encode() for fn in BOOSTERS}
    pins = {fn: _sha(b) for fn, b in new.items()} | {"feats_5v5.txt": "4" * 64, "feats_st.txt": "5" * 64}
    release.served.update(new)
    release.served[MANIFEST] = json.dumps({"xg_model_ps": 0.33, "version": "B", "files": pins}).encode()
    real_fdopen, calls = os.fdopen, []

    def flaky_fdopen(fd, *a, **k):
        calls.append(fd)
        if len(calls) == 2:
            os.close(fd)
            raise OSError(28, "No space left on device")
        return real_fdopen(fd, *a, **k)

    monkeypatch.setattr(os, "fdopen", flaky_fdopen)
    with pytest.raises(OSError, match="No space"):
        ensure_xg_models()
    monkeypatch.setattr(os, "fdopen", real_fdopen)
    assert {p.name: p.read_bytes() for p in release.dir.iterdir()} == before
    _offline(monkeypatch)
    assert ensure_xg_models() == release.dir
