"""The vendor transport's browser profiles are removed, not leaked (2026-11-03).

``_PatchrightTransport`` used to ``mkdtemp`` a ``patchright_udd_*`` profile on every launch (one per
proxy rotation) and never delete it: 35,308 of them, about 88 GB, filled the droplet's root disk.
No browser runs here: ``patchright.sync_api`` is replaced by a recording fake.
"""

from __future__ import annotations

import atexit
import os
import sys
import tempfile
import time
import types
from pathlib import Path

import pytest

from sportsdataverse.mbb import mbb_ncaa_fetch as F
from sportsdataverse.scrape.ncaa import canary


class _Ctx:
    def __init__(self, launches: list, kw: dict) -> None:
        launches.append(kw)
        self.pages = [object()]
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Pw:
    def __init__(self, launches: list) -> None:
        self.chromium = types.SimpleNamespace(launch_persistent_context=lambda **kw: _Ctx(launches, kw))

    def stop(self) -> None:
        pass


@pytest.fixture
def fake_browser(tmp_path, monkeypatch):
    launches: list = []
    module = types.ModuleType("patchright.sync_api")
    module.sync_playwright = lambda: types.SimpleNamespace(start=lambda: _Pw(launches))
    monkeypatch.setitem(sys.modules, "patchright", types.ModuleType("patchright"))
    monkeypatch.setitem(sys.modules, "patchright.sync_api", module)
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    registered: list = []
    monkeypatch.setattr(atexit, "register", registered.append)  # canary imports atexit inside _ensure
    return launches, registered


def _profiles(root: Path) -> "list[str]":
    return sorted(p.name for p in root.iterdir() if p.is_dir())


def test_each_launch_gets_a_marked_profile_and_a_rotation_removes_the_old_one(tmp_path, fake_browser):
    launches, registered = fake_browser
    t = canary._PatchrightTransport()

    t._ensure({"http": "http://u:p@host:10001"})
    first = Path(launches[0]["user_data_dir"])
    assert first.parent == tmp_path and first.name.startswith(F._PROFILE_PREFIX)
    assert (first / F._OWNER_FILE).read_text() == str(os.getpid())

    t._ensure({"http": "http://u:p@host:10002"})  # proxy rotated: relaunch
    second = Path(launches[1]["user_data_dir"])
    assert not first.exists() and second.exists()
    assert _profiles(tmp_path) == [second.name]

    t.close()
    assert _profiles(tmp_path) == []
    # one close() handler, not one per relaunch (weakref.finalize registers its own once, ignored)
    assert [fn for fn in registered if getattr(fn, "__self__", None) is t] == [t.close]


def test_same_proxy_reuses_the_session(tmp_path, fake_browser):
    launches, _ = fake_browser
    t = canary._PatchrightTransport()
    t._ensure({"http": "http://u:p@host:10001"})
    t._ensure({"http": "http://u:p@host:10001"})
    assert len(launches) == 1
    t.close()


def test_sweep_reaps_stale_legacy_patchright_profiles_only(tmp_path):
    old = tmp_path / "patchright_udd_old"
    young = tmp_path / "patchright_udd_young"
    unrelated = tmp_path / "something_else"
    for d in (old, young, unrelated):
        d.mkdir()
    two_days_ago = time.time() - 2 * 86_400
    os.utime(old, (two_days_ago, two_days_ago))
    os.utime(unrelated, (two_days_ago, two_days_ago))

    assert F._sweep_orphan_profiles(str(tmp_path)) == 1
    assert _profiles(tmp_path) == ["patchright_udd_young", "something_else"]


def test_a_failed_launch_removes_its_profile(tmp_path, fake_browser, monkeypatch):
    launches, _ = fake_browser

    def _refuse(self, launches, kw):
        launches.append(kw)
        raise RuntimeError("proxy refused")

    monkeypatch.setattr(_Ctx, "__init__", _refuse)
    t = canary._PatchrightTransport()
    with pytest.raises(RuntimeError, match="proxy refused"):
        t._ensure({"http": "http://u:p@host:10001"})
    assert Path(launches[0]["user_data_dir"]).name.startswith(F._PROFILE_PREFIX)
    assert _profiles(tmp_path) == []
    assert t._pw is None and t._temp_dir is None


def test_a_failed_cleanup_unmarks_the_leftover(tmp_path, fake_browser, monkeypatch):
    launches, _ = fake_browser
    t = canary._PatchrightTransport()
    t._ensure({"http": "http://u:p@host:10001"})
    profile = Path(launches[0]["user_data_dir"])
    (profile / "locked").write_text("x")

    def _busy(self):
        raise PermissionError("in use")

    monkeypatch.setattr(tempfile.TemporaryDirectory, "cleanup", _busy)
    monkeypatch.setattr(canary.shutil, "rmtree", lambda *a, **k: None)  # nothing can be removed
    t.close()
    assert not (profile / F._OWNER_FILE).exists()  # so the sweep ages it out as unmarked
