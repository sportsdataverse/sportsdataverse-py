"""The ``*_pbp_disk`` readers decode saved ESPN payloads as UTF-8, whatever the platform's default
encoding. Without ``encoding="utf-8"`` a Windows run read them as cp1252 and garbled every
non-ASCII name ('Josué' -> 'JosuÃ©').

PRE-1008 D-1: CI runs on a UTF-8 locale, where a reader with the ``encoding=`` dropped still passed. Every test
here runs with ``open()`` defaulting to cp1252, as on Windows, so a reader that leaves the encoding to the
platform fails on every OS.
"""

from __future__ import annotations

import builtins
import importlib
import json

import pytest

NAME = "Josué Ávila’s ‘Night’"


@pytest.fixture
def cp1252_default(monkeypatch):
    """``open()`` in text mode without ``encoding=`` decodes cp1252, the Windows default."""
    real_open = builtins.open

    def open_like_windows(file, mode="r", *args, **kwargs):
        if "b" not in mode and len(args) < 2 and kwargs.get("encoding") is None:
            kwargs["encoding"] = "cp1252"
        return real_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", open_like_windows)


@pytest.fixture
def saved_payload(tmp_path):
    (tmp_path / "401.json").write_text(json.dumps({"name": NAME}, ensure_ascii=False), encoding="utf-8")
    return tmp_path


def test_the_cp1252_default_garbles_a_reader_that_leaves_the_encoding_out(cp1252_default, saved_payload):
    """The guard itself: a reader without ``encoding=`` gets a ``UnicodeDecodeError`` under it, on any OS (``Á`` is
    ``c3 81`` in UTF-8, and cp1252 has no character at ``0x81``)."""
    with pytest.raises(UnicodeDecodeError), open(saved_payload / "401.json") as fh:
        json.load(fh)


@pytest.mark.parametrize("lg", ["mbb", "nba", "nhl", "wbb", "wnba"])
def test_pbp_disk_reads_utf8(cp1252_default, saved_payload, lg):
    fn = getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp"), f"{lg}_pbp_disk")
    assert fn(401, str(saved_payload))["name"] == NAME


@pytest.mark.parametrize(
    ("module", "cls", "method"),
    [
        ("sportsdataverse.cfb.cfb_pbp", "CFBPlayProcess", "cfb_pbp_disk"),
        ("sportsdataverse.nfl.nfl_pbp", "NFLPlayProcess", "nfl_pbp_disk"),
    ],
    ids=["cfb", "nfl"],
)
def test_play_process_disk_reads_utf8(cp1252_default, saved_payload, module, cls, method):
    proc = getattr(importlib.import_module(module), cls)(gameId=401, path_to_json=str(saved_payload))
    assert getattr(proc, method)()["name"] == NAME
