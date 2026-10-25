"""The ``*_pbp_disk`` readers decode saved ESPN payloads as UTF-8, whatever the platform's default
encoding. Without ``encoding="utf-8"`` a Windows run read them as cp1252 and garbled every
non-ASCII name ('Josué' -> 'JosuÃ©')."""

from __future__ import annotations

import importlib
import json

import pytest

NAME = "Josué Ávila’s ‘Night’"


@pytest.mark.parametrize("lg", ["mbb", "nba", "nhl", "wbb", "wnba"])
def test_pbp_disk_reads_utf8(tmp_path, lg):
    (tmp_path / "401.json").write_text(json.dumps({"name": NAME}, ensure_ascii=False), encoding="utf-8")
    fn = getattr(importlib.import_module(f"sportsdataverse.{lg}.{lg}_pbp"), f"{lg}_pbp_disk")
    assert fn(401, str(tmp_path))["name"] == NAME
