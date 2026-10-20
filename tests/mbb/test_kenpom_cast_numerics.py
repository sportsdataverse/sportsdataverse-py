"""KenPom's text-to-number cast keeps zero-padded columns as text (A-M18). Offline."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import kenpom_runtime
from sportsdataverse.mbb.kenpom_runtime import _cast_numerics

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kenpom"


def test_cast_numerics_jersey_00_stays_text() -> None:
    """A jersey column whose text survived ``read_html`` (repeated headers make it text) keeps "00" apart from "0"."""
    out = _cast_numerics(pl.DataFrame({"no": ["00", "0", "23"], "adj_o": ["+9.46", "120.1", "-3"]}))
    assert out.schema["no"] == pl.Utf8 and out["no"].to_list() == ["00", "0", "23"]
    assert out.schema["adj_o"] == pl.Float64  # the signed ratings column still casts


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda p: p.stem)
def test_committed_pages_parse_the_same_with_the_guard(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """No committed KenPom page carries a zero-padded column, so the guard changes none of their dtypes."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    guarded = kenpom_runtime.parse_kenpom_page(raw)
    monkeypatch.setattr(kenpom_runtime, "_zero_padded", lambda s: False)
    unguarded = kenpom_runtime.parse_kenpom_page(raw)
    assert {k: v.schema for k, v in guarded.items()} == {k: v.schema for k, v in unguarded.items()}
