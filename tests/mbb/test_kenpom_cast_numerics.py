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


def _page(stem: str) -> str:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


#: One jersey cell of a committed page re-served as "00": (page, cell, table, column, name column).
_JERSEY_00 = [
    ("team", '<td style="text-align: right">2</td>', "player_table", "column_0", "column_0_rk"),
    ("box", '<td class="number"><b>2</b></td>', "table_1", "column_0_rk", "name"),
]


@pytest.mark.parametrize(("stem", "cell", "table", "column", "name"), _JERSEY_00, ids=["team", "box"])
def test_real_page_jersey_00_survives_read_html(stem: str, cell: str, table: str, column: str, name: str) -> None:
    """R48b-1: ``read_html`` typed KenPom's jersey columns (Float64 / Int64), so "00" became 0 before any cast."""
    raw = _page(stem)
    assert raw.count(cell) == 1
    edited = raw.replace(cell, cell.replace(">2<", ">00<"))
    before, after = kenpom_runtime.parse_kenpom_page(raw), kenpom_runtime.parse_kenpom_page(edited)
    frame = after[table]
    assert frame.schema[column] == pl.Utf8
    flagg = frame.filter(pl.col(name).str.starts_with("Cooper Flagg"))
    assert flagg[column].to_list() == ["00"]
    # only that column changes: same keys, rows (the section-label rows stay) and every other column
    assert before.keys() == after.keys() and before[table].height == frame.height
    changed = [(k, c) for k in before for c in before[k].columns if not before[k][c].equals(after[k][c])]
    assert changed == [(table, column)]


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda p: p.stem)
def test_committed_pages_parse_the_same_with_the_fix(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reach: no committed KenPom page has a zero-padded cell, so the fix changes none of its 29 pages."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    fixed = kenpom_runtime.parse_kenpom_page(raw)
    plain = kenpom_runtime.html_tables
    monkeypatch.setattr(
        kenpom_runtime, "html_tables", lambda html, **kw: plain(html, **{**kw, "keep_zero_padded": False})
    )
    monkeypatch.setattr(kenpom_runtime, "_zero_padded", lambda s: False)
    unfixed = kenpom_runtime.parse_kenpom_page(raw)
    assert fixed.keys() == unfixed.keys()
    for key in fixed:
        assert fixed[key].equals(unfixed[key]), key
