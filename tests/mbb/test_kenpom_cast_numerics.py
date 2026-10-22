"""KenPom jerseys are text on every page, and zero-padded columns stay text (A-M18, R48b-7). Offline."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import kenpom_runtime
from sportsdataverse.mbb.kenpom_runtime import _cast_numerics

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kenpom"


def test_cast_numerics_jersey_00_stays_text() -> None:
    """A zero-padded column keeps "00" apart from "0"; a column named ``jersey`` is never cast."""
    out = _cast_numerics(
        pl.DataFrame({"no": ["00", "0", "23"], "jersey": ["2", "7", "1"], "adj_o": ["+9.46", "120.1", "-3"]})
    )
    assert out.schema["no"] == pl.Utf8 and out["no"].to_list() == ["00", "0", "23"]
    assert out.schema["jersey"] == pl.Utf8 and out["jersey"].to_list() == ["2", "7", "1"]
    assert out.schema["adj_o"] == pl.Float64  # the signed ratings column still casts


def _page(stem: str) -> str:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


#: The jersey columns of the committed pages: (page, table, jersey values as served).
_JERSEYS = [
    ("team", "player_table", [None, "2", None, "7", "1", None, "5", "14"]),
    ("team_players_expanded", "player_table", [None, "2", None]),
    ("team_players_expanded", "player_table_2", [None, "2", None]),
    ("box", "table_1", ["2", "7", "5"]),
    ("box", "table_2", ["3", "5", "24"]),
]


@pytest.mark.parametrize(("stem", "table", "values"), _JERSEYS, ids=[f"{s}-{t}" for s, t, _ in _JERSEYS])
def test_unedited_pages_jersey_is_text(stem: str, table: str, values: list) -> None:
    """R48b-7: one dtype per label column. read_html typed the jersey Float64 (team) / Int64 (box)."""
    frame = kenpom_runtime.parse_kenpom_page(_page(stem))[table]
    assert frame.schema["jersey"] == pl.Utf8
    assert frame["jersey"].head(len(values)).to_list() == values


#: One jersey cell of a committed page re-served as "00": (page, cell, table, player-name column).
_JERSEY_00 = [
    ("team", '<td style="text-align: right">2</td>', "player_table", "column_0_rk"),
    ("box", '<td class="number"><b>2</b></td>', "table_1", "name"),
]


def _edited(stem: str, cell: str) -> str:
    raw = _page(stem)
    assert raw.count(cell) == 1
    return raw.replace(cell, cell.replace(">2<", ">00<"))


@pytest.mark.parametrize(("stem", "cell", "table", "name"), _JERSEY_00, ids=["team", "box"])
def test_real_page_jersey_00_survives_read_html(stem: str, cell: str, table: str, name: str) -> None:
    """R48b-1: read_html typed KenPom's jersey columns, so "00" became 0 before any cast."""
    before = kenpom_runtime.parse_kenpom_page(_page(stem))
    after = kenpom_runtime.parse_kenpom_page(_edited(stem, cell))
    frame = after[table]
    assert frame.filter(pl.col(name).str.starts_with("Cooper Flagg"))["jersey"].to_list() == ["00"]
    # only that value changes: same keys, dtypes, rows (the section-label rows stay) and every other column
    assert before.keys() == after.keys() and before[table].schema == frame.schema
    assert before[table].height == frame.height
    changed = [(k, c) for k in before for c in before[k].columns if not before[k][c].equals(after[k][c])]
    assert changed == [(table, "jersey")]


@pytest.mark.parametrize(("stem", "cell", "table", "name"), _JERSEY_00, ids=["team", "box"])
def test_two_pages_stack_with_one_jersey_dtype(stem: str, cell: str, table: str, name: str) -> None:
    """R48b-7: a season loop stacks pages; a String / Float64 jersey raised SchemaError, and a relaxed
    concat wrote "2.0" beside "00" (the float -> text id path)."""
    pages = [kenpom_runtime.parse_kenpom_page(raw)[table] for raw in (_page(stem), _edited(stem, cell))]
    stacked = pl.concat(pages)
    assert stacked.schema["jersey"] == pl.Utf8
    jerseys = stacked["jersey"].drop_nulls().to_list()
    assert {"2", "00"} <= set(jerseys) and not [j for j in jerseys if "." in j]


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda p: p.stem)
def test_committed_pages_parse_the_same_with_the_zero_padded_guard(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Reach: no committed KenPom page has a zero-padded text cell, so ``_cast_numerics``' guard changes none of
    its 29 pages."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    fixed = kenpom_runtime.parse_kenpom_page(raw)
    monkeypatch.setattr(kenpom_runtime, "_zero_padded", lambda s: False)
    unfixed = kenpom_runtime.parse_kenpom_page(raw)
    assert fixed.keys() == unfixed.keys()
    for key in fixed:
        assert fixed[key].equals(unfixed[key]), key


def test_only_the_jersey_columns_change_on_the_committed_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reach of the jersey rule over the 29 committed pages: exactly the 5 jersey columns are renamed and
    become text, with the same values as integer text; nothing else changes."""
    plain = kenpom_runtime.html_tables
    changed = []
    for path in sorted(FIXTURES.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        new = kenpom_runtime.parse_kenpom_page(raw)
        with monkeypatch.context() as m:
            m.setattr(kenpom_runtime, "html_tables", lambda html, **kw: plain(html, **{**kw, "text_columns": None}))
            old = kenpom_runtime.parse_kenpom_page(raw)
        assert new.keys() == old.keys()
        for key in new:
            assert new[key].height == old[key].height and new[key].width == old[key].width
            for a, b in zip(new[key].columns, old[key].columns):
                if a != b or not new[key][a].equals(old[key][b]):
                    as_text = [None if v is None else str(int(v)) for v in old[key][b].to_list()]
                    assert a == "jersey" and new[key][a].to_list() == as_text, (path.stem, key, b)
                    changed.append((path.stem, key, b))
    assert changed == [
        ("box", "table_1", "column_0_rk"),
        ("box", "table_2", "column_0_rk"),
        ("team", "player_table", "column_0"),
        ("team_players_expanded", "player_table", "column_0"),
        ("team_players_expanded", "player_table_2", "column_0"),
    ]
