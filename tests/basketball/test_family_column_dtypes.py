"""A column name the family's gen script types from its fixtures has one dtype across the family's returns-schemas.

Each pair here was typed two ways before: a column null in every fixture of one wrapper was typed by its name (String)
while a sibling wrapper observed it (fiba, vtb, kzs), or a parser cast differed per table (scb, sportality, tbf). The
gen scripts now borrow the family's observed dtype for an unobserved column, and the parsers cast the rest.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Set

import pytest
import yaml

SCHEMAS = Path(__file__).parents[2] / "tools" / "codegen" / "schemas" / "native"
FIXED = [
    ("fiba_gamepage", "blocks_received", "integer"),
    ("fiba_gdap", "utc_offset", "integer"),
    ("vtb_infobasket", "plus_minus", "integer"),
    ("vtb_infobasket", "player_position", "integer"),
    ("vtb_infobasket", "height", "integer"),
    ("vtb_infobasket", "weight", "integer"),
    ("kzs", "season", "integer"),
    ("scb", "shirt_number", "character"),  # a jersey can be 00
    ("sportality", "blkpg", "numeric"),
    ("tbf", "efficiency", "integer"),
]


def _types(stem: str, column: str) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for path in sorted((SCHEMAS / stem).glob("*.yaml")):
        for col in yaml.safe_load(path.read_text(encoding="utf-8")).get("columns") or []:
            if col.get("name") == column:
                out.setdefault(col["type"], set()).add(path.stem)
    return out


@pytest.mark.parametrize(("stem", "column", "want"), FIXED, ids=[f"{s}.{c}" for s, c, _ in FIXED])
def test_column_has_one_type_across_the_family(stem, column, want):
    types = _types(stem, column)
    assert sum(len(v) for v in types.values()) >= 2, f"{stem}.{column} is no longer in two schemas"
    assert set(types) == {want}, types
