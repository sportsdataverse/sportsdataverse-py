"""The processed NFL schema is identical across the 31 committed captures, except the allowlist (C-M8).

A vertical concat of per-game frames (the season compile) needs one schema; today it relies on
``diagonal_relaxed``. ``schema_allowlist.yaml`` names every column allowed to differ and the open
finding that owns it; it may only shrink.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pytest
import yaml

from sportsdataverse.nfl.nfl_fourth_down import _try_load
from tests.nfl._whole_game import CAPTURES, run

pytestmark = pytest.mark.xdist_group("nfl_whole_game")

ALLOWLIST = yaml.safe_load((Path(__file__).parent / "schema_allowlist.yaml").read_text(encoding="utf-8"))
ALLOWLIST_ROWS = 17  # lower this with every row a fix deletes; never raise it


def test_allowlist_rows_name_an_open_finding_and_only_shrink():
    assert len(ALLOWLIST) <= ALLOWLIST_ROWS
    assert all(row.get("finding") and row["kind"] in ("dtype", "presence") for row in ALLOWLIST)
    assert len({(r["column"], r["kind"]) for r in ALLOWLIST}) == len(ALLOWLIST)


def test_processed_schema_is_identical_across_captures():
    dtypes: dict[str, set[str]] = defaultdict(set)
    present: dict[str, int] = defaultdict(int)
    for game_id in CAPTURES:
        for col, dtype in run(game_id).plays.schema.items():
            dtypes[col].add(str(dtype))
            present[col] += 1
    differs = {(c, "presence") for c, n in present.items() if n != len(CAPTURES)}
    differs |= {(c, "dtype") for c, kinds in dtypes.items() if len(kinds) > 1 and present[c] == len(CAPTURES)}
    allowed = {(r["column"], r["kind"]) for r in ALLOWLIST}
    # a row whose divergence needs a download-on-demand model is only exercised when that model loads
    exercised = {
        (r["column"], r["kind"]) for r in ALLOWLIST if "needs_model" not in r or _try_load(r["needs_model"]) is not None
    }
    assert sorted(differs - allowed) == [], "a column differs across captures: fix it or allowlist it with a finding"
    assert sorted(exercised - differs) == [], "an allowlist row no capture needs any more: delete it"
