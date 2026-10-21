"""The processed NFL schema is identical across the 31 committed captures, except the allowlist (C-M8).

A vertical concat of per-game frames (the season compile) needs one schema; today it relies on
``diagonal_relaxed``. ``schema_allowlist.yaml`` names every column allowed to differ and the open
finding that owns it; it may only shrink. The captures run hermetically (``_whole_game.run``: no
download-on-demand model), so every machine checks the same columns; the one divergence that needs
such a model (C-M7, ``prob_2pt``) has its own test below, which loads it or skips by name.
"""

from __future__ import annotations

import functools
from collections import defaultdict
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.nfl.nfl_fourth_down import _WP_MODEL_FILE, _try_load
from tests.nfl._whole_game import CAPTURES, run

pytestmark = [pytest.mark.whole_game, pytest.mark.xdist_group("nfl_whole_game")]

ALLOWLIST = yaml.safe_load((Path(__file__).parent / "schema_allowlist.yaml").read_text(encoding="utf-8"))
_KEYS = {(r["column"], r["kind"]) for r in ALLOWLIST}

# The allowlist as 1.1 committed it. A row may be deleted (here and in the yaml) when its fix lands;
# it is never added or swapped for another: a new divergence is fixed, not allowlisted.
_PASSTHROUGH = (
    "wallclock", "extra_point_result", "pointAfterAttempt.abbreviation", "pointAfterAttempt.id",
    "pointAfterAttempt.text", "pointAfterAttempt.value", "penalty.status.slug", "penalty.status.text",
    "penalty.type.slug", "penalty.type.text", "penalty.yards", "yardsAfterCatch", "review.type",
    "review.upheld", "review.team.$ref",
)  # fmt: skip
BASELINE = frozenset({("teamParticipants", "dtype"), *((c, "presence") for c in _PASSTHROUGH)})


@functools.cache
def _differs() -> frozenset[tuple[str, str]]:
    """(column, kind) for every column whose presence or dtype differs across the captures."""
    dtypes: dict[str, set[str]] = defaultdict(set)
    present: dict[str, int] = defaultdict(int)
    for game_id in CAPTURES:
        for col, dtype in run(game_id).plays.schema.items():
            dtypes[col].add(str(dtype))
            present[col] += 1
    out = {(c, "presence") for c, n in present.items() if n != len(CAPTURES)}
    return frozenset(out | {(c, "dtype") for c, kinds in dtypes.items() if len(kinds) > 1})


def test_allowlist_rows_name_an_open_finding_and_only_shrink():
    assert all(row.get("finding") and row["kind"] in ("dtype", "presence") for row in ALLOWLIST)
    assert len(_KEYS) == len(ALLOWLIST)
    assert sorted(_KEYS - BASELINE) == [], "allowlist rows may only be deleted, never added"


def test_processed_schema_is_identical_across_captures():
    differs = _differs()
    assert sorted(differs - _KEYS) == [], "a column differs across captures: fix it or allowlist it with a finding"
    assert sorted(_KEYS - differs) == [], "an allowlist row no capture needs any more: delete it"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="C-M7: prob_2pt is Float32 when the two-point model scores (2015+ captures), Float64 when all null",
)
def test_prob_2pt_has_one_documented_dtype():
    # Documented `double` (the CFB twin is Float64). 400791508 scores tries, 221013007 (2002) does not.
    if _try_load(_WP_MODEL_FILE) is None:
        pytest.skip(f"{_WP_MODEL_FILE} cannot be loaded (offline, empty cache): the two-point model cannot score")
    dtypes = {g: run(g, fourth_down_models=True).plays.schema["prob_2pt"] for g in (400791508, 221013007)}
    assert set(dtypes.values()) == {pl.Float64}, dtypes
