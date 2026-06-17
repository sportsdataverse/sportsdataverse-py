import re

from tools.codegen import generate as gen
from tools.codegen import extract_residual_columns as extract


def test_manual_col_desc_schema_then_global_then_empty(monkeypatch):
    fake = {"nfl_load_pbp": {"air_yards": "AY desc"}, "_global": {"season": "SE desc"}}
    monkeypatch.setattr(gen, "_manual_col_descs", lambda: fake)
    assert gen._manual_col_desc("nfl_load_pbp", "air_yards") == "AY desc"
    assert gen._manual_col_desc("other_schema", "season") == "SE desc"  # _global fallback
    assert gen._manual_col_desc("nfl_load_pbp", "season") == "SE desc"  # schema miss -> _global
    assert gen._manual_col_desc("nfl_load_pbp", "unknown") == ""
    assert gen._manual_col_desc(None, "season") == "SE desc"


def test_table_cell_desc_priority(monkeypatch):
    fake = {"nfl_load_pbp": {"cpoe": "manual cpoe"}, "_global": {}}
    monkeypatch.setattr(gen, "_manual_col_descs", lambda: fake)
    monkeypatch.setattr(gen, "_r_col_desc", lambda league, col: "rdict desc")
    # stored wins over everything
    assert gen._table_cell_desc("kept", "nfl", "cpoe", "nfl_load_pbp") == "kept"
    # manual[schema] wins over r-dict
    assert gen._table_cell_desc("", "nfl", "cpoe", "nfl_load_pbp") == "manual cpoe"
    # no manual entry -> r-dict
    assert gen._table_cell_desc("", "nfl", "other", "nfl_load_pbp") == "rdict desc"
    # schema=None still resolves r-dict (back-compat path)
    assert gen._table_cell_desc("", "nfl", "other") == "rdict desc"


def test_residual_columns_have_required_fields():
    rows = extract.residual_columns()
    assert isinstance(rows, list) and rows, "expected a non-empty residual work-list"
    sample = rows[0]
    for k in ("schema", "col", "type", "league", "siblings"):
        assert k in sample, f"missing {k} in residual row"


def test_residual_total_matches_known_baseline():
    # Baseline ratchets DOWN as buckets are filled (only ever lowered, never raised).
    # 3352ed0: 3061 → NFL bucket (Task 3): 1903.
    total = len(extract.residual_columns())
    assert total <= 1903, f"residual grew to {total} (>1903) — new blank columns appeared"


_BANNED = re.compile(r"^(the\s+)?\w+(\s+\w+)?\s+(column|field|value|id|name)\.?$", re.I)


def _all_manual_entries():
    d = gen._manual_col_descs()
    for schema, cols in d.items():
        if not isinstance(cols, dict):
            continue
        for col, desc in cols.items():
            yield schema, col, desc


def test_no_orphan_manual_entries():
    valid = {(r["schema"], r["col"]) for r in extract.iter_schema_columns()}
    valid_cols = {r["col"] for r in extract.iter_schema_columns()}
    orphans = []
    for schema, col, _ in _all_manual_entries():
        if schema == "_global":
            if col not in valid_cols:
                orphans.append(f"_global.{col}")
        elif (schema, col) not in valid:
            orphans.append(f"{schema}.{col}")
    assert not orphans, f"manual dict has stale keys (no matching column): {orphans[:10]}"


def test_no_filler_descriptions():
    bad = []
    for schema, col, desc in _all_manual_entries():
        d = (desc or "").strip()
        if len(d) < 15 or d.lower() == col.lower() or _BANNED.match(d):
            bad.append(f"{schema}.{col}: {desc!r}")
    assert not bad, f"filler/low-quality descriptions: {bad[:10]}"
