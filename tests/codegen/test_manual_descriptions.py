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
    monkeypatch.setattr(gen, "_r_col_desc", lambda league, col, schema=None: "rdict desc")
    # stored wins over everything
    assert gen._table_cell_desc("kept", "nfl", "cpoe", "nfl_load_pbp") == "kept"
    # manual[schema] wins over r-dict
    assert gen._table_cell_desc("", "nfl", "cpoe", "nfl_load_pbp") == "manual cpoe"
    # no manual entry -> r-dict
    assert gen._table_cell_desc("", "nfl", "other", "nfl_load_pbp") == "rdict desc"
    # schema=None still resolves r-dict (back-compat path)
    assert gen._table_cell_desc("", "nfl", "other") == "rdict desc"


def test_residual_columns_have_required_fields():
    # residual_columns() is empty at full coverage (the goal), so assert the row
    # SHAPE against iter_schema_columns() — the full column walk is always non-empty
    # and shares the residual row shape (residual_columns is a filter of it).
    assert isinstance(extract.residual_columns(), list)
    rows = extract.iter_schema_columns()
    assert rows, "expected a non-empty schema-column walk"
    sample = rows[0]
    for k in ("schema", "col", "type", "league", "siblings"):
        assert k in sample, f"missing {k} in schema-column row"


def test_residual_total_matches_known_baseline():
    # Baseline ratchets DOWN as buckets are filled (only ever lowered, never raised).
    # 3061 → NFL 1903 → MLB 1304 → NHL 716 → CFB 539 → remaining (Task 7): 0.
    # Terminal: every return-table column now renders a description. A NEW blank
    # column (e.g. a newly-captured endpoint) fails this test until it is authored
    # in tools/codegen/manual_column_descriptions.yaml.
    total = len(extract.residual_columns())
    assert total <= 0, f"residual grew to {total} (>0) — new blank columns need descriptions"


def test_deferred_buckets_do_not_grow():
    # A deferred bucket is capped at its measured backlog, so a NEW blank column in it
    # still fails; lower the cap (never raise it) as columns are authored.
    counts: dict = {}
    for r in extract.deferred_columns():
        counts[r["bucket"]] = counts.get(r["bucket"], 0) + 1
    grown = {
        b: (counts.get(b, 0), cap)
        for b, cap in extract._DEFERRED_BUCKETS.items()
        if cap is not None and counts.get(b, 0) > cap
    }
    assert not grown, f"deferred buckets grew past their cap (count, cap): {grown}"


def test_native_schemas_resolve_fallback_with_their_page_league():
    # A native schema's path names an API family, not a league, but its reference page renders
    # R-dict fallback text with the FLAT_APIS league (``_return_table(rs, prefix)``). The check
    # must resolve with the same league, or it reads text the page never shows.
    import os

    import yaml

    checked = 0
    for stem, prefix in gen.FLAT_APIS:
        y = gen.ENDPOINTS / f"{stem}.yaml"
        if not y.exists():
            continue
        for ep in yaml.safe_load(y.read_text(encoding="utf-8"))["endpoints"]:
            rs = ep.get("returns_schema")
            path = os.path.join(extract.SCHEMA_DIR, f"{rs}.yaml") if rs else ""
            if path and os.path.exists(path):
                assert extract._league_of(path) == prefix, rs
                checked += 1
    assert checked > 500


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
        if schema.startswith("fox_api_"):
            # sdv-js public names (``fox_api_<short>``): sdv-py has no Fox Bifrost schema
            # (``schema_compatible: false``), but sdv-js vendors this file and resolves a
            # table's blanks by its public name, so the block is read there, not here.
            continue
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
