from tools.codegen import generate as gen


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
