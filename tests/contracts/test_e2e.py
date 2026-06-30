from __future__ import annotations

import polars as pl

from tools.validation.checks import e2e
from tools.validation.findings import Severity


def test_join_key_dtype_disagreement_is_error():
    up = pl.DataFrame({"game_id": [1, 2]})  # Int64
    dn = pl.DataFrame({"game_id": ["1", "2"]})  # String
    findings = e2e.run("nfl_pbp", up, dn, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "dtype disagreement" in f.message for f in findings)


def test_orphan_downstream_keys_is_error():
    up = pl.DataFrame({"game_id": [1, 2]})
    dn = pl.DataFrame({"game_id": [2, 3]})  # 3 absent upstream
    findings = e2e.run("nfl_pbp", up, dn, ("game_id",), "nfl")
    assert any(f.severity is Severity.ERROR and "absent upstream" in f.message for f in findings)
