from __future__ import annotations

import glob
import os

import pytest

_NFL_ROOT = os.environ.get("SDV_VALIDATION_NFL_DATA_ROOT")
_has_nfl = bool(_NFL_ROOT and glob.glob(os.path.join(_NFL_ROOT, "out", "model_pbp_*.parquet")))
skip_if_no_nfl_data = pytest.mark.skipif(
    not _has_nfl, reason="set SDV_VALIDATION_NFL_DATA_ROOT to the nfl-data repo root to run NFL parity"
)


@skip_if_no_nfl_data
def test_nfl_model_pbp_numeric_parity_live():
    from tools.validation import cli

    out = cli.run_dataset("nfl_model_pbp")
    # parity WARNs (corr < floor) are allowed (needs_judgment); hard ERRORs are not
    errors = [d for d in out if d["severity"] == "error"]
    assert not errors, f"unexpected ERROR findings: {errors}"
