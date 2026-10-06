"""Retrieval set: real questions against a full (online) index build.

Runs only when SDV_DOCS_RETRIEVAL_DB is set (unset = skip; set but empty or pointing
at a missing file = FAIL, so a misconfigured CI step cannot pass silently). The docs-index workflow sets it,
and a failure there blocks publishing. Never lower an expectation to make it pass:
fix the builder or the ranking.
"""

import os
from pathlib import Path

import pytest
import yaml

from sdv_docs import server

DB = os.environ.get("SDV_DOCS_RETRIEVAL_DB")
CASES = yaml.safe_load((Path(__file__).parent / "retrieval.yaml").read_text(encoding="utf-8"))

pytestmark = pytest.mark.skipif(DB is None, reason="set SDV_DOCS_RETRIEVAL_DB to a full sdv-docs index build")


def test_retrieval_db_exists():
    assert DB and Path(DB).is_file(), f"retrieval DB missing: {DB!r}"


def test_every_lookup_has_at_least_six_cases():
    groups = [c["id"].split("-")[0] for c in CASES]
    for g in ("col", "fn", "ep", "ds"):
        assert groups.count(g) >= 6, g


@pytest.mark.parametrize("case", CASES, ids=[c["id"] for c in CASES])
def test_retrieval(case, monkeypatch):
    monkeypatch.setenv("SDV_DOCS_DB", DB)
    out = getattr(server, case["tool"])(**case["args"])
    for s in case["expect"]:
        assert s in out, f"{case['id']}: {s!r} not in:\n{out[:2000]}"
    for s in case.get("reject", []):
        assert s not in out, f"{case['id']}: {s!r} unexpectedly in output"
