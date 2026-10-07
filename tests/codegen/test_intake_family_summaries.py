"""The wave-1 intake families' endpoint summaries are prose, not route paths.

The upstream specs' ``summary`` is often just the path (FIFA) or lacks a period; the
reference index and IDE hover show these lines verbatim, so each generator curates or
normalises them.
"""

from pathlib import Path

import pytest
import yaml

ENDPOINTS = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "endpoints"
FAMILIES = ("euroleague", "fotmob", "uefa", "fifa", "sleeper", "f1")


@pytest.mark.parametrize("stem", FAMILIES)
def test_summaries_are_prose_with_a_terminal_period(stem):
    doc = yaml.safe_load((ENDPOINTS / f"{stem}.yaml").read_text(encoding="utf-8"))
    for ep in doc["endpoints"]:
        s = ep["summary"]
        assert not s.startswith(("/", "GET ")), (stem, ep["short"], s)
        assert s.endswith("."), (stem, ep["short"], s)
