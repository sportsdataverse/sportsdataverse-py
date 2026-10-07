"""Every intake family's endpoint summaries are prose, not route paths.

The upstream specs' ``summary`` is often just the path (FIFA) or lacks a period; the
reference index and IDE hover show these lines verbatim, so each generator curates or
normalises them.
"""

from pathlib import Path

import pytest
import yaml

ENDPOINTS = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "endpoints"
FAMILIES = (
    # wave 1
    "euroleague",
    "fotmob",
    "uefa",
    "fifa",
    "sleeper",
    "f1",
    # wave 2
    "espn_content",
    "thesportsdb",
    "football_data",
    "openligadb",
    "polymarket",
    "kalshi",
)


@pytest.mark.parametrize("stem", FAMILIES)
def test_summaries_are_prose_with_a_terminal_period(stem):
    doc = yaml.safe_load((ENDPOINTS / f"{stem}.yaml").read_text(encoding="utf-8"))
    for ep in doc["endpoints"]:
        s = ep["summary"]
        assert not s.startswith(("/", "GET ")), (stem, ep["short"], s)
        assert s.endswith("."), (stem, ep["short"], s)
        # "mmz4281/{season}/{div}.csv." passed both assertions above, so pin the two things
        # that actually separate prose from a route: no path template, and more than one word.
        assert "{" not in s, (stem, ep["short"], s)
        assert len(s.split()) > 1, (stem, ep["short"], s)
