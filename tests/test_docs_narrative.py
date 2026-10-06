"""The narrative docs describe the package as it is in 0.1.5.

Every number asserted here was measured against this tree, not copied from the prose it
replaces -- that is how the old counts (29 leagues, 3,334 wrappers, six parser modules,
89 captures) survived so long.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FILES = {
    "claude": ROOT / "CLAUDE.md",
    "copilot": ROOT / ".github" / "copilot-instructions.md",
    "readme": ROOT / "README.md",
    "contributing": ROOT / "CONTRIBUTING.md",
    "intro": ROOT / "docs" / "docs" / "intro.md",
    "qol": ROOT / "docs" / "docs" / "quality-of-life.md",
    "espn": ROOT / "docs" / "docs" / "architecture" / "espn-cross-league.md",
    "parsers": ROOT / "docs" / "docs" / "parsers" / "index.md",
    "ecosystem": ROOT / "docs" / "docs" / "ecosystem.md",
}


def text(key: str) -> str:
    return FILES[key].read_text(encoding="utf-8")


def flat(key: str) -> str:
    """``text(key)`` with every run of whitespace collapsed to one space.

    These files are hard-wrapped prose, so a phrase under assertion can straddle a line
    break. Matching against the flattened text keeps the assertions about WORDS rather
    than about where the author happened to wrap."""
    return " ".join(text(key).split())


# --- CLAUDE.md -------------------------------------------------------------

_CLAUDE_STALE = [
    "29 league modules",
    "3,334 wrappers",
    "Six parser modules",
    "89 captures across 6 directories",
    "parsers/fixtures.md",
    "24 canonical load_nfl_*",
    "25 nflreadpy-style aliases",
    "226 generated call sites",
    "PWHL + 20",
    "make_league_module",
    "_bind(",
    "4-line files",
    "`docs`, `models`, `all`",
    "README has Standard pip / Modern uv / Conda",
]

_CLAUDE_CURRENT = [
    "30 league modules",
    "3,446 wrappers",
    "33 parser modules",
    "1,459 fixture files across 89 directories",
    "45 canonical load_nfl_*",
    "24 nflreadpy-style aliases",
    "291 generated call sites",
    "PWHL + 19",
    "`tests`, `nflpro`, `models`, `pff`, `mcp`, `all`",
    "architecture/codegen.md",
    "main (latest)",
    "VERSIONS_TO_KEEP=3",
]

_CLAUDE_STRUCTURE_DIRS = [
    "cbs/",
    "fox/",
    "yahoo/",
    "registry/",
    "validation/",
    "wexp/",
    "scrape/",
    "parsed/",
    "sdv_docs/",
    "nbagl/",
]

_MISSING_FAMILIES = [
    "fox_api",
    "cbs_napi",
    "torvik",
    "asa",
    "mls_api",
    "nwsl_api",
    "football/sources",
    "nba_officiating",
    "rolling_windows",
    "metric_curves",
    "defense_vs_position",
    "paper_index",
    "tendencies",
    "usage_box",
    "validation",
    "wexp",
    "nbagl",
]


@pytest.mark.parametrize("stale", _CLAUDE_STALE)
def test_claude_md_drops_the_stale_claim(stale):
    assert stale not in flat("claude"), f"CLAUDE.md still claims {stale!r}"


@pytest.mark.parametrize("current", _CLAUDE_CURRENT)
def test_claude_md_states_the_current_fact(current):
    assert current in flat("claude")


@pytest.mark.parametrize("d", _CLAUDE_STRUCTURE_DIRS)
def test_claude_md_project_structure_lists_the_directory(d):
    body = text("claude").split("## Project Structure", 1)[1].split("\n## ", 1)[0]
    assert d in body


@pytest.mark.parametrize("family", _MISSING_FAMILIES)
def test_claude_md_mentions_the_family(family):
    assert family in text("claude")


def test_claude_md_test_counts_are_current():
    body = text("claude")
    for n in ("| 134 |", "**241**", "| 80 |", "11,446"):
        assert n in body, f"stale test count: {n} missing"
    for n in ("| 128 |", "**235**", "| 41 |"):
        assert n not in body, f"stale test count still present: {n}"


def test_claude_md_has_a_codegen_section_linking_the_architecture_page():
    body = text("claude")
    assert "## Codegen" in body
    assert "docs/docs/architecture/codegen.md" in body


def test_claude_md_error_vocabulary_is_0_1_5():
    body = text("claude")
    assert "### Error vocabulary (0.1.5)" in body
    assert "### Error vocabulary (0.1.0)" not in body
    assert "AssetFetchError" in body and "400 / 422" in body


def test_claude_md_gained_the_copilot_only_sections():
    body = text("claude")
    assert "line-length = 120" in body
    assert "## Module naming" in body or "### Module naming" in body
    assert "conda lockstep" in body


def test_claude_md_flat_api_count_matches_the_code():
    from tools.codegen import generate

    assert f"`FLAT_APIS` has {len(generate.FLAT_APIS)} families" in text("claude")
