"""The narrative docs describe the package as it is in 0.1.5.

Every number asserted here was measured against this tree, not copied from the prose it
replaces -- that is how the old counts (29 leagues, 3,334 wrappers, six parser modules,
89 captures) survived so long.
"""

from __future__ import annotations

import subprocess
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
    "45 canonical load_nfl_*",
    "24 nflreadpy-style aliases",
    "291 generated call sites",
    "PWHL + 19",
    "`tests`, `nflpro`, `models`, `pff`, `soccer`, `mcp`, `all`",
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


def test_claude_md_fixture_count_matches_the_tree():
    """Measured, not pinned: the pinned "1,459 fixture files" went stale on the first merge."""
    # Tracked files only: a run that imports a fixture-dir helper script writes a __pycache__/*.pyc there,
    # which made main's live job count 1,698 against the 1,696 committed.
    tracked = (
        subprocess.run(["git", "ls-files", "-z", "--", "tests/fixtures"], cwd=ROOT, capture_output=True, check=True)
        .stdout.decode()
        .split("\0")
    )
    rel = [f.removeprefix("tests/fixtures/") for f in tracked if f]
    files = len(rel)
    dirs = len({f.split("/")[0] for f in rel if "/" in f})
    assert f"{files:,} fixture files across {dirs} directories" in flat("claude")


def test_claude_md_flat_api_count_matches_the_code():
    from tools.codegen import generate

    assert f"`FLAT_APIS` has {len(generate.FLAT_APIS)} families" in text("claude")


# --- .github/copilot-instructions.md ---------------------------------------

_COPILOT_STALE = [
    "23 canonical",
    "112 wrappers",
    "95 wrappers",
    "[[tool.mypy.overrides]] module",
    "uv run mypy sportsdataverse/<your_module>.py",
    "Use `from __future__ import annotations` only when targeting py3.8",
    "runs only to inject",
    "CI does NOT set this var by default",
    "mlbam_games",
    "`yahoo_cfb_*` wraps Yahoo Sports",
    "Error vocabulary (0.1.0)",
    "Reference-docs build toolchain (codegen)",
]

_COPILOT_CURRENT = [
    "45 canonical",
    "128 wrappers",
    "111 wrappers",
    "[tool.mypy] files",
    "DO use `from __future__ import annotations`",
    "Python 3.13.2 only",
    "fox_api",
    "107 multi-sport `yahoo_*`",
    "AssetFetchError",
    "400 / 422",
    "architecture/codegen.md",
    "returns the last response",
]

# Copilot uses Title Case headings; these match the file's own convention.
_COPILOT_SECTIONS_FROM_CLAUDE = [
    "## Project Structure",
    "## ESPN Cross-League Architecture",
    "## Parser Layer",
    "## PFF",
    "## sdv-docs MCP Server",
    "## Release Utilities",
    "## Rule-Era Models",
    "## ID Column Types",
    "## Error Vocabulary (0.1.5)",
    "## Codegen",
]


@pytest.mark.parametrize("stale", _COPILOT_STALE)
def test_copilot_drops_the_stale_claim(stale):
    assert stale not in flat("copilot"), f"copilot-instructions.md still claims {stale!r}"


@pytest.mark.parametrize("current", _COPILOT_CURRENT)
def test_copilot_states_the_current_fact(current):
    assert current in flat("copilot")


@pytest.mark.parametrize("heading", _COPILOT_SECTIONS_FROM_CLAUDE)
def test_copilot_gained_the_claude_only_section(heading):
    assert heading in text("copilot")


def test_copilot_carries_the_two_pitfalls_it_lacked():
    body = flat("copilot")
    assert "ESPN injuries come from the LEAGUE endpoint" in body
    assert "Regenerate generated files before pushing" in body


def test_copilot_states_the_parsed_deprecation():
    assert "`sportsdataverse.parsed.*` is **deprecated**" in flat("copilot")


# --- README.md and CONTRIBUTING.md -----------------------------------------

_README_STALE = [
    "6,180 exported names",
    "112 NBA",
    "95 WNBA wrappers",
    "once we add a PEP 735",
    "from the next release (0.1.5+)",
    "git+https://github.com/sportsdataverse/sportsdataverse-py' sdv-docs`",
]

_README_CURRENT = [
    "6,456 exported names",
    "128 NBA",
    "111 WNBA wrappers",
    "sportsdataverse[nflpro]",
    "sportsdataverse[pff]",
    "sportsdataverse[mcp]",
    "EXCEPT mcp",
    "fox_api",
    "107 multi-sport `yahoo_*`",
    "cbs_napi",
    "### Errors (0.1.5)",
    "AssetFetchError",
]

_CONTRIBUTING_STALE = [
    "uv run mypy sportsdataverse/\n",
    "without `from __future__ import annotations`",
    "CI runs against the floor (3.9)",
    "there is no in-repo deploy workflow",
    "labelled `main`)",
]

_CONTRIBUTING_CURRENT = [
    "DO add `from __future__ import annotations`",
    "CI runs **3.13.2 only**",
    "docs-deploy.yml",
    "main (latest)",
    "VERSIONS_TO_KEEP=3",
    "had not been done since 0.0.75",
    "`removed_in` is a commitment",
    "architecture/codegen.md",
    "sdv_docs/",
]


@pytest.mark.parametrize("stale", _README_STALE)
def test_readme_drops_the_stale_claim(stale):
    assert stale not in flat("readme"), f"README.md still claims {stale!r}"


@pytest.mark.parametrize("current", _README_CURRENT)
def test_readme_states_the_current_fact(current):
    assert current in flat("readme")


@pytest.mark.parametrize("stale", _CONTRIBUTING_STALE)
def test_contributing_drops_the_stale_claim(stale):
    assert stale.strip() not in flat("contributing"), f"CONTRIBUTING.md still claims {stale!r}"


@pytest.mark.parametrize("current", _CONTRIBUTING_CURRENT)
def test_contributing_states_the_current_fact(current):
    assert current in flat("contributing")


# --- the docs-site conceptual pages ---------------------------------------

_SITE_STALE = {
    "intro": ["PWHL + 20", "import mlb_api_person_stats"],
    "qol": ["~4,400 callables"],
    "espn": ["800+", "| NBA | 113 |"],
    "parsers": ["All 121 wrapper short names", "make_league_module", "18 parsers"],
    "ecosystem": ["ten executed"],
}

_SITE_CURRENT = {
    "intro": ["## Errors (0.1.5)", "AssetFetchError", "sdv-docs", "is deprecated"],
    "qol": ["~6,170 callables", "**deprecated**"],
    "espn": ["126 short names across 30 leagues", "| CFB | 125 |", "default to `return_parsed=True`"],
    "parsers": ["All 131 entries", "33 parser modules", "21-section", "Errors (0.1.5)"],
    "ecosystem": ["fifteen executed"],
}


@pytest.mark.parametrize(
    ("key", "stale"),
    [(k, v) for k, vals in _SITE_STALE.items() for v in vals],
)
def test_site_page_drops_the_stale_claim(key, stale):
    assert stale.strip() not in flat(key), f"{key} still claims {stale!r}"


@pytest.mark.parametrize(
    ("key", "current"),
    [(k, v) for k, vals in _SITE_CURRENT.items() for v in vals],
)
def test_site_page_states_the_current_fact(key, current):
    assert current in flat(key)


def test_espn_page_per_league_counts_match_the_code():
    """The per-league table is a snapshot, but a wrong snapshot is what this program is fixing."""
    from tools.codegen import generate, spec

    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    body = flat("espn")
    for prefix, label in (("cfb", "CFB"), ("mbb", "MBB"), ("nfl", "NFL"), ("nba", "NBA"), ("nhl", "NHL")):
        league = next(x for x in cfg.leagues if x.prefix == prefix)
        n = len(generate._espn_league_views(league, apis, cfg.hosts))
        assert f"| {label} | {n} |" in body, f"{label} should read {n}"


def test_docusaurus_config_comment_is_current():
    cfg = (ROOT / "docs" / "docusaurus.config.ts").read_text(encoding="utf-8")
    assert "main (latest)" in cfg
    assert "docs-deploy.yml" in cfg
    assert "/docs/0.0.50/ tree) are NOT served" in cfg


def test_claude_md_data_sources_names_every_registry_provider():
    """The provider paragraph is hand-written, and it missed Jolpica and kloppy at one merge and
    the five wave-2 providers at the next. Every sources.yaml provider must appear by label, key or
    flat-API stem (ESPN and the two release-loader providers are the paragraph's stated baseline)."""
    from tools.codegen import sources

    body = text("claude").split("## Data sources", 1)[1].split("\n## ", 1)[0]
    flat_body = " ".join(body.split()).lower()
    missing = [
        e.key
        for e in sources.providers()
        if e.key not in ("espn", "nflverse", "sdv_releases")
        and not any(n.lower() in flat_body for n in (e.label, e.key, *e.flat_apis))
    ]
    assert not missing, f"CLAUDE.md's Data sources section omits: {missing}"
