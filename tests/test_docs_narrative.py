"""The narrative docs describe the package as it is in 0.2.0.

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
    # polars floor raised to 2 (2026-10-11): the 1.x-and-2.x framing must not come back.
    "polars 1.x and 2.x",
    "`polars>=1.0,<3`",
    "the 1.36 resolution",
    "run on both 1.x and 2.0",
    "don't exist in 1.x",
    "polars 1.x/2.x",
    # a path argument bypasses the [tool.mypy] files ratchet
    "uv run mypy sportsdataverse/<your_module>.py",
    # only ruff defaults + BLE001 run; `I` is off on purpose (pre-commit config header)
    "`I` rules own import order",
    # counts that went stale within weeks; the prose no longer pins them (see the computed tests below)
    "33 parser modules",
    "universal across all 6 modules",
    "147 directories",
    "11,446",
    "3,446 wrappers",
    "30 league modules",
    "docs_instructions.md",
    "a 4-line",
    "bump polars to >=1.0,<2.0",
    "add py3.14 to the test matrix",
]

_CLAUDE_CURRENT = [
    "45 canonical load_nfl_*",
    "24 nflreadpy-style aliases",
    "291 generated call sites",
    "PWHL + 19",
    "`tests`, `nflpro`, `models`, `pff`, `soccer`, `sdvplot`, `sportypy`, `mcp`, `all`",
    "architecture/codegen.md",
    "main (latest)",
    "VERSIONS_TO_KEEP=3",
    "`polars>=2.0,<3`",
    "polars 1.x is no longer supported",
    "replacements are available now that the floor is 2.0",
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


def test_claude_md_test_table_names_real_files_and_no_counts():
    """The table once pinned per-file and whole-suite test counts, which went stale with every PR.
    It now lists files only (counts come from ``pytest --collect-only``); each file must exist."""
    import re

    body = text("claude").split("### Test infrastructure summary", 1)[1].split("\n## ", 1)[0]
    files = re.findall(r"`(tests/[\w/]+\.py)`", body)
    assert len(files) >= 5
    for f in files:
        assert (ROOT / f).is_file(), f"test table names a missing file: {f}"
    assert "--collect-only" in body
    assert not re.search(r"\| \*{0,2}[\d,]+\*{0,2} \|", body), "the test table pins a count again"


def _espn_wrapper_totals() -> tuple[int, int]:
    """(league modules, generated ``espn_*`` wrappers), resolved the way codegen emits them."""
    from tools.codegen import generate, spec

    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    return len(cfg.leagues), sum(len(generate._espn_league_views(lg, apis, cfg.hosts)) for lg in cfg.leagues)


@pytest.mark.parametrize("key", ["claude", "copilot"])
def test_espn_wrapper_total_matches_the_code(key):
    n_leagues, n_wrappers = _espn_wrapper_totals()
    body = flat(key)
    assert f"{n_leagues} league modules" in body, f"{key} should say {n_leagues} league modules"
    assert f"{n_wrappers:,} wrappers" in body, f"{key} should say {n_wrappers:,} wrappers"


@pytest.mark.parametrize("key", ["claude", "copilot", "parsers"])
def test_any_parser_module_count_matches_the_code(key):
    """The prose may omit the number, but a stated one must be the real count."""
    import re

    real = len(list((ROOT / "sportsdataverse").rglob("*_parsers.py")))
    for claimed in re.findall(r"\*{0,2}(\d+) parser modules", flat(key)):
        assert int(claimed) == real, f"{key} claims {claimed} parser modules; there are {real}"


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


@pytest.mark.parametrize("key", ["claude", "copilot"])
def test_nflreadpy_alias_count_matches_the_code(key):
    """Counted from the ``load_nfl_* as load_*`` imports in ``sportsdataverse/nfl/__init__.py``."""
    import re

    init = (ROOT / "sportsdataverse" / "nfl" / "__init__.py").read_text(encoding="utf-8")
    n = len(re.findall(r"\bload_nfl_\w+ as load_\w+", init))
    assert f"{n} nflreadpy-style" in flat(key), f"{key} should say {n} nflreadpy-style aliases"


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
    "CI runs **Python 3.13.2 only**",
    "No other version is exercised",
    "Polars 1.x and 2.x",
    "`polars>=1.0,<3`",
    "the floor stays 1.0",
    "`I` rules own import order",
    "import sorting, pyupgrade",
    "25 nflreadpy-style",
    "442 entries",
    "682 of 729",
    "4-line module",
    "bump polars to >=1.0,<2.0",
]

_COPILOT_CURRENT = [
    "45 canonical",
    "128 wrappers",
    "111 wrappers",
    "[tool.mypy] files",
    "DO use `from __future__ import annotations`",
    "The suite runs on **3.13.2 only**",
    "installs every extra and imports every module on the **3.10 floor** (`tests.yml` `floor-import`)",
    "No other version runs the full suite",
    "fox_api",
    "107 multi-sport `yahoo_*`",
    "AssetFetchError",
    "400 / 422",
    "architecture/codegen.md",
    "returns the last response",
    "`polars>=2.0,<3`",
    "polars 1.x is not supported",
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
    "polars 1.x or 2.x",
    "will become the one-shot",
    "PFF Developer + Premium clients",
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
    "polars 2.x (`polars>=2.0,<3`)",
]

_CONTRIBUTING_STALE = [
    "uv run mypy sportsdataverse/\n",
    "without `from __future__ import annotations`",
    "CI runs against the floor (3.9)",
    "there is no in-repo deploy workflow",
    "labelled `main`)",
    "CI runs **3.13.2 only**",
    "and ceiling (3.14) at minimum",
    "No other version is exercised",
    "Polars 1.x and 2.x",
    "`polars>=1.0,<3`",
    "the floor stays 1.0",
]

_CONTRIBUTING_CURRENT = [
    "DO add `from __future__ import annotations`",
    "The suite runs on **3.13.2 only**",
    "installs every extra and imports every module on the **3.10 floor** (`tests.yml` `floor-import`)",
    "No other version runs the full suite",
    "docs-deploy.yml",
    "main (latest)",
    "VERSIONS_TO_KEEP=3",
    "had not been done since 0.0.75",
    "`removed_in` is a commitment",
    "architecture/codegen.md",
    "sdv_docs/",
    "`polars>=2.0,<3`",
    "polars 1.x is not supported",
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


def test_docs_state_the_polars_floor_pyproject_declares():
    """The four docs above assert `polars>=2.0,<3`; this keeps that string tied to the real pin."""
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert '"polars>=2.0,<3",' in pyproject


# --- the docs-site conceptual pages ---------------------------------------

_SITE_STALE = {
    "intro": [
        "PWHL + 20",
        "import mlb_api_person_stats",
        "from sportsdataverse.parsed",
        '"event_id", "home_name"',
        # 0.1.5 was never released; headings carry no version stamps (#1021).
        "(0.1.5)",
        "0.1.5 or newer",
        "(0.0.54+)",
        "Lifecycle:experimental",
        "/docs/parsers/index",
        "season = {2021}",
        "# **Our Authors**",
    ],
    "qol": [
        "~4,400 callables",
        "remains fully supported",
        "Explicit parsed namespace (unchanged",
        "~6,170 callables",
        "from sportsdataverse.parsed.nba import espn_nba_summary",
    ],
    "espn": ["800+", "| NBA | 113 |", "**84** wrappers per league", "plain partial", "30 documented leagues"],
    "parsers": [
        "All 121 wrapper short names",
        "make_league_module",
        "18 parsers",
        "33 parser modules",
        "18 sub-frames",
        "20 sub-frames",
        "Errors (0.1.5)",
        "(0.0.54+)",
    ],
    "ecosystem": [
        "ten executed",
        "explicit parsed-by-default namespace",
        "same shape in all 8 leagues",
        "don't use the generated release-loader pages",
        "fastRhockey-data",
    ],
}

_SITE_CURRENT = {
    "intro": ["## Errors", "AssetFetchError", "sdv-docs", "is deprecated", "pip install --pre sportsdataverse"],
    "qol": ["from sportsdataverse.nba import espn_nba_summary", "**deprecated**"],
    "espn": ["| CFB | 125 |", "default to `return_parsed=True`"],
    "parsers": ["21-section", "**Errors.**", "21 sub-frames"],
    "ecosystem": ["nineteen executed", "cfbfastR-cfb-data", "nfl/reference/loaders.md"],
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


# The hand-written pages (everything else under docs/docs is generated by codegen or from notebooks).
_SITE_PAGES = sorted(
    p
    for p in (ROOT / "docs" / "docs").rglob("*.md")
    if p.relative_to(ROOT / "docs" / "docs").parts[0]
    in {"intro.md", "ecosystem.md", "quality-of-life.md", "guides", "architecture", "parsers"}
)


@pytest.mark.parametrize("page", _SITE_PAGES, ids=lambda p: p.relative_to(ROOT / "docs" / "docs").as_posix())
def test_site_page_has_a_description_and_bracketed_admonitions(page):
    """Docusaurus 3 renders ``:::caution Title`` as literal text: the title goes in brackets. And a page
    without ``description:`` front matter gets its first paragraph (once, a badge's alt text) as its
    search snippet."""
    body = page.read_text(encoding="utf-8")
    front = body.split("---", 2)[1] if body.startswith("---") else ""
    assert "\ndescription:" in front, f"{page.name} has no description: front matter"
    bad = [ln for ln in body.splitlines() if ln.startswith(":::") and len(ln.split(None, 1)) > 1]
    assert not bad, f"{page.name} uses the old admonition title form: {bad}"


def test_parsers_page_registry_counts_match_the_code():
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS, SUMMARY_SECTION_PARSERS

    body = flat("parsers")
    assert f"All {len(ENDPOINT_PARSERS)} entries" in body
    assert f">>> len(ENDPOINT_PARSERS) {len(ENDPOINT_PARSERS)} " in body
    assert f"{len(SUMMARY_SECTION_PARSERS)}-section" in body


def test_espn_page_per_league_counts_match_the_code():
    """The per-league table is a snapshot, but a wrong snapshot is what this program is fixing."""
    from tools.codegen import generate, spec

    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    body = flat("espn")
    assert f"126 short names across {len(cfg.leagues)} leagues" in body
    universal = sum(
        1 for a in apis if a.api != "espn_cdn" for e in a.endpoints if e.scope == "universal" and not e.include_prefixes
    )
    assert f"**{universal}** wrappers per league" in body, f"the universal total should read {universal}"
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
