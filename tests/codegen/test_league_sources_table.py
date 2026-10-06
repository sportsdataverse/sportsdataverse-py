"""The league index opens with a Data sources table and one section per provider."""

from __future__ import annotations

import re

import pytest

from tools.codegen import generate


# One name that resolves to a PROVIDER (a release loader) and one that resolves to a
# CATEGORY (the NFL cache layer), so the rendered page exercises both blocks.
_NFL_AUTODOC = ["load_nfl_pbp", "clear_cache"]


@pytest.fixture(scope="module")
def nfl_index() -> str:
    return generate.render_league_index(
        "nfl",
        has_additional=True,
        additional_count=5,
        source_rows=generate._league_source_rows("nfl", autodoc_names=_NFL_AUTODOC),
        category_rows=generate._league_category_rows("nfl", _NFL_AUTODOC),
    )


def generate_sources():
    from tools.codegen import sources

    return sources


def test_sources_table_header_and_registry_order(nfl_index):
    assert "## Data sources" in nfl_index
    assert "| Source | APIs / hosts | Functions | Auth |" in nfl_index
    rows = re.findall(r"^\| \[([^\]]+)\]\(#", nfl_index, re.M)
    order = [e.label for e in generate_sources().providers()]
    assert rows == [lbl for lbl in order if lbl in rows], "sources table must be in registry order"


def test_every_table_row_links_a_section_that_exists(nfl_index):
    for anchor in re.findall(r"^\| \[[^\]]+\]\(#([a-z0-9-]+)\)", nfl_index, re.M):
        assert re.search(rf"^## .+ \{{#{re.escape(anchor)}\}}$", nfl_index, re.M), f"no section for #{anchor}"


def test_nfl_lists_espn_nfl_com_and_nflverse(nfl_index):
    for label in ("ESPN", "NFL.com Shield API", "nflverse data releases"):
        assert label in nfl_index


def test_loaders_appear_under_their_release_base_provider_not_a_separate_row(nfl_index):
    assert "| [Dataset loaders]" not in nfl_index
    nflverse = nfl_index.split("## nflverse data releases", 1)[1]
    assert "reference/loaders" in nflverse.split("\n## ", 1)[0]


def test_tools_and_helpers_lists_each_category_present(nfl_index):
    assert "## Tools and helpers" in nfl_index
    assert "### Cache and configuration" in nfl_index


def test_league_with_no_providers_emits_no_sources_table(monkeypatch):
    monkeypatch.setattr(generate, "_apis_for", lambda prefix: [])
    monkeypatch.setattr(generate, "_loader_doc_views", lambda prefix: [])
    out = generate.render_league_index("odds", source_rows=[], category_rows=[])
    assert "## Data sources" not in out
    assert "| Source |" not in out
    assert "## Tools and helpers" not in out


def test_provider_absent_from_league_emits_no_section():
    rows = generate._league_source_rows("mlb", autodoc_names=[])
    assert "KenPom" not in [r["label"] for r in rows]
    out = generate.render_league_index("mlb", source_rows=rows, category_rows=[])
    assert "KenPom" not in out
    assert "## \n" not in out


def test_every_row_reports_a_nonzero_function_count():
    for prefix in ("nfl", "nba", "mbb", "pwhl", "soccer", "fox"):
        for row in generate._league_source_rows(prefix, autodoc_names=[]):
            assert row["count"] > 0, f"{prefix}: {row['label']} has a zero-count row"
            assert row["entries"], f"{prefix}: {row['label']} has no API/host entries"
