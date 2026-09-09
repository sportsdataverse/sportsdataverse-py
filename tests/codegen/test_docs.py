"""Offline tests for the generated documentation layer (``generate.render_*``).

These cover the pure markdown renderers -- the 8-section reference block, the league
index, the loaders page, and the parameter reference -- without touching the network.
``generate.py --docs`` writes the per-league reference subtree into the live Docusaurus
tree (``docs/docs/{league}/``); a drift guard asserts that tree matches a fresh render.
"""

from __future__ import annotations

import pytest

from tools.codegen import generate


# ===========================================================================
# Reference block -- the 8-section per-function page (Task 1 contract)
# ===========================================================================


def test_reference_block_has_all_sections():
    md = generate.render_reference_page("nba", "espn_site_v2")
    assert "## `espn_nba_scoreboard`" in md
    assert "Endpoint URL" in md
    assert "Valid URL" in md
    # nba_api-style parameter table (6-column: added Description column in F2a)
    assert "| API Parameter | Python | Pattern | Required | Nullable | Description |" in md
    assert "### Returns" in md
    assert "### Example" in md
    assert "Last validated" in md
    assert "```python" in md  # runnable example fence


def test_reference_block_renders_frames_schema_as_multiple_tables():
    """The ESPN summary endpoint uses a ``kind: frames`` schema; each sub-frame
    should render as its own bolded ``@return`` table."""
    md = generate.render_reference_page("nba", "espn_site_v2")
    assert "## `espn_nba_summary`" in md
    assert "**boxscore_player**" in md  # a named sub-frame from summary.yaml


def test_reference_page_documents_resolved_wrapper_names():
    """Flat-API pages must show the SAME names the module codegen emits -- e.g. the
    api-web pbp wrapper is version-qualified to ``nhl_web_pbp`` (collision with the
    ``nhl_pbp`` composite), and a clean name like ``nhl_boxscore`` stays bare."""
    md = generate.render_reference_page("nhl", "nhl_api_web")
    assert "## `nhl_web_pbp`" in md
    assert "## `nhl_boxscore`" in md


# ===========================================================================
# Index / loaders / parameters pages (Task 2 contract)
# ===========================================================================


def test_league_index_lists_api_rows_and_loaders():
    md = generate.render_league_index("nba")
    assert "# NBA (`sportsdataverse.nba`)" in md
    assert "[ESPN site API (v2)](reference/site)" in md
    assert "[Dataset loaders](reference/loaders)" in md


def test_loaders_page_has_mermaid_and_per_loader_blocks():
    md = generate.render_loaders_page("nhl")
    assert "```mermaid" in md
    assert "## Automation status" in md
    assert "## `load_nhl_pbp`" in md
    # a 404-safe loader example call
    assert "load_nhl_pbp(seasons=" in md


def test_parameters_page_escapes_union_types():
    md = generate.render_parameters_page()
    assert "| Python | API | Type | Default | Pattern | Nullable |" in md
    # ``int|str`` must be pipe-escaped so it doesn't break the markdown table
    assert "int\\|str" in md
    assert "| int|str |" not in md


def test_category_json_is_valid_json():
    import json

    out = json.loads(generate.render_category("Reference", 1, True))
    assert out == {"label": "Reference", "position": 1, "collapsed": True}


# ===========================================================================
# Staging drift guard -- the committed tree must match a fresh render
# ===========================================================================


@pytest.mark.xdist_group("codegen_render")
def test_generated_docs_tree_is_current(first_render):
    # Compare against the session render rather than a fourth identical one.
    stale = generate._docs_stale(rendered=first_render(generate._render_docs_all))
    assert stale == [], f"stale generated docs (run `python tools/codegen/generate.py --docs`): {stale}"
