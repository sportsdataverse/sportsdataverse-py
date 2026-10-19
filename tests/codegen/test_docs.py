"""Offline tests for the generated documentation layer (``generate.render_*``).

These cover the pure markdown renderers -- the 8-section reference block, the league
index, the loaders page, and the parameter reference -- without touching the network.
``generate.py --docs`` writes the per-league reference subtree into the live Docusaurus
tree (``docs/docs/{league}/``); a drift guard asserts that tree matches a fresh render.
"""

from __future__ import annotations

import re

import pytest

from tools.codegen import generate


# ===========================================================================
# Reference block -- the 8-section per-function page (Task 1 contract)
# ===========================================================================


def test_reference_block_has_all_sections():
    md = generate.render_reference_page("nba", "espn_site_v2")
    assert "## espn_nba_scoreboard\n" in md
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
    assert "## espn_nba_summary\n" in md
    assert "**boxscore_player**" in md  # a named sub-frame from summary.yaml


def test_reference_page_documents_resolved_wrapper_names():
    """Flat-API pages must show the SAME names the module codegen emits -- e.g. the
    api-web pbp wrapper is version-qualified to ``nhl_web_pbp`` (collision with the
    ``nhl_pbp`` composite), and a clean name like ``nhl_boxscore`` stays bare."""
    md = generate.render_reference_page("nhl", "nhl_api_web")
    assert "## nhl_web_pbp\n" in md
    assert "## nhl_boxscore\n" in md


# ===========================================================================
# Index / loaders / parameters pages (Task 2 contract)
# ===========================================================================


def test_league_index_lists_api_rows_and_loaders():
    """Each API and the loaders page are reached from their provider's own section (0.1.5 layout)."""
    md = generate.render_league_index(
        "nba",
        source_rows=generate._league_source_rows("nba", autodoc_names=[]),
        category_rows=[],
    )
    assert "# NBA (`sportsdataverse.nba`)" in md
    assert "## Data sources" in md
    assert "[ESPN site API (v2)](reference/site)" in md
    assert "[sportsdataverse-data releases](reference/loaders)" in md


# --- Highlights: curated functions pulled out of the Additional bucket ------


def test_highlighted_names_intersects_against_the_real_autodoc_set():
    # A curated name that doesn't actually exist in this call's autodoc set (typo,
    # or since moved to a generated page) is silently dropped, not surfaced.
    highlighted = generate._highlighted_names("mbb", ["espn_mbb_schedule", "not_a_real_function"])
    assert highlighted == {"espn_mbb_schedule"}


def test_highlighted_names_is_always_empty_for_the_global_scope():
    assert generate._highlighted_names(None, ["espn_mbb_schedule"]) == set()


def test_autodoc_family_checks_highlighted_before_the_registry():
    # espn_mbb_schedule resolves to the ESPN provider through sources.yaml (0.1.5 replaced the
    # name-token families); a curated "start here" pick is still pulled out into Highlights.
    mod = "sportsdataverse.mbb.mbb_schedule"
    assert generate._autodoc_family("espn_mbb_schedule", module=mod) == "ESPN"
    assert generate._autodoc_family("espn_mbb_schedule", frozenset({"espn_mbb_schedule"}), module=mod) == "Highlights"


def test_render_league_index_highlights_row_and_additional_count_do_not_overlap():
    md = generate.render_league_index(
        "mbb",
        has_additional=True,
        additional_count=317,
        has_highlights=True,
        highlights_count=9,
        source_rows=generate._league_source_rows("mbb", autodoc_names=[]),
        category_rows=[],
    )
    assert '| [Highlights](reference/additional#highlights) | curated "start here" functions | 9 |' in md
    assert "| [Additional functions](reference/additional) | hand-written wrappers & helpers | 317 |" in md


def test_render_league_index_omits_highlights_row_by_default():
    md = generate.render_league_index("nba")
    assert "Highlights" not in md


def test_mbb_additional_page_has_a_highlights_group_first():
    names = generate._autodoc_names("mbb", "")
    groups = generate._autodoc_groups("mbb", names)
    assert groups[0]["family"] == "Highlights"
    highlighted_fns = {fn["name"] for fn in groups[0]["functions"]}
    assert "espn_mbb_pbp" in highlighted_fns
    assert "espn_mbb_schedule" in highlighted_fns


def test_loaders_page_states_the_pipeline_and_has_per_loader_blocks():
    md = generate.render_loaders_page("nhl")
    # the docs site has no Mermaid theme, so a fence is published as raw `flowchart` source
    assert "```mermaid" not in md
    assert "scrape / raw → enrich → release asset → `load_*()`" in md
    assert "## Automation status" in md
    assert "## load_nhl_pbp\n" in md
    # a 404-safe loader example call
    assert "load_nhl_pbp(seasons=" in md


def test_reference_headings_are_plain_names_with_unique_sub_heading_ids():
    md = generate.render_reference_page("nba", "espn_site_v2")
    assert "## `" not in md
    assert "toc_max_heading_level: 2\n" in md
    assert "### Returns {#espn_nba_scoreboard-returns}" in md
    assert "### Example {#espn_nba_scoreboard-example}" in md
    ids = re.findall(r"\{#([\w-]+)\}", md)
    assert len(ids) == len(set(ids)) > 0


def test_loaders_page_uses_the_bracketed_admonition_title():
    md = generate.render_loaders_page("cfb")
    assert ":::caution[Coverage]" in md
    assert ":::caution Coverage" not in md
    assert "### Returns {#load_cfb_pbp-returns}" in md
    assert "toc_max_heading_level: 2\n" in md


def test_autodoc_heading_is_the_plain_name_and_the_signature_follows():
    md = generate.render_autodoc_page("mbb", "")
    assert "### `" not in md
    assert "### espn_mbb_pbp {#espn_mbb_pbp}\n\n`espn_mbb_pbp(" in md


def test_autodoc_page_names_a_nested_home_by_its_full_path(monkeypatch):
    # A home nested two levels down must name its real import path, not ``sportsdataverse.<prefix>``.
    monkeypatch.setattr(generate, "_home_module", lambda p: f"sportsdataverse.basketball.ncaa.{p}")
    md = generate.render_autodoc_page("mbb", "")
    assert "`sportsdataverse.basketball.ncaa.mbb`" in md


def test_container_groups_list_a_two_level_home_by_its_direct_child(monkeypatch):
    monkeypatch.setattr(generate, "_NESTED_HOMES", {"sihf": "hockey.sihf", "zzz": "hockey.swiss.zzz"})
    hockey = generate._container_groups({})["hockey"]
    assert "sihf" in hockey and "swiss" in hockey
    assert "zzz" not in hockey


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


def test_render_league_index_see_also_comes_from_companions_yaml():
    """soccer has companion entries; a league without any renders no ``## See also``."""
    soccer = generate.render_league_index("soccer")
    assert "## See also" in soccer
    assert "[kloppy](https://kloppy.pysport.org)" in soccer
    # The block sits between the reference table and the Examples section.
    assert soccer.index("## See also") < soccer.index("## Examples")
    assert "## See also" not in generate.render_league_index("nba")


def test_companions_yaml_entries_are_well_formed():
    for prefix, entries in generate._companions_map().items():
        assert entries, prefix
        for entry in entries:
            assert set(entry) == {"name", "url", "note"}, (prefix, entry)
            assert entry["url"].startswith("https://"), (prefix, entry["url"])


def test_companion_notes_do_not_shadow_autodoc_names():
    """A bare function name in a companion note reads as "already documented" to the
    autodoc corpus scan (``_is_documented``) and silently drops that function from its
    helpers page, so check every in-scope name against the notes with the generator's
    own predicate -- not against the autodoc survivors, which would no longer list it."""
    notes = " ".join(e["note"] for entries in generate._companions_map().values() for e in entries)
    per_league, global_names = generate._coverage_scope_names()
    in_scope = set(global_names).union(*per_league.values())
    shadowed = sorted(n for n in in_scope if generate._is_documented(n, notes))
    assert shadowed == [], shadowed


def test_flat_reference_page_heads_with_the_family_notes():
    # A family's docstring notes (licence, owner rulings, sanctions flags) must reach the docs
    # site: the per-function reference blocks have no Notes section of their own.
    from tools.codegen import generate as _gen

    page = _gen.render_reference_page("khl", "khl_rest")
    head = page.split("\n## ", 1)[0]
    assert "> **Note:**" in head
    assert "> **Note:**" not in _gen.render_reference_page("nhl", "nhl_api_web").split("\n## ", 1)[0]


def test_a_dict_of_frames_autodoc_function_renders_one_table_per_frame():
    """B-M32: the live boxscore returns six frames; its Returns section is six tables, not one flat
    table of ``game.game_id``-style names."""
    groups = generate._autodoc_groups("nba", ["nba_live_boxscore"])
    fn = groups[0]["functions"][0]
    assert [f["section"] for f in fn["return_frames"]] == [
        "game",
        "officials",
        "home_players",
        "away_players",
        "home_team",
        "away_team",
    ]
    assert fn["return_columns"] == [] and fn["return_unverified"] == ""
    assert not any("." in c["name"] for f in fn["return_frames"] for c in f["columns"])


def test_the_espn_reference_pages_document_only_the_wrappers_their_names_run():
    """B-M31: in seven leagues a hand-written ``espn_<lg>_calendar`` (and wbb/wnba standings, roster,
    draft) holds the package-level name, so the generated wrapper's block documented a signature and
    table that name never runs. The reference pages document a view only when the package name
    resolves to the generated ``*_espn_ext`` wrapper; the autodoc page takes the hand-written one."""
    import importlib

    cfg = generate.spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    params = generate.spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    apis = [generate.spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    wrong = []
    for lg in cfg.leagues:
        pkg = importlib.import_module(f"sportsdataverse.{generate._league_module(lg.prefix)}")
        for v in generate._documented_espn_views(lg, apis, cfg.hosts):
            if not getattr(getattr(pkg, v.fn_name, None), "__module__", "").endswith("_espn_ext"):
                wrong.append(v.fn_name)
    assert wrong == []
    cfb = next(lg for lg in cfg.leagues if lg.prefix == "cfb")
    assert "espn_cfb_calendar" in {v.fn_name for v in generate._espn_league_views(cfb, apis, cfg.hosts)}
    assert "## espn_cfb_calendar" not in generate.render_reference_page("cfb", "espn_site_v2").splitlines()
