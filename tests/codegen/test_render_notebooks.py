"""Tutorial pages: the head links the notebook, `--relink` rebuilds it without executing, and a link to a function
that moved to a family page goes to that page."""

from __future__ import annotations

from tools.codegen import render_notebooks as rn

BODY = "# 🏈 College football\n\nIntro text with [pbp](../cfb/reference/loaders.md#load_cfb_pbp).\n"
NB = "https://github.com/sportsdataverse/sportsdataverse-py/blob/main/examples/notebooks/02_cfb_intro.ipynb"
RAW = "https://raw.githubusercontent.com/sportsdataverse/sportsdataverse-py/main/examples/notebooks/02_cfb_intro.ipynb"


def test_page_head_links_the_notebook_under_the_title():
    page = rn._normalize_md(rn._frontmatter("02_cfb_intro", "CFB", 7) + rn._with_links("02_cfb_intro", BODY))
    assert (
        "custom_edit_url: https://github.com/sportsdataverse/sportsdataverse-py/edit/main/examples/notebooks/02_cfb_intro.ipynb\n"
        in page
    )
    body = page.split("---\n\n", 1)[1]
    assert body.startswith(
        f"# 🏈 College football\n\n> This page is the executed notebook [`02_cfb_intro.ipynb`]({NB}): "
        f"[download it]({RAW}) to run it yourself.\n\nIntro text"
    )


def test_relink_is_idempotent_and_adds_the_links_once():
    page = "---\ntitle: CFB tutorial\nsidebar_label: CFB\nsidebar_position: 7\n---\n\n" + BODY
    once = rn._relink(page, "02_cfb_intro", "CFB", 7)
    assert rn._relink(once, "02_cfb_intro", "CFB", 7) == once
    assert once.count("> This page is the executed notebook") == 1
    assert "Intro text with [pbp](" in once


def test_a_link_to_a_moved_anchor_goes_to_its_family_page(monkeypatch):
    monkeypatch.setattr(rn, "_anchor_map", lambda: {"/docs/cfb/reference/loaders": {"load_cfb_pbp": "pbp"}})
    text = "[a](../cfb/reference/loaders.md#load_cfb_pbp) [b](../cfb/reference/loaders.md#load_cfb_x)"
    assert (
        rn._fix_links(text)
        == "[a](../cfb/reference/loaders/pbp.md#load_cfb_pbp) [b](../cfb/reference/loaders.md#load_cfb_x)"
    )


def test_relinked_pages_follow_the_anchor_map_generate_passes_not_the_committed_file(monkeypatch):
    # generate.py hands over the anchor map of the pages it is writing: a function its splitter just moved
    # (mlb_awards: other.md -> other-2.md) must be followed even before docs/static/anchor-map.json is rewritten
    monkeypatch.setattr(rn, "_anchor_map", lambda: {})
    page = "---\ntitle: MLB tutorial\n---\n\n# MLB\n\n[a](../mlb/reference/mlb_api/other.md#mlb_awards)\n"
    out = rn._relink(page, "09_mlb_intro", "MLB", 9, {"/docs/mlb/reference/mlb_api": {"mlb_awards": "other-2"}})
    assert "](../mlb/reference/mlb_api/other-2.md#mlb_awards)" in out
    pages = rn.relinked_pages({})
    assert pages and all(k.startswith("tutorials/") and k.endswith(".md") for k in pages)


def test_a_cell_with_an_image_renders_once_per_output(tmp_path, monkeypatch):
    """MarkdownExporter enables ExtractOutputPreprocessor itself; listing it again ran it twice and
    the second pass raised on the first image (notebook 16's EuroLeague shot chart, 2026-10-26)."""
    import nbformat

    png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
    out = nbformat.v4.new_output("display_data", data={"image/png": png, "text/plain": "<Figure>"})
    nb = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("plt.show()", outputs=[out])])
    monkeypatch.setattr(rn, "OUT_DIR", tmp_path)
    body = rn._to_markdown(nb, "16_demo")
    assert "16_demo_files/16_demo_0_0.png" in body
    assert (tmp_path / "16_demo_files" / "16_demo_0_0.png").exists()
