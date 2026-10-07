"""intro.md's leagues x sources table is generated between markers and drift-gated."""

from __future__ import annotations

import pytest

from tools.codegen import generate

BEGIN, END = generate._INTRO_MARKERS


def test_intro_has_both_markers():
    text = (generate.DOCS / "intro.md").read_text(encoding="utf-8")
    assert text.count(BEGIN) == 1 and text.count(END) == 1
    assert text.index(BEGIN) < text.index(END)


def test_patch_between_markers_replaces_only_the_span():
    out = generate._patch_between_markers(f"a\n{BEGIN}\nold\n{END}\nb\n", BEGIN, END, "new")
    assert out == f"a\n{BEGIN}\nnew\n{END}\nb\n"


def test_patch_between_markers_names_the_missing_marker():
    with pytest.raises(SystemExit) as e:
        generate._patch_between_markers("no markers here\n", BEGIN, END, "new")
    assert BEGIN in str(e.value)


def test_table_lists_every_documented_league_with_its_providers():
    body = generate.render_intro_sources_table({p: [] for p in generate._doc_leagues()})
    assert body.startswith("| League | Module | Data sources |")
    for prefix in ("nfl", "nba", "pwhl", "soccer", "fox", "cbs", "yahoo"):
        assert f"`sportsdataverse.{prefix}`" in body or f"sportsdataverse.hockey.{prefix}" in body
    assert "ESPN" in body and "nflverse data releases" in body


@pytest.mark.xdist_group("codegen_render")
def test_render_docs_all_owns_intro_md(first_render):
    """The generator must keep EMITTING intro.md. If it stopped, the committed table would
    simply never be refreshed again, and neither --check nor the idempotence test below
    would notice: intro.md sits outside the generated roots' orphan check."""
    rendered = first_render(generate._render_docs_all)
    assert "intro.md" in rendered
    assert BEGIN in rendered["intro.md"] and END in rendered["intro.md"]


def test_the_committed_intro_table_is_current():
    """The generator owns the marker span, so a stale table must fail -- this is the drift gate's
    own comparison, done without re-rendering all 800 docs pages."""
    text = (generate.DOCS / "intro.md").read_text(encoding="utf-8")
    body = generate.render_intro_sources_table(generate._autodoc_names_by_scope())
    assert generate._patch_between_markers(text, BEGIN, END, body) == text
