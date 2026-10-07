"""Additional-page families are registry labels in registry order, never name prefixes."""

from __future__ import annotations

from tools.codegen import generate, sources

# The headings the old name-prefix grouping produced: the first word of a function name,
# which told a reader nothing about where the data came from.
_NAME_PREFIX_HEADINGS = {"Build", "Calc", "Calculate", "Nfl", "Fox", "Get", "Load", "Most", "Year", "Nba", "Mlb"}


def test_family_is_the_registry_label():
    assert (
        generate._autodoc_family("load_nfl_pbp", module="sportsdataverse.nfl.nfl_loaders") == "nflverse data releases"
    )
    assert generate._autodoc_family("kenpom_ratings", module="sportsdataverse.mbb.kenpom") == "KenPom"
    assert generate._autodoc_family("rolling_mean", module="sportsdataverse.rolling_windows") == "Analytics"


def test_highlights_still_wins():
    fam = generate._autodoc_family(
        "load_nfl_pbp", frozenset({"load_nfl_pbp"}), module="sportsdataverse.nfl.nfl_loaders"
    )
    assert fam == "Highlights"


def test_family_rank_is_registry_order_with_highlights_first():
    assert generate._family_rank()["Highlights"] == -1
    labels = [e.label for e in sources.load()]
    ranks = [generate._family_rank()[lbl] for lbl in labels]
    assert ranks == sorted(ranks)


def test_pack_families_respects_the_rank_instead_of_sorting_alphabetically():
    items = [
        ("b_fn", "Analytics", 20_000),
        ("a_fn", "ESPN", 20_000),
        ("c_fn", "Analytics", 20_000),
        ("d_fn", "ESPN", 20_000),
    ]
    rank = {"ESPN": 0, "Analytics": 10}
    labels = [lbl for _slug, lbl, _part, _names in generate._pack_families(items, rank=rank)]
    assert labels == ["ESPN", "Analytics"]


def test_a_registry_label_keeps_its_own_capitalisation():
    """`nflverse` is a brand, not a sentence start -- the old fallback title-cased it to `Nflverse`."""
    items = [
        ("a_fn", "nflverse data releases", 20_000),
        ("b_fn", "nflverse data releases", 20_000),
        ("c_fn", "ESPN", 20_000),
        ("d_fn", "ESPN", 20_000),
    ]
    labels = [lbl for _slug, lbl, _part, _names in generate._pack_families(items, rank=generate._family_rank())]
    assert "nflverse data releases" in labels
    assert "Nflverse data releases" not in labels


def test_no_rendered_additional_page_has_a_name_prefix_heading():
    """Read the committed tree: `generate.py --check` already proves it matches a fresh render."""
    bad = []
    for page in sorted(generate.DOCS.rglob("*.md")):
        if page.name not in ("additional.md", "python-helpers.md") and page.parent.name not in (
            "additional",
            "python-helpers",
        ):
            continue
        for line in page.read_text(encoding="utf-8").splitlines():
            if line.startswith("## ") and line[3:].strip() in _NAME_PREFIX_HEADINGS:
                bad.append(f"{page.relative_to(generate.DOCS)}: {line}")
    assert not bad, "name-prefix family headings survive:\n" + "\n".join(bad)


def test_oversize_provider_section_still_splits_and_forwards():
    """A provider section over the markdown budget keeps splitting, and every moved anchor forwards."""
    page = "---\ntitle: NBA\nsidebar_position: 50\n---\n# NBA\n\n"
    body = []
    for i in range(6):
        body.append(f"## NBA Stats API\n\n### nba_stats_fn{i} {{#nba_stats_fn{i}}}\n\n" + "x" * 30_000 + "\n\n")
    content = page + "".join(body)
    split = generate._family_pages("nba/reference/additional.md", content, "nba")
    assert split is not None
    overview, family_pages, fn_slug = split
    assert len(family_pages) > 1, "an over-budget provider section must continue on -2"
    assert all(f"nba_stats_fn{i}" in fn_slug for i in range(6))
    for slug in fn_slug.values():
        assert slug in family_pages


def test_pack_families_never_folds_a_registry_family_into_other():
    """An autodoc family is a source or a helper category. Folding a small one into "Other" un-names
    it, so the page says "Other" where the league index says "Cache and configuration"."""
    items = [("a_fn", "Cache and configuration", 500), ("b_fn", "ESPN", 20_000), ("c_fn", "ESPN", 20_000)]
    labels = [lbl for _slug, lbl, _part, _names in generate._pack_families(items, rank=generate._family_rank())]
    assert labels == ["ESPN", "Cache and configuration"]


def test_no_committed_autodoc_page_has_an_other_family():
    """Read the committed tree: `generate.py --check` already proves it matches a fresh render."""
    autodoc = ("additional", "python-helpers")
    bad = [str(p.relative_to(generate.DOCS)) for p in generate.DOCS.rglob("other*.md") if p.parent.name in autodoc]
    for page in generate.DOCS.rglob("*.md"):
        if page.stem in autodoc and "## Other" in page.read_text(encoding="utf-8").splitlines():
            bad.append(f"{page.relative_to(generate.DOCS)}: ## Other")
    assert not sorted(bad)


def test_family_rank_is_never_frozen_at_import():
    """A module-level copy ignores `_family_rank.cache_clear()`, so a split rendered after the
    registry changed (a test patching it, a long-lived process) would still use the import-time order."""
    assert not hasattr(generate, "_FAMILY_RANK")
