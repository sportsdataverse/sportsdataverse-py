"""Family pages: a reference page over the size budget becomes an overview at its old path plus one page per
function family in a folder beside it, and an anchor map sends an old ``#anchor`` to the page that holds it."""

from __future__ import annotations

import json
import re

import pytest

from tools.codegen import generate

_FUNCTION_HEADING = re.compile(r"(?m)^## [A-Za-z_]\w*$|^### ([A-Za-z_]\w*) \{#\1\}$")


def _endpoint_page(sizes: dict[str, int]) -> str:
    blocks = "".join(f"## {name}\n\n{name} does one thing.\n\n{'x' * size}\n\n" for name, size in sizes.items())
    return (
        "---\ntitle: X — Some API\nsidebar_label: Some API\nsidebar_position: 10\ntoc_max_heading_level: 2\n---\n"
        "# X — Some API\n\n`sportsdataverse.x` — 4 endpoints.\n\n" + blocks
    )


def test_small_families_join_other_and_big_ones_continue_on_a_second_page():
    pages = generate._pack_families(
        [
            ("a1", "season", 6_000),
            ("a2", "season", 6_000),
            ("b1", "event", 30_000),
            ("b2", "event", 30_000),
            ("b3", "event", 30_000),
            ("c1", "venue", 500),
            ("d1", "teams", 6_000),
            ("d2", "team", 6_000),
        ]
    )
    assert pages == [
        ("event", "Event", 1, ["b1", "b2"]),
        ("event-2", "Event", 2, ["b3"]),
        ("season", "Season", 1, ["a1", "a2"]),
        ("team", "Team", 1, ["d1", "d2"]),
        ("other", "Other", 1, ["c1"]),
    ]


def test_name_families_use_the_first_unshared_word_and_the_glued_prefixes():
    assert generate._name_families(["espn_cfb_season_types", "espn_cfb_event_plays", "espn_cfb_venues"]) == [
        "season",
        "event",
        "other",
    ]
    assert generate._name_families(
        ["nba_stats_boxscoreadvancedv3", "nba_stats_leaguedashplayerstats", "nba_stats_alltimeleadersgrids"]
    ) == ["boxscore", "leaguedash", "other"]


def test_loader_families_put_another_source_together():
    names = ["load_cfb_adv_passing", "load_cfb_pbp", "load_ncaa_mfb_pbp"]
    assert generate._loader_families(names, "cfb") == ["adv", "pbp", "ncaa"]


def test_family_pages_write_an_overview_that_links_every_function():
    page = _endpoint_page(
        {"x_api_season_a": 30_000, "x_api_season_b": 30_000, "x_api_event_c": 30_000, "x_api_event_d": 30_000}
    )
    overview, pages, anchors = generate._family_pages("x/reference/some_api.md", page, "x")
    assert sorted(pages) == ["event", "season"]
    assert anchors == {
        "x_api_season_a": "season",
        "x_api_season_b": "season",
        "x_api_event_c": "event",
        "x_api_event_d": "event",
    }
    assert overview.startswith("---\ntitle: X — Some API\n")
    assert "## Season\n\n| Function | Summary |" in overview
    assert "| [x_api_season_a](some_api/season.md#x_api_season_a) | x_api_season_a does one thing. |" in overview
    assert "x" * 1_000 not in overview
    assert pages["season"].startswith(
        '---\ntitle: "X — Some API — Season"\nsidebar_label: "Season"\nsidebar_position: 2\n'
    )
    assert "toc_max_heading_level: 2\n" in pages["season"]
    assert "## x_api_season_a\n" in pages["season"]
    assert "## x_api_event_c\n" not in pages["season"]


def test_a_page_with_one_function_is_not_split():
    assert generate._family_pages("x/reference/some_api.md", _endpoint_page({"x_api_season_a": 90_000}), "x") is None


def test_family_pages_keep_autodoc_anchor_case():
    body = (
        "## Highlights\n\n### CFBPlayProcess {#CFBPlayProcess}\n\n`CFBPlayProcess(game_id)`\n\n"
        "Runs the play-by-play.\n\n" + "y" * 40_000 + "\n\n"
        "### espn_cfb_pbp {#espn_cfb_pbp}\n\n`espn_cfb_pbp(game_id)`\n\nOne game.\n\n" + "z" * 40_000 + "\n\n"
    )
    page = (
        "---\ntitle: CFB — additional Python functions\nsidebar_label: Additional functions\nsidebar_position: 50\n"
        "---\n# CFB — additional Python functions\n\nHand-written.\n\n" + body
    )
    overview, pages, anchors = generate._family_pages("cfb/reference/additional.md", page, "cfb")
    assert anchors == {"CFBPlayProcess": "highlights", "espn_cfb_pbp": "highlights-2"}
    assert "## Highlights\n" in overview
    assert "| [CFBPlayProcess](additional/highlights.md#CFBPlayProcess) | Runs the play-by-play. |" in overview


def test_split_writes_a_category_that_opens_the_overview():
    out = {
        "x/reference/some_api.md": _endpoint_page({"x_api_season_a": 40_000, "x_api_event_b": 40_000}),
        "x/reference/small.md": _endpoint_page({"x_api_season_c": 100, "x_api_event_d": 100}),
    }
    anchor_map: dict = {}
    moved = generate._split_family_pages(out, "x", anchor_map)
    # two one-function families: with no bigger family to join, each keeps its own page
    assert moved == {"x_api_season_a": "some_api/season", "x_api_event_b": "some_api/event"}
    assert json.loads(out["x/reference/some_api/_category_.json"]) == {
        "label": "Some API",
        "position": 10,
        "collapsed": True,
        "link": {"type": "doc", "id": "x/reference/some_api"},
    }
    assert anchor_map == {"/docs/x/reference/some_api": {"x_api_season_a": "season", "x_api_event_b": "event"}}
    assert "x/reference/small/_category_.json" not in out


@pytest.mark.xdist_group("codegen_render")
def test_no_rendered_reference_page_with_two_functions_is_over_the_budget(first_render):
    rendered = first_render(generate._render_docs_all)
    # a family page adds its frontmatter and title (under 1 KB) to the blocks the packer measured
    over = [
        rel
        for rel, text in rendered.items()
        if "reference/" in rel
        and rel.endswith(".md")
        and len(text.encode()) > generate._PAGE_MD_BUDGET + 1_000
        and len(_FUNCTION_HEADING.findall(text)) > 1
    ]
    assert over == []


@pytest.mark.xdist_group("codegen_render")
def test_anchor_map_points_at_the_page_that_holds_each_anchor(first_render):
    rendered = first_render(generate._render_docs_all)
    anchor_map = json.loads(rendered[generate._ANCHOR_MAP_REL])
    assert "/docs/cfb/reference/loaders" in anchor_map
    for old, anchors in anchor_map.items():
        for anchor, slug in anchors.items():
            text = rendered[f"{old.removeprefix('/docs/')}/{slug}.md"]
            # a family section folded into "Other" (``#dataset-loaders``) forwards to that page, which has no such heading
            assert slug.startswith("other") or anchor in {i for _, i in generate._heading_ids(text)}, (
                old,
                anchor,
                slug,
            )


def test_every_old_heading_id_stays_on_the_overview_or_forwards():
    body = (
        "## Dataset loaders\n\n"
        "### load_a {#load_a}\n\nLoads a.\n\n" + "a" * 40_000 + "\n\n"
        "## Utilities & helpers\n\n"
        "### CFBPlayProcess {#CFBPlayProcess}\n\nRuns it.\n\n"
        "#### CFBPlayProcess.run_pipeline\n\nRuns the pipeline.\n\n```\n# not a heading\n```\n\n"
        + "b"
        * 40_000
        + "\n\n"
        "### helper_b {#helper_b}\n\nHelps.\n\n"
    )
    page = (
        "---\ntitle: X — additional Python functions\nsidebar_label: Additional functions\nsidebar_position: 50\n---\n"
        "# X — additional Python functions\n\nHand-written.\n\n" + body
    )
    old_ids = {i for _, i in generate._heading_ids(page)}
    assert {
        "load_a",
        "CFBPlayProcess",
        "cfbplayprocessrun_pipeline",
        "dataset-loaders",
        "utilities--helpers",
    } <= old_ids
    assert "not-a-heading" not in old_ids
    overview, pages, anchors = generate._family_pages("x/reference/additional.md", page, "x")
    on_overview = {i for _, i in generate._heading_ids(overview)}
    on_family = {slug: {i for _, i in generate._heading_ids(text)} for slug, text in pages.items()}
    for old_id in old_ids:
        if old_id in on_overview:
            continue
        assert old_id in anchors, old_id
        if old_id not in ("dataset-loaders", "utilities--helpers"):  # a folded family heading lands on its page
            assert old_id in on_family[anchors[old_id]], old_id


def test_overflow_pages_are_labelled_by_the_shared_name_token():
    labels = {
        "pff_api_facet_defense_a": 30_000,
        "pff_api_facet_defense_b": 30_000,
        "pff_api_facet_offense_c": 30_000,
        "pff_api_facet_passing_d": 30_000,
        "pff_api_facet_passing_e": 30_000,
        "pff_api_facet_rushing_f": 30_000,
        "pff_api_games_g": 30_000,
    }
    page = _endpoint_page(labels)
    _, pages, _ = generate._family_pages("x/reference/pff_api.md", page, "x")
    shown = {slug: re.search(r'sidebar_label: "(.+)"', text).group(1) for slug, text in pages.items()}
    assert shown == {
        "facet": "Facet: defense",
        "facet-2": "Facet: offense–passing",
        "facet-3": "Facet: passing–rushing",
        "other": "Other",
    }


def test_overflow_pages_with_the_same_token_are_labelled_by_name_tails():
    sizes = {
        "pff_api_facet_passing_concept_a": 30_000,
        "pff_api_facet_passing_depth_b": 30_000,
        "pff_api_facet_passing_pressure_c": 30_000,
        "pff_api_facet_passing_summary_d": 30_000,
        "pff_api_games_g": 30_000,
    }
    _, pages, _ = generate._family_pages("x/reference/pff_api.md", _endpoint_page(sizes), "x")
    shown = {slug: re.search(r'sidebar_label: "(.+)"', text).group(1) for slug, text in pages.items()}
    assert shown["facet"] == "Facet: passing"
    assert shown["facet-2"] == "Facet: passing_pressure–passing_summary"
    assert not [label for label in shown.values() if re.search(r"\(\d+\)", label)]


@pytest.mark.xdist_group("codegen_render")
def test_no_family_page_label_carries_a_page_number(first_render):
    rendered = first_render(generate._render_docs_all)
    numbered = [
        (rel, m.group(0))
        for rel, text in rendered.items()
        if "/reference/" in rel and rel.endswith((".md", ".json"))
        for m in re.finditer(r'(?m)^(?:sidebar_label|title): .*\(\d+\).*$|"label": ".*\(\d+\).*"', text)
    ]
    assert numbered == []
