"""The CHANGELOG hook writes the docs changelog pages: releases from CHANGELOG.md, unreleased changes from changelog.d/."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
_spec = importlib.util.spec_from_file_location(
    "sync_docs_changelog", ROOT / "tools" / "hooks" / "sync_docs_changelog.py"
)
sync = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync)

SAMPLE = (
    "<!-- START doctoc generated TOC please keep comment here to allow auto update -->\n- [0.2.0](#020)\n"
    "<!-- END doctoc generated TOC please keep comment here to allow auto update -->\n\n"
    "## 0.2.0 Release: October 1, 2026\n\n### Fixed\n\n```python\n## not a heading\n```\n\n" + "x" * 15_000 + "\n\n"
    "## 0.1.9 Release: September 1, 2026\n\n" + "y" * 9_000 + "\n\n"
    "## 0.1.8 Release: August 1, 2026\n\nz\n"
)


def _heads(sections: list[str]) -> list[str]:
    return [s.split("\n", 1)[0] for s in sections]


def _fragments(tmp_path: Path, files: dict[str, str]) -> Path:
    for name, body in files.items():
        (tmp_path / name).write_text(body, encoding="utf-8")
    return tmp_path


def test_split_keeps_every_section_once_and_drops_the_toc():
    recent, older = sync.split(SAMPLE)
    assert _heads(recent) == ["## 0.2.0 Release: October 1, 2026"]
    assert "## not a heading" in recent[0]
    assert _heads(older) == ["## 0.1.9 Release: September 1, 2026", "## 0.1.8 Release: August 1, 2026"]
    assert "doctoc" not in "".join(recent + older)


def test_one_release_over_the_budget_still_heads_the_page():
    recent, older = sync.split("## 0.3.0 Release: x\n\n" + "a" * 50_000 + "\n\n## 0.2.0 Release: y\n\nb\n")
    assert _heads(recent) == ["## 0.3.0 Release: x"]
    assert _heads(older) == ["## 0.2.0 Release: y"]


def test_render_writes_only_the_tracked_pages():
    """The unreleased page is built from changelog.d/ at docs-build time, so it is not one of the committed pages."""
    pages = sync.render("## 0.3.0 Release: x\n\nnotes\n")
    assert set(pages) == {"CHANGELOG.md", "changelog-archive.md"}
    assert pages["CHANGELOG.md"].startswith("---\ntitle: Changelog\n---\n\n# Changelog\n")


def test_an_unreleased_section_is_an_error_pointing_to_changelog_d():
    with pytest.raises(ValueError, match=r"line 1: .*changelog\.d/"):
        sync.split("## Unreleased\n\n### Fixed\n\n- x\n\n## 0.1.0 Release: x\n\ny\n")


def test_the_real_changelog_round_trips():
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    recent, older = sync.split(text)
    assert "".join([*recent, *older]).split() == sync._TOC.sub("", text).split()
    assert len(recent) == 1 or sum(len(s.encode()) for s in recent) <= sync.RECENT_BYTES


def test_committed_pages_match_the_changelog():
    """Catches a GitHub-UI edit, a --no-verify commit or a changed RECENT_BYTES: the hook alone cannot."""
    pages = sync.render((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"))
    for name, content in pages.items():
        assert (sync.PAGES / name).read_text(encoding="utf-8") == content, (
            f"{name} is stale: run `uv run python tools/codegen/generate.py` (or `python tools/hooks/sync_docs_changelog.py`)"
        )


def test_unrecognised_heading_is_an_error_naming_its_line():
    with pytest.raises(ValueError, match=r"line 3: unrecognised heading '## \[Unreleased\]'"):
        sync.split("# Changelog\n\n## [Unreleased]\n\nx\n\n## 0.1.0 Release: x\n\ny\n")


def test_text_before_the_first_heading_is_an_error():
    with pytest.raises(ValueError, match="line 1: text before the first release heading"):
        sync.split("stray\n\n## 0.1.0 Release: x\n")


def test_fragments_read_one_entry_per_bullet_in_file_order(tmp_path):
    d = _fragments(
        tmp_path,
        {
            "nfl-thing.fixed.md": "- **NFL:** b. (#2)\n",
            "two.fixed.md": "- **CFB:** a. (#1)\n  more\n\n- **NBA:** d. (#4)\n",
            "new.added.md": "- **NHL:** c. (#3)\n",
            "README.md": "# How to write a fragment\n",
        },
    )
    assert sync.read_fragments(d) == {
        "added": ["- **NHL:** c. (#3)"],
        "fixed": ["- **NFL:** b. (#2)", "- **CFB:** a. (#1)\n  more", "- **NBA:** d. (#4)"],
    }


def test_a_fragment_saved_with_a_bom_reads_the_same(tmp_path):
    """Windows editors may save a UTF-8 byte-order mark; it is not part of the bullet."""
    d = _fragments(tmp_path, {"x.fixed.md": "﻿- **NFL:** b. (#2)\n"})
    assert sync.read_fragments(d) == {"fixed": ["- **NFL:** b. (#2)"]}


def test_no_fragments_reads_as_empty(tmp_path):
    assert sync.read_fragments(tmp_path) == {}
    assert sync.read_fragments(tmp_path / "absent") == {}


@pytest.mark.parametrize("name", ["thing.fix.md", "thing.md", "Thing.fixed.md", "thing.fixed.txt"])
def test_a_misnamed_fragment_is_an_error_naming_it(tmp_path, name):
    d = _fragments(tmp_path, {name: "- **NFL:** b. (#2)\n"})
    with pytest.raises(ValueError, match=r"changelog\.d/" + re.escape(name)):
        sync.read_fragments(d)


@pytest.mark.parametrize(
    ("body", "why"),
    [
        ("", "is empty"),
        ("### Fixed\n\n- **NFL:** b.\n", "line 1"),
        ("- **NFL:** b.\nnot indented\n", "line 2"),
        ("text first\n- **NFL:** b.\n", "line 1"),
        ("- **NFL:** b.\n- \n", "line 2: an empty bullet"),
    ],
)
def test_a_fragment_that_is_not_bullets_is_an_error(tmp_path, body, why):
    d = _fragments(tmp_path, {"x.fixed.md": body})
    with pytest.raises(ValueError, match=rf"changelog\.d/x\.fixed\.md.*{why}"):
        sync.read_fragments(d)


def test_assemble_orders_groups_and_sorts_bullets():
    fragments = {
        "fixed": ["- **NFL:** b. (#2)", "- **CFB:** a. (#1)\n  more"],
        "added": ["- **NBA:** c. (#3)"],
    }
    assert sync.assemble(fragments, "Unreleased") == (
        "## Unreleased\n\n"
        "### Added\n\n- **NBA:** c. (#3)\n\n"
        "### Fixed\n\n- **CFB:** a. (#1)\n  more\n- **NFL:** b. (#2)\n"
    )


def test_the_unreleased_page_lists_the_fragments():
    page = sync.render_unreleased("## 0.3.0 Release: x\n\nnotes\n", {"fixed": ["- **NFL:** b. (#2)"]})
    assert page.startswith("---\ntitle: Unreleased changes\n---\n\n# Unreleased changes\n")
    assert "since 0.3.0" in page
    assert "## Unreleased\n\n### Fixed\n\n- **NFL:** b. (#2)\n" in page


def test_the_unreleased_page_says_when_nothing_is_merged():
    page = sync.render_unreleased("## 0.3.0 Release: x\n\nnotes\n", {})
    assert "Nothing has been merged since the last release." in page


def test_the_real_fragments_are_valid_and_render():
    """Every committed changelog.d/ fragment parses, so the docs-deploy render cannot fail after a merge."""
    fragments = sync.read_fragments(sync.FRAGMENTS)
    page = sync.render_unreleased((ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), fragments)
    assert all(bullet in page for bullets in fragments.values() for bullet in bullets)
