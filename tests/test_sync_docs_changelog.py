"""The CHANGELOG hook writes three docs pages that together hold every section of CHANGELOG.md exactly once."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
_spec = importlib.util.spec_from_file_location(
    "sync_docs_changelog", ROOT / "tools" / "hooks" / "sync_docs_changelog.py"
)
sync = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync)

SAMPLE = (
    "<!-- START doctoc generated TOC please keep comment here to allow auto update -->\n- [Unreleased](#unreleased)\n"
    "<!-- END doctoc generated TOC please keep comment here to allow auto update -->\n\n"
    "## Unreleased\n\n### Fixed — a thing\n\n```python\n## not a heading\n```\n\n"
    "## 0.2.0 Release: October 1, 2026\n\n" + "x" * 15_000 + "\n\n"
    "## 0.1.9 Release: September 1, 2026\n\n" + "y" * 9_000 + "\n\n"
    "## 0.1.8 Release: August 1, 2026\n\nz\n"
)


def _heads(sections: list[str]) -> list[str]:
    return [s.split("\n", 1)[0] for s in sections]


def test_split_keeps_every_section_once_and_drops_the_toc():
    unreleased, recent, older = sync.split(SAMPLE)
    assert unreleased.startswith("## Unreleased\n")
    assert "## not a heading" in unreleased
    assert _heads(recent) == ["## 0.2.0 Release: October 1, 2026"]
    assert _heads(older) == ["## 0.1.9 Release: September 1, 2026", "## 0.1.8 Release: August 1, 2026"]
    assert "doctoc" not in unreleased + "".join(recent + older)


def test_one_release_over_the_budget_still_heads_the_page():
    _, recent, older = sync.split("## 0.3.0 Release: x\n\n" + "a" * 50_000 + "\n\n## 0.2.0 Release: y\n\nb\n")
    assert _heads(recent) == ["## 0.3.0 Release: x"]
    assert _heads(older) == ["## 0.2.0 Release: y"]


def test_empty_unreleased_still_writes_a_page():
    pages = sync.render("## 0.3.0 Release: x\n\nnotes\n")
    assert set(pages) == {"CHANGELOG.md", "changelog-unreleased.md", "changelog-archive.md"}
    assert "Nothing has been merged since the last release." in pages["changelog-unreleased.md"]
    assert pages["CHANGELOG.md"].startswith("---\ntitle: Changelog\n---\n\n# Changelog\n")


def test_the_real_changelog_round_trips():
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    unreleased, recent, older = sync.split(text)
    assert "".join([unreleased, *recent, *older]).split() == sync._TOC.sub("", text).split()
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
