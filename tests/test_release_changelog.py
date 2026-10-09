"""`tools/release_changelog.py` folds changelog.d/ into CHANGELOG.md as a dated release section."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from tools import release_changelog

TOC = (
    "<!-- START doctoc generated TOC please keep comment here to allow auto update -->\n"
    "- [0.1.0 Release: September 1, 2026](#010-release-september-1-2026)\n"
    "<!-- END doctoc generated TOC please keep comment here to allow auto update -->\n"
)
OLD = "## 0.1.0 Release: September 1, 2026\n\nold\n"


def _tree(tmp_path: Path, fragments: dict[str, str]) -> tuple[Path, Path]:
    log = tmp_path / "CHANGELOG.md"
    log.write_text(TOC + "\n" + OLD, encoding="utf-8")
    d = tmp_path / "changelog.d"
    d.mkdir()
    (d / "README.md").write_text("How to write a fragment.\n", encoding="utf-8")
    for name, body in fragments.items():
        (d / name).write_text(body, encoding="utf-8")
    return log, d


def test_release_folds_the_fragments_in_above_the_newest_release(tmp_path):
    log, d = _tree(tmp_path, {"a.fixed.md": "- **NFL:** b. (#2)\n", "b.added.md": "- **NBA:** c. (#3)\n"})
    removed = release_changelog.release("0.1.1", dt.date(2026, 10, 8), log, d)
    assert log.read_text(encoding="utf-8") == (
        TOC + "\n## 0.1.1 Release: October 8, 2026\n\n"
        "### Added\n\n- **NBA:** c. (#3)\n\n"
        "### Fixed\n\n- **NFL:** b. (#2)\n\n" + OLD
    )
    assert sorted(p.name for p in removed) == ["a.fixed.md", "b.added.md"]
    assert [p.name for p in d.iterdir()] == ["README.md"]


def test_release_refuses_without_fragments(tmp_path):
    log, d = _tree(tmp_path, {})
    with pytest.raises(ValueError, match=r"no changelog\.d/ fragments"):
        release_changelog.release("0.1.1", dt.date(2026, 10, 8), log, d)
    assert log.read_text(encoding="utf-8") == TOC + "\n" + OLD


def test_release_refuses_a_version_that_is_already_released(tmp_path):
    log, d = _tree(tmp_path, {"a.fixed.md": "- **NFL:** b. (#2)\n"})
    with pytest.raises(ValueError, match="0.1.0 is already released"):
        release_changelog.release("0.1.0", dt.date(2026, 10, 8), log, d)
    assert (d / "a.fixed.md").exists()
    assert log.read_text(encoding="utf-8") == TOC + "\n" + OLD
