"""The release commit's version, changelog heading and install wording agree."""

from __future__ import annotations

import datetime as dt
import importlib.metadata
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.1.5"
CHANGELOG = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")


def test_pyproject_version():
    m = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(encoding="utf-8"), re.M)
    assert m and m.group(1) == VERSION


def test_installed_distribution_version_matches():
    assert importlib.metadata.version("sportsdataverse") == VERSION


def test_lockfile_self_entry_matches():
    assert f'name = "sportsdataverse"\nversion = "{VERSION}"' in (ROOT / "uv.lock").read_text(encoding="utf-8")


def test_the_first_changelog_section_is_the_dated_release():
    """This changelog titles a release `## <version> Release: <Month D, YYYY>` (not Keep-a-Changelog)."""
    first = re.search(r"^## (.+)$", CHANGELOG, re.M).group(1)
    m = re.fullmatch(rf"{re.escape(VERSION)} Release: (\w+ \d{{1,2}}, \d{{4}})", first)
    assert m, f"first section is {first!r}"
    dt.datetime.strptime(m.group(1), "%B %d, %Y")
    assert not re.search(r"^## Unreleased\b", CHANGELOG, re.M)


def test_install_wording_names_the_mcp_extra():
    for name in ("README.md", "CONTRIBUTING.md"):
        body = (ROOT / name).read_text(encoding="utf-8")
        assert "sportsdataverse[mcp]" in body and "sdv-docs" in body, name


def test_the_docs_changelog_pages_render_the_release():
    from tools.hooks import sync_docs_changelog

    pages = sync_docs_changelog.render(CHANGELOG)
    assert any(f"{VERSION} Release" in v for v in pages.values())
