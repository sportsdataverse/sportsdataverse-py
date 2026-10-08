"""The version, lock entry, installed distribution, changelog heading and install wording agree.

The version is read from pyproject.toml, so these hold for every release, not just the one that added them.
"""

from __future__ import annotations

import datetime as dt
import importlib.metadata
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(encoding="utf-8"), re.M).group(1)
CHANGELOG = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")


def test_installed_distribution_version_matches():
    """Fails in a venv not re-synced after the version bump: `uv sync` fixes it."""
    assert importlib.metadata.version("sportsdataverse") == VERSION


def test_lockfile_self_entry_matches():
    assert f'name = "sportsdataverse"\nversion = "{VERSION}"' in (ROOT / "uv.lock").read_text(encoding="utf-8")


def test_the_newest_release_section_is_this_version_and_dated():
    """This changelog titles a release `## <version> Release: <Month D, YYYY>`; an `## Unreleased` may sit above it."""
    first = re.search(r"^## (\S+ Release: .+)$", CHANGELOG, re.M).group(1)
    m = re.fullmatch(rf"{re.escape(VERSION)} Release: (\w+ \d{{1,2}}, \d{{4}})", first)
    assert m, f"newest release section is {first!r}, pyproject says {VERSION}"
    dt.datetime.strptime(m.group(1), "%B %d, %Y")


def test_install_wording_names_the_mcp_extra():
    for name in ("README.md", "CONTRIBUTING.md"):
        body = (ROOT / name).read_text(encoding="utf-8")
        assert "sportsdataverse[mcp]" in body and "sdv-docs" in body, name


def test_the_docs_changelog_pages_render_the_release():
    from tools.hooks import sync_docs_changelog

    pages = sync_docs_changelog.render(CHANGELOG)
    assert any(f"{VERSION} Release" in v for v in pages.values())
