"""The released version has a frozen docs snapshot, and `current` is still the default version."""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
# The release PR snapshots the docs as the version it bumps to, so the two always agree.
VERSION = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(encoding="utf-8"), re.M).group(1)


def test_versions_json_lists_the_release_first():
    versions = json.loads((DOCS / "versions.json").read_text(encoding="utf-8"))
    assert versions[0] == VERSION


def test_the_snapshot_directory_exists_with_the_league_pages():
    snap = DOCS / "versioned_docs" / f"version-{VERSION}"
    assert snap.is_dir()
    assert (snap / "intro.md").exists()
    for prefix in ("nfl", "nba", "pwhl", "soccer"):
        assert (snap / prefix / "index.md").exists()


def test_the_snapshot_carries_the_sources_table():
    text = (DOCS / "versioned_docs" / f"version-{VERSION}" / "nfl" / "index.md").read_text(encoding="utf-8")
    assert "## Data sources" in text
    assert "| Source | APIs / hosts | Functions | Auth |" in text


def test_the_versioned_sidebar_exists():
    assert (DOCS / "versioned_sidebars" / f"version-{VERSION}-sidebars.json").exists()


def test_current_is_still_the_default_version():
    cfg = (DOCS / "docusaurus.config.ts").read_text(encoding="utf-8")
    assert "lastVersion: 'current'" in cfg
