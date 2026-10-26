"""A final release has a frozen docs snapshot, a pre-release has none, and `current` is still the default version."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
# The release commit snapshots the docs as the version it bumps to, so the two agree. A pre-release
# (0.2.0b1, 0.2.0rc1) is served from `current` (main) and is not frozen: the snapshot comes with the final.
VERSION = re.search(r'^version = "([^"]+)"$', (ROOT / "pyproject.toml").read_text(encoding="utf-8"), re.M).group(1)
PRERELEASE = re.search(r"(a|b|rc)\d+$", VERSION) is not None
SNAPSHOTS = json.loads((DOCS / "versions.json").read_text(encoding="utf-8"))
final_only = pytest.mark.skipif(PRERELEASE, reason=f"{VERSION} is a pre-release: no snapshot")


@pytest.mark.skipif(not PRERELEASE, reason=f"{VERSION} is a final release")
def test_a_prerelease_has_no_snapshot_and_follows_the_last_one():
    assert VERSION not in SNAPSHOTS
    assert not (DOCS / "versioned_docs" / f"version-{VERSION}").exists()
    base = tuple(int(x) for x in re.match(r"(\d+)\.(\d+)\.(\d+)", VERSION).groups())
    assert tuple(int(x) for x in SNAPSHOTS[0].split(".")) < base


@final_only
def test_versions_json_lists_the_release_first():
    assert SNAPSHOTS[0] == VERSION


@final_only
def test_the_snapshot_directory_exists_with_the_league_pages():
    snap = DOCS / "versioned_docs" / f"version-{VERSION}"
    assert snap.is_dir()
    assert (snap / "intro.md").exists()
    for prefix in ("nfl", "nba", "pwhl", "soccer"):
        assert (snap / prefix / "index.md").exists()


@final_only
def test_the_snapshot_carries_the_sources_table():
    text = (DOCS / "versioned_docs" / f"version-{VERSION}" / "nfl" / "index.md").read_text(encoding="utf-8")
    assert "## Data sources" in text
    assert "| Source | APIs / hosts | Functions | Auth |" in text


@final_only
def test_the_versioned_sidebar_exists():
    assert (DOCS / "versioned_sidebars" / f"version-{VERSION}-sidebars.json").exists()


def test_current_is_still_the_default_version():
    cfg = (DOCS / "docusaurus.config.ts").read_text(encoding="utf-8")
    assert "lastVersion: 'current'" in cfg
