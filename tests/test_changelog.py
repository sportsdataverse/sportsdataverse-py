"""The current release section names every PR that shipped in it.

Roughly 250 PRs merged between 0.1.4 and 0.1.5 and the section cited eleven of them, so
whole families (the football source adapters, NFL Pro, fox_api, the validation gate, the
docs-site rework) shipped with no changelog line at all. These are the ones a reader of
the release notes would be misled by their absence.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHANGELOG = ROOT / "CHANGELOG.md"

# A release is grouped Keep-a-Changelog style: these are its only `###` headings, in this order.
_GROUPS = ["Breaking changes", "Added", "Changed", "Deprecated", "Removed", "Fixed", "Security", "Data"]

_PRS = [
    540,
    541,
    542,
    543,
    544,
    545,
    546,
    547,
    548,  # football source adapters
    496,
    497,
    498,
    614,  # tendencies + usage box
    454,
    481,
    489,  # NFL Pro NGS
    680,  # fox_api
    452,  # CBS / Yahoo / Fox providers
    451,
    678,  # torvik + KenPom
    592,  # NBA officiating
    645,  # metric registry
    590,
    657,  # rolling windows
    652,  # metric curves
    659,  # defense vs position
    661,  # paper index
    553,
    554,
    555,
    556,
    557,
    558,  # validation gate
    503,
    533,  # processor bug sweeps (range ends)
    528,
    536,  # Shield pbp
    681,  # ESPN CDN
    684,  # nbagl
    563,  # OMP threads
    568,
    569,
    570,  # NCAA Terms gate
    664,
    671,
    672,
    673,
    674,
    676,  # docs-site rework
]


def _sections() -> dict[str, str]:
    """Every `## ` section (`Unreleased` and each release), keyed by its heading line."""
    parts = re.split(r"^(?=## )", CHANGELOG.read_text(encoding="utf-8"), flags=re.M)[1:]
    return {p.splitlines()[0]: p for p in parts}


def _release() -> str:
    """The 0.1.5 section, found by heading so an `## Unreleased` added above it changes nothing."""
    return next(s for h, s in _sections().items() if h.startswith("## 0.1.5 Release"))


@pytest.mark.parametrize("pr", _PRS)
def test_the_release_section_names_the_pr(pr):
    assert f"#{pr}" in _release(), f"the 0.1.5 section has no entry citing #{pr}"


@pytest.mark.parametrize("heading", list(_sections()))
def test_every_group_heading_is_known_and_in_order(heading):
    """`###` headings are the change groups only, in the fixed order; entries are bullets under them."""
    section = _sections()[heading]
    heads = re.findall(r"^### (.+?)\s*$", section, re.M)
    unknown = [h for h in heads if h not in _GROUPS]
    assert not unknown, f"unknown group headings: {unknown}"
    assert heads == sorted(heads, key=_GROUPS.index), f"groups out of order: {heads}"
    assert not re.search(r"^#### ", section, re.M), "entries are bullets, not #### headings"


def test_the_backfilled_entries_each_cite_a_pr():
    """The 0.1.5 backfill made every backfilled entry traceable: at least 25 bullets cite a PR."""
    bullets = re.findall(r"^- .*(?:\n  .*)*", _release(), re.M)
    assert sum(1 for b in bullets if re.search(r"#\d+", b)) >= 25, "the backfill should keep >= 25 PR-citing entries"


def test_the_docs_changelog_pages_still_render():
    from tools.hooks import sync_docs_changelog

    pages = sync_docs_changelog.render(CHANGELOG.read_text(encoding="utf-8"))
    assert pages and all(v.strip() for v in pages.values())
