"""Every fitting script cited from package source resolves to a tracked file or a documented gap (task 3.10).

supp-005 / C-L31: constants cited ``dev/**`` fitters that were gitignored (or never existed), so a clone could
not re-derive them. The register of the ones still unresolved is the gap table of
``docs/docs/architecture/model-lineage.md``; this test keeps code, git and that page in agreement (C-L26: the
page also carries one row per bundled booster).
"""

from __future__ import annotations

import functools
import re
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "docs" / "architecture" / "model-lineage.md"
# An sdv-py-relative script path: `dev/...py` or `tools/...py` not preceded by another path segment, so a
# citation of another repo's script (`game-on-paper-app/python/tools/fit_paper_index.py`,
# `sdv-internal-refs/<x>/tools/capture.py`) is not read as one of ours.
CITATION = re.compile(r"(?<![\w./-])((?:dev|tools)/[\w./-]+?\.py)\b")


@functools.cache
def _tracked() -> frozenset[str]:
    try:
        out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    return frozenset(out.splitlines())


@functools.cache
def _citations() -> dict[str, list[str]]:
    cited: dict[str, list[str]] = {}
    pkg = ROOT / "sportsdataverse"
    for path in sorted([*pkg.rglob("*.py"), *pkg.glob("*/models/*.json")]):
        for m in CITATION.finditer(path.read_text(encoding="utf-8")):
            cited.setdefault(m.group(1), []).append(path.relative_to(ROOT).as_posix())
    return cited


def _documented_gaps() -> set[str]:
    text = DOC.read_text(encoding="utf-8")
    block = text.split("<!-- lineage-gaps:start -->", 1)[1].split("<!-- lineage-gaps:end -->", 1)[0]
    return set(re.findall(r"^\| `([^`]+\.py)` \|", block, flags=re.M))


def test_every_cited_script_is_tracked_or_a_documented_gap():
    tracked = _tracked()
    gaps = _documented_gaps()
    unresolved = {p: srcs for p, srcs in _citations().items() if p not in tracked and p not in gaps}
    assert not unresolved, f"cited fitting scripts neither tracked nor in the model-lineage gap table: {unresolved}"


def test_documented_gaps_are_still_gaps():
    """A gap that got committed, or is no longer cited, must leave the table (the register only shrinks)."""
    tracked = _tracked()
    cited = _citations()
    stale = sorted(p for p in _documented_gaps() if p in tracked or p not in cited)
    assert not stale, f"model-lineage gap rows that are now tracked or no longer cited: {stale}"


def test_every_bundled_booster_has_a_registry_row():
    tracked = _tracked()
    boosters = sorted(p for p in tracked if re.fullmatch(r"sportsdataverse/\w+/models/[\w.]+\.ubj", p))
    assert len(boosters) >= 26
    rows = set(re.findall(r"^\| `(sportsdataverse/\w+/models/[\w.]+\.ubj)` \|", DOC.read_text(encoding="utf-8"), re.M))
    assert set(boosters) == rows, {"no row": sorted(set(boosters) - rows), "no artifact": sorted(rows - set(boosters))}


def test_sdv_py_scripts_the_page_names_are_tracked():
    """The page's own provenance claims: every sdv-py `tools/` / `dev/` script it names outside the gap table."""
    text = DOC.read_text(encoding="utf-8")
    before, rest = text.split("<!-- lineage-gaps:start -->", 1)
    after = rest.split("<!-- lineage-gaps:end -->", 1)[1]
    named = {m.group(1) for m in re.finditer(r"sdv-py `((?:dev|tools)/[\w./-]+\.py)`", before + after)}
    named |= set(re.findall(r"`(tools/fit_\w+\.py)`", before))
    assert named, "the page names no sdv-py script"
    missing = sorted(named - _tracked())
    assert not missing, f"model-lineage cites sdv-py scripts that are not tracked: {missing}"
