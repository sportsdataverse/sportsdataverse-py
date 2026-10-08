"""The 11 aliases marked removed_in="0.1.0" are gone in 0.1.5, with no caller left behind."""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PKG = ROOT / "sportsdataverse"

REMOVED = [
    "load_nfl_ngs_passing",
    "load_nfl_ngs_rushing",
    "load_nfl_ngs_receiving",
    "load_nfl_pfr_pass",
    "load_nfl_pfr_weekly_pass",
    "load_nfl_pfr_rush",
    "load_nfl_pfr_weekly_rush",
    "load_nfl_pfr_rec",
    "load_nfl_pfr_weekly_rec",
    "load_nfl_pfr_def",
    "load_nfl_pfr_weekly_def",
]


@pytest.mark.parametrize("name", REMOVED)
def test_the_alias_is_gone_from_the_nfl_namespace(name):
    nfl = importlib.import_module("sportsdataverse.nfl")
    assert not hasattr(nfl, name), f"{name} is still exported"


@pytest.mark.parametrize("name", REMOVED)
def test_the_alias_is_gone_from_the_top_level_namespace(name):
    assert not hasattr(importlib.import_module("sportsdataverse"), name)


def test_no_internal_caller_references_a_removed_loader():
    """Nothing inside the package may still call one of the removed names."""
    hits = []
    for py in PKG.rglob("*.py"):
        tree = ast.parse(py.read_text(encoding="utf-8"), filename=str(py))
        for node in ast.walk(tree):
            ref = None
            if isinstance(node, ast.Name):
                ref = node.id
            elif isinstance(node, ast.Attribute):
                ref = node.attr
            elif isinstance(node, ast.ImportFrom):
                for a in node.names:
                    if a.name in REMOVED:
                        hits.append(f"{py.relative_to(ROOT)}:{node.lineno} imports {a.name}")
                continue
            if ref in REMOVED:
                hits.append(f"{py.relative_to(ROOT)}:{node.lineno} references {ref}")
    assert not hits, "internal callers of removed loaders:\n" + "\n".join(hits)


def test_the_unified_replacements_still_work_offline():
    from sportsdataverse.nfl import load_nfl_nextgen_stats, load_nfl_pfr_advstats

    assert "stat_type" in load_nfl_nextgen_stats.__wrapped__.__code__.co_varnames
    assert "summary_level" in load_nfl_pfr_advstats.__wrapped__.__code__.co_varnames


def test_the_changelog_names_every_removal():
    body = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    release = body[body.index("## 0.1.5 Release") :].split("\n## ", 1)[0]
    assert "\n### Breaking changes\n" in release
    breaking = release.split("\n### Breaking changes\n", 1)[1].split("\n### ", 1)[0]
    for name in REMOVED:
        assert name in breaking, f"0.1.5 Breaking changes does not name {name}"
