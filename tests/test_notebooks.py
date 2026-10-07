"""Notebook hygiene: the error-class-preserving safe(), no stale version text, real coverage."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "examples" / "notebooks"
NOTEBOOKS = sorted(NB_DIR.glob("*.ipynb"))
CRON = ROOT / ".github" / "workflows" / "live-tests-cron.yml"
DOCS = ROOT / "docs" / "docs"

# Families that had no notebook cell at all (exploration notes, "Missing coverage").
_REQUIRED_CELLS = {
    "01_quickstart": ["rolling_windows", "metric_curves", "sportsdataverse.registry"],
    "03_nfl_intro": ["nflpro", "pff_api", "shield"],
    "04_nba_intro": ["nba_officiating"],
    "05_wbb_intro": ["bart_wbb"],
    "06_mbb_intro": ["torvik", "kenpom"],
    "08_wnba_intro": ["wnba_game_officials"],
    "11_junior_hockey_intro": ["echl", "sphl", "ushl", "bchl", "mjhl"],
}


def _src(path: Path) -> str:
    doc = json.loads(path.read_text(encoding="utf-8"))
    return "\n".join("".join(c["source"]) for c in doc["cells"])


def test_there_are_fifteen_notebooks():
    assert len(NOTEBOOKS) == 15


def _safe_cell(path: Path) -> str | None:
    """The source of the cell defining the notebook's ``safe()`` helper, if it has one."""
    doc = json.loads(path.read_text(encoding="utf-8"))
    for cell in doc["cells"]:
        src = "".join(cell["source"])
        if "def safe" in src:
            return src
    return None


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.stem)
def test_safe_names_the_error_classes(nb):
    """`safe()` must distinguish "nothing there" from "the fetch failed", not hide both.

    Scoped to the cell that defines `safe()`: a narrow `except Exception` elsewhere (say,
    around an `ast.literal_eval` of a string column) is unrelated to fetch handling.
    """
    src = _safe_cell(nb)
    if src is None:
        pytest.skip(f"{nb.stem} defines no safe() helper")
    assert "except (NoDataError, AssetFetchError) as e:" in src, "safe() must not swallow every exception"
    assert "except Exception" not in src
    assert "type(e).__name__" in src


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.stem)
def test_no_stale_version_text(nb):
    assert "New in 0.0.72" not in _src(nb)


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.stem)
def test_no_redundant_return_parsed_true(nb):
    assert "return_parsed=True" not in _src(nb), "return_parsed=True is the default"


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.stem)
def test_no_committed_outputs(nb):
    doc = json.loads(nb.read_text(encoding="utf-8"))
    assert all(not c.get("outputs") for c in doc["cells"])


@pytest.mark.parametrize("stem,needles", sorted(_REQUIRED_CELLS.items()))
def test_notebook_covers_the_missing_families(stem, needles):
    src = _src(NB_DIR / f"{stem}.ipynb")
    missing = [n for n in needles if n not in src]
    assert not missing, f"{stem}.ipynb does not cover: {missing}"


def test_cron_says_fifteen_notebooks():
    body = CRON.read_text(encoding="utf-8")
    assert "15 live notebooks" in body
    assert "12 live notebooks" not in body


@pytest.mark.parametrize("nb", NOTEBOOKS, ids=lambda p: p.stem)
def test_every_reference_link_resolves(nb):
    """A notebook link must point at a page that exists.

    `render_notebooks.py` expands a `reference/<page>.md#fn` link to its family page via
    `docs/static/anchor-map.json`, so the page form is what a notebook should carry -- but
    the league prefix has to be real. 14 links in notebook 15 pointed at `football/`,
    `hockey/` and `baseball/` groupings the docs tree has never used.
    """
    broken = []
    for lg, page in re.findall(r"\]\(\.\./([a-z0-9_]+)/reference/([a-z0-9_-]+)\.md#", _src(nb)):
        if not (DOCS / lg / "reference" / f"{page}.md").exists():
            broken.append(f"../{lg}/reference/{page}.md")
    assert not broken, f"{nb.stem} links to pages that do not exist: {sorted(set(broken))}"


def test_rendered_tutorials_have_no_broken_family_links():
    """The committed tutorial pages must match the CURRENT family slugs.

    They are rendered by `render_notebooks.py`, not by `_render_docs_all`, so
    `generate.py --check` does not cover them: renaming an autodoc family silently
    strands every tutorial deep link until the notebooks are re-rendered.
    """
    from tools.codegen import generate

    broken = []
    for page in sorted((DOCS / "tutorials").glob("*.md")):
        body = page.read_text(encoding="utf-8")
        for lg, ref, slug, anchor in re.findall(
            r"\]\(\.\./([a-z0-9_]+)/reference/([a-z0-9_-]+)/([a-z0-9-]+)\.md#([A-Za-z0-9_-]+)\)", body
        ):
            target = DOCS / lg / "reference" / ref / f"{slug}.md"
            ids = (
                {i for _off, i in generate._heading_ids(target.read_text(encoding="utf-8"))} if target.exists() else ()
            )
            if anchor not in ids:
                broken.append(f"{page.name} -> {lg}/reference/{ref}/{slug}.md#{anchor}")
    assert not broken, "stale tutorial links (re-run render_notebooks.py):\n" + "\n".join(sorted(set(broken))[:20])


@pytest.mark.skipif(not os.environ.get("SDV_PY_LIVE_TESTS"), reason="notebook execution hits live APIs")
def test_notebooks_are_valid_json_and_declare_a_kernel():
    for nb in NOTEBOOKS:
        doc = json.loads(nb.read_text(encoding="utf-8"))
        assert doc["metadata"]["kernelspec"]["name"]
