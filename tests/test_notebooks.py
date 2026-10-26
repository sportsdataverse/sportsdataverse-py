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


def test_every_notebook_is_rendered():
    """Each notebook has a tutorial-page entry in render_notebooks.TUTORIALS, and each entry a notebook."""
    from tools.codegen.render_notebooks import TUTORIALS

    assert {p.stem for p in NOTEBOOKS} == {stem for stem, _label, _pos in TUTORIALS}


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


def test_cron_counts_the_notebooks():
    body = CRON.read_text(encoding="utf-8")
    assert f"{len(NOTEBOOKS)} live notebooks" in body


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


@pytest.mark.parametrize(
    "nb",
    NOTEBOOKS,
    ids=lambda p: p.stem,
)
def test_every_page_form_link_names_the_page_that_documents_it(nb):
    """A page that exists is not enough: the function must be documented on it.

    `reference/<page>.md#fn` is right when `fn` is in that page's family map (the relinker expands it)
    or is a heading on the page itself. `06_mbb_intro` sent `espn_mbb_conferences` and
    `espn_mbb_team_schedule` to `additional.md`; they live on `site.md`, and the relinker cannot heal that.
    """
    from tools.codegen import generate

    families = json.loads((ROOT / "docs" / "static" / "anchor-map.json").read_text(encoding="utf-8"))
    rendered = DOCS / "tutorials" / f"{nb.stem}.md"
    bodies = [_src(nb)] + ([rendered.read_text(encoding="utf-8")] if rendered.exists() else [])
    wrong = []
    for body in bodies:
        for lg, page, anchor in re.findall(
            r"\]\(\.\./([a-z0-9_]+)/reference/([a-z0-9_-]+)\.md#([A-Za-z0-9_-]+)\)", body
        ):
            target = DOCS / lg / "reference" / f"{page}.md"
            if anchor in families.get(f"/docs/{lg}/reference/{page}", {}) or not target.exists():
                continue  # a family link the relinker expands; a missing page is the test above's
            if anchor not in {i for _off, i in generate._heading_ids(target.read_text(encoding="utf-8"))}:
                wrong.append(f"../{lg}/reference/{page}.md#{anchor}")
    assert not wrong, f"{nb.stem} links to pages that do not document the function: {sorted(set(wrong))}"


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
