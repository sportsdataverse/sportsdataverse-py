"""Execute the example notebooks and render them to Docusaurus tutorial pages.

The example notebooks (``examples/notebooks/*.ipynb``) are committed with their
outputs cleared. This script EXECUTES each one against the live APIs and renders
the executed result (code + outputs) to a themed Docusaurus page under
``docs/docs/tutorials/<stem>.md`` -- so the docs site shows real DataFrames /
values, not just code.

Because execution hits live ESPN / MLB / nflverse / PWHL APIs, this is **not**
part of the offline ``generate.py`` docs build. It is meant to run in the weekly
``live-tests-cron`` workflow (execute -> render -> commit); the regular doc build
just consumes the committed ``.md``. Run locally with:

    python tools/codegen/render_notebooks.py            # execute + render
    python tools/codegen/render_notebooks.py --no-execute  # render as-is (no live calls)

Determinism / safety:

* ``JUPYTER_CONFIG_DIR`` is pointed at a throwaway dir so a polluted global
  jupyter/nbconvert config can't inject preprocessors.
* Pages are emitted as ``.md`` (CommonMark via Docusaurus ``format: detect``) so
  bare ``{`` / ``<`` in DataFrame reprs don't trip the MDX parser.
* The source ``.ipynb`` files are never modified -- execution happens on an
  in-memory copy.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

_LEAGUES = "cfb|nfl|nba|wnba|mbb|wbb|mlb|nhl|pwhl"

# Avoid loading a polluted global jupyter/nbconvert config (some machines register
# a missing ``jupyter_contrib_nbextensions`` preprocessor that breaks nbconvert).
os.environ["JUPYTER_CONFIG_DIR"] = tempfile.mkdtemp(prefix="sdv-nbrender-")

ROOT = Path(__file__).resolve().parents[2]
NB_DIR = ROOT / "examples" / "notebooks"
OUT_DIR = ROOT / "docs" / "docs" / "tutorials"

# (stem, sidebar label, sidebar_position). Order groups by sport family to match
# the docs sidebar (Basketball, Football, Baseball, Hockey); position is what the
# site uses, so the on-disk filename numbering is irrelevant to display order.
TUTORIALS: list[tuple[str, str, int]] = [
    ("01_quickstart", "Quickstart", 1),
    ("04_nba_intro", "NBA", 2),
    ("08_wnba_intro", "WNBA", 3),
    ("06_mbb_intro", "MBB", 4),
    ("05_wbb_intro", "WBB", 5),
    ("03_nfl_intro", "NFL", 6),
    ("02_cfb_intro", "CFB", 7),
    ("09_mlb_intro", "MLB", 8),
    ("07_nhl_intro", "NHL", 9),
    ("10_pwhl_intro", "PWHL", 10),
    ("11_junior_hockey_intro", "Junior & minor hockey", 11),
    ("12_odds_intro", "Betting odds", 12),
]


def _execute(nb):
    """Execute a notebook in-memory; raise on the first failing cell."""
    from nbclient import NotebookClient

    NotebookClient(nb, timeout=180, kernel_name="python3", allow_errors=False).execute()


def _to_markdown(nb, stem: str) -> str:
    """Render an (executed) notebook node to a markdown body string."""
    from nbconvert import MarkdownExporter
    from traitlets.config import Config

    cfg = Config()
    # Image outputs (if any) are extracted to <stem>_files/ alongside the page.
    cfg.MarkdownExporter.preprocessors = ["nbconvert.preprocessors.ExtractOutputPreprocessor"]
    cfg.ExtractOutputPreprocessor.output_filename_template = (
        f"{stem}_files/{{unique_key}}_{{cell_index}}_{{index}}{{extension}}"
    )
    exporter = MarkdownExporter(config=cfg)
    body, resources = exporter.from_notebook_node(nb, resources={"unique_key": stem})
    # Persist any extracted image outputs.
    for fname, data in (resources.get("outputs") or {}).items():
        dest = OUT_DIR / fname
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
    return body


def _clean_outputs(nb) -> None:
    """In-place: tidy executed cell outputs for clean, theme-safe rendering.

    * Drop ``stderr`` stream outputs (warning noise -- e.g. env-specific version
      warnings -- that isn't pedagogically useful in a rendered tutorial).
    * Prefer the plain-text repr over the styled HTML one for DataFrames: polars /
      pandas ``text/html`` carries a scoped ``<style>`` block that can clash with
      the Docusaurus theme, whereas the ``text/plain`` box-drawing table renders as
      a clean monospace code block. Image outputs (``image/*``) are kept.
    """
    for cell in nb.cells:
        if cell.get("cell_type") != "code":
            continue
        kept = []
        for o in cell.get("outputs", []):
            ot = o.get("output_type")
            if ot == "stream" and o.get("name") == "stderr":
                continue
            if ot in ("execute_result", "display_data"):
                data = o.get("data", {})
                if "text/html" in data and "text/plain" in data:
                    data.pop("text/html", None)
            kept.append(o)
        cell["outputs"] = kept


def _fix_links(body: str) -> str:
    """Rewrite notebook cross-reference links so they resolve from the rendered page.

    The source notebooks live in ``examples/notebooks/`` and link siblings as
    ``other_intro.ipynb`` and league pages as ``docs/docs/<lg>/index.md`` -- both
    correct from the notebook's location but broken once rendered under
    ``docs/docs/tutorials/``. Rewrite:

    * ``](<anything>/NN_<name>.ipynb)`` -> ``](NN_<name>.md)`` (sibling tutorial page)
    * ``](<anything><lg>/index.md)``     -> ``](../<lg>/index.md)`` (league index)
    """
    body = re.sub(r"\]\([^)]*?(\d\d_[a-z0-9_]+)\.ipynb\)", r"](\1.md)", body)
    body = re.sub(rf"\]\([^)]*?({_LEAGUES})/index\.md\)", r"](../\1/index.md)", body)
    return body


def _normalize_md(text: str) -> str:
    """Match the repo's whitespace hooks (trailing-whitespace + end-of-file-fixer).

    nbconvert emits trailing spaces / no final newline; those hooks are NOT excluded
    for ``docs/docs/``. Normalizing here keeps the render output byte-identical to
    what a committed-then-hooked file would be, so the weekly cron only opens a PR
    when the *data* changed -- not because of cosmetic whitespace churn."""
    return "\n".join(ln.rstrip() for ln in text.splitlines()).rstrip() + "\n"


def _frontmatter(label: str, position: int) -> str:
    return f"---\ntitle: {label} tutorial\nsidebar_label: {label}\nsidebar_position: {position}\n---\n\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-execute", action="store_true", help="render notebooks as-is (no live API calls)")
    ap.add_argument(
        "--only", action="append", default=[], help="only render this stem (repeatable); for retries/debugging"
    )
    args = ap.parse_args()

    import nbformat

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    tutorials = [t for t in TUTORIALS if not args.only or t[0] in args.only]
    failures = []
    for stem, label, position in tutorials:
        src = NB_DIR / f"{stem}.ipynb"
        if not src.exists():
            print(f"  WARNING: missing {src}", file=sys.stderr)
            failures.append(stem)
            continue
        print(f"Rendering {stem} ...", flush=True)
        nb = nbformat.read(src, as_version=4)
        if not args.no_execute:
            try:
                _execute(nb)
            except Exception as e:  # noqa: BLE001 -- surface which notebook broke
                print(f"  EXECUTION FAILED for {stem}: {type(e).__name__}: {str(e)[:160]}", file=sys.stderr)
                failures.append(stem)
                continue
        _clean_outputs(nb)
        body = _fix_links(_to_markdown(nb, stem))
        (OUT_DIR / f"{stem}.md").write_text(_normalize_md(_frontmatter(label, position) + body), encoding="utf-8")
        print(f"  wrote {OUT_DIR / f'{stem}.md'}")

    if failures:
        print(f"\nFAILED ({len(failures)}): {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\nrendered {len(TUTORIALS)} tutorial pages -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
