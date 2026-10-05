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
    python tools/codegen/render_notebooks.py --relink      # re-apply the page head to the committed pages

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
import functools
import json
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
    ("13_soccer_intro", "Soccer", 12),
    ("14_cricket_intro", "Cricket", 13),
    ("15_other_espn_leagues_intro", "Other ESPN leagues", 14),
    ("12_odds_intro", "Betting odds", 15),
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
    return _REF_ANCHOR_LINK.sub(_to_family_page, body)


# A link to a function on a reference page that generate.py split into family pages
# (``../cfb/reference/loaders.md#load_cfb_pbp``) goes straight to the family page
# (``../cfb/reference/loaders/pbp.md#load_cfb_pbp``), from the anchor map the docs build writes.
_REF_ANCHOR_LINK = re.compile(r"\]\((\.\./([a-z0-9_]+)/reference/([a-z0-9_]+))\.md#([A-Za-z0-9_-]+)\)")


@functools.lru_cache(maxsize=1)
def _anchor_map() -> dict:
    path = ROOT / "docs" / "static" / "anchor-map.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _to_family_page(m: re.Match) -> str:
    slug = _anchor_map().get(f"/docs/{m.group(2)}/reference/{m.group(3)}", {}).get(m.group(4))
    return f"]({m.group(1)}/{slug}.md#{m.group(4)})" if slug else m.group(0)


def _normalize_md(text: str) -> str:
    """Match the repo's whitespace hooks (trailing-whitespace + end-of-file-fixer).

    nbconvert emits trailing spaces / no final newline; those hooks are NOT excluded
    for ``docs/docs/``. Normalizing here keeps the render output byte-identical to
    what a committed-then-hooked file would be, so the weekly cron only opens a PR
    when the *data* changed -- not because of cosmetic whitespace churn."""
    return "\n".join(ln.rstrip() for ln in text.splitlines()).rstrip() + "\n"


_REPO = "sportsdataverse/sportsdataverse-py"
_LINKS_PREFIX = "> This page is the executed notebook"


def _frontmatter(stem: str, label: str, position: int) -> str:
    # "Edit this page" opens the notebook, the source of this page, not the generated markdown.
    edit = f"https://github.com/{_REPO}/edit/main/examples/notebooks/{stem}.ipynb"
    return (
        f"---\ntitle: {label} tutorial\nsidebar_label: {label}\nsidebar_position: {position}\n"
        f"custom_edit_url: {edit}\n---\n\n"
    )


def _with_links(stem: str, body: str) -> str:
    """The body with a line linking its notebook (GitHub view and raw download) under the first heading.

    Under, not above: Docusaurus takes a leading ``#`` heading as the page title."""
    github = f"https://github.com/{_REPO}/blob/main/examples/notebooks/{stem}.ipynb"
    raw = f"https://raw.githubusercontent.com/{_REPO}/main/examples/notebooks/{stem}.ipynb"
    line = f"{_LINKS_PREFIX} [`{stem}.ipynb`]({github}): [download it]({raw}) to run it yourself."
    head, _, rest = body.partition("\n")
    return f"{head}\n\n{line}\n{rest}" if head.startswith("# ") else f"{line}\n\n{body}"


def _relink(text: str, stem: str, label: str, position: int) -> str:
    """A committed page with its head (frontmatter, notebook links) rebuilt, without executing anything."""
    body = text.split("\n---\n", 1)[1].lstrip("\n")
    body = re.sub(rf"^{re.escape(_LINKS_PREFIX)}.*\n\n", "", body, count=1, flags=re.M)
    return _normalize_md(_frontmatter(stem, label, position) + _with_links(stem, _fix_links(body)))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--no-execute", action="store_true", help="render notebooks as-is (no live API calls)")
    ap.add_argument(
        "--only", action="append", default=[], help="only render this stem (repeatable); for retries/debugging"
    )
    ap.add_argument(
        "--relink", action="store_true", help="rebuild the head of the committed pages (no execution, no outputs lost)"
    )
    args = ap.parse_args()
    if args.relink:
        for stem, label, position in TUTORIALS:
            page = OUT_DIR / f"{stem}.md"
            page.write_text(
                _relink(page.read_text(encoding="utf-8"), stem, label, position), encoding="utf-8", newline="\n"
            )
            print(f"  relinked {page}")
        return 0

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
        (OUT_DIR / f"{stem}.md").write_text(
            _normalize_md(_frontmatter(stem, label, position) + _with_links(stem, body)), encoding="utf-8", newline="\n"
        )
        print(f"  wrote {OUT_DIR / f'{stem}.md'}")

    if failures:
        print(f"\nFAILED ({len(failures)}): {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\nrendered {len(TUTORIALS)} tutorial pages -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
