"""Execute the example notebooks and render them to Docusaurus tutorial pages.

The example notebooks (``examples/notebooks/*.ipynb``) are committed with their
outputs cleared. This script EXECUTES each one against the live APIs and renders
the executed result (code + outputs) to a themed Docusaurus page under
``docs/docs/tutorials/<stem>.md`` -- so the docs site shows real DataFrames /
values, not just code.

Because execution hits live ESPN / MLB / nflverse / PWHL APIs, this is **not**
part of the offline ``generate.py`` docs build. It is meant to run in the weekly
``tutorials-refresh`` workflow (execute -> render -> PR); the regular doc build
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
import unicodedata
from pathlib import Path
from typing import Optional

_LEAGUES = "cfb|nfl|nba|wnba|mbb|wbb|mlb|nhl|pwhl"


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
    ("16_international_basketball_intro", "International basketball", 16),
    ("17_european_college_hockey_intro", "European & college hockey", 17),
    ("18_prediction_markets_intro", "Prediction markets", 18),
    ("19_more_sources_intro", "More sources", 19),
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
    # Image outputs (if any) are extracted to <stem>_files/ alongside the page. MarkdownExporter
    # already enables ExtractOutputPreprocessor; listing it in `preprocessors` too ran it twice, and
    # the second pass raised on the first image ("filename ... associated with more than one output").
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


def _fix_links(body: str, anchor_map: Optional[dict] = None) -> str:
    """Rewrite notebook cross-reference links so they resolve from the rendered page.

    The source notebooks live in ``examples/notebooks/`` and link siblings as
    ``other_intro.ipynb`` and league pages as ``docs/docs/<lg>/index.md`` -- both
    correct from the notebook's location but broken once rendered under
    ``docs/docs/tutorials/``. Rewrite:

    * ``](<anything>/NN_<name>.ipynb)`` -> ``](NN_<name>.md)`` (sibling tutorial page)
    * ``](<anything><lg>/index.md)``     -> ``](../<lg>/index.md)`` (league index)
    * ``](../<lg>/reference/<page>[/<family>].md#<fn>)`` -> the family page that holds ``<fn>`` NOW, from
      ``anchor_map`` (the one ``generate.py`` builds while splitting pages; default: the committed
      ``docs/static/anchor-map.json``)
    """
    body = re.sub(r"\]\([^)]*?(\d\d_[a-z0-9_]+)\.ipynb\)", r"](\1.md)", body)
    body = re.sub(rf"\]\([^)]*?({_LEAGUES})/index\.md\)", r"](../\1/index.md)", body)
    amap = _anchor_map() if anchor_map is None else anchor_map
    return _REF_ANCHOR_LINK.sub(lambda m: _to_family_page(m, amap), body)


# A link to a function on a reference page that generate.py split into family pages
# (``../cfb/reference/loaders.md#load_cfb_pbp``) goes straight to the family page
# (``../cfb/reference/loaders/pbp.md#load_cfb_pbp``), from the anchor map the docs build writes.
# A link that already names a family is re-resolved too, so a renamed or removed family
# (``additional/other.md``) heals on the next render instead of stranding the link.
_REF_ANCHOR_LINK = re.compile(r"\]\((\.\./([a-z0-9_]+)/reference/([a-z0-9_-]+))(?:/[a-z0-9-]+)?\.md#([A-Za-z0-9_-]+)\)")


@functools.lru_cache(maxsize=1)
def _anchor_map() -> dict:
    path = ROOT / "docs" / "static" / "anchor-map.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _to_family_page(m: re.Match, anchor_map: dict) -> str:
    slug = anchor_map.get(f"/docs/{m.group(2)}/reference/{m.group(3)}", {}).get(m.group(4))
    return f"]({m.group(1)}/{slug}.md#{m.group(4)})" if slug else m.group(0)


def _normalize_md(text: str) -> str:
    """Match the repo's whitespace hooks (trailing-whitespace + end-of-file-fixer).

    nbconvert emits trailing spaces / no final newline; those hooks are NOT excluded
    for ``docs/docs/``. Normalizing here keeps the render output byte-identical to
    what a committed-then-hooked file would be, so the weekly cron only opens a PR
    when the *data* changed -- not because of cosmetic whitespace churn."""
    return "\n".join(ln.rstrip() for ln in text.splitlines()).rstrip() + "\n"


_SKIP_MARK = "⏭"  # the notebooks' safe() helper starts its skip line with this
_SKIP_MAX = 160
_HTML_BODY = re.compile(r"\s*<(?:!DOCTYPE|html|head|body|title)\b", re.I)
_REPEATED_CLASS = re.compile(r"\b(\w+(?:Error|Exception|Warning)): \1: ")


def _short_skip(line: str) -> str:
    """One ``safe()`` skip line as one short line: no repeated class prefix, no HTML body, at most _SKIP_MAX chars.

    ``NoDataError``'s message already starts with its class name, so ``f"{type(e).__name__}: {e}"`` printed it
    twice; a 403 from an HTML site carried the whole error page."""
    line = _REPEATED_CLASS.sub(r"\1: ", line)
    line = _HTML_BODY.split(line, maxsplit=1)[0].rstrip(" :")
    return line if len(line) <= _SKIP_MAX else line[: _SKIP_MAX - 1].rstrip() + "…"


def _tidy_skips(text: str) -> str:
    """Every skip line of a page (or a stdout output) collapsed with _short_skip; other lines unchanged."""
    return "\n".join(_short_skip(ln) if ln.lstrip().startswith(_SKIP_MARK) else ln for ln in text.split("\n"))


_MD_INLINE = re.compile(r"`([^`]*)`|\*\*([^*]+)\*\*|\*([^*\s][^*]*)\*|\[([^\]]+)\]\([^)]*\)")


def _plain(md: str) -> str:
    """Markdown inline text as plain text: code, emphasis and links unwrapped, emoji dropped, whitespace collapsed."""
    text = _MD_INLINE.sub(lambda m: next(g for g in m.groups() if g is not None), md)
    text = "".join(ch for ch in text if unicodedata.category(ch) not in ("So", "Cf", "Mn"))
    return " ".join(text.split())


def _description(stem: str, limit: int = 200) -> Optional[str]:
    """The page's meta description: the first paragraph of the notebook's first markdown cell, as plain text.

    Without it Docusaurus took the notebook-links line, whose underscores it read as emphasis ("04nbaintro.ipynb")."""
    path = NB_DIR / f"{stem}.ipynb"
    if not path.exists():
        return None
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    first = next(("".join(c["source"]) for c in cells if c["cell_type"] == "markdown"), "")
    para = next((b for b in first.split("\n\n") if b.strip() and not b.lstrip().startswith(("#", "|", "```"))), "")
    text = _plain(para)
    if len(text) <= limit:  # a lead-in to a table ("...tours four:") becomes a sentence
        return (text[:-1] + "." if text.endswith(":") else text) or None
    ends = [m.end() for m in re.finditer(r"[.!?](?=\s)", text[:limit])]
    if ends and ends[-1] >= 60:
        return text[: ends[-1]]
    return text[: limit - 1].rsplit(" ", 1)[0].rstrip(",;:—–- ") + "…"


_REPO = "sportsdataverse/sportsdataverse-py"
_LINKS_PREFIX = "> This page is the executed notebook"


def _frontmatter(stem: str, label: str, position: int) -> str:
    # "Edit this page" opens the notebook, the source of this page, not the generated markdown.
    edit = f"https://github.com/{_REPO}/edit/main/examples/notebooks/{stem}.ipynb"
    desc = _description(stem)
    # JSON string = a valid double-quoted YAML scalar, so quotes and colons in the text are safe.
    desc_line = f"description: {json.dumps(desc, ensure_ascii=False)}\n" if desc else ""
    return (
        f"---\ntitle: {label} tutorial\n{desc_line}sidebar_label: {label}\nsidebar_position: {position}\n"
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


def _relink(text: str, stem: str, label: str, position: int, anchor_map: Optional[dict] = None) -> str:
    """A committed page with its head (frontmatter, notebook links) rebuilt, without executing anything.

    ``generate.py`` calls this on every committed tutorial page with the anchor map of the pages it is
    writing, so a function a page split moves never strands a tutorial link (and ``--check`` covers them).
    """
    body = text.split("\n---\n", 1)[1].lstrip("\n")
    body = re.sub(rf"^{re.escape(_LINKS_PREFIX)}.*\n\n", "", body, count=1, flags=re.M)
    page = _frontmatter(stem, label, position) + _with_links(stem, _fix_links(body, anchor_map))
    return _normalize_md(_tidy_skips(page))


def relinked_pages(anchor_map: dict) -> dict:
    """``{"tutorials/<stem>.md": page}`` for every committed tutorial page, relinked against ``anchor_map``."""
    out = {}
    for stem, label, position in TUTORIALS:
        page = OUT_DIR / f"{stem}.md"
        if page.exists():
            out[f"tutorials/{stem}.md"] = _relink(page.read_text(encoding="utf-8"), stem, label, position, anchor_map)
    return out


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

    # Avoid loading a polluted global jupyter/nbconvert config (some machines register
    # a missing ``jupyter_contrib_nbextensions`` preprocessor that breaks nbconvert). Set here, not at
    # import, so generate.py importing this module for relinked_pages() leaves the environment alone.
    os.environ["JUPYTER_CONFIG_DIR"] = tempfile.mkdtemp(prefix="sdv-nbrender-")
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
                # A CellExecutionError's text starts with the cell source; the error itself is ename/evalue.
                err = f"{e.ename}: {e.evalue}" if hasattr(e, "ename") else f"{type(e).__name__}: {e}"
                print(f"  EXECUTION FAILED for {stem}: {err[:600]}", file=sys.stderr)
                failures.append(stem)
                continue
        _clean_outputs(nb)
        try:
            body = _fix_links(_to_markdown(nb, stem))
        except Exception as e:  # noqa: BLE001 -- one bad export must not stop the remaining notebooks
            print(f"  RENDER FAILED for {stem}: {type(e).__name__}: {str(e)[:600]}", file=sys.stderr)
            failures.append(stem)
            continue
        (OUT_DIR / f"{stem}.md").write_text(
            _normalize_md(_tidy_skips(_frontmatter(stem, label, position) + _with_links(stem, body))),
            encoding="utf-8",
            newline="\n",
        )
        print(f"  wrote {OUT_DIR / f'{stem}.md'}")

    if failures:
        print(f"\nFAILED ({len(failures)}): {', '.join(failures)}", file=sys.stderr)
        return 1
    print(f"\nrendered {len(TUTORIALS)} tutorial pages -> {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
