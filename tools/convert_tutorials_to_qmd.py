"""One-shot converter: Docusaurus tutorial markdown -> Great Docs user_guide .qmd.

Reads docs/docs/tutorials/NN_slug.md and emits user_guide/MM-slug.qmd, where the
tutorials are renumbered to sit *after* the two hand-authored getting-started
guides (00, 01). Code fences become non-executing ``{python}`` cells
(``#| eval: false``) so the build stays fast and never hits live ESPN / odds /
Statcast APIs at render time — the two executable guides (00, 01) already prove
the live path. Internal Docusaurus links are rewritten to the Great Docs tree.

This is a build-time authoring tool, not shipped in the wheel. Re-run it if the
source tutorials change:  python tools/convert_tutorials_to_qmd.py
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TUT = ROOT / "docs" / "docs" / "tutorials"
OUT = ROOT / "user_guide"

# Docusaurus NN_slug.md -> (new MM, qmd basename). Tutorials start at 02 (00/01
# are the hand-authored getting-started + scoreboards guides we keep).
SRC = sorted(p for p in TUT.glob("*.md") if re.match(r"^\d+_", p.name))


def _newname(src: Path) -> tuple[str, str]:
    num = int(re.match(r"^(\d+)_", src.name).group(1))
    slug = re.sub(r"^\d+_", "", src.stem).replace("_", "-")
    return f"{num + 1:02d}", f"{num + 1:02d}-{slug}.qmd"


# old docusaurus tutorial filename (stem) -> new qmd basename, for link rewrites.
LINKMAP = {src.stem: _newname(src)[1] for src in SRC}


def _frontmatter(src: Path, body: str) -> tuple[str, str]:
    """Return (title, description) from the Docusaurus frontmatter / first H1."""
    title = src.stem
    fm = re.match(r"^---\n(.*?)\n---\n", body, re.S)
    if fm:
        m = re.search(r"^title:\s*(.+)$", fm.group(1), re.M)
        if m:
            title = m.group(1).strip().strip("\"'")
    # description = first sentence of the first prose paragraph after the H1.
    return title, ""


def convert(src: Path) -> Path:
    body = src.read_text(encoding="utf-8")
    title, _ = _frontmatter(src, body)
    # strip Docusaurus frontmatter
    body = re.sub(r"^---\n.*?\n---\n", "", body, count=1, flags=re.S)

    # rewrite the leading H1 into prose-friendly: drop it (Quarto renders the
    # frontmatter title as the page H1 already) to avoid a double title.
    body = re.sub(r"^\s*#\s+.*\n", "", body, count=1)

    # ---- code fences: ```python -> ```{python}\n#| eval: false ----
    def py_fence(m: re.Match) -> str:
        return "```{python}\n#| eval: false\n"

    body = re.sub(r"^```python\s*$", py_fence, body, flags=re.M)
    # shell fences stay as plain ```bash for copy-paste (not executed)
    body = re.sub(r"^```sh\s*$", "```bash", body, flags=re.M)

    # ---- internal links ----
    # ../tutorials/NN_slug.md (with optional #anchor) -> NN-slug.qmd
    def tut_link(m: re.Match) -> str:
        stem, anchor = m.group(1), m.group(2) or ""
        return f"]({LINKMAP.get(stem, stem + '.qmd')}{anchor})"

    body = re.sub(r"\]\(\.\./tutorials/([\w-]+)\.md(#[\w-]+)?\)", tut_link, body)
    # bare same-dir tutorial link form: NN_slug.md  ->  NN-slug.qmd
    body = re.sub(r"\]\((\d+_[\w-]+)\.md(#[\w-]+)?\)", tut_link, body)
    # ../<sport>/index.md, ../<sport>/reference/*.md, ../reference/*.md, etc.
    # -> the Great Docs API reference index (closest equivalent).
    body = re.sub(r"\]\(\.\./[\w-]+/(?:reference/)?[\w./-]+\.md(?:#[\w-]+)?\)", "](../reference/index.qmd)", body)
    body = re.sub(r"\]\(\.\./[\w-]+/index\.md(?:#[\w-]+)?\)", "](../reference/index.qmd)", body)
    # any remaining ../something.md -> reference index (defensive)
    body = re.sub(r"\]\(\.\./[\w./-]+\.md(?:#[\w-]+)?\)", "](../reference/index.qmd)", body)

    desc = (
        f"{title.replace(' tutorial', '')} walkthrough — the canonical "
        "sportsdataverse surface, ported from the Docusaurus tutorials. Code "
        "cells show the calls; run them in your own session."
    )
    front = f'---\ntitle: "{title}"\ndescription: "{desc}"\njupyter: python3\n---\n\n'
    # banner note so readers know cells are illustrative (not build-executed)
    note = (
        "::: {.callout-note}\n"
        "The code cells below are shown for reference and are **not executed at "
        "build time** (they call the live ESPN / native APIs). Copy them into a "
        "Python session to run them. For executable, build-time examples see "
        "[Getting started](getting-started.qmd) and "
        "[Scoreboards & schedules](scoreboards-and-schedules.qmd).\n"
        ":::\n\n"
    )
    out_name = _newname(src)[1]
    dest = OUT / out_name
    dest.write_text(front + note + body.lstrip("\n"), encoding="utf-8", newline="\n")
    return dest


def main() -> None:
    written = [convert(src) for src in SRC]
    for d in written:
        print(d.name)
    print(f"converted {len(written)} tutorials -> {OUT}")


if __name__ == "__main__":
    main()
