"""Mirror CHANGELOG.md onto the docs site as three pages (the `sync-docs-changelog` pre-commit hook).

    docs/src/pages/CHANGELOG.md             /CHANGELOG             the newest releases
    docs/src/pages/changelog-unreleased.md  /changelog-unreleased  merged since the last release
    docs/src/pages/changelog-archive.md     /changelog-archive     every older release

The repo-root CHANGELOG.md is the copy people edit; this script never changes it. Its doctoc table of contents
is left out: each page gets Docusaurus's own. Run with --no-git-add to write the pages without staging them.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "CHANGELOG.md"
PAGES = ROOT / "docs" / "src" / "pages"
# The newest releases fill the CHANGELOG page up to this many bytes of markdown (always at least one). Measured
# on the live page at 390 px on 2026-10-04: a release renders at 0.73-0.86 px per byte, so the page stays under
# 20,000 px. ponytail: a single release over the budget still lands whole; split it if one ever reaches 25 KB.
RECENT_BYTES = 20_000
_TOC = re.compile(r"<!-- START doctoc.*?<!-- END doctoc[^>]*-->\n*", re.S)
_SECTION = re.compile(r"^## (?=Unreleased\b|\d[\w.-]* Release\b)", re.M)
_VERSION = re.compile(r"## (\S+) Release")


def _check(text: str) -> None:
    """Raise ValueError naming the line of an unrecognised ``## `` heading or of text before the first section."""
    fenced = seen = False
    for no, line in enumerate(text.splitlines(), 1):
        if line.startswith("```"):
            fenced = not fenced
        elif fenced:
            continue
        elif line.startswith("## "):
            if not _SECTION.match(line):
                raise ValueError(f"CHANGELOG.md line {no}: unrecognised heading {line!r}")
            seen = True
        elif not seen and line.strip() and not line.startswith("# "):
            raise ValueError(f"CHANGELOG.md line {no}: text before the first release heading: {line!r}")


def split(text: str) -> tuple[str, list[str], list[str]]:
    """``(unreleased section, recent release sections, older release sections)`` in file order."""
    _check(_TOC.sub(lambda m: "\n" * m.group().count("\n"), text))  # blanked, not cut: line numbers stay true
    sections = ["## " + s.rstrip() + "\n" for s in _SECTION.split(_TOC.sub("", text))[1:]]
    unreleased = "".join(s for s in sections if s.startswith("## Unreleased"))
    releases = [s for s in sections if not s.startswith("## Unreleased")]
    recent: list[str] = []
    size = 0
    for s in releases:
        if recent and size + len(s.encode()) > RECENT_BYTES:
            break
        recent.append(s)
        size += len(s.encode())
    return unreleased, recent, releases[len(recent) :]


def _page(title: str, intro: str, sections: list[str]) -> str:
    # One final newline, whatever the sections: codegen's docs normalisation must leave these bytes alone.
    return (f"---\ntitle: {title}\n---\n\n# {title}\n\n{intro}\n\n" + "\n".join(sections)).rstrip() + "\n"


def render(text: str) -> dict[str, str]:
    """``{file name under docs/src/pages: content}`` for the three pages."""
    unreleased, recent, older = split(text)
    newest = _VERSION.match(recent[0]).group(1) if recent else "the first release"
    oldest = _VERSION.match(recent[-1]).group(1) if recent else "the first release"
    return {
        "CHANGELOG.md": _page(
            "Changelog",
            f"The newest releases. Changes merged since {newest} are under [Unreleased](/changelog-unreleased);"
            " older releases are in the [archive](/changelog-archive).",
            recent,
        ),
        "changelog-unreleased.md": _page(
            "Unreleased changes",
            f"Merged to `main` since {newest} and not yet released. Released versions are on the"
            " [Changelog](/CHANGELOG).",
            [unreleased] if unreleased else ["Nothing has been merged since the last release.\n"],
        ),
        "changelog-archive.md": _page(
            "Changelog archive",
            f"Releases before {oldest}. The newest are on the [Changelog](/CHANGELOG).",
            older,
        ),
    }


def main(argv: list[str]) -> int:
    paths = []
    try:
        pages = render(SOURCE.read_text(encoding="utf-8"))
    except ValueError as e:
        print(f"sync_docs_changelog: {e}", file=sys.stderr)
        return 1
    for name, content in pages.items():
        path = PAGES / name
        path.write_text(content, encoding="utf-8", newline="\n")
        paths.append(str(path.relative_to(ROOT)))
    if "--no-git-add" not in argv:
        subprocess.run(["git", "add", *paths], cwd=ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
