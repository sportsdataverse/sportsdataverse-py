"""Mirror CHANGELOG.md and changelog.d/ onto the docs site as three pages (the `sync-docs-changelog` pre-commit hook).

    docs/src/pages/CHANGELOG.md             /CHANGELOG             the newest releases
    docs/src/pages/changelog-archive.md     /changelog-archive     every older release
    docs/src/pages/changelog-unreleased.md  /changelog-unreleased  merged since the last release

The first two come from the repo-root CHANGELOG.md, which holds released sections only; they are committed and
drift-gated. The unreleased page comes from the changelog.d/ fragments, one file per change, so two open PRs
never edit the same lines. That page is gitignored: docs-deploy.yml writes it just before the site build. This
script changes neither source, and leaves out CHANGELOG.md's doctoc table of contents (each page gets
Docusaurus's own). Run with --no-git-add to write the pages without staging the committed two.

Stdlib only, so docs-deploy runs it with the runner's own python3 and no uv sync.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "CHANGELOG.md"
FRAGMENTS = ROOT / "changelog.d"
PAGES = ROOT / "docs" / "src" / "pages"
UNRELEASED_PAGE = "changelog-unreleased.md"
# The newest releases fill the CHANGELOG page up to this many bytes of markdown (always at least one). Measured
# on the live page at 390 px on 2026-10-04: a release renders at 0.73-0.86 px per byte, so the page stays under
# 20,000 px. ponytail: a single release over the budget still lands whole; split it if one ever reaches 25 KB.
RECENT_BYTES = 20_000
# A fragment is `changelog.d/<slug>.<group>.md`; each group lands under its `###` heading, in this order.
GROUPS = {
    "breaking": "Breaking changes",
    "added": "Added",
    "changed": "Changed",
    "deprecated": "Deprecated",
    "removed": "Removed",
    "fixed": "Fixed",
    "security": "Security",
    "data": "Data",
}
_FRAGMENT = re.compile(rf"[a-z0-9][a-z0-9._-]*\.({'|'.join(GROUPS)})\.md")
_TOC = re.compile(r"<!-- START doctoc.*?<!-- END doctoc[^>]*-->\n*", re.S)
_SECTION = re.compile(r"^## (?=\d[\w.-]* Release\b)", re.M)
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
            if re.match(r"## Unreleased\b", line):
                raise ValueError(
                    f"CHANGELOG.md line {no}: an unreleased change goes in a changelog.d/ fragment"
                    " (see changelog.d/README.md), not under '## Unreleased'"
                )
            if not _SECTION.match(line):
                raise ValueError(f"CHANGELOG.md line {no}: unrecognised heading {line!r}")
            seen = True
        elif not seen and line.strip() and not line.startswith("# "):
            raise ValueError(f"CHANGELOG.md line {no}: text before the first release heading: {line!r}")


def split(text: str) -> tuple[list[str], list[str]]:
    """``(recent release sections, older release sections)`` in file order."""
    _check(_TOC.sub(lambda m: "\n" * m.group().count("\n"), text))  # blanked, not cut: line numbers stay true
    releases = ["## " + s.rstrip() + "\n" for s in _SECTION.split(_TOC.sub("", text))[1:]]
    recent: list[str] = []
    size = 0
    for s in releases:
        if recent and size + len(s.encode()) > RECENT_BYTES:
            break
        recent.append(s)
        size += len(s.encode())
    return recent, releases[len(recent) :]


def _bullets(name: str, text: str) -> list[str]:
    """A fragment's top-level ``- `` bullets, each with its continuation lines (indented two spaces)."""
    bullets: list[list[str]] = []
    for no, line in enumerate(text.splitlines(), 1):
        if line.startswith("- "):
            bullets.append([line])
        elif not line.strip():
            if bullets:
                bullets[-1].append("")
        elif line.startswith("  ") and bullets:
            bullets[-1].append(line)
        else:
            raise ValueError(
                f"{name} line {no}: a fragment is '- ' bullets with continuation lines indented two spaces, got {line!r}"
            )
    if not bullets:
        raise ValueError(f"{name} is empty")
    return ["\n".join(b).rstrip() for b in bullets]


def read_fragments(directory: Path = FRAGMENTS) -> dict[str, list[str]]:
    """``{group: [bullet, ...]}`` from the fragments in ``directory``, files in name order; README.md is skipped.

    Raises ValueError naming a misnamed fragment, or one that is not a bullet list.
    """
    out: dict[str, list[str]] = {}
    if not directory.is_dir():
        return out
    for path in sorted(directory.iterdir()):
        if path.name == "README.md" or path.name.startswith("."):
            continue
        name = f"changelog.d/{path.name}"
        m = _FRAGMENT.fullmatch(path.name)
        if not m:
            raise ValueError(
                f"{name}: name a fragment <slug>.<group>.md, slug lowercase, group one of {', '.join(GROUPS)}"
            )
        out.setdefault(m.group(1), []).extend(_bullets(name, path.read_text(encoding="utf-8")))
    return out


def assemble(fragments: dict[str, list[str]], heading: str) -> str:
    """A ``## <heading>`` section: each group under its ``###`` heading in GROUPS order, its bullets sorted."""
    parts = [f"## {heading}\n"]
    for key, title in GROUPS.items():
        if fragments.get(key):
            parts.append(f"### {title}\n\n" + "\n".join(sorted(fragments[key], key=str.casefold)) + "\n")
    return "\n".join(parts)


def _page(title: str, intro: str, sections: list[str]) -> str:
    # One final newline, whatever the sections: codegen's docs normalisation must leave these bytes alone.
    return (f"---\ntitle: {title}\n---\n\n# {title}\n\n{intro}\n\n" + "\n".join(sections)).rstrip() + "\n"


def _newest(recent: list[str]) -> str:
    return _VERSION.match(recent[0]).group(1) if recent else "the first release"


def render(text: str) -> dict[str, str]:
    """``{file name under docs/src/pages: content}`` for the two committed pages."""
    recent, older = split(text)
    oldest = _VERSION.match(recent[-1]).group(1) if recent else "the first release"
    return {
        "CHANGELOG.md": _page(
            "Changelog",
            f"The newest releases. Changes merged since {_newest(recent)} are under [Unreleased](/changelog-unreleased);"
            " older releases are in the [archive](/changelog-archive).",
            recent,
        ),
        "changelog-archive.md": _page(
            "Changelog archive",
            f"Releases before {oldest}. The newest are on the [Changelog](/CHANGELOG).",
            older,
        ),
    }


def render_unreleased(text: str, fragments: dict[str, list[str]]) -> str:
    """The unreleased page: ``fragments`` (as read_fragments returns them) since the newest release in ``text``."""
    recent, _ = split(text)
    section = assemble(fragments, "Unreleased") if fragments else "Nothing has been merged since the last release.\n"
    return _page(
        "Unreleased changes",
        f"Merged to `main` since {_newest(recent)} and not yet released. Released versions are on the"
        " [Changelog](/CHANGELOG).",
        [section],
    )


def main(argv: list[str]) -> int:
    try:
        text = SOURCE.read_text(encoding="utf-8")
        pages = render(text)
        unreleased = render_unreleased(text, read_fragments())
    except ValueError as e:
        print(f"sync_docs_changelog: {e}", file=sys.stderr)
        return 1
    paths = []
    for name, content in pages.items():
        path = PAGES / name
        path.write_text(content, encoding="utf-8", newline="\n")
        paths.append(str(path.relative_to(ROOT)))
    (PAGES / UNRELEASED_PAGE).write_text(unreleased, encoding="utf-8", newline="\n")  # gitignored: never staged
    if "--no-git-add" not in argv:
        subprocess.run(["git", "add", *paths], cwd=ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
