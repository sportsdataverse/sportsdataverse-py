"""Fold the changelog.d/ fragments into CHANGELOG.md as a dated release section, then delete them.

    uv run python tools/release_changelog.py 0.1.6 [--date 2026-10-20] [--no-git-add]

The section lands above the newest release, as ``## <version> Release: <Month D, YYYY>`` with the groups in
their fixed order. The release commit still needs the doctoc table of contents, the codegen regen and the
hand-written Highlights paragraph; the script prints those steps.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

# Run as a script, sys.path[0] is tools/ rather than the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.hooks import sync_docs_changelog as sync  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def release(version: str, date: dt.date, changelog: Path = sync.SOURCE, fragments: Path = sync.FRAGMENTS) -> list[Path]:
    """Write ``version``'s section into ``changelog`` from ``fragments`` and delete them; returns the deleted paths.

    Raises ValueError, leaving both untouched, when there are no fragments or ``version`` is already released.
    """
    text = changelog.read_text(encoding="utf-8")
    if re.search(rf"^## {re.escape(version)} Release\b", text, re.M):
        raise ValueError(f"{version} is already released in {changelog.name}")
    groups = sync.read_fragments(fragments)
    if not groups:
        raise ValueError(f"no changelog.d/ fragments to release in {fragments}")
    section = sync.assemble(groups, f"{version} Release: {date:%B} {date.day}, {date.year}")
    first = re.search(r"^## ", text, re.M)
    at = first.start() if first else len(text)
    changelog.write_text(text[:at] + section + "\n" + text[at:], encoding="utf-8", newline="\n")
    removed = [p for p in sorted(fragments.iterdir()) if sync._FRAGMENT.fullmatch(p.name)]
    for path in removed:
        path.unlink()
    return removed


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("version", help="the version being released, e.g. 0.1.6")
    parser.add_argument("--date", type=dt.date.fromisoformat, default=dt.date.today(), help="YYYY-MM-DD; default today")
    parser.add_argument("--no-git-add", action="store_true", help="leave CHANGELOG.md and changelog.d/ unstaged")
    args = parser.parse_args(argv)
    try:
        removed = release(args.version, args.date)
    except ValueError as e:
        print(f"release_changelog: {e}", file=sys.stderr)
        return 1
    if not args.no_git_add:
        subprocess.run(["git", "add", "-A", "--", "CHANGELOG.md", "changelog.d"], cwd=ROOT, check=True)
    print(
        f"CHANGELOG.md: {args.version} section written from {len(removed)} fragments, which are deleted. Next:\n"
        "  1. write the **Highlights** paragraph under the new heading\n"
        "  2. npx --yes doctoc@2 --github CHANGELOG.md\n"
        "  3. uv run python tools/codegen/generate.py && uv run python tools/codegen/generate.py --check"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
