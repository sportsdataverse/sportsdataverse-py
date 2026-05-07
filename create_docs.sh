#!/bin/bash
# Regenerate the Sphinx markdown API reference and copy the per-sport pages
# into the Docusaurus tree.
#
# By default the per-sport pages are written into the unversioned "next"
# tree at docs/docs/<sport>/index.md (the Docusaurus 3 docs versioning
# convention: docs/docs/ is the in-development surface; frozen snapshots
# live under docs/versioned_docs/version-X.Y.Z/).
#
# Pass --version <X.Y.Z> to instead refresh the markdown for an already-
# snapshotted version, e.g.:
#
#     bash create_docs.sh --version 0.0.50
#
# The script will refuse to write to a version that does not already exist
# under docs/versioned_docs/.
#
# After the markdown lands, a post-processing pass scrubs Sphinx-isms that
# would break the Docusaurus / MDX build:
#   1. Escape angle-bracketed lowercase identifiers like `<factory>` /
#      `<class>` / `<lambda>` that aren't real JSX/HTML tags. MDX 3 reads
#      them as opening JSX tags and demands a matching close tag.
#   2. Strip cross-refs to the parent package page sportsdataverse.md
#      (emitted by sphinx-apidoc but not shipped under docs/docs/).
#   3. Escape bare `{` / `}` outside fenced or inline code. MDX 3 reads
#      them as JSX expression delimiters even in CommonMark contexts.
#      Sphinx-markdown-builder happily emits them in API signatures and
#      definition-list lines, so we defang them with a backslash escape.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
SPHINX_SRC="$ROOT/Sphinx-docs"
CHANGELOG_DST="$ROOT/docs/src/pages/CHANGELOG.md"

# Default target = the unversioned "next" tree.
DOCS_DST="$ROOT/docs/docs"
VERSION_LABEL=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version)
            VERSION_LABEL="${2:-}"
            if [[ -z "$VERSION_LABEL" ]]; then
                echo "create_docs.sh: --version requires a value" >&2
                exit 2
            fi
            DOCS_DST="$ROOT/docs/versioned_docs/version-$VERSION_LABEL"
            if [[ ! -d "$DOCS_DST" ]]; then
                echo "create_docs.sh: refusing to write to non-existent version directory $DOCS_DST" >&2
                echo "  Snapshot the version first with: yarn --cwd docs docusaurus docs:version $VERSION_LABEL" >&2
                exit 2
            fi
            shift 2
            ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *)
            echo "create_docs.sh: unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

sphinx-apidoc -o "$SPHINX_SRC" "$ROOT/sportsdataverse" -f
(
    cd "$SPHINX_SRC"
    make markdown
)

for sport in cfb mbb nba nfl nhl wbb wnba; do
    cp "$SPHINX_SRC/_build/markdown/sportsdataverse.$sport.md" "$DOCS_DST/$sport/index.md"
done
cp "$ROOT/CHANGELOG.md" "$CHANGELOG_DST"

# Post-process the per-sport pages so the Docusaurus build passes cleanly.
# The target dir is forwarded via env so the python block doesn't need to
# parse argv again.
DOCS_DST="$DOCS_DST" python3 - <<'PY'
import os
import re
from pathlib import Path

DOCS = Path(os.environ["DOCS_DST"])
SPORT_PAGES = sorted(DOCS.glob("*/index.md"))

# 1. Sphinx renders ``field(default_factory=...)`` defaults as the literal
#    string ``<factory>``. MDX interprets that as an opening JSX tag and
#    demands ``</factory>`` to balance it. Replace with HTML entities so
#    it renders as text.
ANGLE_LITERAL = re.compile(r"<(factory|lambda|function|method|class|attribute)>")

# 2. Sphinx-apidoc emits cross-refs to the parent package page as
#    ``[text](sportsdataverse.md)`` and ``[text](sportsdataverse.md#anchor)``.
#    Strip the link wrapper but keep the text; the text is always the
#    fully-qualified dotted symbol, which reads fine as inline.
PARENT_LINK = re.compile(r"\[([^\]]+)\]\(sportsdataverse\.md(?:#[^)]*)?\)")

# 3. MDX 3's parser treats `{` / `}` as JSX expression delimiters even in
#    CommonMark contexts. Sphinx-markdown-builder emits them in API
#    signatures and definition-list lines (e.g. `{path_to_json}/{gameId}.json`).
#    Wrap with backticks if not already inside a code span; otherwise
#    escape with a backslash so MDX prints them literally.
#
#    We process line-by-line, tracking whether we're inside a fenced code
#    block (``` or ~~~) and skipping those entirely. Inside a single line
#    we walk the chars and toggle on backticks.
FENCE = re.compile(r"^\s*(```|~~~)")


def escape_braces_outside_code(text: str) -> str:
    out = []
    in_fence = False
    for line in text.splitlines(keepends=True):
        if FENCE.match(line):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue
        out.append(_escape_line(line))
    return "".join(out)


def _escape_line(line: str) -> str:
    # Walk character-by-character, toggling on backticks so we leave
    # inline code spans alone. MDX 3 treats `{`/`}` as JSX expression
    # delimiters; escape with `\{` / `\}` outside code spans.
    chars = []
    in_code = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "`":
            in_code = not in_code
            chars.append(ch)
            i += 1
            continue
        if not in_code and ch in "{}":
            # Don't double-escape if already escaped.
            if chars and chars[-1] == "\\":
                chars.append(ch)
            else:
                chars.append("\\")
                chars.append(ch)
            i += 1
            continue
        chars.append(ch)
        i += 1
    return "".join(chars)


scrubs = 0
for page in SPORT_PAGES:
    text = page.read_text(encoding="utf-8")
    new = ANGLE_LITERAL.sub(lambda m: f"&lt;{m.group(1)}&gt;", text)
    new = PARENT_LINK.sub(r"\1", new)
    new = escape_braces_outside_code(new)
    if new != text:
        page.write_text(new, encoding="utf-8")
        scrubs += 1
print(f"create_docs.sh: post-processed {scrubs} sport pages under {DOCS}")
PY

if [[ -n "$VERSION_LABEL" ]]; then
    echo "create_docs.sh: refreshed version-$VERSION_LABEL pages."
else
    echo "create_docs.sh: refreshed unversioned 'next' pages under docs/docs/."
fi
echo "create_docs.sh: done"
