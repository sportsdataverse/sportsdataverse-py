#!/bin/bash
# Regenerate the Sphinx markdown API reference and copy the per-sport pages
# into the Docusaurus tree under docs/docs/<sport>/index.md.
#
# After the markdown lands in docs/docs/, a post-processing pass scrubs
# Sphinx-isms that break the Docusaurus MDX parser:
#   - escape angle-bracketed lowercase identifiers like `<factory>` /
#     `<class>` that aren't real JSX tags
#   - point unresolved sibling links (e.g. `(sportsdataverse.md)`) at the
#     in-site landing page so Docusaurus stops warning about them.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
SPHINX_SRC="$ROOT/Sphinx-docs"
DOCS_DST="$ROOT/docs/docs"
CHANGELOG_DST="$ROOT/docs/src/pages/CHANGELOG.md"

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
python3 - <<'PY'
from pathlib import Path
import re

# Each page now lives under docs/docs/<sport>/index.md. Iterate them and
# scrub MDX-incompatible literals that Sphinx-markdown-builder emits.
DOCS = Path("docs/docs")
SPORT_PAGES = sorted(DOCS.glob("*/index.md"))

# 1. Sphinx renders ``field(default_factory=...)`` defaults as the literal
#    string ``<factory>``. Docusaurus's MDX parser interprets that as an
#    opening JSX tag and demands ``</factory>`` to balance it. Replace
#    with the HTML entity form so it renders as text.
ANGLE_LITERAL = re.compile(r"<(factory|lambda|function|method)>")

# 2. Sphinx-apidoc emits cross-refs to the parent package page as
#    ``[text](sportsdataverse.md)`` and ``[text](sportsdataverse.md#anchor)``.
#    We don't ship that page under docs/docs/, so Docusaurus's broken-link
#    checker fails the build with `Docusaurus found broken links`. Strip
#    the link wrapper but keep the link text — the text is always the
#    fully-qualified dotted symbol (e.g. ``sportsdataverse.dl_utils.download()``)
#    which reads fine as inline code.
PARENT_LINK = re.compile(r"\[([^\]]+)\]\(sportsdataverse\.md(?:#[^)]*)?\)")

scrubs = 0
for page in SPORT_PAGES:
    text = page.read_text(encoding="utf-8")
    new = ANGLE_LITERAL.sub(lambda m: f"&lt;{m.group(1)}&gt;", text)
    new = PARENT_LINK.sub(r"\1", new)
    if new != text:
        page.write_text(new, encoding="utf-8")
        scrubs += 1
print(f"create_docs.sh: post-processed {scrubs} sport pages under {DOCS}")
PY

echo "create_docs.sh: done"
