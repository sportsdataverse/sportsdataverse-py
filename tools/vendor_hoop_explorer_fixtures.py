"""One-off vendoring of hoop-explorer jest snapshots as JSON oracle fixtures.

Re-run after bumping the upstream clone:
    uv run python tools/vendor_hoop_explorer_fixtures.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import json5

UPSTREAM = Path(r"c:/Users/saiem/Documents/GitHub-Data/cbb-on-off-analyzer")
SNAP_DIR = UPSTREAM / "src/utils/stats/__tests__/__snapshots__"
OUT_DIR = Path(__file__).resolve().parents[1] / "tests/fixtures/hoop_explorer"

SNAPS = {
    "LineupUtils.test.ts.snap": "lineup_utils_snap.json",
    "RatingUtils.test.ts.snap": "rating_utils_snap.json",
    "LuckUtils.test.ts.snap": "luck_utils_snap.json",
    "RapmUtils.test.ts.snap": "rapm_utils_snap.json",
}

# Jest .snap files are CommonJS modules of the form
#   exports[`name`] = `\n<pretty-printed JS value>\n`;
# The value is JSON5-ish (unquoted or quoted keys, `undefined`, trailing
# commas, sometimes prefixed by a jest type tag like `Object {` / `Array [`).
ENTRY_RE = re.compile(r"exports\[`(?P<name>[^`]+)`\] = `\n?(?P<body>.*?)\n?`;", re.S)

# Jest's pretty-format prefixes a bare `{`/`[` with a constructor-name tag
# for some serializers (older jest / class instances): `Object {`, `Array [`.
# Strip the tag so json5 sees a plain object/array literal.
OBJECT_TAG_RE = re.compile(r"\b(?:Object|Array)\s(?=[\[{])")


def parse_snap(path: Path) -> tuple[dict[str, object], int, int]:
    """Parse one .snap file.

    Returns (entries, n_total, n_failed) so the caller can report a
    per-file parse rate.
    """
    entries: dict[str, object] = {}
    text = path.read_text(encoding="utf-8")
    n_total = 0
    n_failed = 0
    for m in ENTRY_RE.finditer(text):
        n_total += 1
        body = m.group("body").strip()
        body = OBJECT_TAG_RE.sub("", body)
        # `undefined` is a bare JS identifier jest emits for undefined
        # object values; json5 has no concept of it, so fold to null
        # (matches JSON.stringify's own undefined->omitted-or-null
        # behavior closely enough for oracle-comparison purposes).
        body = re.sub(r"\bundefined\b", "null", body)
        try:
            entries[m.group("name")] = json5.loads(body)
        except ValueError:
            entries[m.group("name")] = body  # keep raw string; test decides
            n_failed += 1
    return entries, n_total, n_failed


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for snap_name, out_name in SNAPS.items():
        parsed, n_total, n_failed = parse_snap(SNAP_DIR / snap_name)
        out = OUT_DIR / out_name
        out.write_text(json.dumps(parsed, indent=1, sort_keys=True), encoding="utf-8")
        rate = 100.0 * (n_total - n_failed) / n_total if n_total else 0.0
        print(f"{snap_name}: {len(parsed)} entries -> {out.name} ({n_total - n_failed}/{n_total} parsed, {rate:.1f}%)")


if __name__ == "__main__":
    main()
