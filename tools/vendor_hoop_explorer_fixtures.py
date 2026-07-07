"""One-off vendoring of hoop-explorer jest snapshots as JSON oracle fixtures.

Re-run after bumping the upstream clone:
    uv run python tools/vendor_hoop_explorer_fixtures.py
"""

from __future__ import annotations

import json
import re
import sys
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

# Test *inputs* (as opposed to the snapshot *outputs* above). These are
# `export const X = {...}` / `const X = {...}` object-literal sources feeding
# `LineupUtils.test.ts` (either an imported `src/sample-data/*.ts` module, or
# a `const` declared inline in the test file itself). Multiple source files
# may map to the same output name -- their parsed top-level constants are
# merged into one JSON file keyed by constant name.
INPUT_SOURCES = {
    UPSTREAM / "src/sample-data/sampleLineupStatsResponse.ts": "lineup_utils_inputs.json",
    UPSTREAM / "src/utils/stats/__tests__/LineupUtils.test.ts": "lineup_utils_inputs.json",
}

# `export const X = {...};` (sample-data modules) or bare `const X = {...};`
# (inline test-file locals, e.g. `testIn` in LineupUtils.test.ts) -- both are
# JSON5-parseable object/array literals once `undefined` is folded to `null`.
# Non-literal consts (arrow functions, expressions referencing other
# constants) fail json5.loads() and are silently skipped -- they aren't
# vendorable input data.
CONST_RE = re.compile(
    r"(?:export\s+)?const\s+(?P<name>\w+)(?:\s*:[^=]+)?\s*=\s*(?P<body>[\[{].*?[\]}])\s*(?:as\s+const)?\s*;",
    re.S,
)

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


def parse_sample_module(path: Path) -> tuple[dict[str, object], int, int]:
    """Parse one TS source's top-level `const` object/array literals.

    Returns (entries, n_total, n_failed) mirroring `parse_snap`'s per-file
    parse-rate signature -- `n_total` counts every `const` the regex found
    (including non-literal ones like arrow functions or expressions that
    reference other constants), `n_failed` counts those that didn't survive
    a json5 parse. Failed entries are omitted from `entries` (unlike
    `parse_snap`, there's no raw-string fallback here -- a non-literal const
    isn't vendorable input data).
    """
    entries: dict[str, object] = {}
    text = path.read_text(encoding="utf-8")
    n_total = 0
    n_failed = 0
    for m in CONST_RE.finditer(text):
        n_total += 1
        body = m.group("body").replace("undefined", "null")
        try:
            entries[m.group("name")] = json5.loads(body)
        except ValueError:
            n_failed += 1
    return entries, n_total, n_failed


def main() -> None:
    if not UPSTREAM.exists():
        sys.exit(
            f"Upstream clone not found at {UPSTREAM}.\n"
            "Clone https://github.com/Alex-At-Home/cbb-on-off-analyzer to "
            "that exact path before re-running this vendoring script.\n"
            "(Update UPSTREAM in this file if your clone lives elsewhere.)"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for snap_name, out_name in SNAPS.items():
        parsed, n_total, n_failed = parse_snap(SNAP_DIR / snap_name)
        out = OUT_DIR / out_name
        out.write_text(json.dumps(parsed, indent=1, sort_keys=True), encoding="utf-8")
        rate = 100.0 * (n_total - n_failed) / n_total if n_total else 0.0
        print(f"{snap_name}: {len(parsed)} entries -> {out.name} ({n_total - n_failed}/{n_total} parsed, {rate:.1f}%)")

    # Merge per-output-name across every source file that targets it (e.g.
    # a sample-data module + the test file's own inline `const testIn = ...`
    # both feed lineup_utils_inputs.json).
    merged: dict[str, dict[str, object]] = {}
    totals: dict[str, tuple[int, int]] = {}
    for src_path, out_name in INPUT_SOURCES.items():
        parsed, n_total, n_failed = parse_sample_module(src_path)
        bucket = merged.setdefault(out_name, {})
        for const_name, value in parsed.items():
            # A later INPUT_SOURCES entry silently overwriting an earlier
            # one's same-named const would corrupt an oracle input without
            # any signal -- fail loudly instead. Two sources are allowed to
            # coexist in the same out_name (that's the whole point of the
            # merge), but not to redefine the same const name.
            assert const_name not in bucket, (
                f"{src_path.name} defines const {const_name!r}, which "
                f"collides with an entry already merged into {out_name!r} "
                f"from an earlier INPUT_SOURCES source. Rename one of the "
                f"upstream consts (or this script's merge logic) instead of "
                f"silently clobbering an existing oracle input."
            )
            bucket[const_name] = value
        prev_total, prev_failed = totals.get(out_name, (0, 0))
        totals[out_name] = (prev_total + n_total, prev_failed + n_failed)
    for out_name, entries in merged.items():
        out = OUT_DIR / out_name
        out.write_text(json.dumps(entries, indent=1, sort_keys=True), encoding="utf-8")
        n_total, n_failed = totals[out_name]
        rate = 100.0 * (n_total - n_failed) / n_total if n_total else 0.0
        print(
            f"[inputs] -> {out.name}: {sorted(entries.keys())} "
            f"({n_total - n_failed}/{n_total} consts parsed, {rate:.1f}%)"
        )


if __name__ == "__main__":
    main()
