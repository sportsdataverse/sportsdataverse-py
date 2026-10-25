"""Additive merge: union new descriptions INTO each loader block.

Block-REPLACE is wrong here -- two generators contribute to the same loader
(adv_* vocabulary plus the cfb-rest tail), so replacing the block makes the
second generator erase the first one's columns. Existing entries win, so a
hand-authored description is never clobbered by a composed one.

``--replace``: the snippet's entries win instead (columns the snippet does not
name are still kept). Use it for a generator-OWNED snippet such as
``notes/cba_descriptions.yaml``, whose rows change when the generator does.

Run: ``python tools/codegen/merge_column_descriptions.py [--replace] <snippet.yaml>...``
(from any directory; the snippet paths are relative to the CWD, the manual yaml is
found from this file). Importing the module does nothing.
"""

import pathlib
import re
import sys

import yaml

MANUAL = pathlib.Path(__file__).resolve().parent / "manual_column_descriptions.yaml"
KEY = re.compile(r"^[A-Za-z_]\w*:\s*$")


def main(argv: list[str]) -> None:
    replace = "--replace" in argv
    files = [a for a in argv if a != "--replace"]
    new: dict = {}
    for f in files:
        for k, v in (yaml.safe_load(pathlib.Path(f).read_text(encoding="utf-8")) or {}).items():
            new.setdefault(k, {}).update(v)

    p = MANUAL
    cur = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    added = changed = 0
    for k, cols in new.items():
        # `or {}` not `.get(k, {})`: a loader can be present in the yaml with an
        # EMPTY block, which yaml parses to None. The two-arg default only covers a
        # missing key, so the None case fell through and crashed on `c not in have`.
        have = cur.get(k) or {}
        for c, d in cols.items():
            if c not in have:
                have[c] = d
                added += 1
            elif replace and have[c] != d:
                have[c] = d
                changed += 1
        cur[k] = have

    def dump(k):
        return yaml.safe_dump({k: dict(sorted(cur[k].items()))}, sort_keys=False, allow_unicode=True, width=120)

    # Rewrite each merged block WHERE IT ALREADY SITS, so the diff is only the added
    # entries; a loader new to the file goes before load_contracts: as before.
    lines = p.read_text(encoding="utf-8").splitlines(keepends=True)
    out, i, placed = [], 0, set()
    while i < len(lines):
        k = lines[i].rstrip()[:-1] if KEY.match(lines[i]) else None
        if k in new:
            if cur.get(k):
                out.append(dump(k))
            placed.add(k)
            i += 1
            while i < len(lines) and not KEY.match(lines[i]):
                i += 1
            continue
        out.append(lines[i])
        i += 1
    block = "".join(dump(k) for k in sorted(new) if k not in placed and cur.get(k))
    # newline="\n": the yaml is committed LF; text mode on Windows would rewrite every line as CRLF.
    p.write_text(
        "".join(out).replace("\nload_contracts:", "\n" + block + "load_contracts:", 1), encoding="utf-8", newline="\n"
    )
    print(f"merged: +{added} new, {changed} replaced entries across {len(new)} loaders")


if __name__ == "__main__":
    main(sys.argv[1:])
