"""Additive merge: union new descriptions INTO each loader block.

Block-REPLACE is wrong here -- two generators contribute to the same loader
(adv_* vocabulary plus the cfb-rest tail), so replacing the block makes the
second generator erase the first one's columns. Existing entries win, so a
hand-authored description is never clobbered by a composed one.
"""

import re
import sys
import pathlib
import yaml

files = sys.argv[1:]
new = {}
for f in files:
    for k, v in (yaml.safe_load(pathlib.Path(f).read_text(encoding="utf-8")) or {}).items():
        new.setdefault(k, {}).update(v)

p = pathlib.Path("tools/codegen/manual_column_descriptions.yaml")
cur = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
added = 0
for k, cols in new.items():
    have = cur.get(k, {})
    for c, d in cols.items():
        if c not in have:
            have[c] = d
            added += 1
    cur[k] = have

lines = p.read_text(encoding="utf-8").splitlines(keepends=True)
KEY = re.compile(r"^[A-Za-z_]\w*:\s*$")
out, i = [], 0
while i < len(lines):
    k = lines[i].rstrip()[:-1] if KEY.match(lines[i]) else None
    if k in new:
        i += 1
        while i < len(lines) and not KEY.match(lines[i]):
            i += 1
        continue
    out.append(lines[i])
    i += 1
block = "".join(
    yaml.safe_dump({k: dict(sorted(cur[k].items()))}, sort_keys=False, allow_unicode=True, width=120)
    for k in sorted(new)
    if cur.get(k)
)
p.write_text("".join(out).replace("\nload_contracts:", "\n" + block + "load_contracts:", 1), encoding="utf-8")
print(f"additively merged: +{added} new entries across {len(new)} loaders")
