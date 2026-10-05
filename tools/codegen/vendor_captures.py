"""Vendor trimmed copies of the private sdv-internal-refs real captures into
``tests/fixtures`` so the returns-schema generators (``gen_nba_stats.py``,
``gen_on3.py``) can derive column names and types from what the parsers emit.

Trimming keeps the first two records of every record list, plus every later
record that adds a ``(field path, value type)`` pair the kept ones lack. Parser
dtypes depend only on that set of pairs, so the trimmed capture parses to the
same columns and dtypes as the full one; ``main`` asserts this for every file
and refuses to write a fixture that drifts.

Run (needs the private repo checkout)::

    SDV_INTERNAL_REFS_REPO=<path> uv run python tools/codegen/vendor_captures.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Tuple

import yaml

from sportsdataverse.cfb.on3_parsers import parse_on3_rdb
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

ROOT = Path(__file__).resolve().parents[2]
_KEEP = 2


def _records(node: list) -> bool:
    """A trimmable list: every item is a record (list/dict) and none is a result-set."""
    return (
        bool(node)
        and all(isinstance(v, (list, dict)) for v in node)
        and not any(isinstance(v, dict) and "rowSet" in v for v in node)
    )


def _sig(node: Any, path: Tuple = ()) -> set:
    """``(path, type)`` for every leaf; record-list items share one ``[]`` path."""
    if isinstance(node, dict):
        return set().union(*(_sig(v, path + (k,)) for k, v in node.items())) if node else {(path, "{}")}
    if isinstance(node, list):
        if not node:
            return {(path, "[]")}
        rec = _records(node)
        return set().union(*(_sig(v, path + ("[]" if rec else i,)) for i, v in enumerate(node)))
    return {(path, type(node).__name__)}


def trim(node: Any, key: str = "") -> Any:
    """Copy of ``node`` with every record list cut to its first ``_KEEP`` records plus
    each later record that adds a ``(path, type)`` pair the kept ones lack."""
    if isinstance(node, dict):
        return {k: trim(v, k) for k, v in node.items()}
    if not isinstance(node, list):
        return node
    items = [trim(v) for v in node]
    if key == "headers" or not _records(items):  # never trim headers or scalar tuples
        return items
    out, seen = [], set()
    for i, v in enumerate(items):
        s = _sig(v)
        if i < _KEEP or not s <= seen:
            out.append(v)
            seen |= s
    return out


def _shape(parsed: Any) -> Dict[str, List[Tuple[str, str]]]:
    frames = parsed if isinstance(parsed, dict) else {"": parsed}
    return {name: [(c, str(t)) for c, t in df.schema.items()] for name, df in frames.items()}


def _vendor(src: Path, dst: Path, parse: Callable[[Any], Any]) -> int:
    full = json.loads(src.read_text(encoding="utf-8"))
    small = trim(full)
    if _shape(parse(small)) != _shape(parse(full)):
        raise ValueError(f"trimmed {src} parses to a different shape than the full capture")
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(small, ensure_ascii=False, indent=1) + "\n", encoding="utf-8", newline="\n")
    return dst.stat().st_size


def _endpoint_shorts(stem: str) -> Iterator[dict]:
    yield from yaml.safe_load((ROOT / f"tools/codegen/endpoints/{stem}.yaml").read_text(encoding="utf-8"))["endpoints"]


def vendor_nba(refs: Path) -> None:
    """Every ``nba_stats`` / ``wnba_stats`` endpoint from the 2026-08 capture sweep
    (except those whose schema comes from an earlier pilot capture)."""
    from tools.codegen.gen_nba_stats import CAPTURE_OVERRIDES

    for stem, league_id in (("nba_stats", "00"), ("wnba_stats", "10")):
        n = size = 0
        for ep in _endpoint_shorts(stem):
            if (stem, ep["short"]) in CAPTURE_OVERRIDES:
                continue
            src = refs / "nba/captures/_sample" / league_id / f"{ep['short']}.json"
            size += _vendor(
                src, ROOT / f"tests/fixtures/{stem}/endpoints/{ep['short']}.json", parse_nba_stats_result_sets
            )
            n += 1
        print(f"{stem}: vendored {n} captures ({size // 1024} KiB)")


def vendor_on3(refs: Path) -> None:
    """Every captured On3 RDB endpoint (manifest status 200) not already committed."""
    templates = {}
    for ep in _endpoint_shorts("on3"):
        parts = re.split(r"\{[^}]+\}", ep.get("host", "https://api.on3.com/public/rdb/v1") + ep["path"])
        templates[ep["short"]] = re.compile("[^/]+".join(map(re.escape, parts)) + "$")
    n = 0
    with (refs / "on3/captures/manifest.csv").open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            src = refs / "on3/captures/_sample" / f"{row['name']}.json"
            if row["status"] != "200" or not src.exists():
                continue
            url = row["url"].split("?")[0]
            shorts = [s for s, rx in templates.items() if rx.match(url)]
            if len(shorts) != 1:
                raise ValueError(f"{row['url']} matches {shorts}")
            dst = ROOT / "tests/fixtures/on3" / f"{shorts[0]}.json"
            if dst.exists():
                continue
            _vendor(src, dst, parse_on3_rdb)
            n += 1
    print(f"on3: vendored {n} captures")


def main() -> None:
    refs = Path(
        os.environ.get("SDV_INTERNAL_REFS_REPO", "C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-internal-refs")
    )
    vendor_nba(refs)
    vendor_on3(refs)


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))  # run as a script: make ``tools.codegen`` importable
    main()
