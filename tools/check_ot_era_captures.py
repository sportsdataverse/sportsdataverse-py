"""Assert a committed overtime capture exists inside every CFB and NFL overtime rule set (C-M6).

    uv run python tools/check_ot_era_captures.py

Prints one line per rule set naming the capture that covers it, then
``OT ERAS: cfb 3/3, nfl 3/3`` and exits 0; any uncovered set prints ``MISSING`` and exits 1.

A capture covers a rule set when its ESPN header season falls inside the set, its plays reach the
period that set's rule is about (so a test can tell the sets apart, which is what C-M6 asks for),
and its game id is named in the league's ``tests/<league>/fixtures/README.md``. Rule ids are the
sdv-internal-refs ``rules/*.yaml`` entries.

CFB: overtime from the 25 since 1996, a two-point try forced from the third overtime; 2019-20 add the
two-point shootout from the fifth overtime (period 9); from 2021 the try is forced from the second
overtime and the shootout starts in the third (period 7). NFL, any overtime (period 5): a 15-minute
regular-season overtime before 2017 (sudden death, modified from 2012), 10 minutes from 2017, and both
teams possess from 2025. The quality-program ledger wrote the third set as 2024+; the catalogue dates
the regular-season both-possess rule 2025 (2024 still played the 2017 rule), so the sets are cut there.
"""

from __future__ import annotations

import gzip
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_CAPTURE = re.compile(r"^summary_(\d+)(?:_trimmed)?\.json(?:\.gz)?$")

#: league -> [(label, first season, last season, minimum period reached, rules/*.yaml id)]
ERAS: dict[str, list[tuple[str, int, int, int, str]]] = {
    "cfb": [
        ("<2019", 1996, 2018, 7, "cfb-1996-overtime-introduced"),
        ("2019-20", 2019, 2020, 9, "cfb-2019-overtime-shootout-from-fifth"),
        ("2021+", 2021, 9999, 7, "cfb-2021-overtime-shootout-from-third"),
    ],
    "nfl": [
        ("<2017", 1974, 2016, 5, "nfl-2012-overtime-modified-sudden-death-regular-season"),
        ("2017-24", 2017, 2024, 5, "nfl-2017-overtime-ten-minutes"),
        ("2025+", 2025, 9999, 5, "nfl-2025-overtime-both-possess-regular-season"),
    ],
}


def _load(path: Path) -> dict:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    return json.loads(path.read_text(encoding="utf-8"))


def _captures(league: str) -> list[tuple[int, int, int]]:
    """``(game id, season, max period)`` for every committed summary capture of ``league``."""
    out = []
    for path in sorted((ROOT / "tests" / league / "fixtures").glob("summary_*.json*")):
        hit = _CAPTURE.match(path.name)
        if not hit:
            continue
        summary = _load(path)
        drives = (summary.get("drives") or {}).get("previous", [])
        plays = [p for d in drives for p in d.get("plays", [])] or summary.get("plays", [])
        top = max(((p.get("period") or {}).get("number") or 0 for p in plays), default=0)
        out.append((int(hit.group(1)), int(summary["header"]["season"]["year"]), top))
    return out


def main() -> int:
    tally = {}
    for league, eras in ERAS.items():
        readme = ROOT / "tests" / league / "fixtures" / "README.md"
        named = readme.read_text(encoding="utf-8") if readme.exists() else ""
        captures = _captures(league)
        covered = 0
        for label, first, last, min_period, rule in eras:
            hits = [
                g for g, season, top in captures if first <= season <= last and top >= min_period and str(g) in named
            ]
            if hits:
                covered += 1
                print(f"{league} {label:8} {rule}: {', '.join(map(str, hits))}")
            else:
                print(
                    f"{league} {label:8} {rule}: MISSING (no capture reaching period {min_period} named in {readme.relative_to(ROOT).as_posix()})"
                )
        tally[league] = (covered, len(eras))
    print("OT ERAS: " + ", ".join(f"{lg} {c}/{n}" for lg, (c, n) in tally.items()))
    return 0 if all(c == n for c, n in tally.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
