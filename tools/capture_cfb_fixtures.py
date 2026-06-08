# tools/capture_cfb_fixtures.py
"""One-off: capture ESPN CFB summary payloads as offline test fixtures.

Run once with network access:
    .venv/Scripts/python -m tools.capture_cfb_fixtures
Commit the resulting tests/cfb/fixtures/summary_*.json files.
"""

from __future__ import annotations

import json
from pathlib import Path

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

GAMES = [401754598, 401309854, 401112081, 401135269, 401032062]
OUT = Path("tests/cfb/fixtures")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for gid in GAMES:
        raw = CFBPlayProcess(gameId=gid, raw=True).espn_cfb_pbp()
        path = OUT / f"summary_{gid}.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        print(f"wrote {path} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
