"""Trim StatsBomb open-data match 8658 (France v Croatia, 2018 World Cup final) to a kloppy-loadable fixture.

Keeps the first ``KEEP`` events, every ``Half Start`` / ``Half End`` event (kloppy needs both
boundaries of every period to deserialize) and the first ``Shot`` (so a test can check a shot's
coordinates against the raw JSON). Lineups are kept whole (18 KB). Re-run to re-capture::

    uv run python tests/fixtures/kloppy/trim_statsbomb_8658.py [<dir with 8658 events/lineups json>]

Source: https://github.com/statsbomb/open-data (StatsBomb's non-commercial public-data license; see README.md).
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
MATCH = 8658
KEEP = 50
PERIOD_BOUNDS = {"Half Start", "Half End"}


def _read(kind: str, src: Path | None) -> list:
    if src is not None:
        return json.loads((src / f"{kind}_{MATCH}.json").read_text(encoding="utf-8"))
    with urllib.request.urlopen(f"{RAW}/{kind}/{MATCH}.json") as resp:  # noqa: S310 -- fixed https host
        return json.load(resp)


def main(argv: list[str]) -> None:
    src = Path(argv[1]) if len(argv) > 1 else None
    events = _read("events", src)
    first_shot = next(e for e in events if e["type"]["name"] == "Shot")
    keep = [e for i, e in enumerate(events) if i < KEEP or e["type"]["name"] in PERIOD_BOUNDS or e is first_shot]
    (HERE / f"statsbomb_{MATCH}_events.json").write_text(json.dumps(keep, indent=1) + "\n", encoding="utf-8")
    (HERE / f"statsbomb_{MATCH}_lineups.json").write_text(
        json.dumps(_read("lineups", src), indent=1) + "\n", encoding="utf-8"
    )
    print(f"kept {len(keep)} of {len(events)} events")


if __name__ == "__main__":
    main(sys.argv)
