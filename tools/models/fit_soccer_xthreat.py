"""Fit the bundled Expected Threat grid on StatsBomb open data.

    uv run python tools/models/fit_soccer_xthreat.py            # all default competitions (230 matches)
    uv run python tools/models/fit_soccer_xthreat.py --limit 5  # smoke run

Default competitions (ids verified against open-data competitions.json on 2026-10-07): World Cup 2018 (43/3),
World Cup 2022 (43/106), Euro 2020 (55/43), Euro 2024 (55/282). Match discovery reads
data/matches/{competition_id}/{season_id}.json from the open-data repository; events load through
``soccer_open_dataset`` (kloppy). StatsBomb open data is for research and non-commercial use.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import requests

from sportsdataverse.soccer import soccer_open_dataset, soccer_spadl
from sportsdataverse.soccer.xthreat import XThreat

RAW = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
DEFAULT = [
    (43, 3, "FIFA World Cup 2018"),
    (43, 106, "FIFA World Cup 2022"),
    (55, 43, "UEFA Euro 2020"),
    (55, 282, "UEFA Euro 2024"),
]
OUT = Path("sportsdataverse/soccer/models/xthreat_statsbomb_open.json")


def match_ids(competition_id: int, season_id: int) -> list[int]:
    r = requests.get(f"{RAW}/matches/{competition_id}/{season_id}.json", timeout=60)
    r.raise_for_status()
    return [m["match_id"] for m in r.json()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="matches per competition (0 = all)")
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--sleep", type=float, default=0.25)
    args = ap.parse_args()
    import kloppy
    import kloppy.statsbomb  # noqa: F401 - kloppy does not attach provider submodules on a bare import

    frames: list[pl.DataFrame] = []
    used: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for cid, sid, name in DEFAULT:
        ids = match_ids(cid, sid)
        if args.limit:
            ids = ids[: args.limit]
        n = 0
        for mid in ids:
            try:
                frames.append(soccer_spadl(soccer_open_dataset("statsbomb", mid)))
                n += 1
            except Exception as exc:  # noqa: BLE001 - skip and record, never abort the fit
                print(f"WARNING: skipping match {mid}: {exc!r}", file=sys.stderr, flush=True)
                skipped.append({"match_id": mid, "error": repr(exc)})
            time.sleep(args.sleep)
        used.append({"competition_id": cid, "season_id": sid, "name": name, "matches": n})
        print(f"{name}: {n} matches", flush=True)
    actions = pl.concat(frames, how="diagonal_relaxed")
    model = XThreat().fit(actions)
    model.meta = {
        "competitions": used,
        "matches": sum(u["matches"] for u in used),
        "skipped": skipped,
        "actions": actions.height,
        "kloppy_version": kloppy.__version__,
        "fit_date": dt.date.today().isoformat(),
        "eps": model.eps,
        "iterations": model.iterations,
        "grid": [model.w, model.l],
        "method": "Expected Threat (Singh 2019) as implemented by socceraction (MIT), ported to sportsdataverse.soccer.xthreat",
        "license": "Fit on StatsBomb open data (research / non-commercial use; https://github.com/statsbomb/open-data).",
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    model.to_json(args.out)
    print(json.dumps(model.meta, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
