"""Scratch (not committed to package) capture script for T7.3 Phase 0/5 —
college baseball/softball feasibility probe.

Finds one completed 2026-season college baseball game + one completed
college softball game via the ESPN scoreboard, then captures raw
summary + game_plays payloads to tests/fixtures/league_ports/.

Run: SDV_PY_LIVE_TESTS=1 uv run python dev/league_ports/capture_college_baseball_softball.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "league_ports"

# 2026 college baseball/softball postseason window (season runs Feb-Jun;
# CWS/WCWS finals land mid-June). Sweep backward from a plausible date.
CANDIDATE_DATES = ["20260615", "20260610", "20260601", "20260515", "20260501", "20260415", "20260315"]


def _find_completed_event(scoreboard_fn, dates):
    for d in dates:
        raw = scoreboard_fn(dates=d, return_parsed=False)
        events = raw.get("events") or []
        for ev in events:
            comps = ev.get("competitions") or []
            if not comps:
                continue
            status = (comps[0].get("status") or {}).get("type") or {}
            if status.get("completed") is True:
                return ev["id"], d
    return None, None


def main() -> int:
    from sportsdataverse.baseball.college_baseball import (
        espn_college_baseball_game_plays,
        espn_college_baseball_scoreboard,
        espn_college_baseball_summary,
    )
    from sportsdataverse.baseball.college_softball import (
        espn_college_softball_game_plays,
        espn_college_softball_scoreboard,
        espn_college_softball_summary,
    )

    results = {}
    for league, sb_fn, sum_fn, plays_fn in [
        (
            "college_baseball",
            espn_college_baseball_scoreboard,
            espn_college_baseball_summary,
            espn_college_baseball_game_plays,
        ),
        (
            "college_softball",
            espn_college_softball_scoreboard,
            espn_college_softball_summary,
            espn_college_softball_game_plays,
        ),
    ]:
        event_id, found_date = _find_completed_event(sb_fn, CANDIDATE_DATES)
        if event_id is None:
            print(f"[{league}] no completed event found in candidate dates {CANDIDATE_DATES}")
            results[league] = {"event_id": None}
            continue
        print(f"[{league}] event_id={event_id} date={found_date}")
        summary = sum_fn(event_id=event_id, return_parsed=False)
        plays = plays_fn(event_id=event_id, return_parsed=False)
        (FIXTURES / f"{league}_summary.json").write_text(json.dumps(summary), encoding="utf-8")
        (FIXTURES / f"{league}_game_plays.json").write_text(json.dumps(plays), encoding="utf-8")
        items = plays.get("items") or plays.get("plays") or []
        sample = items[0] if items else {}
        print(f"[{league}] plays.items count={len(items)}")
        print(f"[{league}] sample play keys: {sorted(sample.keys())}")
        print(f"[{league}] sample play: {json.dumps(sample, indent=2)[:2000]}")
        results[league] = {"event_id": event_id, "date": found_date, "n_plays": len(items)}

    print("SUMMARY:", json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
