"""Capture WCH championship fixture + season scoreboard samples for the T7.3
NCAA hockey ratings oracle gate (Phase 4 downscope: no shot-level data exists
for MCH/WCH, so ratings are opponent-adjusted goals-for/against, not xG).

Run: SDV_PY_LIVE_TESTS=1 uv run python dev/league_ports/capture_wch_and_scoreboards.py
"""

from __future__ import annotations

import json
from pathlib import Path

from sportsdataverse.hockey.mch import espn_mch_scoreboard
from sportsdataverse.hockey.wch import espn_wch_game_plays, espn_wch_scoreboard, espn_wch_summary

OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "league_ports"

MCH_DATES = [
    "20241012",
    "20241109",
    "20241207",
    "20250111",
    "20250208",
    "20250301",
    "20250308",
    "20250315",
]
WCH_DATES = [
    "20241012",
    "20241109",
    "20241207",
    "20250111",
    "20250208",
    "20250301",
    "20250308",
    "20250315",
    "20250321",
    "20250322",
    "20250323",
]


def _scan(get_sb, dates: list[str]) -> list[dict]:
    events = []
    for d in dates:
        raw = get_sb(dates=d, return_parsed=False)
        for ev in raw.get("events", []):
            comp = (ev.get("competitions") or [{}])[0]
            status = (comp.get("status") or {}).get("type", {})
            if status.get("completed"):
                events.append(ev)
    return events


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # Season scoreboard samples (for the ratings oracle gate -- offline, no PBP needed).
    mch_events = _scan(espn_mch_scoreboard, MCH_DATES)
    (OUT / "mch_scoreboard_sample.json").write_text(json.dumps(mch_events), encoding="utf-8")
    print(f"mch scoreboard sample: {len(mch_events)} completed games")

    wch_events = _scan(espn_wch_scoreboard, WCH_DATES)
    (OUT / "wch_scoreboard_sample.json").write_text(json.dumps(wch_events), encoding="utf-8")
    print(f"wch scoreboard sample: {len(wch_events)} completed games")

    # WCH championship-caliber game -- richest available plays feed (scoring + penalties).
    candidates = [ev["id"] for ev in wch_events if "national" in json.dumps(ev).lower()] or [
        ev["id"] for ev in wch_events[-5:]
    ]
    for eid in candidates:
        summary = espn_wch_summary(event_id=eid, return_parsed=False)
        plays = summary.get("plays", [])
        print(f"wch candidate {eid}: {len(plays)} plays")
        if plays:
            (OUT / "wch_summary.json").write_text(json.dumps(summary), encoding="utf-8")
            raw_plays = espn_wch_game_plays(event_id=eid, return_parsed=False)
            (OUT / "wch_game_plays.json").write_text(json.dumps(raw_plays), encoding="utf-8")
            print(f"  -> captured wch_summary.json / wch_game_plays.json from event {eid}")
            break


if __name__ == "__main__":
    main()
