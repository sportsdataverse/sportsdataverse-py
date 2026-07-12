"""Scan several 2024-25 dates for MCH/WCH events with full (not 'basic') play-by-play."""

from __future__ import annotations

from sportsdataverse.hockey.mch import espn_mch_scoreboard, espn_mch_summary
from sportsdataverse.hockey.wch import espn_wch_scoreboard, espn_wch_summary

DATES = [
    "20241012",
    "20241101",
    "20241115",
    "20241201",
    "20241215",
    "20250110",
    "20250118",
    "20250201",
    "20250215",
    "20250301",
    "20250308",
    "20250315",
    "20250321",
    "20250328",
    "20250404",
    "20250405",
    "20250410",
    "20250412",
]


def scan(league: str) -> None:
    get_sb = espn_mch_scoreboard if league == "mch" else espn_wch_scoreboard
    get_summary = espn_mch_summary if league == "mch" else espn_wch_summary
    print(f"\n=== {league} ===")
    for d in DATES:
        raw = get_sb(dates=d, return_parsed=False)
        events = raw.get("events", [])
        for ev in events:
            comp = (ev.get("competitions") or [{}])[0]
            status = (comp.get("status") or {}).get("type", {})
            if not status.get("completed"):
                continue
            pbp_src = comp.get("playByPlaySource")
            shot_chart = comp.get("shotChartAvailable")
            eid = ev["id"]
            flag = "FULL" if pbp_src == "full" else pbp_src
            print(f"{d} eid={eid} pbpSource={flag} shotChart={shot_chart}")
            if pbp_src == "full":
                print(f"  -> CANDIDATE: {league} event {eid}")


if __name__ == "__main__":
    scan("mch")
    scan("wch")
