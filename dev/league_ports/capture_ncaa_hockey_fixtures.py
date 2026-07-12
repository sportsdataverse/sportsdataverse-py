"""Capture real MCH/WCH fixtures + probe xG feasibility (Phase 0 slice, T7.3 hockey port).

Scratch script (not committed as a package module) -- writes raw ESPN payloads to
``tests/fixtures/league_ports/`` and prints the feasibility signal (coordinates?
strength state? shift data?) that decides Phase 4's scope.

Run: SDV_PY_LIVE_TESTS=1 uv run python dev/league_ports/capture_ncaa_hockey_fixtures.py
"""

from __future__ import annotations

import json
from pathlib import Path

from sportsdataverse.hockey.mch import espn_mch_game_plays, espn_mch_scoreboard, espn_mch_summary
from sportsdataverse.hockey.wch import espn_wch_game_plays, espn_wch_scoreboard, espn_wch_summary

OUT = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "league_ports"


def _find_completed_event(league: str, dates: str) -> str | None:
    get_sb = espn_mch_scoreboard if league == "mch" else espn_wch_scoreboard
    raw = get_sb(dates=dates, return_parsed=False)
    for ev in raw.get("events", []):
        comp = (ev.get("competitions") or [{}])[0]
        status = (comp.get("status") or {}).get("type", {})
        if status.get("completed"):
            return str(ev["id"])
    return None


def _capture(league: str, event_id: str) -> None:
    get_summary = espn_mch_summary if league == "mch" else espn_wch_summary
    get_plays = espn_mch_game_plays if league == "mch" else espn_wch_game_plays

    summary = get_summary(event_id=event_id, return_parsed=False)
    (OUT / f"{league}_summary.json").write_text(json.dumps(summary), encoding="utf-8")

    plays = get_plays(event_id=event_id, return_parsed=False)
    (OUT / f"{league}_game_plays.json").write_text(json.dumps(plays), encoding="utf-8")

    # Feasibility probe: inspect the first few play items for shot geometry / strength state.
    items = plays.get("items", plays.get("entries", []))
    sample = items[0] if items else {}
    print(f"\n--- {league} event {event_id} feasibility probe ---")
    print(f"plays count: {plays.get('count', len(items))}")
    print(f"sample play keys: {sorted(sample.keys()) if isinstance(sample, dict) else type(sample)}")
    has_coords = isinstance(sample, dict) and ("coordinate" in sample or "x" in sample or "y" in sample)
    print(f"has coordinate-like keys: {has_coords}")
    has_strength = isinstance(sample, dict) and any(
        "strength" in str(k).lower() or "power" in str(k).lower() for k in sample
    )
    print(f"has strength-state-like keys: {has_strength}")
    # shift/on-ice personnel is not part of the summary/plays payload at all for ESPN --
    # there is no dedicated shift-chart endpoint in the mch/wch wrapper surface (grepped
    # __all__ above: no *_shifts wrapper), so this is always False for ESPN college hockey.
    print("has shift-chart endpoint: False (no espn_mch_shifts / espn_wch_shifts wrapper exists)")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for league in ("mch", "wch"):
        # 2024-25 season ran Oct 2024-Apr 2025; probe a January date for a completed slate.
        event_id = _find_completed_event(league, "20250118")
        if event_id is None:
            print(f"{league}: no completed event found on probe date, trying a wider scoreboard scan")
            for d in ("20250111", "20250201", "20250215"):
                event_id = _find_completed_event(league, d)
                if event_id:
                    break
        if event_id is None:
            print(f"{league}: FAILED to find any completed event -- skipping capture")
            continue
        _capture(league, event_id)


if __name__ == "__main__":
    main()
