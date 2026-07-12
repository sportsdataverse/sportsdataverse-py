"""Fit the MCH home-advantage constant from the committed scoreboard fixture.

Reproduces ``COLLEGE_HOCKEY_CONSTANTS["mch"].hfa_goals``: mean
(home_goals - away_goals) over the non-neutral-site completed games in
``tests/fixtures/league_ports/mch_scoreboard_sample.json``. Fully offline --
reads the committed fixture, no network.

Run: uv run python dev/league_ports/fit_mch_hfa.py
"""

from __future__ import annotations

import json
from pathlib import Path

FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "league_ports" / "mch_scoreboard_sample.json"


def main() -> None:
    events = json.loads(FIXTURE.read_text(encoding="utf-8"))
    margins: list[float] = []
    for ev in events:
        comp = ev["competitions"][0]
        if comp.get("neutralSite"):
            continue
        scores = {c["homeAway"]: float(c["score"]) for c in comp["competitors"]}
        margins.append(scores["home"] - scores["away"])
    hfa = sum(margins) / len(margins)
    print(f"non-neutral games: {len(margins)}")
    print(f"hfa_goals = mean(home - away) = {hfa:.4f}")


if __name__ == "__main__":
    main()
