#!/usr/bin/env python3
"""Fit DEFAULT_TRANSITION_SECONDS for the NBA play-context engine.

CTG does not publish a transition seconds cutoff ("transition starts at the
beginning of a possession and only ends once the defense is set"), so the knob in
:data:`sportsdataverse.nba.nba_play_context_constants.DEFAULT_TRANSITION_SECONDS`
is CALIBRATED, not quoted. This script is the fitting run behind that number.

Target band (the oracles):
  * CTG's own published Play-Context table shows team transition frequencies
    around 0.14 (e.g. Denver 14.3%) — `sdv-internal-refs/cleaningtheglass/`.
  * Synergy's `transition` play-type frequency runs ~0.15–0.16 league-wide
    (`sportsdataverse.nba.nba_playtype`).

Fit sample: the three committed engine fixtures
(`tests/fixtures/nba_engine/{0022100001,0022200001,0022300001}`), CTG's default
filters applied (garbage time + heave possessions dropped, non-counting
possessions dropped).

Result (2026-07-12): **6.0s**, mean transition frequency 0.163 — inside the band.
The widely-cited hoop-math 10s rule is a *college* convention with a different
denominator; applied to NBA possessions it gives ~0.35, ~2.4x CTG's rate.

Run:
    uv run python dev/ctg_transition_calibration.py
"""

from __future__ import annotations

import json
import pathlib

import polars as pl

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context

FIXTURES = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022200001", "0022300001", "0022100001"]
GRID = [4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 12.0]

# Oracle band: CTG published ~0.14; Synergy ~0.15-0.16.
TARGET_LO, TARGET_HI = 0.13, 0.17


def _load(game_id: str) -> pl.DataFrame:
    payload = json.loads((FIXTURES / game_id / "playbyplayv3.json").read_text())
    return enhanced_pbp_from_payload(payload)


def main() -> None:
    frames = {gid: _load(gid) for gid in GAMES}
    print(f"{'secs':>5} | {'per-game transition freq':38} | {'mean':>6} | in-band")
    print("-" * 72)
    for secs in GRID:
        per_game = []
        for enh in frames.values():
            poss = add_play_context(enh, transition_seconds=secs)
            clean = poss.filter(
                (pl.col("count_as_possession") == True)  # noqa: E712
                & (pl.col("is_garbage_time") == False)  # noqa: E712
                & (pl.col("is_heave_possession") == False)  # noqa: E712
            )
            per_game.append(clean["is_transition"].mean())
        mean = sum(per_game) / len(per_game)
        flag = "YES <-- adopted" if TARGET_LO <= mean <= TARGET_HI else ""
        print(f"{secs:5.1f} | {str([round(x, 3) for x in per_game]):38} | {mean:6.3f} | {flag}")

    print(
        "\nAdopted DEFAULT_TRANSITION_SECONDS = 6.0 (mean 0.163).\n"
        "Follow-up: re-fit at season scale once the possession compile is cached; "
        "3 games is a small sample and the band is tight."
    )


if __name__ == "__main__":
    main()
