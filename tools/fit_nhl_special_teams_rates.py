# tools/fit_nhl_special_teams_rates.py
"""Fit ``LEAGUE_CONSTANTS["nhl"].league_xg_rate_pp`` / ``_pk``: the league's power-play xGF per 60.

``nhl_special_teams_value`` measures every PP / PK unit against this rate. It is the same number
for both sides (the PP's shots are the PK's shots against), so one fit sets both constants. It runs
the production path -- ``nhl_xg`` with the published boosters, then ``build_stints`` -- over each
season's ``load_nhl_pbp_full`` / ``load_nhl_shifts`` and pools every special-teams stint
(``strength_state`` with unequal skater counts): rate = sum(PP-side xG) * 3600 / sum(seconds).

Refit after every xG retrain (the rate is on the boosters' scale) and record the per-season rates
and their mean in the constants' comment.

Usage::

    uv run python tools/fit_nhl_special_teams_rates.py --seasons 2023 2024 2025 2026
"""

from __future__ import annotations

import argparse
import sys
import time

from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full, load_nhl_shifts
from sportsdataverse.nhl.nhl_rapm import build_stints
from sportsdataverse.nhl.nhl_special_teams import special_teams_split
from sportsdataverse.nhl.nhl_xg import nhl_xg


def pp_rate(stints) -> tuple[float, float]:
    """Pooled PP xGF per 60 over the special-teams stints, and their total seconds."""
    xg = secs = 0.0
    for rec in stints.select("strength_state", "duration", "xgf_home", "xgf_away").iter_rows(named=True):
        split = special_teams_split(rec)  # the runtime's own stint filter and PP-side choice
        if split is not None:
            xg += split[2]
            secs += rec["duration"]
    return (xg * 3600.0 / secs if secs else float("nan")), secs


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="END years, e.g. 2025 = 2024-25")
    args = ap.parse_args(argv)
    rates = []
    for season in args.seasons:
        t0 = time.time()
        stints = build_stints(load_nhl_shifts([season]), nhl_xg(load_nhl_pbp_full([season])))
        rate, secs = pp_rate(stints)
        if secs <= 0:  # a missing shifts/pbp asset loads empty: fail, never average a NaN in
            print(f"{season}: no power-play stints (missing release asset?)", file=sys.stderr)
            return 1
        rates.append(rate)
        print(f"{season}: PP xGF/60 = {rate:.3f} over {secs / 3600:.0f} h ({time.time() - t0:.0f}s)", flush=True)
    print(f"mean = {sum(rates) / len(rates):.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
