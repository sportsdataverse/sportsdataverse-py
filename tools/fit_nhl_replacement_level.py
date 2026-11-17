# tools/fit_nhl_replacement_level.py
"""Fit ``LEAGUE_CONSTANTS["nhl"]``'s even-strength replacement level and league EV rate.

``nhl_skater_war`` charges each skater's 5v5 RAPM against ``ImpactConfig.replacement_rapm()`` =
``((replacement_ev_off - league_xg_rate_ev) / 5, (league_xg_rate_ev - replacement_ev_def) / 5)``.
This fits all three numbers on the production path -- ``nhl_xg`` with the published boosters,
``build_stints``, then ``nhl_skater_rapm(strength_states=["5v5"])`` exactly as the WAR runs it:

* ``league_xg_rate_ev``: pooled 5v5 xGF per team per 60 = sum(xGF both sides) * 3600 / (2 * seconds).
* the replacement pool: every skater outside the season's regular lineups, i.e. below the top
  ``18 * n_teams`` skaters by 5v5 TOI (12 forwards + 6 defensemen dress per team; the shift chart
  carries no positions, so the cut is by TOI rank, league-wide).
* the pool's RAPM level: the TOI-weighted mean of its ``xg_rapm_off`` / ``xg_rapm_def`` -- the shrunk
  coefficients the WAR itself uses, so the pool as a whole earns zero EV GAR. Mapped back to team
  rates: ``replacement_ev_off = league + 5 * off``, ``replacement_ev_def = league - 5 * def``.

Refit after every xG retrain (all three are on the boosters' scale) and record the per-season values
and their means in the constants' comment.

Usage::

    uv run python tools/fit_nhl_replacement_level.py --seasons 2023 2024 2025 2026
"""

from __future__ import annotations

import argparse
import math
import sys
import time

import polars as pl

from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full, load_nhl_shifts
from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_rapm import build_stints, nhl_skater_rapm
from sportsdataverse.nhl.nhl_xg import nhl_xg

REGULARS_PER_TEAM = 18  # 12 F + 6 D dress each game


def ev_rate(stints: pl.DataFrame) -> tuple[float, float]:
    """Pooled 5v5 xGF per team per 60 over ``stints``, and their total seconds."""
    s = stints.filter((pl.col("strength_state") == "5v5") & (pl.col("duration") > 0))
    secs = float(s["duration"].sum() or 0)
    xg = float((s["xgf_home"].fill_null(0.0) + s["xgf_away"].fill_null(0.0)).sum() or 0.0)
    return (xg * 3600.0 / (2.0 * secs) if secs else float("nan")), secs


def replacement_pool(rapm: pl.DataFrame, n_regulars: int) -> tuple[float, float, int]:
    """TOI-weighted mean ``(xg_rapm_off, xg_rapm_def)`` of the skaters below the top ``n_regulars`` by TOI."""
    pool = rapm.sort(["toi_minutes", "player_id"], descending=[True, False]).slice(n_regulars)
    toi = float(pool["toi_minutes"].sum() or 0.0)
    if toi <= 0:
        return float("nan"), float("nan"), 0
    off = float((pool["xg_rapm_off"] * pool["toi_minutes"]).sum()) / toi
    dfn = float((pool["xg_rapm_def"] * pool["toi_minutes"]).sum()) / toi
    return off, dfn, pool.height


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="END years, e.g. 2025 = 2024-25")
    args = ap.parse_args(argv)
    seed_off, seed_def = get_constants("nhl").replacement_rapm()
    fits = []
    for season in args.seasons:
        t0 = time.time()
        scored = nhl_xg(load_nhl_pbp_full([season]))
        stints = build_stints(load_nhl_shifts([season]), scored)
        league, secs = ev_rate(stints)
        if secs <= 0:  # a missing shifts/pbp asset loads empty: fail, never average a NaN in
            print(f"{season}: no 5v5 stints (missing release asset?)", file=sys.stderr)
            return 1
        if not league > 0:  # 5v5 time but no scored shot: a feature/schema break, not a zero-xG season
            print(f"{season}: no 5v5 xG over {secs / 3600:.0f} h (did nhl_xg score any shot?)", file=sys.stderr)
            return 1
        rapm = nhl_skater_rapm(pl.DataFrame(), pl.DataFrame(), strength_states=["5v5"], _stints=stints)
        n_teams = scored["home_abbr"].drop_nulls().n_unique()
        n_reg = REGULARS_PER_TEAM * n_teams
        off, dfn, n_pool = replacement_pool(rapm, n_reg)
        if n_pool == 0 or not (math.isfinite(off) and math.isfinite(dfn)):
            print(f"{season}: no replacement pool below the top {n_reg} of {rapm.height} skaters", file=sys.stderr)
            return 1
        fits.append((league, off, dfn))
        # the pool earns zero EV GAR against its own level; against the current constants it earns
        # (off - seed_off + def - seed_def) xG per pool skater-hour
        print(
            f"{season}: league EV xGF/60 {league:.3f} over {secs / 3600:.0f} h; {n_teams} teams, "
            f"pool {n_pool} of {rapm.height} skaters; pool RAPM off {off:+.4f} def {dfn:+.4f} "
            f"(current {seed_off:+.4f} / {seed_def:+.4f}) -> rep_off {league + 5 * off:.3f} "
            f"rep_def {league - 5 * dfn:.3f} ({time.time() - t0:.0f}s)",
            flush=True,
        )
    k = len(fits)
    league, off, dfn = (sum(f[i] for f in fits) / k for i in range(3))
    print(
        f"mean: league_xg_rate_ev {league:.3f}, pool off {off:+.4f} def {dfn:+.4f} -> "
        f"replacement_ev_off {league + 5 * off:.3f}, replacement_ev_def {league - 5 * dfn:.3f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
