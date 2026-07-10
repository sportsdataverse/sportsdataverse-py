"""Widen the RAPM oracle sample to ~100-150 games for the model-2 concurrent-validity gate.

The initial 25-game (opening-night) sample produced d_rapm dominated by ridge shrinkage
(std ~0.009 on a 100-poss scale -- too noisy to validate matchup DRAPM against). This
samples games spread across the whole season, RESUMABLY: each game's possessions are cached
to dev/nba_playtype/_rapm_cache/<game_id>.parquet, so a mid-run throttle loses nothing --
just rerun and it skips what's on disk. The final RAPM refit reads every cached game.

Pace is env-tunable (defaults are gentle so we don't re-trip the stats.nba.com throttle):
  RAPM_N_GAMES     (default 150)  target game count, evenly spread across the season
  RAPM_DELAY_S     (default 2.0)  sleep between per-game fetches
  RAPM_BACKOFF_S   (default 30.0) extra sleep after a failed game before continuing

Run: SDV_PY_NBA_STATS_LIVE=1 uv run python -u dev/nba_playtype/capture_more_rapm.py
Rerun the SAME command after any throttle -- it resumes from the on-disk checkpoint.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_possessions import nba_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_playtype"
CACHE = Path(__file__).resolve().parent / "_rapm_cache"
CACHE.mkdir(exist_ok=True)

N_GAMES = int(os.environ.get("RAPM_N_GAMES", "150"))
DELAY_S = float(os.environ.get("RAPM_DELAY_S", "2.0"))
BACKOFF_S = float(os.environ.get("RAPM_BACKOFF_S", "30.0"))


def _target_game_ids() -> list[str]:
    gamelog = pl.read_parquet(FIX / "gamelog_2024.parquet")
    all_ids = sorted(gamelog["game_id"].unique().to_list())
    step = max(1, len(all_ids) // N_GAMES)
    return all_ids[::step][:N_GAMES]


def _refit() -> None:
    frames = [pl.read_parquet(p) for p in sorted(CACHE.glob("*.parquet"))]
    if not frames:
        print("no cached games yet -- nothing to fit")
        return
    all_poss = pl.concat(frames, how="diagonal_relaxed")
    rapm = nba_rapm(all_poss)
    rapm.write_parquet(FIX / "rapm_2024.parquet")
    print(f"REFIT: {len(frames)} games, {all_poss.height} possessions -> rapm={rapm.height} players")
    print(rapm.select("d_rapm").describe())


def main() -> None:
    game_ids = _target_game_ids()
    todo = [g for g in game_ids if not (CACHE / f"{g}.parquet").exists()]
    print(f"target={len(game_ids)} games; already cached={len(game_ids) - len(todo)}; to fetch={len(todo)}")
    print(f"pace: delay={DELAY_S}s backoff={BACKOFF_S}s")

    fetched = 0
    for i, gid in enumerate(todo):
        try:
            poss = nba_possessions(gid, "00")
            poss.write_parquet(CACHE / f"{gid}.parquet")
            fetched += 1
            print(f"  [{i + 1}/{len(todo)}] {gid}: {poss.height} poss (cached ok)", flush=True)
            time.sleep(DELAY_S)
        except Exception as e:
            print(f"  [{i + 1}/{len(todo)}] {gid}: FAILED {type(e).__name__} -- backing off {BACKOFF_S}s", flush=True)
            time.sleep(BACKOFF_S)

    print(f"fetched {fetched} new games this run")
    _refit()
    print("Done.")


if __name__ == "__main__":
    main()
