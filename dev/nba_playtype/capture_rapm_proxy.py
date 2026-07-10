"""Capture a WIDE RAPM oracle sample via the ProxyBonanza pool (fast, un-throttled).

The direct residential IP throttled to ~80s/game; routing stats.nba.com pbp through the
ProxyBonanza pool (curl_cffi impersonate=chrome + rotating proxies) runs ~1s/game. Reuses
the SAME per-game checkpoint cache as capture_more_rapm.py
(dev/nba_playtype/_rapm_cache/<game_id>.parquet) so the 107 games already fetched direct are
kept -- this only fills the gap up to N_GAMES.

Creds are read from ~/.Renviron AT CALL TIME (never OS env, never logged) via the proven
dev/ncaa_proxy.py loader. Transport is injected by monkeypatching the module-global
nba_stats_runtime._curl_transport, which _get reads at call time.

Env: RAPM_N_GAMES (default 400), RAPM_DELAY_S (default 0.3).
Run: SDV_PY_NBA_STATS_LIVE=1 uv run python -u dev/nba_playtype/capture_rapm_proxy.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import curl_cffi
import polars as pl

# ncaa_proxy.py lives in the MAIN checkout's gitignored dev/, not this worktree.
sys.path.insert(0, "C:/Users/saiem/Documents/GitHub-Data/sdv-dev/sdv-py/dev")
from ncaa_proxy import load_proxy_pool  # noqa: E402

import sportsdataverse.nba.nba_stats_runtime as rt  # noqa: E402
from sportsdataverse.nba.nba_possessions import nba_possessions  # noqa: E402
from sportsdataverse.nba.nba_rapm import nba_rapm  # noqa: E402

FIX = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_playtype"
CACHE = Path(__file__).resolve().parent / "_rapm_cache"
CACHE.mkdir(exist_ok=True)

N_GAMES = int(os.environ.get("RAPM_N_GAMES", "400"))
DELAY_S = float(os.environ.get("RAPM_DELAY_S", "0.3"))

_LOGIN, _PWD, _POOL = load_proxy_pool()
_STATE = {"i": 0}
_SESS = curl_cffi.Session(impersonate="chrome")


def _proxy_transport(url, params, headers, proxy_url):
    """Rotating-proxy transport; advances the pool index each call, retries on error."""
    last = None
    for _ in range(4):
        pk = _POOL[_STATE["i"] % len(_POOL)]
        _STATE["i"] += 1
        p = f"http://{_LOGIN}:{_PWD}@{pk['ip']}:{pk['port_http']}"
        try:
            r = _SESS.get(
                url, params=params, headers=headers, proxies={"http": p, "https": p}, timeout=30, impersonate="chrome"
            )
            return r.status_code, r.text
        except Exception as e:  # rotate to next IP and retry
            last = e
    raise RuntimeError(f"proxy transport failed after rotation: {last}")


rt._curl_transport = _proxy_transport


def _target_game_ids() -> list[str]:
    gamelog = pl.read_parquet(FIX / "gamelog_2024.parquet")
    all_ids = sorted(gamelog["game_id"].unique().to_list())
    step = max(1, len(all_ids) // N_GAMES)
    return all_ids[::step][:N_GAMES]


def _refit() -> None:
    frames = [pl.read_parquet(p) for p in sorted(CACHE.glob("*.parquet"))]
    all_poss = pl.concat(frames, how="diagonal_relaxed")
    rapm = nba_rapm(all_poss)
    rapm.write_parquet(FIX / "rapm_2024.parquet")
    print(f"REFIT: {len(frames)} games, {all_poss.height} poss -> rapm={rapm.height} players")
    print(rapm.select("d_rapm").describe())


def main() -> None:
    game_ids = _target_game_ids()
    todo = [g for g in game_ids if not (CACHE / f"{g}.parquet").exists()]
    print(f"target={len(game_ids)}; cached={len(game_ids) - len(todo)}; to fetch={len(todo)} (proxy pool={len(_POOL)})")
    fetched = 0
    for i, gid in enumerate(todo):
        try:
            poss = nba_possessions(gid, "00")
            poss.write_parquet(CACHE / f"{gid}.parquet")
            fetched += 1
            if fetched % 20 == 0:
                print(f"  [{i + 1}/{len(todo)}] {gid}: {poss.height} poss (fetched {fetched})", flush=True)
            time.sleep(DELAY_S)
        except Exception as e:
            print(f"  [{i + 1}/{len(todo)}] {gid}: FAILED {type(e).__name__}", flush=True)
    print(f"fetched {fetched} new games this run")
    _refit()
    print("Done.")


if __name__ == "__main__":
    main()
