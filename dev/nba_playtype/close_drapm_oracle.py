"""Widen the stint-RAPM oracle sample via ProxyBonanza and track the
matchup_drapm-vs-stint-d_rapm Spearman trajectory as the sample grows (T3.5
external-oracle closure task).

``test_matchup_drapm_vs_shipped_rapm_DEFERRED`` (see
``tests/nba/test_nba_playtype_oracle.py``, since renamed
``test_matchup_drapm_vs_stint_rapm_construct_gap``) was deferred/skipped at
126 games because the trajectory was FLAT (~-0.03 at both 107 and 126
games). This script
fetches more games -- RESUMABLE, same per-game checkpoint cache as
``capture_more_rapm.py`` / ``capture_rapm_proxy.py``
(``dev/nba_playtype/_rapm_cache/<game_id>.parquet``, so games already fetched
by either script are reused, nothing wasted) -- and at each checkpoint game
count refits ``nba_rapm`` over ALL cached games and recomputes the Spearman.

Checkpoint refits are written to ``dev/nba_playtype/_rapm_checkpoints/
rapm_<n>games.parquet`` -- **NOT** the committed
``tests/fixtures/nba_playtype/rapm_2024.parquet``. That fixture is only
overwritten, deliberately, by a separate one-line command after a decision is
made (close the gate / document construct-gap). This script never mutates
committed fixtures.

``matchup_drapm`` needs no new fetch: ``matchups_2024.parquet`` is already a
**full-season** fixture (``nba_stats_leagueseasonmatchups`` covers the whole
season in one call), so it is computed ONCE up front and reused at every
checkpoint -- only the stint side of the comparison grows.

Env:
    RAPM_N_GAMES     (default 500)   overall target game count.
    RAPM_DELAY_S     (default 0.3)   sleep between per-game proxy fetches.
    RAPM_CHECKPOINTS (default "50,100,150,200,300,400,500") comma-separated
                     cumulative cached-game counts to stop and measure at.
    RAPM_LINEUP_SRC  (default "pbp") forwarded to ``nba_possessions(...,
                     lineup_source=...)``. The shipped default "auto" tries
                     rotation, then falls back to a per-period quarter_box
                     fetch, then pbp -- up to ~4-7 network calls/game through
                     the pool, measured at ~50-55s/game (proxy latency
                     dominates, not local compute). "pbp" needs only the 2
                     unconditional calls (pbp + box), no fallback chain, and
                     the docstring documents ~96.7% on-court agreement with
                     rotation -- an acceptable trade for a several-x speedup
                     on a sample-size-bound problem. Set to "auto" to match
                     the original 126-game snapshot's method exactly (slower).

Run: SDV_PY_NBA_STATS_LIVE=1 uv run python -u dev/nba_playtype/close_drapm_oracle.py
Ctrl-C safe -- rerun the same command to resume (cached games + already-
logged checkpoints are skipped). Progress streams to stdout AND appends to
dev/nba_playtype/_rapm_trajectory.log.

**Resolution (2026-07-11)**: run to completion with
``RAPM_N_GAMES=1230 RAPM_LINEUP_SRC=pbp`` -- the full season, ~27 min of
fetch time at ~1.3s/game. The trajectory plateaued from ~800 games on
(rho 0.111 at both 800 and 1000, 0.115 at the full 1230); see
``tests/fixtures/nba_playtype/README.md``'s "Model (2) construct-gap
finding" for the conclusion and
``tests/nba/test_nba_playtype_oracle.py::test_matchup_drapm_vs_stint_rapm_construct_gap``
for the permanent gate this produced. ``rapm_2024.parquet`` now IS this
full-season fit (copied from
``_rapm_checkpoints/rapm_1230games.parquet``).
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
from sportsdataverse.nba.nba_matchup_drapm import nba_matchup_drapm  # noqa: E402
from sportsdataverse.nba.nba_playtype_constants import spearman_corr  # noqa: E402
from sportsdataverse.nba.nba_possessions import nba_possessions  # noqa: E402
from sportsdataverse.nba.nba_rapm import nba_rapm  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "nba_playtype"
CACHE = Path(__file__).resolve().parent / "_rapm_cache"
CKPT = Path(__file__).resolve().parent / "_rapm_checkpoints"
LOG = Path(__file__).resolve().parent / "_rapm_trajectory.log"
CACHE.mkdir(exist_ok=True)
CKPT.mkdir(exist_ok=True)

N_GAMES = int(os.environ.get("RAPM_N_GAMES", "500"))
DELAY_S = float(os.environ.get("RAPM_DELAY_S", "0.3"))
CHECKPOINTS = sorted({int(x) for x in os.environ.get("RAPM_CHECKPOINTS", "50,100,150,200,300,400,500").split(",")})
LINEUP_SRC = os.environ.get("RAPM_LINEUP_SRC", "pbp")

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


def _log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _target_game_ids() -> list[str]:
    gamelog = pl.read_parquet(FIX / "gamelog_2024.parquet")
    all_ids = sorted(gamelog["game_id"].unique().to_list())
    step = max(1, len(all_ids) // N_GAMES)
    return all_ids[::step][:N_GAMES]


def _measure(n_games_done: int, matchup_d: pl.DataFrame) -> None:
    ckpt_path = CKPT / f"rapm_{n_games_done}games.parquet"
    frames = [pl.read_parquet(p) for p in sorted(CACHE.glob("*.parquet"))]
    all_poss = pl.concat(frames, how="diagonal_relaxed")
    rapm = nba_rapm(all_poss)
    rapm.write_parquet(ckpt_path)

    j = matchup_d.join(rapm.select("player_id", "d_rapm"), on="player_id", how="inner").filter(
        pl.col("matchup_poss") >= 200
    )
    n = j.height
    rho = spearman_corr(j["matchup_drapm"].to_numpy(), j["d_rapm"].to_numpy()) if n >= 10 else float("nan")
    d_std = float(rapm["d_rapm"].std())
    _log(
        f"CHECKPOINT games={n_games_done} cached_games={len(frames)} poss={all_poss.height} "
        f"rapm_players={rapm.height} d_rapm_std={d_std:.4f} join_n={n} rho={rho:.4f} -> {ckpt_path.name}"
    )


def main() -> None:
    game_ids = _target_game_ids()
    already = [g for g in game_ids if (CACHE / f"{g}.parquet").exists()]
    todo_count = len(game_ids) - len(already)
    _log(
        f"target={len(game_ids)} games; already cached={len(already)}; to fetch={todo_count} "
        f"(proxy pool={len(_POOL)}, lineup_source={LINEUP_SRC!r})"
    )

    matchup_d = nba_matchup_drapm("2023-24", matchups=pl.read_parquet(FIX / "matchups_2024.parquet"))
    _log(f"matchup_drapm computed once: {matchup_d.height} defenders (full-season matchups fixture, no fetch)")

    done_ids: list[str] = list(already)
    fetched = 0
    t0 = time.time()
    already_measured = {int(p.stem.replace("rapm_", "").replace("games", "")) for p in CKPT.glob("rapm_*games.parquet")}

    for gid in game_ids:
        if (CACHE / f"{gid}.parquet").exists():
            continue
        try:
            poss = nba_possessions(gid, "00", lineup_source=LINEUP_SRC)
            poss.write_parquet(CACHE / f"{gid}.parquet")
            fetched += 1
            done_ids.append(gid)
        except Exception as e:  # noqa: BLE001 -- resumable: log and continue past a bad game
            _log(f"  {gid}: FAILED {type(e).__name__}: {e}")
            continue
        time.sleep(DELAY_S)

        n_done = len(done_ids)
        elapsed = time.time() - t0
        rate = elapsed / fetched if fetched else 0.0
        if fetched % 10 == 0:
            _log(f"-- progress: {n_done} games cached ({fetched} fetched this run, {rate:.2f}s/game) --")
        if n_done in CHECKPOINTS and n_done not in already_measured:
            _log(f"-- {n_done} games cached, {elapsed:.0f}s elapsed this run ({rate:.2f}s/game fetched) --")
            _measure(n_done, matchup_d)
            already_measured.add(n_done)

    _log(f"DONE. fetched {fetched} new games this run, {len(done_ids)} total cached.")
    final_n = len(done_ids)
    if final_n not in already_measured:
        _measure(final_n, matchup_d)


if __name__ == "__main__":
    main()
