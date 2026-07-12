"""Capture the offline oracle corpus for the WNBA draft/projection re-fit (T3.4 Phase 5).

Run with ``SDV_PY_NBA_STATS_LIVE=1 uv run python dev/wnba_draft/capture_corpus.py`` from a
residential IP (stats.wnba.com shares stats.nba.com's TLS/JA3 fingerprint block).

**Direct residential access degraded mid-session (2026-07-11).** It worked cleanly for the
first ~1250 requests (drafthistory + ~70 playercareerstats calls, including a burst test), then
started timing out (30s, 0 bytes) on every subsequent call -- including the cheap single bulk
``drafthistory`` call that had succeeded easily earlier, retried 3x with zero success. That is
a cumulative rate/IP throttle, not a per-request fluke. Per the ops rule for this exact
situation, this script now ALWAYS routes through the ProxyBonanza pool
(``proxy_transport.ProxyRotator``, round-robins ~50 IPs, one new IP per request) rather than
falling back to direct access only on failure -- direct access never recovered within this
session. See ``dev/ncaa_proxy.py`` for the sibling pattern against a different host.

Mirrors ``dev/nba_draft/capture_corpus.py`` (Task 0.1) but scoped to what actually exists for
WNBA:

- No combine capture. Live 2026-07-08 capture already confirmed
  ``wnba_stats_draftcombinestats`` returns 0 rows for every WNBA season -- there is no
  anthro/drill/spot-shooting/non-stationary-shooting family for WNBA at all (see
  ``wnba_draft_constants.py``'s coverage caveat). This script does not attempt it again.
- ``wnba_stats_drafthistory()`` is a SINGLE bulk call (no per-year loop needed like NBA's
  combine capture) -- it returns every WNBA draft class in one shot: 1201 rows, 29 classes
  (1997-2025), confirmed live 2026-07-11.
- Season totals come from ``wnba_stats_playercareerstats(player_id=...)``'s
  ``SeasonTotalsRegularSeason`` set, same 27-column schema as the NBA endpoint (player_id,
  season_id, player_age, gp, fga, fta, tov, pts, reb, ast, stl, blk, min, ...) -- confirmed
  live 2026-07-11, so the shared ``BOX_VALUE_FEATURES`` / ``box_value_per100`` /
  ``build_aging_deltas`` / ``availability_features`` primitives apply unchanged.
- No ``wnba_bpm`` exists, so there is no anchor to *re-fit* ``LEAGUE_CONSTANTS["wnba"]
  .box_value_coef`` against (out of scope for this task -- stays NBA-borrowed, a pre-existing,
  still-documented caveat). This corpus only needs to *apply* that existing formula.
"""

from __future__ import annotations

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from proxy_transport import ProxyRotator  # noqa: E402

from sportsdataverse.wnba.wnba_stats import wnba_stats_drafthistory, wnba_stats_playercareerstats  # noqa: E402

FIXTURE_DIR = "tests/fixtures/wnba_draft"
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_S = 1.5
# Proxy exit IPs are diverse (round-robin over ~50), so per-IP throttling that hit direct
# access is far less of a concern here -- a modest bump from the direct-access value (2) is
# reasonable, still well short of a full fan-out per "keep parallelism low".
MAX_WORKERS = 6
CHECKPOINT_EVERY = 100  # write a resumable partial parquet this often

_rotator_lock = threading.Lock()
_rotator: "ProxyRotator | None" = None


def _proxy_url() -> str:
    global _rotator
    with _rotator_lock:
        if _rotator is None:
            _rotator = ProxyRotator()
        return _rotator.next_url()


def _pid_utf8(col: str = "player_id") -> pl.Expr:
    return pl.col(col).cast(pl.Int64).cast(pl.Utf8)


def capture_draft_history() -> pl.DataFrame:
    for attempt in range(RETRY_ATTEMPTS):
        try:
            hist = wnba_stats_drafthistory(proxy_url=_proxy_url())
        except Exception as exc:  # pragma: no cover - live network guard
            print(f"  [drafthistory] attempt {attempt + 1} ERROR {exc!r}")
            continue
        if not hist.is_empty():
            return hist.with_columns(
                pl.col("person_id").cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
                pl.col("season").cast(pl.Int64).alias("draft_year"),
            )
    raise RuntimeError("wnba_stats_drafthistory returned empty after retries (even via proxy)")


def _fetch_one(pid: str) -> "pl.DataFrame | None":
    for attempt in range(RETRY_ATTEMPTS):
        try:
            career = wnba_stats_playercareerstats(player_id=pid, proxy_url=_proxy_url())
        except Exception as exc:  # pragma: no cover - live network guard
            if attempt + 1 == RETRY_ATTEMPTS:
                print(f"  [playercareerstats] {pid} FAILED after {RETRY_ATTEMPTS} attempts: {exc!r}", flush=True)
            time.sleep(RETRY_BACKOFF_S)
            continue
        seasons = career.get("SeasonTotalsRegularSeason") if isinstance(career, dict) else None
        if seasons is None or seasons.is_empty():
            return None
        return seasons.with_columns(_pid_utf8())
    return None


def capture_season_stats(player_ids: list[str], *, resume_path: "str | None" = None) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    done_ids: set[str] = set()
    if resume_path:
        try:
            prior = pl.read_parquet(resume_path)
            frames.append(prior)
            done_ids = set(prior["player_id"].unique().to_list())
            print(f"  resumed {len(done_ids)} players already captured from {resume_path}", flush=True)
        except FileNotFoundError:
            pass

    todo = [p for p in player_ids if p not in done_ids]
    completed = len(done_ids)
    total = len(player_ids)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_one, pid): pid for pid in todo}
        for fut in as_completed(futures):
            pid = futures[fut]
            try:
                seasons = fut.result()
            except Exception as exc:  # pragma: no cover - defensive
                print(f"  [playercareerstats] {pid} unexpected error {exc!r}", flush=True)
                seasons = None
            if seasons is not None:
                frames.append(seasons)
            completed += 1
            if completed % 25 == 0:
                elapsed = time.time() - t0
                print(f"  [playercareerstats] {completed}/{total} ({elapsed:.0f}s elapsed)", flush=True)
                sys.stdout.flush()
            if completed % CHECKPOINT_EVERY == 0 and resume_path:
                pl.concat(frames, how="diagonal_relaxed").write_parquet(resume_path)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def main() -> None:
    print("Capturing full WNBA draft history (single bulk call, via proxy) ...")
    history_path = Path(f"{FIXTURE_DIR}/draft_history.parquet")
    try:
        history = capture_draft_history()
        history.write_parquet(history_path)
        print(f"  wrote draft_history.parquet ({history.height} rows, {history['draft_year'].n_unique()} classes)")
    except RuntimeError as exc:
        if not history_path.exists():
            raise
        print(f"  [drafthistory] re-capture failed ({exc!r}); reusing already-committed fixture")
        history = pl.read_parquet(history_path)

    player_ids = history["player_id"].unique().to_list()
    print(f"Capturing season totals (playercareerstats) for {len(player_ids)} drafted players ...")
    out_path = f"{FIXTURE_DIR}/season_stats_raw.parquet"
    season_stats = capture_season_stats(player_ids, resume_path=out_path)
    season_stats.write_parquet(out_path)
    print(
        f"  wrote season_stats_raw.parquet ({season_stats.height} rows, {season_stats['player_id'].n_unique()} players)"
    )

    print("Done. career_values.parquet / rookie_values.parquet are materialized by")
    print("dev/wnba_draft/fit_box_value.py from season_stats_raw.parquet + the existing")
    print("(NBA-borrowed) box_value_coef -- see that script's docstring.")


if __name__ == "__main__":
    main()
