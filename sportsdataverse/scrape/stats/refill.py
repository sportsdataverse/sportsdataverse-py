"""Refill season-level payloads that were persisted as empty ``{}``.

Why this exists
---------------
``season_capture.write_payload`` originally had no guard, and resume is
``path.exists()`` -- presence, not content. So a failed fetch that returned
``{}`` was written to disk and every later sweep counted it "present", never
refetched it, and reported the season complete. A backfill no-ops.
hoopR-nba-stats-raw reached 3,347 such files and wehoop-wnba-stats-raw 3,872
before the guard landed.

The guard now refuses to persist a contentless payload, but it cannot undo the
files already on disk: they still exist, so they are still skipped. This module
deletes them and refetches exactly those ``(endpoint, season, variant)`` tuples.

Safety
------
* Only files whose size is <= 2 bytes are touched. That is exactly ``{}`` /
  ``[]``. It is NOT "small": a valid 201-byte envelope whose entity list is
  legitimately empty must survive.
* The deleted files are tracked in git in the owning repo, so
  ``git checkout -- <store>/`` restores them if a run goes wrong.
* Nothing is deleted until its replacement is about to be fetched, and a fetch
  that comes back empty leaves NO file -- so a partial run is resumable and
  never worse than where it started.

Usage (from an owning ``-raw`` repo's ``python/refill_empty.py`` shim)
----------------------------------------------------------------------
    python python/refill_empty.py --check          # census only, no network
    python python/refill_empty.py                  # refill everything
    python python/refill_empty.py 2015:2026        # season range
    python python/refill_empty.py --endpoint matchupsrollup
"""

import argparse
import datetime
import importlib
import os
from collections import defaultdict
from pathlib import Path
from typing import Optional

from .league_config import LeagueConfig
from .proxy import ProxyHealth, RoundRobin, load_proxies
from .season_capture import payload_path, plan_season, write_payload
from .session_transport import SessionTransport

#: A payload this small cannot hold an envelope; anything larger is real.
CONTENTLESS_MAX_BYTES = 2


def _log(msg: str) -> None:
    print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def store_root(cfg: LeagueConfig, default_root: Path) -> Path:
    """Same resolution the sweep uses, so both agree on where payloads live."""
    return Path(os.environ.get(cfg.store_env) or default_root)


def find_empty(root: Path) -> "dict[int, set[tuple[str, Optional[str]]]]":
    """``season -> {(endpoint, variant)}`` for every contentless payload on disk."""
    out: dict[int, set[tuple[str, Optional[str]]]] = defaultdict(set)
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if not fn.endswith(".json"):
                continue
            fp = Path(dirpath) / fn
            try:
                if fp.stat().st_size > CONTENTLESS_MAX_BYTES:
                    continue
            except OSError:
                continue
            rel = fp.relative_to(root).parts
            # <endpoint>/<season>/<variant>.json  or  <endpoint>/<season>.json
            if len(rel) == 3:
                endpoint, season, variant = rel[0], rel[1], rel[2][: -len(".json")]
            elif len(rel) == 2:
                endpoint, season, variant = rel[0], rel[1][: -len(".json")], None
            else:
                continue
            if season.isdigit():
                out[int(season)].add((endpoint, variant))
    return out


def main(cfg: LeagueConfig, argv: "Optional[list[str]]" = None, *, default_root: Path) -> int:
    """Run the refill for one league. Returns a process exit code.

    ``default_root`` is the owning repo's on-disk store location (the shim
    passes it, since only the repo knows where it lives); ``cfg.store_env``
    overrides it, exactly as the sweep resolves the root.
    """
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("seasons", nargs="?", default=None, help="START:END (default: all found)")
    ap.add_argument("--check", action="store_true", help="census only; no deletes, no network")
    ap.add_argument("--endpoint", default=None, help="restrict to one endpoint")
    ap.add_argument(
        "--allow-direct",
        action="store_true",
        help="run without proxies (serial, residential IP only -- hangs on datacenter IPs)",
    )
    args = ap.parse_args(argv)

    root = store_root(cfg, default_root)
    _log(f"scanning {root} for contentless payloads")
    empty = find_empty(root)
    if args.seasons:
        lo, _, hi = args.seasons.partition(":")
        lo_i, hi_i = int(lo), int(hi or lo)
        empty = {s: v for s, v in empty.items() if lo_i <= s <= hi_i}
    if args.endpoint:
        empty = {s: {(e, v) for e, v in pairs if e == args.endpoint} for s, pairs in empty.items()}
        empty = {s: p for s, p in empty.items() if p}

    per_endpoint: dict[str, int] = defaultdict(int)
    for pairs in empty.values():
        for endpoint, _v in pairs:
            per_endpoint[endpoint] += 1
    total = sum(per_endpoint.values())

    _log(f"{total} contentless payloads across {len(empty)} seasons")
    for endpoint, n in sorted(per_endpoint.items(), key=lambda kv: -kv[1]):
        _log(f"  {n:>6}  {endpoint}")
    if args.check or not total:
        return 0

    stats = importlib.import_module(cfg.stats_module)

    health = ProxyHealth(error_log=os.environ.get("STATS_ERROR_LOG", "logs/errors.jsonl"))
    pool = load_proxies()
    timeout = os.environ.get("SDV_PY_NBA_STATS_TIMEOUT", "30")
    transport: Optional[SessionTransport]
    if pool:
        _log(f"proxy pool: {len(pool)} entries | timeout={timeout}s")
        transport = SessionTransport(RoundRobin(pool, health=health), health)
    elif args.allow_direct:
        # Serial + residential only. The live probe that proved these payloads
        # are recoverable ran this way, so it is supported -- but never silently.
        _log(f"no proxy pool; --allow-direct given, going direct | timeout={timeout}s")
        transport = None
    else:
        # Same contract as the main sweep: un-proxied stats-host calls HANG
        # rather than fail, so a missing pool must stop the run, not degrade it.
        _log(
            "ERROR: no proxies. PROXY_ENDPOINT / PROXY_KEY / PROXY_PKG live in"
            " ~/.Renviron, which Python does not read -- export them (the wrapper"
            " scripts/refill_empty_payloads.sh does) or pass --allow-direct."
        )
        return 1

    def fetch(endpoint: str, kwargs: dict) -> object:
        fn = getattr(stats, f"{cfg.stats_prefix}_{endpoint}")
        if transport is None:
            return fn(return_parsed=False, **kwargs)
        return fn(return_parsed=False, transport=transport, **kwargs)

    refilled = still_empty = failed = 0
    for season in sorted(empty):
        wanted = empty[season]
        # Reuse the sweep's own plan so kwargs are built exactly as a normal
        # capture builds them -- no second, drifting copy of the matrix.
        for endpoint, variant, kwargs in plan_season(season, stats, cfg.stats_prefix, cfg.league_id):
            if (endpoint, variant) not in wanted:
                continue
            path = payload_path(root, endpoint, season, variant)
            try:
                payload = fetch(endpoint, kwargs)
            except Exception as exc:  # noqa: BLE001 - one gap must not kill the refill
                _log(f"{season} {endpoint}[{variant}]: FAILED {exc}")
                failed += 1
                continue
            # Delete only now, with a replacement in hand.
            path.unlink(missing_ok=True)
            if write_payload(path, payload):
                refilled += 1
                _log(f"{season} {endpoint}[{variant}]: refilled")
            else:
                still_empty += 1
                _log(f"{season} {endpoint}[{variant}]: STILL EMPTY (left absent for retry)")

    _log(f"refill complete: {refilled} refilled | {still_empty} still empty | {failed} failed")
    for ep, errs, ec in health.endpoint_summary():
        _log(f"  {ep}: {errs} faults {ec}")
    health.close()
    return 0
