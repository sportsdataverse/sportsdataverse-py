"""Season team-roster capture: crosswalk team ids -> roster pages -> JSON.

Fetches ``teams/{team_id}/roster`` for every team in the season's crosswalk
through the same canary-vendor transport as capture/discovery, parses it with
sdv-py's ``parse_ncaa_bb_team_roster`` (which extracts the stats.ncaa.org
``player_id`` from each row's ``/players/{id}`` link), and writes, per team:

- ``{root}/{league}/rosters/html/{season}/{team_id}.html`` -- the raw page,
  persisted from the fetch this stage was already making (zero extra HTTP).
- ``{root}/{league}/rosters/json/{season}/{team_id}.json`` -- the tidy frame,
  carrying ``player_id`` AND ``clean_name`` (properly-cased display name) AND
  ``player`` (the ALL-CAPS ``FIRST.LAST`` play-by-play join key), plus
  ``team_id`` + ``team``.
- ``{root}/{league}/team_rosters/{season}/{team_id}.json`` -- the original
  payload shape, still written for existing consumers.

The compiled ``rosters/parquet/{season}.parquet`` is NOT written here (a
``--shard`` worker sees only its slice, and one shared output file would race)
-- ``scripts/run_datasets.sh`` compiles it in one non-sharded pass.

Disk-is-checkpoint: an existing team file is skipped, so Ctrl-C + re-run
resumes. Tolerant sweep (same rationale as discovery): per-team retries, skip
after retries, abort only on a consecutive-failure run (real-ban signature).
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, List, Optional

import polars as pl

# The roster parser is league-agnostic (sdv-py's mbb schedule module reuses this
# exact function); only the (team, season) -> id crosswalk is league-specific,
# and ncaa_datasets owns that two-entry mapping plus the tree writers.
from .datasets import TEAM_ID_CROSSWALKS as _TEAM_ID_CROSSWALKS
from .datasets import persist_roster, read_html
from sportsdataverse.mbb.mbb_ncaa_schedule import parse_ncaa_bb_team_roster

logger = logging.getLogger(__name__)

_TEAM_TRIES = 3
_MAX_CONSECUTIVE_TEAM_FAILURES = 5

__all__ = ["capture_rosters"]


def _default_fetch_fn(shard_i: int = 0, shard_n: int = 1) -> Callable[[str], str]:
    vendor = os.environ.get("NCAA_VENDOR")
    if vendor:
        from .capture import _vendor_fetcher

        repo_root = Path(__file__).resolve().parents[1]
        fetcher = _vendor_fetcher(vendor, repo_root, shard_i=shard_i, shard_n=shard_n)
        return fetcher.fetch_html
    from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

    return NcaaFetcher.with_browser().fetch_html


def capture_rosters(
    season: int,
    *,
    league: str,
    root: Optional[Path] = None,
    limit_teams: Optional[int] = None,
    fetch_fn: Optional[Callable[[str], str]] = None,
    team_ids: Optional[List[int]] = None,
    shard: "tuple[int, int]" = (0, 1),
) -> "tuple[int, int, int]":
    """Capture every team roster for *season*. Returns (written, skipped_existing, failed)."""
    root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    out_dir = root / league / "team_rosters" / str(season)

    if team_ids is None:
        season_str = f"{season - 1}-{str(season)[-2:]}"
        crosswalk = _TEAM_ID_CROSSWALKS[league]()
        rows = crosswalk.filter(pl.col("season") == season_str)
        if rows.height == 0:
            raise ValueError(f"no crosswalk teams for season={season} ({season_str})")
        pairs = list(zip(rows.get_column("id").to_list(), rows.get_column("team").to_list()))
    else:
        pairs = [(t, None) for t in team_ids]
    if limit_teams is not None:
        pairs = pairs[:limit_teams]
    i, n = shard
    pairs = pairs[i::n]  # disjoint slice per worker

    fn = fetch_fn if fetch_fn is not None else _default_fetch_fn(shard_i=i, shard_n=n)
    out_dir.mkdir(parents=True, exist_ok=True)

    written = skipped_existing = 0
    consecutive = 0
    failed: List[int] = []
    for team_id, team_name in pairs:
        dest = out_dir / f"{team_id}.json"
        if dest.exists():
            # Captured already. Seasons captured BEFORE the rosters tree existed
            # have no html to re-parse; ncaa_datasets.rebuild_missing seeds their
            # json + parquet from this payload instead -- still no re-fetch.
            skipped_existing += 1
            continue
        # A committed roster page is re-parsed offline rather than re-fetched:
        # that is how an interrupted run resumes without spending a request, and
        # how a parser fix reaches every captured season for free.
        html: Optional[str] = read_html(root, league, "rosters", season, team_id)
        if html is None:
            for _ in range(_TEAM_TRIES):
                try:
                    html = fn(f"teams/{team_id}/roster")
                    break
                except RuntimeError:
                    continue
        if html is None:
            consecutive += 1
            failed.append(team_id)
            logger.warning(
                "team_id=%s roster fetch failed after %d tries (%d consecutive)",
                team_id,
                _TEAM_TRIES,
                consecutive,
            )
            if consecutive >= _MAX_CONSECUTIVE_TEAM_FAILURES:
                raise RuntimeError(
                    f"roster capture aborted: {consecutive} consecutive team failures "
                    f"(ban-suspect); {written} rosters written before abort"
                )
            continue
        consecutive = 0
        # rosters/{html,json,parquet}/ -- the tidy tree, ids + readable names.
        persist_roster(html, team_id, season, league=league, root=root)
        df = parse_ncaa_bb_team_roster(html, int(team_id))
        payload = {
            "team_id": int(team_id),
            "team": team_name,
            "season": season,
            "league": league,
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "players": df.to_dicts(),
        }
        dest.write_text(json.dumps(payload), encoding="utf-8")
        written += 1
        logger.info(
            "team_id=%s (%s): %d players%s",
            team_id,
            team_name or "?",
            df.height,
            "" if df.height else " (EMPTY roster table)",
        )
    if failed:
        logger.warning("finished with %d/%d teams failed: %s", len(failed), len(pairs), failed[:10])
    return written, skipped_existing, len(failed)


def _main(default_league: str) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Capture a season's team rosters.")
    parser.add_argument("--season", type=int, required=True, help="Ending year, e.g. 2025.")
    parser.add_argument("--league", default=default_league, help="League slug: 'mbb' or 'wbb'.")
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Root of the raw data tree (default: repo root). Point a live smoke at a scratch dir to keep it out of the committed tree.",
    )
    parser.add_argument("--limit-teams", type=int, default=None)
    parser.add_argument("--shard", default="0/1", help="This process's shard as 'i/N'.")
    args = parser.parse_args()
    i, n = (int(x) for x in args.shard.split("/"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    w, s, f = capture_rosters(
        args.season, league=args.league, root=Path(args.root), limit_teams=args.limit_teams, shard=(i, n)
    )
    print(f"rosters season={args.season} shard={args.shard}: written={w} skipped_existing={s} failed={f}")
