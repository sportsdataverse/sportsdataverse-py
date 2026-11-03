"""Build ``sportsdataverse/scrape/ncaa/data/ncaa_season_divisions.csv`` from stats.ncaa.org.

The scoreboard page names every id this table needs, so nothing is typed by hand:

* the ``sport_code_select`` options -> every sport the site serves (20 in 2026-27);
* a sport's ``game_sport_year_ctl_id_select`` options -> each of its seasons (back to 1996-97);
* one season's ``season_division_id_select`` options -> its D-I / D-II / D-III ``season_division_id``.

So one page per sport plus one page per sport-season builds the table (about 620 fetches, a few
KB each over an in-page fetch). Each sport is checkpointed to ``--cache`` as JSON, so a re-run
resumes, and ``--refresh SPORT`` re-fetches one sport (for example at the start of a season).

Run from a residential vendor; stats.ncaa.org edge-blocks datacenter IPs::

    NCAA_VENDOR=decodo_patchright uv run python tools/build_ncaa_season_divisions.py \\
        --vendor-root ../ncaa-mbb-hoops-raw --cache build/ncaa_season_divisions

The vendor root is the directory holding ``canary_vendors.toml``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Callable

import polars as pl

from sportsdataverse.scrape.ncaa.seasons import (
    _CSV_SCHEMA,
    _LABEL_RE,
    _SCOREBOARD,
    _parse_scoreboard_page,
    _select_options,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "sportsdataverse" / "scrape" / "ncaa" / "data" / "ncaa_season_divisions.csv"

logger = logging.getLogger("build_ncaa_season_divisions")


def sport_rows(fetch: Callable[[str], str], sport_code: str) -> "list[dict]":
    """Every (season, division) row for one sport: one page for the season list, one per season."""
    first = fetch(f"{_SCOREBOARD}&sport_code={sport_code}&academic_year=&division=1&commit=Submit")
    seasons = _select_options(first, "game_sport_year_ctl_id_select")
    if not seasons:
        raise RuntimeError(f"{sport_code}: no season list on the scoreboard page (layout drift?)")
    rows: "list[dict]" = []
    for ctl_id, label, _ in seasons:
        page = fetch(f"{_SCOREBOARD}&game_sport_year_ctl_id={ctl_id}&division=1&commit=Submit")
        got = _parse_scoreboard_page(page, sport_code)
        m = _LABEL_RE.match(label)
        # Football has no plain "division 1" (FBS / FCS), and the season-id form answers it with an
        # empty page; the sport_code + academic_year form resolves it. Try each division number.
        for division in (1, 2, 3) if not got and m else ():
            ay = int(m.group(1)) + 1
            page = fetch(f"{_SCOREBOARD}&sport_code={sport_code}&academic_year={ay}&division={division}&commit=Submit")
            got = [r for r in _parse_scoreboard_page(page, sport_code) if r["academic_year"] == ay]
            if got:
                break
        if got and got[0]["game_sport_year_ctl_id"] != int(ctl_id):
            # the site fell back to another season: this one has no scoreboard. Record nothing.
            logger.warning("%s %s: site answered another season; skipped", sport_code, label)
            continue
        if not got:
            logger.warning("%s %s: no divisions listed", sport_code, label)
        logger.debug("%s %s: %d divisions", sport_code, label, len(got))
        rows.extend(got)
    if not rows:
        # an empty sport is a page-shape problem, never a fact: fail so the checkpoint is not written
        raise RuntimeError(f"{sport_code}: {len(seasons)} seasons listed but no division ids parsed")
    return rows


def _vendor_fetch(vendor: str, vendor_root: Path) -> Callable[[str], str]:
    from sportsdataverse.scrape.ncaa.capture import _vendor_fetcher

    fetcher = _vendor_fetcher(vendor, vendor_root)
    return lambda path: fetcher.fetch_html(path, force=True)


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--vendor-root", type=Path, required=True, help="directory holding canary_vendors.toml")
    ap.add_argument("--cache", type=Path, default=ROOT / "build" / "ncaa_season_divisions")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--sports", nargs="*", help="limit to these sport codes (default: every sport the site lists)")
    ap.add_argument("--refresh", nargs="*", default=[], help="re-fetch these sport codes even if checkpointed")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    vendor = os.environ.get("NCAA_VENDOR")
    if not vendor:
        ap.error("set NCAA_VENDOR (e.g. decodo_patchright): stats.ncaa.org blocks datacenter IPs")
    fetch = _vendor_fetch(vendor, args.vendor_root)
    args.cache.mkdir(parents=True, exist_ok=True)

    sports = args.sports or [
        v
        for v, _, _ in _select_options(
            fetch(f"{_SCOREBOARD}&sport_code=MBB&division=1&commit=Submit"), "sport_code_select"
        )
    ]
    logger.info("sports: %s", " ".join(sports))
    failed: "list[str]" = []
    for code in sports:
        ckpt = args.cache / f"{code}.json"
        if ckpt.exists() and code not in args.refresh:
            continue
        try:
            rows = sport_rows(fetch, code)
        except Exception as exc:  # noqa: BLE001 -- one sport failing must not lose the others; re-run resumes it
            logger.error("%s failed: %s", code, str(exc).splitlines()[0][:200])
            failed.append(code)
            continue
        ckpt.write_text(json.dumps(rows))
        logger.info("%s: %d rows", code, len(rows))

    frames = [pl.DataFrame(json.loads(p.read_text()), schema=_CSV_SCHEMA) for p in sorted(args.cache.glob("*.json"))]
    table = pl.concat(frames).sort("sport_code", "academic_year", "season_division_id", descending=[False, True, False])
    args.out.parent.mkdir(parents=True, exist_ok=True)
    table.write_csv(args.out)
    logger.info("wrote %s: %d rows, %d sports", args.out, table.height, table["sport_code"].n_unique())
    if failed:
        logger.error("failed sports (re-run to resume): %s", " ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
