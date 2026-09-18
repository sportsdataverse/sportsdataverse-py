"""Get one NCAA (``stats.ncaa.org``) contest bundle and resolve its ids (private, experimental).

A "bundle" here is the same three HTML pages the ``ncaa-mfb-football-raw`` producer stores per
contest -- ``play_by_play``, ``box_score``, ``drives`` -- because those three are exactly what
the graduated parsers (:mod:`sportsdataverse.cfb.cfb_ncaa_pbp`,
:mod:`sportsdataverse.cfb.cfb_ncaa_box`) and :func:`sportsdataverse.cfb.to_cfbfastr` read. Two
ways in, both producing that same dict:

* **offline** -- the archive payload by contest id (``mfb/raw/{academic year}/{id}.json.gz`` in a
  ``ncaa-mfb-football-raw`` checkout, pointed at by ``SDV_NCAA_MFB_ARCHIVE``). The archive covers
  **fall 2013 onwards, FBS + FCS**, and is the only source that carries an FCS-hosted game in any
  era. The academic year is ``season + 1`` (season 2024 lives under ``mfb/raw/2025/``).
* **live** -- the contest page itself, through the repo's existing NCAA transport
  (:class:`sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher`, browser transport for the
  Akamai ``bm-verify`` wall, residential proxy). stats.ncaa.org is an unfriendly host and bans
  per IP **permanently**, so every request is paced to at most one per
  :data:`MIN_REQUEST_INTERVAL` seconds, process-wide. **No test may reach it**: the pacing gate
  and the transport are only touched on the fetch path, and every test injects a payload.

Practical note the coverage study measured: the live path needs a browser plus a *residential*
IP (datacenter egress gets an instant edge 403) and runs about 16 s per page, and stats.ncaa.org
only lists a contest once it has been played. It is a **post-game** source. The archive path is
the one that matters.
"""

from __future__ import annotations

import gzip
import io
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import polars as pl
from bs4 import BeautifulSoup

#: Environment variable naming a ``ncaa-mfb-football-raw`` checkout (the offline archive root).
ARCHIVE_DIR_ENV = "SDV_NCAA_MFB_ARCHIVE"

#: Minimum seconds between two stats.ncaa.org requests, process-wide. The host bans per IP and
#: those bans are permanent, so this is a floor, never a target.
MIN_REQUEST_INTERVAL = 3.0

#: The bundle keys the projection reads. ``drives`` refines ``period`` when the pbp page ships no
#: quarter markers; the other two are mandatory.
BUNDLE_PAGES = ("play_by_play", "box_score", "drives")

_RATE_LOCK = threading.Lock()
_LAST_REQUEST = 0.0


def _pace() -> None:
    """Block until :data:`MIN_REQUEST_INTERVAL` has passed since the last request."""
    global _LAST_REQUEST
    with _RATE_LOCK:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _LAST_REQUEST)
        if wait > 0:
            time.sleep(wait)
        _LAST_REQUEST = time.monotonic()


def _archive_root(root: "str | Path | None" = None) -> Optional[Path]:
    """The ``ncaa-mfb-football-raw`` checkout to read, or None when none is configured."""
    root = root or os.environ.get(ARCHIVE_DIR_ENV)
    return Path(root) if root else None


def _archive_bundle(
    contest_id: Any, *, season: Optional[int] = None, root: "str | Path | None" = None
) -> Optional[Dict[str, Any]]:
    """The archive bundle for one contest id, or None when the archive does not hold it.

    Args:
        contest_id: stats.ncaa.org contest id.
        season: the **starting** year (2024 = fall 2024). The archive speaks academic years, so
            the payload lives under ``season + 1``; without a season every captured academic year
            is searched.
        root: a ``ncaa-mfb-football-raw`` checkout; defaults to ``$SDV_NCAA_MFB_ARCHIVE``.

    Returns:
        The stored bundle (``play_by_play`` / ``box_score`` / ``drives`` HTML plus
        ``contest_id`` and ``captured_at``), or None. Never raises: an unreadable or truncated
        payload is a miss, so dispatch falls through rather than 500s.
    """
    base = _archive_root(root)
    if base is None:
        return None
    raw = base / "mfb" / "raw"
    if season is not None:
        candidates = [raw / str(int(season) + 1) / f"{contest_id}.json.gz"]
    else:
        candidates = sorted(raw.glob(f"*/{contest_id}.json.gz"))
    for path in candidates:
        if not path.is_file():
            continue
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                bundle = json.load(fh)
        except Exception:  # noqa: BLE001 -- a corrupt payload is a miss, never a raise
            continue
        if isinstance(bundle, dict):
            return bundle
    return None


def _fetch_bundle(contest_id: Any, *, fetcher: Any = None) -> Dict[str, Any]:
    """Fetch one contest's three pages live from stats.ncaa.org, paced and cache-first.

    ``fetcher`` is any object with ``fetch_html(path)`` (the dependency-injection seam
    :class:`~sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher` already exposes); without one a
    browser-transport ``NcaaFetcher`` is built, which is what clears the Akamai ``bm-verify``
    wall. ``NcaaFetcher`` is itself cache-first, so a re-run of the same contest makes zero
    requests -- but a fresh page is still three requests, hence :func:`_pace`.
    """
    if fetcher is None:
        from sportsdataverse.mbb.mbb_ncaa_fetch import NcaaFetcher

        with NcaaFetcher.with_browser() as owned:
            return _fetch_bundle(contest_id, fetcher=owned)
    bundle: Dict[str, Any] = {"contest_id": str(contest_id)}
    for page in BUNDLE_PAGES:
        _pace()
        bundle[page] = fetcher.fetch_html(f"contests/{contest_id}/{page}")
    return bundle


def _resolve_contest_id(idmap_row: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], str]:
    """``(contest id, provenance)`` from the id map; ``(None, "unresolved")`` means hand over.

    The id map's ``ncaa_game_id`` is the **stats.ncaa.org contest id**, not an ncaa.com GraphQL
    contest id -- the two namespaces are disjoint and of similar magnitude, so reading the wrong
    one resolves to a real, different game. It is filled from ``ncaa-mfb-football-raw``'s own
    schedule, and there is no formula to fall back on: a contest id is a fact or it is nothing.
    """
    stored = (idmap_row or {}).get("ncaa_game_id")
    return (str(stored), "idmap") if stored else (None, "unresolved")


_TEAM_HREF = "/teams/"
_CROSSWALK_PATH = Path(__file__).with_name("data") / "ncaa_espn_team_ids_mfb.csv"
_CROSSWALK: Optional[Dict[str, str]] = None


def _crosswalk() -> Dict[str, str]:
    """The vendored ``ncaa_team_id -> espn_team_id`` map (3,191 season-scoped team ids).

    Vendored from ``ncaa-mfb-football-raw/mfb/xwalk/espn_team_id.json``. It is the *only* way
    this adapter learns an ESPN team id from a payload, and it is a lookup, never a guess: a
    team it does not carry yields no id and the adapter falls back or hands over.
    """
    global _CROSSWALK
    if _CROSSWALK is None:
        frame = pl.read_csv(
            io.BytesIO(_CROSSWALK_PATH.read_bytes()),
            schema_overrides={"ncaa_team_id": pl.Utf8, "espn_team_id": pl.Utf8},
        )
        _CROSSWALK = dict(zip(frame.get_column("ncaa_team_id"), frame.get_column("espn_team_id")))
    return _CROSSWALK


def _ncaa_team_ids(html: str) -> Dict[str, str]:
    """``{team label: ncaa_team_id}`` from a contest page's ``/teams/{id}`` links.

    Every tab of a contest links both clubs; the labelled anchor carries the full team name
    ("FIU Panthers"), which is what matches the linescore's shorter name ("FIU") by prefix.
    """
    out: Dict[str, str] = {}
    for anchor in BeautifulSoup(html or "", "html.parser").find_all("a"):
        href = anchor.get("href") or ""
        label = anchor.get_text(" ", strip=True)
        if href.startswith(_TEAM_HREF) and label:
            out.setdefault(label, href[len(_TEAM_HREF) :].split("/")[0])
    return out


def _espn_team_ids_from_bundle(html: str, linescore_teams: Mapping[str, str]) -> Dict[str, str]:
    """``{"home"/"away": espn_team_id}`` for the two clubs, from the page + the vendored map.

    Args:
        html: any contest tab (both clubs are linked on all of them).
        linescore_teams: ``{team name: "home"/"away"}`` from the parsed linescore -- the only
            surface that states which side is home.

    Returns:
        A dict with whichever sides resolved; a club the crosswalk does not carry is simply
        absent, so the caller can fall back rather than emit an invented id.
    """
    crosswalk = _crosswalk()
    out: Dict[str, str] = {}
    for label, ncaa_id in _ncaa_team_ids(html).items():
        espn_id = crosswalk.get(str(ncaa_id))
        if not espn_id:
            continue
        for name, side in linescore_teams.items():
            if name and label.startswith(name):
                out.setdefault(side, str(espn_id))
    return out


def _espn_team_ids_from_schedule(espn_id: Any, game_date: Optional[str]) -> Dict[str, str]:
    """``{"home"/"away": espn_team_id}`` read off ESPN's own schedule for one date, or ``{}``.

    The last leg, and the only one that fills a club the vendored crosswalk is missing (303 of
    1,685 payloads in the newest captured season have a null ``espn_team_id``). ``game_date`` is
    the linescore's own ``MM/DD/YYYY ...`` stamp; one **day** is asked for rather than a season,
    because ``espn_cfb_schedule`` caps at 500 events and a season is ~900. It reaches the
    network, so it runs only on the fetch path; never raises -- an unreachable ESPN is a miss,
    which is the whole point of a failover source.
    """
    parts = str(game_date or "").split()[0].split("/")
    if len(parts) != 3:
        return {}
    month, day, year = parts
    try:
        from sportsdataverse.cfb.cfb_schedule import espn_cfb_schedule

        frame = espn_cfb_schedule(dates=int(f"{int(year):04d}{int(month):02d}{int(day):02d}"))
    except Exception:  # noqa: BLE001 -- ESPN is the source we are failing over FROM
        return {}
    if not isinstance(frame, pl.DataFrame) or frame.is_empty():
        return {}
    wanted = {"game_id", "home_id", "away_id"}
    if not wanted <= set(frame.columns):
        return {}
    hit = frame.filter(pl.col("game_id").cast(pl.Utf8) == str(espn_id))
    if hit.is_empty():
        return {}
    row = hit.row(0, named=True)
    return {"home": str(row["home_id"]), "away": str(row["away_id"])}


def _has_plays(bundle: Mapping[str, Any]) -> bool:
    """True when the bundle carries a play-by-play page with drive markup.

    stats.ncaa.org answers **HTTP 200** with a real contest page for a game it holds no
    play-by-play for -- two of 1,687 contests in the 2024 capture, plus every FBS game outside
    the FBS/FCS divisions it sweeps. Detecting that by shape is the only way to tell "NCAA does
    not carry this game" from "the fetch failed"; the first is permanent, the second is not.
    """
    html = bundle.get("play_by_play")
    return bool(html) and 'class="drives' in str(html).replace("'", '"')
