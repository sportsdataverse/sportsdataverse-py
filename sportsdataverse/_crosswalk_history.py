"""One-time history harvest of Fox, Yahoo and CBS raw payloads for the crosswalks (private).

Captures **raw** payloads only; matching to ESPN comes later, from the stored files. Per
provider and league it lists a season's games first, then fetches one per-game box payload
per completed game, through the existing generated wrappers (``fox_api_*``,
``yahoo_*``, ``cbs_*``). Player history comes from per-game box scores only: every
provider's roster endpoint ignores the season and returns today's roster.

Layout under ``root`` (one ``*-alt-raw`` repo checkout)::

    {provider}/{league}/{endpoint}/{season}/{key}.json      listings (plain JSON)
    {provider}/{league}/{endpoint}/{season}/{id}.json.gz    per-game payloads (gzip)
    {provider}/{league}/{endpoint}/{season}/_missing.tsv    keys that answered nothing

* **Resumable.** A key with a file, or listed in ``_missing.tsv``, is never fetched again.
  Listings of a season that is still in progress are always re-fetched, so a re-run picks
  up newly completed games.
* **Atomic, never empty.** Files are written to a temp name and renamed. An empty body,
  a provider's 404 envelope or a payload that fails its season check is recorded in
  ``_missing.tsv`` (with the reason), never written as a file.
* **Season labels are checked in every response**, because Fox and Yahoo answer an
  unknown season with the current one: a listed game whose date is outside the season
  window is dropped, and a box payload whose own date (Fox) or season (Yahoo) disagrees
  is not stored. Ids are never used to infer a season (Fox and CBS ids encode when the
  record was created).
* **Pace is env-only:** ``XWALK_{PROVIDER}_RPS``, else ``XWALK_RPS``, else 1/s (Fox,
  Yahoo) or 1 per 2 s (CBS, behind a WAF). A 403 or 429 stops the run (exit 3); it is
  never retried into a ban.

Seasons follow SDV: basketball by ending year (NBA 2025 = 2024-25), WNBA and football by
calendar year. Yahoo keys NBA and college basketball by the starting year.

Usage::

    python -m sportsdataverse._crosswalk_history --provider fox --league nba \
        --season 2025 --root /mnt/sdv_repos/hoopR-nba-alt-raw
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path
from statistics import median
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sportsdataverse.errors import AssetFetchError, NoDataError

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")

#: Earliest season worth harvesting, per provider and league (probed 2026-10-11). Fox NBA
#: goes back to 1947, but the ESPN hub it is matched to starts in 1994.
FLOORS: Dict[str, Dict[str, int]] = {
    "fox": {"nfl": 2018, "cfb": 2017, "mbb": 2018, "wbb": 2019, "nba": 1994, "wnba": 2019},
    "yahoo": {"nfl": 2013, "cfb": 2013, "nba": 2014, "mbb": 2014, "wbb": 2015, "wnba": 2015},
    "cbs": {"nfl": 2016, "cfb": 2015},
}
#: CBS serves box scores from these seasons (CFB 2015 is listings only) and plays from 2018.
_CBS_BOX_FLOOR = {"nfl": 2016, "cfb": 2016}
_CBS_PLAYS_FLOOR = 2018
#: Fox WBB box scores carry players only from 2023; earlier seasons are listings only.
_FOX_BOX_FLOOR = {"wbb": 2023}

_DEFAULT_RPS = {"fox": 1.0, "yahoo": 1.0, "cbs": 0.5}
#: Retry only server errors; a 403/429 must surface at once and stop the run.
_FETCH_KW: Dict[str, Any] = {"num_retries": 2, "retry_statuses": (500, 502, 503, 504), "timeout": 45}
_MAX_CONSECUTIVE_FAILURES = 10

_FOX_SPORT = {"nfl": "nfl", "cfb": "cfb", "nba": "nba", "wnba": "wnba", "mbb": "cbk", "wbb": "wcbk"}
#: Fox ``groupId``: FBS for CFB, Division I for college basketball.
_FOX_GROUP = {"cfb": 2, "mbb": 2, "wbb": 2}
_YAHOO_LEAGUE = {"nfl": "nfl", "cfb": "ncaaf", "nba": "nba", "wnba": "wnba", "mbb": "ncaab", "wbb": "ncaaw"}
_YAHOO_FLAG = {
    "nfl": "is_football",
    "cfb": "is_football",
    "nba": "is_pro_basketball",
    "wnba": "is_pro_basketball",
    "mbb": "is_college_basketball",
    "wbb": "is_college_basketball",
}
#: leagueGameIdsByDate stops at 1000 rows; a busy college week holds about 450 games.
_YAHOO_ROW_CAP = 1000
_YAHOO_CHUNK_DAYS = 7
#: Leagues whose leagueGameIdsByDate ignores startRange/endRange (every range answers the
#: season's first 1,000 games, probed 2026-10-26): list them one date at a time with ``dates=``.
_YAHOO_BY_DATE = frozenset({"cfb"})


class HarvestBlocked(RuntimeError):
    """The host refused (403 / 429) or kept failing; the run stops instead of retrying into a ban."""


# --------------------------------------------------------------------------- seasons


def season_window(league: str, season: int) -> Tuple[date, date]:
    """The ET dates a season's games fall in (listing sweep and season check).

    Args:
        league: ``nfl``, ``cfb``, ``nba``, ``wnba``, ``mbb`` or ``wbb``.
        season: SDV season (basketball: ending year; WNBA and football: calendar year).

    Returns:
        ``(first, last)`` dates, inclusive.
    """
    y = int(season)
    if league == "nba":
        special = {
            1999: (date(1999, 2, 1), date(1999, 6, 30)),
            2012: (date(2011, 12, 20), date(2012, 6, 30)),
            2020: (date(2019, 10, 15), date(2020, 10, 15)),
            2021: (date(2020, 12, 10), date(2021, 7, 25)),
        }
        return special.get(y, (date(y - 1, 10, 15), date(y, 6, 30)))
    if league in ("mbb", "wbb"):
        return date(y - 1, 11, 1), date(y, 4, 12)
    if league == "wnba":
        return date(y, 5, 1), date(y, 10, 31)
    if league == "nfl":
        return date(y, 9, 1), date(y + 1, 2, 20)
    if league == "cfb":
        return date(y, 8, 20), date(y + 1, 1, 25)
    raise ValueError(f"unknown league {league!r}")


def _et_date(value: Optional[str]) -> Optional[date]:
    """An ISO timestamp (``Z`` or offset) or ``YYYYMMDD`` -> its US-Eastern calendar date."""
    if not value:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.date()
    return parsed.astimezone(_ET).date()


def _in_window(day: Optional[date], window: Tuple[date, date], slack: int = 3) -> bool:
    return day is not None and window[0] - timedelta(days=slack) <= day <= window[1] + timedelta(days=slack)


# --------------------------------------------------------------------------- storage


class Store:
    """Raw payload files for one provider and league under an ``*-alt-raw`` checkout."""

    def __init__(self, root: Path, provider: str, league: str):
        self.base = Path(root) / provider / league
        self._missing: Dict[Tuple[str, int], Dict[str, str]] = {}

    def _dir(self, endpoint: str, season: int) -> Path:
        return self.base / endpoint / str(season)

    def path(self, endpoint: str, season: int, key: str, gz: bool) -> Path:
        return self._dir(endpoint, season) / f"{key}.json{'.gz' if gz else ''}"

    def missing(self, endpoint: str, season: int) -> Dict[str, str]:
        """``key -> reason`` for every key recorded as answering nothing."""
        cache_key = (endpoint, int(season))
        if cache_key not in self._missing:
            out: Dict[str, str] = {}
            f = self._dir(endpoint, season) / "_missing.tsv"
            if f.exists():
                for line in f.read_text().splitlines():
                    k, _, reason = line.partition("\t")
                    if k:
                        out[k] = reason
            self._missing[cache_key] = out
        return self._missing[cache_key]

    def has(self, endpoint: str, season: int, key: str) -> bool:
        return (
            self.path(endpoint, season, key, True).exists()
            or self.path(endpoint, season, key, False).exists()
            or key in self.missing(endpoint, season)
        )

    def load(self, endpoint: str, season: int, key: str) -> Optional[Any]:
        for gz in (False, True):
            p = self.path(endpoint, season, key, gz)
            if p.exists():
                raw = p.read_bytes()
                return json.loads(gzip.decompress(raw) if gz else raw)
        return None

    def put(self, endpoint: str, season: int, key: str, payload: Any, gz: bool) -> Path:
        """Write ``payload`` atomically; an empty payload raises instead of landing on disk."""
        if not payload:
            raise ValueError(f"refusing to persist an empty payload for {endpoint}/{season}/{key}")
        p = self.path(endpoint, season, key, gz)
        p.parent.mkdir(parents=True, exist_ok=True)
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        if gz:
            body = gzip.compress(body, compresslevel=6, mtime=0)
        tmp = p.with_name(f".{p.name}.tmp{os.getpid()}")
        tmp.write_bytes(body)
        os.replace(tmp, p)
        return p

    def miss(self, endpoint: str, season: int, key: str, reason: str) -> None:
        d = self._dir(endpoint, season)
        d.mkdir(parents=True, exist_ok=True)
        with open(d / "_missing.tsv", "a") as fh:
            fh.write(f"{key}\t{reason}\n")
        self.missing(endpoint, season)[key] = reason


# --------------------------------------------------------------------------- pacing


class Pacer:
    """One host's request pace, set only by env, with a running request tally."""

    def __init__(self, provider: str):
        rps = os.environ.get(f"XWALK_{provider.upper()}_RPS") or os.environ.get("XWALK_RPS")
        self.rps = float(rps) if rps else _DEFAULT_RPS[provider]
        if self.rps <= 0:
            raise ValueError("XWALK_RPS must be positive")
        self.gap = 1.0 / self.rps
        self.n = 0
        self.failures = 0
        self._consecutive = 0
        self._last = 0.0
        self._t0: Optional[float] = None

    def call(self, label: str, fn: Callable[[], Any]) -> Tuple[str, Any]:
        """Run one request at pace.

        Returns:
            ``("ok", payload)``, ``("none", None)`` for a 404, or ``("fail", None)`` for a
            fetch that failed (not recorded as missing, so a re-run retries it).

        Raises:
            HarvestBlocked: on a 403 / 429, or after too many consecutive failures.
        """
        wait = self._last + self.gap - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        if self._t0 is None:
            self._t0 = time.monotonic()
        self._last = time.monotonic()
        self.n += 1
        try:
            payload = fn()
        except NoDataError:
            self._consecutive = 0
            return "none", None
        except AssetFetchError as exc:
            status = re.search(r"HTTP (\d{3})", str(exc))
            if status and status.group(1) in ("403", "429"):
                raise HarvestBlocked(f"{label}: HTTP {status.group(1)}") from exc
            return self._fail(label, exc)
        except ValueError as exc:  # 400 / 422: a wrong request, never a retry
            return self._fail(label, exc)
        self._consecutive = 0
        return "ok", payload

    def _fail(self, label: str, exc: Exception) -> Tuple[str, Any]:
        self.failures += 1
        self._consecutive += 1
        logger.warning("%s failed: %s: %s", label, type(exc).__name__, str(exc)[:200])
        if self._consecutive >= _MAX_CONSECUTIVE_FAILURES:
            raise HarvestBlocked(f"{self._consecutive} consecutive failures, last {label}") from exc
        return "fail", None

    @property
    def seconds_per_request(self) -> Optional[float]:
        if not self.n or self._t0 is None:
            return None
        return round((time.monotonic() - self._t0) / self.n, 3)


# --------------------------------------------------------------------------- listings


def _today() -> date:
    return datetime.now(_ET).date()


def _sweep_dates(window: Tuple[date, date]) -> List[date]:
    last = min(window[1], _today() - timedelta(days=1))
    return [window[0] + timedelta(days=i) for i in range((last - window[0]).days + 1)]


def _fox_events(raw: Any, window: Tuple[date, date]) -> Tuple[List[Dict[str, Any]], int]:
    """Fox segment -> ``([{id, date, final}], dropped)``, keeping only in-window games."""
    from sportsdataverse._fox_layout import parse_segment_events

    games, dropped = [], 0
    for r in parse_segment_events(raw or {}):
        day = _et_date(r.get("date"))
        if not r.get("game_id"):
            continue
        if day is None or not _in_window(day, window, slack=0):
            dropped += 1
            continue
        games.append({"id": str(r["game_id"]), "date": day.isoformat(), "final": r.get("event_status") == 3})
    return games, dropped


def _fox_segments(league: str, season: int) -> List[str]:
    """Football segment ids (regular weeks, then postseason); basketball sweeps dates."""
    if league == "nfl":
        return [f"{season}-{w}-1" for w in range(1, 19)] + [f"{season}-{w}-2" for w in range(1, 5)]
    if league == "cfb":
        return [f"{season}-{w}-1" for w in range(1, 17)] + [f"{season}-bowls-2", f"{season}-cfp-2"]
    return [d.strftime("%Y%m%d") for d in _sweep_dates(season_window(league, season))]


def fox_listing(store: Store, pacer: Pacer, league: str, season: int, refresh: bool) -> List[Dict[str, Any]]:
    """Every Fox game of a season from ``league/scores-segment`` (weeks or ET dates)."""
    from sportsdataverse.providers.fox.fox_api import fox_api_league_scores_segment

    sport, group, window = _FOX_SPORT[league], _FOX_GROUP.get(league), season_window(league, season)
    games: Dict[str, Dict[str, Any]] = {}
    for seg in _fox_segments(league, season):
        raw = None if refresh else store.load("scores_segment", season, seg)
        pending = raw is not None and not all(g["final"] for g in _fox_events(raw, window)[0])
        if pending:
            raw = None  # stored while games were pending: list it again
        if raw is None and (refresh or pending or not store.has("scores_segment", season, seg)):
            status, raw = pacer.call(
                f"fox {sport} scores-segment {seg}",
                partial(fox_api_league_scores_segment, sport, seg, group_id=group, return_parsed=False, **_FETCH_KW),
            )
            if status == "fail":
                continue
            events, dropped = _fox_events(raw, window)
            if not events:
                reason = "season-mismatch" if dropped else "no-events"
                if not refresh:
                    store.miss("scores_segment", season, seg, reason)
                continue
            store.put("scores_segment", season, seg, raw, gz=False)
        for g in _fox_events(raw, window)[0]:
            games.setdefault(g["id"], g)
    return list(games.values())


def _yahoo_season(league: str, season: int) -> int:
    return season - 1 if league in ("nba", "mbb", "wbb") else season


def _yahoo_games(raw: Any) -> List[Dict[str, Any]]:
    return [g for lg in ((raw or {}).get("data") or {}).get("leagues") or [] for g in lg.get("games") or []]


def _yahoo_answers_date(raw: Any, day: date) -> bool:
    """A ``dates=`` listing is for ``day``: every dated game within a day of it, and under the row cap.

    Empty is left to the no-games branch. One game on the day is not enough: a season page asked for an
    early-season date holds that day's games too, so any game off the day rejects the whole answer, and a
    cap-sized answer is truncated (a one-day chunk cannot be split further)."""
    games = _yahoo_games(raw)
    if len(games) >= _YAHOO_ROW_CAP:
        return False
    days = [d for d in (_et_date(g.get("startTime")) for g in games) if d is not None]
    return all(abs((d - day).days) <= 1 for d in days)


def yahoo_listing(store: Store, pacer: Pacer, league: str, season: int, refresh: bool) -> List[Dict[str, Any]]:
    """Every Yahoo game id of a season from ``leagueGameIdsByDate`` (``season=`` set), by week."""
    from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_league_game_ids_by_date

    slug, window = _YAHOO_LEAGUE[league], season_window(league, season)
    by_date = league in _YAHOO_BY_DATE
    dates = _sweep_dates(window)
    chunks: List[Tuple[date, date]] = []
    start = dates[0] if dates else None
    while start is not None and start <= dates[-1]:
        end = start if by_date else min(start + timedelta(days=_YAHOO_CHUNK_DAYS - 1), dates[-1])
        chunks.append((start, end))
        start = end + timedelta(days=1)
    games: Dict[str, Dict[str, Any]] = {}
    while chunks:
        lo, hi = chunks.pop(0)
        # by-date keys differ from range keys, so a stored range listing is never trusted for them
        key = f"{lo:%Y%m%d}" if by_date else f"{lo:%Y%m%d}_{hi:%Y%m%d}"
        when = {"dates": lo.isoformat()} if by_date else {"start_range": lo.isoformat(), "end_range": hi.isoformat()}
        raw = None if refresh else store.load("game_ids_by_date", season, key)
        pending = raw is not None and any(g.get("status") != "FINAL" for g in _yahoo_games(raw))
        if pending:
            raw = None  # stored while games were pending: list it again
        if raw is None and (refresh or pending or not store.has("game_ids_by_date", season, key)):
            status, raw = pacer.call(
                f"yahoo {slug} leagueGameIdsByDate {key}",
                partial(
                    yahoo_league_game_ids_by_date,
                    leagues=slug,
                    **when,
                    season=str(_yahoo_season(league, season)),
                    return_parsed=False,
                    **_FETCH_KW,
                ),
            )
            if status == "fail":
                continue
            if by_date and not _yahoo_answers_date(raw, lo):
                # Yahoo ignored `dates=` (as it ignores ranges for ncaaf): storing this would file the same
                # season page under every day, so leave the day unlisted and let the next run retry it
                logger.warning("yahoo %s %s: listing is not for that date; not stored", slug, key)
                continue
            if len(_yahoo_games(raw)) >= _YAHOO_ROW_CAP and hi > lo:
                mid = lo + (hi - lo) // 2  # a capped list is truncated: split the range and refetch
                chunks[:0] = [(lo, mid), (mid + timedelta(days=1), hi)]
                continue
            if not _yahoo_games(raw):
                if not refresh:
                    store.miss("game_ids_by_date", season, key, "no-games")
                continue
            store.put("game_ids_by_date", season, key, raw, gz=False)
        for g in _yahoo_games(raw):
            day = _et_date(g.get("startTime"))
            if not g.get("gameId") or day is None or not _in_window(day, window, slack=0):
                continue
            if (g.get("seasonPhase") or "").upper() == "PRESEASON":
                continue
            games.setdefault(
                g["gameId"], {"id": g["gameId"], "date": day.isoformat(), "final": g.get("status") == "FINAL"}
            )
    return list(games.values())


_NAV_RE = re.compile(r"college-football/scoreboard/(FBS|FCS)/(\d{4})/(regular|postseason)/(\d+)")


def _cbs_page(pacer: Pacer, url: str) -> Tuple[str, str]:
    from sportsdataverse._codegen_runtime import _text_body
    from sportsdataverse.dl_utils import download

    def fetch() -> str:
        return _text_body(download(url=url, **_FETCH_KW), url)

    status, html = pacer.call(f"cbs page {url}", fetch)
    return status, html or ""


def cbs_listing(store: Store, pacer: Pacer, league: str, season: int, refresh: bool) -> List[Dict[str, Any]]:
    """Every CBS game card of a season from the week scoreboard pages (cards stored as JSON).

    NFL reads the regular weeks and CBS's postseason weeks (numbered on from the regular
    season). CFB reads FBS and FCS: week 1's page links every week of that season, so the
    page decides which weeks exist (CBS's CFB week count is its own).
    """
    from sportsdataverse.football.cbs_common import _parse_scoreboard

    window = season_window(league, season)
    if league == "nfl":
        from sportsdataverse.nfl.cbs_pbp.game_id import _CARD_RE, _regular_season_weeks

        n = _regular_season_weeks(season)
        pages = [
            (f"regular_{w}", f"https://www.cbssports.com/nfl/scoreboard/{season}/regular/{w}/") for w in range(1, n + 1)
        ]
        pages += [
            (f"postseason_{w}", f"https://www.cbssports.com/nfl/scoreboard/{season}/postseason/{w}/")
            for w in range(n + 1, n + 6)
        ]
    else:
        from sportsdataverse.cfb.cbs_pbp.game_id import _CARD_RE

        pages = []
        for div in ("FBS", "FCS"):
            first = f"https://www.cbssports.com/college-football/scoreboard/{div}/{season}/regular/1/"
            pages.append((f"{div}_regular_1", first))
    games: Dict[str, Dict[str, Any]] = {}
    queue = list(pages)
    seen = {k for k, _ in queue}
    while queue:
        key, url = queue.pop(0)
        nav = league == "cfb" and key.endswith("_regular_1")  # week 1 links the season's weeks: always read it
        stored = None if refresh else store.load("scoreboard", season, key)
        html = ""
        if nav or (stored is None and (refresh or not store.has("scoreboard", season, key))):
            status, html = _cbs_page(pacer, url)
            if status == "fail":
                continue
            parsed = _parse_scoreboard(html, _CARD_RE)
            cards = [c for c in parsed if _in_window(_et_date(c["date"]), window, 0)]
            if cards:
                stored = {"url": url, "season": season, "cards": cards}
                store.put("scoreboard", season, key, stored, gz=False)
            elif not store.has("scoreboard", season, key):
                store.miss("scoreboard", season, key, "season-mismatch" if parsed else "no-cards")
        if nav:
            links = {(d, p, int(w)) for d, s, p, w in _NAV_RE.findall(html) if int(s) == season and d == key[:3]}
            for d, p, w in sorted(links, key=lambda t: (t[1] != "regular", t[2])):
                k = f"{d}_{p}_{w}"
                if k not in seen:
                    seen.add(k)
                    queue.append((k, f"https://www.cbssports.com/college-football/scoreboard/{d}/{season}/{p}/{w}/"))
        for c in (stored or {}).get("cards") or []:
            games.setdefault(
                c["cbs_game_id"],
                {
                    "id": c["cbs_game_id"],
                    "date": f"{c['date'][:4]}-{c['date'][4:6]}-{c['date'][6:]}",
                    "enhanced": c["enhanced"],
                },
            )
    for g in games.values():
        g["final"] = date.fromisoformat(g["date"]) < _today()
    return list(games.values())


# --------------------------------------------------------------------------- per-game payloads


def box_players(provider: str, payload: Any) -> int:
    """Distinct player ids in one stored box payload (the parse check)."""
    if provider == "fox":
        from sportsdataverse._fox_layout import parse_boxscore

        return len({r["athlete_id"] for r in parse_boxscore(payload, "") if r.get("athlete_id")})
    if provider == "yahoo":
        game = (((payload or {}).get("data") or {}).get("games") or [{}])[0]
        ids = {
            ((p or {}).get("player") or {}).get("playerId")
            for side in ("awayTeamLineup", "homeTeamLineup")
            for p in game.get(side) or []
        }
        return len(ids - {None})
    return len(set(re.findall(r'"playerId":\s*"?(\d+)', json.dumps((payload or {}).get("boxScores")))))


def _cbs_envelope(payload: Any, key: str) -> bool:
    """True when a NAPI body carries ``key`` data rather than CBS's 404 envelope."""
    if not isinstance(payload, dict):
        return False
    value = payload.get(key)
    return bool(value) and not (payload.get("errors") and not value)


def _fetch_games(
    store: Store,
    pacer: Pacer,
    provider: str,
    league: str,
    season: int,
    games: List[Dict[str, Any]],
    limit: Optional[int],
) -> Dict[str, Any]:
    """Fetch and store every completed game's payload(s); returns the season tally."""
    window = season_window(league, season)
    tally: Dict[str, Any] = {"written": 0, "skipped": 0, "missing": 0, "failed": 0, "players": []}
    jobs: List[Tuple[str, Dict[str, Any]]] = []
    if provider == "fox":
        jobs = [("event_data", g) for g in games] if season >= _FOX_BOX_FLOOR.get(league, 0) else []
    elif provider == "yahoo":
        jobs = [("playbook_boxscore", g) for g in games]
    else:
        if season >= _CBS_BOX_FLOOR[league]:
            jobs += [("game_boxscore", g) for g in games]
        if season >= _CBS_PLAYS_FLOOR:
            jobs += [("game_scoring_plays", g) for g in games if g.get("enhanced")]
    fetched = 0
    for endpoint, g in jobs:
        if not g.get("final"):
            continue
        gid = g["id"]
        key = re.sub(r"[^A-Za-z0-9._-]", "_", gid)
        if store.has(endpoint, season, key):
            tally["skipped"] += 1
            continue
        if limit is not None and fetched >= limit:
            break
        fetched += 1
        status, payload = pacer.call(f"{provider} {league} {endpoint} {gid}", _getter(provider, league, endpoint, gid))
        if status == "fail":
            tally["failed"] += 1
            continue
        reason = None if status == "ok" else "404"
        if reason is None:
            reason = _payload_problem(provider, league, endpoint, payload, gid, season, window)
        if reason:
            store.miss(endpoint, season, key, reason)
            tally["missing"] += 1
            continue
        store.put(endpoint, season, key, payload, gz=True)
        tally["written"] += 1
        if fetched % 100 == 0:
            logger.info(
                "%s %s %s: %d fetched (%d written, %d missing, %d failed), %s s/request",
                provider,
                league,
                season,
                fetched,
                tally["written"],
                tally["missing"],
                tally["failed"],
                pacer.seconds_per_request,
            )
        if endpoint in ("event_data", "playbook_boxscore", "game_boxscore"):
            tally["players"].append(box_players(provider, payload))
    return tally


def _getter(provider: str, league: str, endpoint: str, gid: str) -> Callable[[], Any]:
    if provider == "fox":
        from sportsdataverse.providers.fox.fox_api import fox_api_event_data

        return lambda: fox_api_event_data(_FOX_SPORT[league], gid, return_parsed=False, **_FETCH_KW)
    if provider == "yahoo":
        from sportsdataverse.providers.yahoo.yahoo_shangrila import yahoo_playbook_boxscore

        flag = {_YAHOO_FLAG[league]: "true"}
        return lambda: yahoo_playbook_boxscore(game_id=gid, return_parsed=False, **flag, **_FETCH_KW)
    from sportsdataverse.providers.cbs.cbs_napi import cbs_game_boxscore, cbs_game_scoring_plays

    fn = cbs_game_boxscore if endpoint == "game_boxscore" else cbs_game_scoring_plays
    return lambda: fn(gid, return_parsed=False, **_FETCH_KW)


def _payload_problem(
    provider: str, league: str, endpoint: str, payload: Any, gid: str, season: int, window: Tuple[date, date]
) -> Optional[str]:
    """Why a fetched payload must not be stored, or None when it is good."""
    if not payload:
        return "empty"
    if provider == "fox":
        day = _et_date(((payload.get("header") or {}) if isinstance(payload, dict) else {}).get("eventTime"))
        if not _in_window(day, window):
            return f"season-mismatch:{day}"
        return None
    if provider == "yahoo":
        games = ((payload.get("data") or {}) if isinstance(payload, dict) else {}).get("games") or []
        if not games or games[0].get("gameId") != gid:
            return "no-game"
        if str(games[0].get("season")) != str(_yahoo_season(league, season)):
            return f"season-mismatch:{games[0].get('season')}"
        return None
    key = "boxScores" if endpoint == "game_boxscore" else "plays"
    return None if _cbs_envelope(payload, key) else "cbs-404-envelope"


# --------------------------------------------------------------------------- driver


_LISTERS = {"fox": fox_listing, "yahoo": yahoo_listing, "cbs": cbs_listing}


def harvest_season(
    root: Path, provider: str, league: str, season: int, *, limit: Optional[int] = None, listing_only: bool = False
) -> Dict[str, Any]:
    """List, then fetch, one provider/league/season into ``root``; returns a summary dict.

    Raises:
        HarvestBlocked: the host answered 403 / 429, or kept failing.
        ValueError: the season is below the provider's floor for the league.
    """
    if season < FLOORS[provider][league]:
        raise ValueError(f"{provider} {league} starts in {FLOORS[provider][league]}; got {season}")
    store, pacer = Store(Path(root), provider, league), Pacer(provider)
    refresh = season_window(league, season)[1] >= _today() - timedelta(days=1)
    t0 = time.monotonic()
    games = _LISTERS[provider](store, pacer, league, season, refresh)
    listing_requests = pacer.n
    logger.info(
        "%s %s %s: listed %d games (%d final) in %d requests",
        provider,
        league,
        season,
        len(games),
        sum(1 for g in games if g.get("final")),
        listing_requests,
    )
    tally: Dict[str, Any] = {"written": 0, "skipped": 0, "missing": 0, "failed": 0, "players": []}
    if not listing_only:
        tally = _fetch_games(store, pacer, provider, league, season, games, limit)
    players: List[int] = tally.pop("players")
    return {
        "provider": provider,
        "league": league,
        "season": season,
        "games_listed": len(games),
        "games_final": sum(1 for g in games if g.get("final")),
        "listing_requests": listing_requests,
        "box_requests": pacer.n - listing_requests,
        "failures": pacer.failures,
        **tally,
        "players_per_box_min": min(players) if players else None,
        "players_per_box_median": median(players) if players else None,
        "boxes_without_players": sum(1 for p in players if p == 0),
        "seconds_per_request": pacer.seconds_per_request,
        "rps_setting": pacer.rps,
        "wall_seconds": round(time.monotonic() - t0, 1),
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    """CLI entry point; exit 0 done, 3 blocked by the host, 2 bad arguments."""
    p = argparse.ArgumentParser(prog="python -m sportsdataverse._crosswalk_history", description=__doc__.split("\n")[0])
    p.add_argument("--provider", required=True, choices=sorted(FLOORS))
    p.add_argument("--league", required=True, choices=sorted(_FOX_SPORT))
    p.add_argument("--season", required=True, type=int)
    p.add_argument("--root", required=True, type=Path, help="the *-alt-raw checkout")
    p.add_argument("--limit", type=int, default=None, help="fetch at most N per-game payloads (validation runs)")
    p.add_argument("--listing-only", action="store_true")
    args = p.parse_args(list(argv) if argv is not None else None)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    if args.league not in FLOORS[args.provider]:
        logger.error("%s has no %s harvest", args.provider, args.league)
        return 2
    try:
        summary = harvest_season(
            args.root, args.provider, args.league, args.season, limit=args.limit, listing_only=args.listing_only
        )
    except HarvestBlocked as exc:
        logger.error("BLOCKED: %s", exc)
        return 3
    except ValueError as exc:
        logger.error("%s", exc)
        return 2
    print("SUMMARY " + json.dumps(summary), flush=True)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
