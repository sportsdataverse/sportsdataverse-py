"""Resolve CBS's own NFL game id for an ESPN event (private, experimental).

CBS is the one measured alternate source with **no** offline id map: its NAPI payloads
carry an empty ``vendorMappings``, and the pre-kickoff id map's ``cbs_game_id`` column
(:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`) is null because no builder
can fill it without this page. The id is published on the **week scoreboard page**, days
before kickoff, as ``<div id="game-{cbs id}" data-enhanced="true"
data-abbrev="NFL_{YYYYMMDD in US-Eastern}_{AWAY}@{HOME}">`` -- so the adapter scrapes one
page per (season, season type, week), caches it for the life of the process, and joins on
the Eastern date plus both club codes. That join is unique inside an NFL week.

The join is by **label**, never by arithmetic on the id: CBS ids are 7-digit in 2019
(``3115572``) and 8-digit in 2026 (``50029216``) with no relation between eras, and
nothing here ever synthesizes one -- an unresolved game raises out of the adapter and
dispatch falls through to the next source.

Two club-code divergences from ESPN, both era-stable: CBS writes ``JAC`` where ESPN writes
``JAX`` and ``WAS`` where ESPN writes ``WSH``. Relocated franchises carry their era's code
on the page they appeared in (``OAK`` in 2019, ``LV`` in 2026), so every acceptable spelling
per ESPN franchise id is listed in :data:`CBS_SCOREBOARD_ABBRS`.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sportsdataverse.dl_utils import download

_ET = ZoneInfo("America/New_York")

#: One scoreboard card: the CBS game id, whether CBS says it will carry enhanced (play-level)
#: data, and the ``NFL_{date}_{away}@{home}`` abbreviation. ``data-enhanced`` is set days
#: before kickoff and is the only pre-kickoff signal that CBS will serve play-by-play at all.
_CARD_RE = re.compile(
    r'id="game-(?P<id>\d+)"\s+data-enhanced="(?P<enhanced>true|false)"\s+'
    r'data-abbrev="NFL_(?P<date>\d{8})_(?P<away>[A-Z0-9]+)@(?P<home>[A-Z0-9]+)"'
)

#: ESPN season type -> the CBS scoreboard path segment.
_SEGMENT = {1: "preseason", 2: "regular", 3: "postseason"}

#: ESPN franchise id -> every club code that franchise has appeared under on a CBS
#: scoreboard page. First entry is the current one. ``JAC``/``WAS`` are CBS's spellings of
#: ESPN's ``JAX``/``WSH``; the rest are relocations, which keep the ESPN franchise id.
CBS_SCOREBOARD_ABBRS: Dict[str, Tuple[str, ...]] = {
    "1": ("ATL",),
    "2": ("BUF",),
    "3": ("CHI",),
    "4": ("CIN",),
    "5": ("CLE",),
    "6": ("DAL",),
    "7": ("DEN",),
    "8": ("DET",),
    "9": ("GB",),
    "10": ("TEN",),
    "11": ("IND",),
    "12": ("KC",),
    "13": ("LV", "OAK"),
    "14": ("LAR", "LA", "STL"),
    "15": ("MIA",),
    "16": ("MIN",),
    "17": ("NE",),
    "18": ("NO",),
    "19": ("NYG",),
    "20": ("NYJ",),
    "21": ("PHI",),
    "22": ("ARI",),
    "23": ("PIT",),
    "24": ("LAC", "SD"),
    "25": ("SF",),
    "26": ("SEA",),
    "27": ("TB",),
    "28": ("WAS", "WSH"),
    "29": ("CAR",),
    "30": ("JAC", "JAX"),
    "33": ("BAL",),
    "34": ("HOU",),
}

#: Parsed scoreboard pages, keyed by ``(season, season_type, cbs_week)``. Cached for the
#: life of the process: the page is ~1-5 MB and one NFL week is read once per worker.
_PAGE_CACHE: Dict[Tuple[int, int, int], List[Dict[str, Any]]] = {}

#: Weeks whose page came back with no cards, same key. A miss is remembered too, because the
#: alternative is worse: ``dl_utils.download`` retries a 403 or a 5xx 15 times, and a
#: postseason lookup tries three weeks, so one unreachable page re-billed per game is ~45
#: requests and minutes of wall clock **per game** on Game on Paper's request path. A worker
#: that has already failed this week hands over to the next source immediately instead.
_PAGE_MISSES: set = set()

#: Retries for the scoreboard page. The default 15 is sized for an asset a whole job depends
#: on; this one is a best-effort lookup with a fall-through behind it, so it fails fast.
_SCOREBOARD_RETRIES = 2


def _regular_season_weeks(season: int) -> int:
    """18 regular-season weeks from 2021, 17 before (the 17-game schedule moved the playoffs)."""
    return 18 if int(season) >= 2021 else 17


def _cbs_week(season: int, season_type: int, week: int) -> int:
    """ESPN ``(season_type, week)`` -> the week number in the CBS scoreboard URL.

    CBS numbers postseason weeks as a continuation of the regular season, and it counts the
    Pro Bowl week that ESPN also counts (ESPN postseason week 5 = Super Bowl), so the Super
    Bowl of an 18-week season is week 23 -- verified on 2025 postseason week 23 (SEA @ NE).
    Preseason and regular-season weeks are ESPN's own.
    """
    if int(season_type) == 3:
        return _regular_season_weeks(season) + int(week)
    return int(week)


def _scoreboard_url(season: int, season_type: int, cbs_week: int) -> str:
    segment = _SEGMENT.get(int(season_type))
    if segment is None:
        raise ValueError(f"season_type must be one of {sorted(_SEGMENT)}, got {season_type!r}")
    return f"https://www.cbssports.com/nfl/scoreboard/{int(season)}/{segment}/{int(cbs_week)}/"


def _parse_scoreboard(html: str) -> List[Dict[str, Any]]:
    """Every game card on a week scoreboard page, in page order."""
    return [
        {
            "cbs_game_id": m.group("id"),
            "enhanced": m.group("enhanced") == "true",
            "date": m.group("date"),
            "away": m.group("away"),
            "home": m.group("home"),
        }
        for m in _CARD_RE.finditer(html or "")
    ]


def _week_cards(
    season: int,
    season_type: int,
    cbs_week: int,
    *,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Cards for one CBS week, read at most once per process. Any failure is an empty week."""
    key = (int(season), int(season_type), int(cbs_week))
    if key in _PAGE_CACHE:
        return _PAGE_CACHE[key]
    if key in _PAGE_MISSES:
        return []
    kwargs.setdefault("num_retries", _SCOREBOARD_RETRIES)
    try:
        resp = transport(url=_scoreboard_url(season, season_type, cbs_week), **kwargs)
    except Exception:  # noqa: BLE001 -- an unreachable page is a miss, never a raise
        _PAGE_MISSES.add(key)
        return []
    cards = _parse_scoreboard(getattr(resp, "text", "") or "")
    if cards:
        _PAGE_CACHE[key] = cards
    else:
        _PAGE_MISSES.add(key)
    return cards


def _et_date(kickoff_utc: Optional[str]) -> Optional[str]:
    """``"2026-09-14T00:15Z"`` -> ``"20260913"`` -- CBS stamps the card with the Eastern date."""
    if not kickoff_utc:
        return None
    text = str(kickoff_utc).strip().replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        # a bare calendar date is already local (the nflverse schedule's ``gameday``);
        # re-zoning it from UTC midnight would roll it back a day
        return text.replace("-", "")
    for fmt in ("%Y-%m-%dT%H:%M%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return f"{parsed.astimezone(_ET):%Y%m%d}"
    return None


def _match_card(
    cards: List[Dict[str, Any]],
    home_espn_team_id: str,
    away_espn_team_id: str,
    et_date: Optional[str],
) -> Optional[Dict[str, Any]]:
    """The card for one matchup: both clubs must match, and the date too when one is known."""
    home = CBS_SCOREBOARD_ABBRS.get(str(home_espn_team_id), ())
    away = CBS_SCOREBOARD_ABBRS.get(str(away_espn_team_id), ())
    if not home or not away:
        return None
    hits = [c for c in cards if c["home"] in home and c["away"] in away]
    if et_date:
        dated = [c for c in hits if c["date"] == et_date]
        # A date miss is not a reason to drop the matchup: CBS stamps the card from its own
        # schedule, which is right about who plays whom even when a game is rescheduled.
        # Two teams meet at most once per week, so the pair alone is already unique.
        hits = dated or hits
    # A page can render the same matchup twice (the grid card plus a "game of the week"
    # module), both carrying the same ``id="game-{id}"``; that is still one game, so the
    # uniqueness test counts distinct ids, not cards.
    return hits[0] if len({c["cbs_game_id"] for c in hits}) == 1 else None


def _resolve_cbs_game_id(
    season: int,
    season_type: int,
    week: int,
    home_espn_team_id: str,
    away_espn_team_id: str,
    *,
    kickoff_utc: Optional[str] = None,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """The CBS game id for one ESPN event, from the week scoreboard page.

    Args:
        season: ESPN season year.
        season_type: ESPN season type (1 preseason, 2 regular, 3 postseason).
        week: ESPN week within that season type.
        home_espn_team_id: ESPN franchise id of the home club.
        away_espn_team_id: ESPN franchise id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.
        transport: injection point for tests -- anything with ``download``'s signature.
        **kwargs: forwarded to ``transport`` (headers, timeout, ...).

    Returns:
        ``(cbs_game_id or None, provenance)``. ``provenance`` carries ``how`` (the URL the id
        came from, or ``"unresolved"``), ``enhanced`` (CBS's own pre-kickoff signal that it
        will carry play-by-play) and ``weeks_tried``.

        The id is **never** synthesized: an unmatched game returns ``None`` and the adapter
        hands over to the next source.
    """
    primary = _cbs_week(season, season_type, week)
    # CBS's postseason numbering has moved with the schedule (a 17th regular-season week was
    # added in 2021, the Pro Bowl week has come and gone), so a neighbouring week is tried
    # before giving up. Regular-season weeks match on the first try and never reach this.
    candidates = [primary] if int(season_type) != 3 else [primary, primary + 1, primary - 1]
    tried: List[int] = []
    for cbs_week in candidates:
        if cbs_week < 1:
            continue
        tried.append(cbs_week)
        card = _match_card(
            _week_cards(season, season_type, cbs_week, transport=transport, **kwargs),
            home_espn_team_id,
            away_espn_team_id,
            _et_date(kickoff_utc),
        )
        if card:
            return card["cbs_game_id"], {
                "how": _scoreboard_url(season, season_type, cbs_week),
                "enhanced": card["enhanced"],
                "weeks_tried": tried,
            }
    return None, {"how": "unresolved", "enhanced": None, "weeks_tried": tried}
