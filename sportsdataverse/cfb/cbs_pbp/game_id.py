"""Resolve CBS's own college-football game id for an ESPN event (private, experimental).

CBS is the one measured alternate CFB source with **no** id map of any kind: its NAPI
payloads carry an empty ``vendorMappings``, ``cfb_crosswalk`` has no CBS provider, and the
pre-kickoff id map's ``cbs_game_id`` column
(:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`) is null because no builder can
fill it without the page this module reads. The id is published on the **week scoreboard
page**, days before kickoff, as ``<div id="game-{cbs id}" data-enhanced="true"
data-abbrev="NCAAF_{YYYYMMDD in US-Eastern}_{AWAY}@{HOME}">`` -- so the adapter reads one
page per (season, division, CBS week), caches it for the life of the process, and joins on
the Eastern date plus both clubs.

**The join is on CBS's numeric team id, not on an abbreviation.** Each card embeds both
clubs' logo URLs (``team-logos/alt/{cbs team id}.svg``, away row first), and CBS spells one
school three different ways across its own surfaces -- Texas is ``TEX`` in the team
directory, ``TEXAS`` in ``data-abbrev`` and ``UT`` in ``shortName`` -- so a number is the
only stable key. :mod:`sportsdataverse.cfb.cbs_pbp.teams` holds the ESPN -> CBS team id
snapshot; an ESPN team it does not carry resolves to nothing, never to a guess.

**CBS's CFB week is not ESPN's.** It is its own count, level with ESPN's in 2022-2025 and one
behind it in 2026, so a small window of weeks is tried and the *match* decides, rather than
an offset rule that is wrong in some season. Nothing here reads a stated week number: the
page that holds the card is the one that names it.
Only the ``FBS`` division page is read: CBS serves no play-by-play for an FCS-hosted game in
any era, so not finding the card there is itself the right answer (:func:`_resolve_cbs_game_id`
returns None and the adapter hands over).

# ponytail: this file is a CFB copy of ``nfl/cbs_pbp/game_id.py`` (PR #542, unmerged at the
# time of writing), which cannot be imported without depending on an open branch. TODO: when
# #542 lands, lift the shared half (``_PAGE_CACHE``/``_week_cards``/``_et_date``/the card
# regex) into ``football/cbs_common.py`` and leave only the league's own URL, division and
# week rules here.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from sportsdataverse.cfb.cbs_pbp.teams import _cbs_team
from sportsdataverse.dl_utils import download

_ET = ZoneInfo("America/New_York")

#: One scoreboard card's header. ``data-enhanced`` is set days before kickoff and is CBS's
#: own pre-kickoff signal that it will carry play-level data for the game at all.
_CARD_RE = re.compile(
    r'id="game-(?P<id>\d+)"\s+data-enhanced="(?P<enhanced>true|false)"\s+'
    r'data-abbrev="NCAAF_(?P<date>\d{8})_(?P<away>[A-Z0-9&.\-]+)@(?P<home>[A-Z0-9&.\-]+)"'
)
#: The CBS team id, from the logo URL inside a card. Away row first, home row second.
_LOGO_RE = re.compile(r"team-logos/alt/(\d+)\.svg")

#: Parsed scoreboard pages, keyed by ``(season, division, cbs_week)``. Cached for the life of
#: the process -- the page is large and one CFB Saturday is one page.
_PAGE_CACHE: Dict[Tuple[int, str, int], List[Dict[str, Any]]] = {}

#: Weeks whose page came back unreadable or with no cards, same key. A miss is remembered too,
#: because the alternative is worse: ``dl_utils.download`` retries a 403 or a 5xx 15 times and
#: :func:`_resolve_cbs_game_id` tries three weeks, so one unreachable page re-billed per game
#: is ~45 requests and minutes of wall clock **per game** on Game on Paper's request path. The
#: NFL twin took the same fix (#542); this file was copied from it before that landed.
_PAGE_MISSES: set = set()

#: Retries for the scoreboard page. The default 15 is sized for an asset a whole job depends
#: on; this one is a best-effort lookup with a fall-through behind it, so it fails fast.
_SCOREBOARD_RETRIES = 2


def _scoreboard_url(season: int, cbs_week: int, division: str = "FBS") -> str:
    """The week scoreboard page every card is read from."""
    return f"https://www.cbssports.com/college-football/scoreboard/{division}/{int(season)}/regular/{int(cbs_week)}/"


def _parse_scoreboard(html: str) -> List[Dict[str, Any]]:
    """Every game card on a week scoreboard page, in page order.

    A card's two logo URLs are read out of the slice that runs to the next card, so a page
    whose markup grows around them still parses; a card missing one of them keeps a ``None``
    team id, which simply never matches.
    """
    out: List[Dict[str, Any]] = []
    hits = list(_CARD_RE.finditer(html or ""))
    for index, match in enumerate(hits):
        end = hits[index + 1].start() if index + 1 < len(hits) else len(html)
        teams = _LOGO_RE.findall(html[match.end() : end])[:2]
        out.append(
            {
                "cbs_game_id": match.group("id"),
                "enhanced": match.group("enhanced") == "true",
                "date": match.group("date"),
                "away": match.group("away"),
                "home": match.group("home"),
                "away_cbs_team_id": teams[0] if len(teams) > 0 else None,
                "home_cbs_team_id": teams[1] if len(teams) > 1 else None,
            }
        )
    return out


def _week_cards(
    season: int,
    cbs_week: int,
    division: str = "FBS",
    *,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Cards for one CBS week, read at most once per process. Any failure is an empty week."""
    key = (int(season), division, int(cbs_week))
    if key in _PAGE_CACHE:
        return _PAGE_CACHE[key]
    if key in _PAGE_MISSES:
        return []
    kwargs.setdefault("num_retries", _SCOREBOARD_RETRIES)
    try:
        response = transport(url=_scoreboard_url(season, cbs_week, division), **kwargs)
    except Exception:  # noqa: BLE001 -- an unreachable page is a miss, never a raise
        _PAGE_MISSES.add(key)
        return []
    cards = _parse_scoreboard(getattr(response, "text", "") or "")
    if cards:
        _PAGE_CACHE[key] = cards
    else:
        _PAGE_MISSES.add(key)
    return cards


def _et_date(kickoff_utc: Optional[str]) -> Optional[str]:
    """``"2026-09-13T00:15Z"`` -> ``"20260912"`` -- CBS stamps the card with the Eastern date."""
    if not kickoff_utc:
        return None
    text = str(kickoff_utc).strip().replace("Z", "+00:00")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        # a bare calendar date is already local; re-zoning it from UTC midnight rolls it back
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
    cards: List[Dict[str, Any]], home_cbs_team_id: str, away_cbs_team_id: str, et_date: Optional[str]
) -> Optional[Dict[str, Any]]:
    """The one card for a matchup: both clubs must match, and the date when one is known."""
    hits = [
        c
        for c in cards
        if c["home_cbs_team_id"] == str(home_cbs_team_id) and c["away_cbs_team_id"] == str(away_cbs_team_id)
    ]
    if et_date:
        # A date miss is not a reason to drop the matchup -- CBS stamps the card from its own
        # schedule, which is right about who plays whom even when a game is moved -- but two
        # cards for the same pair in one week must be separated by it.
        hits = [c for c in hits if c["date"] == et_date] or hits
    return hits[0] if len(hits) == 1 else None


def _resolve_cbs_game_id(
    season: int,
    week: int,
    home_espn_team_id: Any,
    away_espn_team_id: Any,
    *,
    kickoff_utc: Optional[str] = None,
    division: str = "FBS",
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """The CBS game id for one ESPN college-football event, from the week scoreboard page.

    Args:
        season: ESPN season year.
        week: ESPN week within the regular season.
        home_espn_team_id: ESPN team id of the home club.
        away_espn_team_id: ESPN team id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.
        division: CBS scoreboard division segment. ``"FBS"`` is the only one with
            play-by-play; ``"FCS"`` exists but every game on it answers NAPI with a 404 body.
        transport: injection point for tests -- anything with ``download``'s signature.
        **kwargs: forwarded to ``transport`` (headers, timeout, ...).

    Returns:
        ``(cbs_game_id or None, provenance)``.

        | key | description |
        |---|---|
        | how | the scoreboard URL the id came from, or `"unresolved"` |
        | enhanced | CBS's own pre-kickoff signal that it will carry play-by-play |
        | weeks_tried | the CBS week numbers read, in order |

        The id is **never** synthesized: an unmatched game returns ``None`` and the adapter
        hands the game to the next source.
    """
    home, away = _cbs_team(home_espn_team_id), _cbs_team(away_espn_team_id)
    if not home or not away:
        return None, {"how": "unresolved", "enhanced": None, "weeks_tried": []}
    et_date = _et_date(kickoff_utc)
    tried: List[int] = []
    for cbs_week in (int(week), int(week) - 1, int(week) + 1):
        if cbs_week < 1:
            continue
        tried.append(cbs_week)
        card = _match_card(
            _week_cards(season, cbs_week, division, transport=transport, **kwargs), str(home[0]), str(away[0]), et_date
        )
        if card:
            return card["cbs_game_id"], {
                "how": _scoreboard_url(season, cbs_week, division),
                "enhanced": card["enhanced"],
                "weeks_tried": tried,
            }
    return None, {"how": "unresolved", "enhanced": None, "weeks_tried": tried}
