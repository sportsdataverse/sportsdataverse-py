"""League-neutral CBS handling shared by the CFB and NFL adapters (private, experimental).

CBS serves college and pro football from the same two surfaces, and everything that is about
the *surface* rather than about the football is owned once here:

* **Id resolution.** Neither league has an offline CBS id map -- NAPI's ``vendorMappings`` is
  empty in both -- so the id is scraped from the **week scoreboard page**, which has the same
  card markup, the same ``data-enhanced`` pre-kickoff signal and the same logo-URL team ids in
  both. The page cache, its miss set and its retry budget (:data:`_SCOREBOARD_RETRIES`) live
  here too: a miss must be remembered, because ``dl_utils.download`` retries a 403 or a 5xx 15
  times and a lookup tries three weeks, so one unreachable page re-billed per game is ~45
  requests and minutes of wall clock **per game** on Game on Paper's request path.
* **Fetching.** The four NAPI bodies, and which of them are optional per league.
* **Presentation.** Text normalisation, the status block, the stoppage row CBS never emits,
  and the small type/int helpers the projections are written on.

The play-by-play projection itself is **not** shared: ESPN's play-type vocabulary, its text
grammar, its scoring conventions and the two feeds' own spot conventions differ per league --
CBS's CFB feed states a sack's *pre-snap* spot (144 of 144 sacks, 2022-2026) where its NFL
feed states the post-sack one, and CFB subplays state their own frame where the NFL feed has
to vote on it. That is what ``{cfb,nfl}/cbs_pbp/to_espn_summary.py`` own.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from sportsdataverse.dl_utils import download

# the same "ESPN never zero-pads the minutes" formatter both leagues' CBS modules already use
from sportsdataverse.football.yahoo_common import _clock

_ET = ZoneInfo("America/New_York")

_WHITESPACE_RE = re.compile(r"\s+")
#: GSIS/StatCrew jersey prefix on a name: ``"39-C.Little"``, ``"CLE-28-E.McNeil"``. CBS keeps
#: them; ESPN does not. Era-dependent in both feeds, so the normalizer must be idempotent,
#: which it is. ``Center-D.Riggs`` is deliberately untouched -- the prefix must be digits.
_JERSEY_RE = re.compile(r"\b\d{1,3}-(?=[A-Z])")
#: A replay reversal: CBS keeps the whole overturned narrative and appends the final ruling
#: after ``"... was REVERSED. "``; ESPN keeps only the ruling. Keeping CBS's version invents a
#: fumble on a play that had none and adds phantom defenders to the participants.
_REVERSED_RE = re.compile(r"(?i)\bwas\s+REVERSED\.\s*")
_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}

#: The CBS team id, from the logo URL inside a scoreboard card. Away row first, home second.
_LOGO_RE = re.compile(r"team-logos/alt/(\d+)\.svg")

#: Parsed scoreboard pages, keyed by the page URL -- which already encodes the league, the
#: season, the segment or division and the week. Cached for the life of the process: the page
#: is ~1-5 MB and one week is read once per worker.
_PAGE_CACHE: Dict[str, List[Dict[str, Any]]] = {}

#: URLs whose page came back unreadable or with no cards. A miss is remembered too: a worker
#: that has already failed this week hands over to the next source immediately instead.
_PAGE_MISSES: set = set()

#: Retries for the scoreboard page. The default 15 is sized for an asset a whole job depends
#: on; this one is a best-effort lookup with a fall-through behind it, so it fails fast.
_SCOREBOARD_RETRIES = 2

#: CBS ``game_status.status`` -> ESPN status ``(type id, name, state, completed, description)``.
_STATUS = {
    "FINAL": ("3", "STATUS_FINAL", "post", True, "Final"),
    "FINAL OT": ("3", "STATUS_FINAL", "post", True, "Final/OT"),
    "HALFTIME": ("23", "STATUS_HALFTIME", "in", False, "Halftime"),
    "INPROGRESS": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "IN PROGRESS": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "SCHEDULED": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
    "PREGAME": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
}


def _int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    """``"12"`` -> ``12``; anything unparseable -> ``default``."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _norm_text(description: Optional[str], abbr_fixes: Optional[Mapping[str, str]] = None) -> str:
    """CBS ``description`` -> ESPN ``text``: reversal tail, jersey prefixes, whitespace.

    Order matters: the reversal tail is cut **first**, so the jersey strip and the optional
    club-code rewrite only ever run on the ruling that actually stands. ``abbr_fixes`` is the
    league's whole-word club-code rewrite (the NFL's ``JAC`` -> ``JAX``, ``WAS`` -> ``WSH``);
    CFB has none, because CBS writes school names in the text rather than codes.
    """
    text = _WHITESPACE_RE.sub(" ", str(description or "").replace("\r", " ").replace("\n", " ")).strip()
    match = None
    for match in _REVERSED_RE.finditer(text):  # noqa: B007 -- the LAST reversal is the ruling
        pass
    if match is not None:
        text = text[match.end() :].strip()
    text = _JERSEY_RE.sub("", text)
    if abbr_fixes:
        text = re.sub(r"\b(" + "|".join(abbr_fixes) + r")\b", lambda m: abbr_fixes[m.group(1)], text)
    return text.strip()


def _subplay_body(subplay: Mapping[str, Any]) -> Mapping[str, Any]:
    """The one nested object on a CBS subplay (``{"type": "Rush", "order": "1", "rush": {...}}``)."""
    return next((v for v in subplay.values() if isinstance(v, dict)), {})


def _type_object(play_types: Mapping[str, Tuple[str, Optional[str]]], type_id: str) -> Dict[str, Any]:
    """``{"id", "text", "abbreviation"}`` -- Game on Paper bracket-reads all three.

    ``play_types`` is the league's own ESPN vocabulary: the two feeds type the same football
    differently (CFB types a returned punt ``52``), so the table is the caller's.
    """
    label, abbreviation = play_types[type_id]
    return {"id": type_id, "text": label, "abbreviation": abbreviation}


def _admin_row(template: Mapping[str, Any], type_object: Mapping[str, Any], text: str) -> Dict[str, Any]:
    """A stoppage row carrying the state of the play it follows; ``down 0`` keeps it a stoppage.

    CBS ships **no** administrative row of any kind, so timeouts and period ends are
    synthesized from the state of the play before them. ``down = 0`` is what keeps a
    synthesized row out of the processors' duplicate-text filter's state comparison.
    """
    return {
        "id": None,
        "sequenceNumber": None,
        "type": dict(type_object),
        "text": text,
        "awayScore": template["awayScore"],
        "homeScore": template["homeScore"],
        "period": dict(template["period"]),
        "clock": dict(template["clock"]),
        "scoringPlay": False,
        "priority": False,
        "statYardage": 0,
        "start": {
            **template["start"],
            "down": 0,
            "distance": 0,
            "downDistanceText": None,
            "team": dict(template["start"]["team"]),
        },
        "end": {},
    }


def _status(game_status: Mapping[str, Any], period: Optional[int]) -> Dict[str, Any]:
    """``header.competitions[0].status`` from CBS's own ``game_status`` block."""
    raw = str(game_status.get("status") or "SCHEDULED").upper()
    type_id, name, state, completed, description = _STATUS.get(
        raw, ("2", "STATUS_IN_PROGRESS", "in", False, raw.title())
    )
    clock = _clock(str(game_status.get("time_remaining") or "0:00"))
    detail = description
    if state == "in" and name != "STATUS_HALFTIME" and period:
        detail = f"{clock} - {_ORDINAL.get(period, str(period))}" if period <= 4 else f"{clock} - OT"
    return {
        "clock": 0.0,
        "displayClock": clock,
        "period": period or 0,
        "type": {
            "id": type_id,
            "name": name,
            "state": state,
            "completed": completed,
            "description": description,
            "detail": detail,
            "shortDetail": detail,
        },
        "cbsStatus": game_status.get("status"),
    }


def _napi_list(payload: Any, key: str) -> Optional[List[Mapping[str, Any]]]:
    """The ``key`` list out of a NAPI body, or None.

    CBS answers a game it does not carry with a ``{"errors"|"warnings": [{"code": 404, ...}]}``
    envelope -- sometimes under an HTTP 404, sometimes under a 200 -- so "the key is there and
    is a list" is the only honest coverage test. Treating the status as coverage is how a
    green-but-empty game reaches the processor.
    """
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(key)
    return list(value) if isinstance(value, list) else None


def _fetch_cbs_game(cbs_game_id: str, *, optional: Sequence[str] = ("drives",), **kwargs: Any) -> Dict[str, Any]:
    """The NAPI bodies one game needs, through the package's own retry/backoff getter.

    Sequential requests on :func:`sportsdataverse.dl_utils.download`'s retry budget. ``plays``
    and ``scoreboard`` are required; everything in ``optional`` is a degradation rather than a
    failure and comes back None when CBS 404s it -- ``drives`` (absent in the early era, and
    rebuilt from the plays' own ``drive_id``), ``odds`` (the NFL's consensus line) and
    ``game`` (the CFB venue).
    """
    from sportsdataverse.cbs.cbs_napi import (
        cbs_game,
        cbs_game_odds,
        cbs_game_scoring_drives,
        cbs_game_scoring_plays,
        cbs_game_scoring_scoreboard,
    )

    getters = {"drives": cbs_game_scoring_drives, "odds": cbs_game_odds, "game": cbs_game}
    out: Dict[str, Any] = {
        "cbs_game_id": str(cbs_game_id),
        "plays": cbs_game_scoring_plays(cbs_game_id, return_parsed=False, **kwargs),
        "scoreboard": cbs_game_scoring_scoreboard(cbs_game_id, return_parsed=False, **kwargs),
    }
    for key in optional:
        try:
            out[key] = getters[key](cbs_game_id, return_parsed=False, **kwargs)
        except Exception:  # noqa: BLE001 -- an optional resource is a degradation, not a failure
            out[key] = None
    return out


def _register_cbs(league: str, adapter: Callable[..., Any]) -> None:
    """Register one league's ``source="cbs"`` adapter with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register(league, "cbs")(adapter)


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


def _parse_scoreboard(html: str, card_re: "re.Pattern[str]") -> List[Dict[str, Any]]:
    """Every game card on a week scoreboard page, in page order.

    ``card_re`` is the league's own card header (the ``data-abbrev`` prefix and the club-code
    alphabet differ). A card's two logo URLs are read out of the slice that runs to the next
    card, so a page whose markup grows around them still parses; a card missing one of them
    keeps a ``None`` team id, which simply never matches.
    """
    out: List[Dict[str, Any]] = []
    hits = list(card_re.finditer(html or ""))
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
    url: str,
    card_re: "re.Pattern[str]",
    *,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> List[Dict[str, Any]]:
    """Cards for one scoreboard page, read at most once per process. Any failure is an empty week."""
    if url in _PAGE_CACHE:
        return _PAGE_CACHE[url]
    if url in _PAGE_MISSES:
        return []
    kwargs.setdefault("num_retries", _SCOREBOARD_RETRIES)
    try:
        response = transport(url=url, **kwargs)
    except Exception:  # noqa: BLE001 -- an unreachable page is a miss, never a raise
        _PAGE_MISSES.add(url)
        return []
    cards = _parse_scoreboard(getattr(response, "text", "") or "", card_re)
    if cards:
        _PAGE_CACHE[url] = cards
    else:
        _PAGE_MISSES.add(url)
    return cards


def _match_card(
    cards: List[Dict[str, Any]],
    home: Sequence[str],
    away: Sequence[str],
    et_date: Optional[str],
    *,
    fields: Tuple[str, str] = ("home", "away"),
) -> Optional[Dict[str, Any]]:
    """The one card for a matchup: both clubs must match, and the date too when one is known.

    ``fields`` is the pair of card keys the join runs on and ``home`` / ``away`` the acceptable
    values: CFB joins on CBS's numeric team id (one school is spelled three ways across CBS's
    own surfaces, so a number is the only stable key), the NFL on every club code a franchise
    has appeared under. An empty candidate list is an unknown club and matches nothing.
    """
    home_field, away_field = fields
    if not home or not away:
        return None
    hits = [c for c in cards if c[home_field] in home and c[away_field] in away]
    if et_date:
        # A date miss is not a reason to drop the matchup: CBS stamps the card from its own
        # schedule, which is right about who plays whom even when a game is rescheduled. Two
        # teams meet at most once per week, so the pair alone is already unique.
        hits = [c for c in hits if c["date"] == et_date] or hits
    # A page can render the same matchup twice (the grid card plus a "game of the week"
    # module), both carrying the same ``id="game-{id}"``; that is still one game, so the
    # uniqueness test counts distinct ids, not cards.
    return hits[0] if len({c["cbs_game_id"] for c in hits}) == 1 else None


def _resolve_from_scoreboard(
    pages: Iterable[Tuple[int, str]],
    card_re: "re.Pattern[str]",
    home: Sequence[str],
    away: Sequence[str],
    et_date: Optional[str],
    *,
    fields: Tuple[str, str] = ("home", "away"),
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Walk one league's candidate ``(week, page URL)`` list until a card matches.

    Returns ``(cbs_game_id or None, provenance)``; the id is **never** synthesized, so an
    unmatched game returns None and the adapter hands over to the next source.
    """
    tried: List[int] = []
    for week, url in pages:
        tried.append(week)
        card = _match_card(_week_cards(url, card_re, transport=transport, **kwargs), home, away, et_date, fields=fields)
        if card:
            return card["cbs_game_id"], {"how": url, "enhanced": card["enhanced"], "weeks_tried": tried}
    return None, {"how": "unresolved", "enhanced": None, "weeks_tried": tried}
