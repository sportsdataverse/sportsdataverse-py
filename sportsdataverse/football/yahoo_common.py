"""League-neutral Yahoo shangrila handling shared by the CFB and NFL adapters (private, experimental).

Yahoo serves both college and pro football from the same ``playbookBoxscore`` surface, with the
same envelope, the same failure shapes and the same presentational fields. Everything here is
identical for ``ncaaf`` and ``nfl`` and is therefore owned once:

* **Fetching and its failure shapes.** The rate limit answers with a 23-byte ``text/html``
  body, not JSON (:func:`_game_block` -> None, "try again"), and a game Yahoo does not cover
  answers **HTTP 200** with a real game object carrying no plays (:func:`_has_plays` -> False,
  "never worth another try"). The two must not collapse into one error.
* **Id arithmetic.** Both leagues' game ids embed the kickoff's **US-Eastern** calendar date
  (:func:`_kickoff_et_date`); only the league prefix and the home-team number differ, which is
  what each league's ``fetch`` module owns.
* **Presentation.** Clock, down-and-distance text, status, venue, the pregame line and the
  lineup name map are read the same way from the same keys in both feeds.

The play-by-play projection itself is **not** shared: ESPN's play-type vocabulary, its text
grammar and its scoring conventions differ per league, and that is what
``{cfb,nfl}/yahoo_pbp/to_espn_summary.py`` own.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

#: ``kickoff_utc`` formats the id map has shipped. The minutes-only form is what
#: ``idmap.GAME_SCHEMA`` documents; the others are what a schedule row carries.
_KICKOFF_FORMATS = ("%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M%z")

_PLAYER_REF_RE = re.compile(r"\[(\w+\.p\.\d+)\]")
_WHITESPACE_RE = re.compile(r"\s+")
_CLOCK_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_ODDS_RE = re.compile(r"(?i)(-?\d+(?:\.\d+)?)\s*,\s*O/U\s*(\d+(?:\.\d+)?)")

ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}


def _kickoff_et_date(kickoff: Optional[str]) -> Optional[str]:
    """``"2026-09-13T00:00Z"`` -> ``"20260912"``: the kickoff's **US-Eastern** calendar date.

    Eastern, not the venue's local zone: it is what reproduces Yahoo's own ids on a full
    season (CFB 934/934 in 2025, NFL 17/17 in 2013 and 16/16 in 2026 week 1), including the
    late-window West-coast games that roll past midnight UTC.
    """
    if not kickoff:
        return None
    text = str(kickoff).strip()
    for fmt in _KICKOFF_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return f"{parsed.astimezone(_ET):%Y%m%d}"
    return None


def _game_block(payload: Any) -> Optional[Dict[str, Any]]:
    """``data.games[0]`` of a shangrila playbook payload, or None when the envelope is absent.

    None means "this is not a game payload" -- a 429's 23-byte ``text/html`` body, an error
    envelope, a truncated response. It is deliberately **not** the same as a game with no
    plays (:func:`_has_plays`), which is Yahoo saying it does not cover the game: the first
    is worth another try, the second never is.
    """
    if not isinstance(payload, dict):
        return None
    games = (payload.get("data") or {}).get("games") if isinstance(payload.get("data"), dict) else None
    if not isinstance(games, list) or not games or not isinstance(games[0], dict):
        return None
    return games[0]


def _has_plays(game: Mapping[str, Any]) -> bool:
    """True when the payload carries play-by-play.

    Yahoo answers **HTTP 200** for a game it does not cover -- every FCS-hosted college game,
    and every game before its play floor -- with a real game object that has scores, odds and a
    win-probability stub but an empty ``playByPlay``. Detecting that by shape is the only way
    to tell "no coverage" from "fetch failed".
    """
    return bool(game.get("playByPlay"))


def _envelope_games(payload: Any) -> Optional[list]:
    """``data.games`` when the payload IS a shangrila envelope, else None.

    ``[]`` and None are different answers and must not be collapsed. An **empty list** is a
    well-formed envelope saying "no such game" -- Yahoo's NFL answer for a Pro Bowl and for
    anything before its play floor, HTTP 200 -- and is never worth another try. ``None`` means
    the body was not an envelope at all (a 429's 23-byte ``text/html``, an error page, a
    truncated response) and is.
    """
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), dict):
        return None
    games = payload["data"].get("games")
    return games if isinstance(games, list) else None


def _resolve_game(payload: Any) -> Optional[Dict[str, Any]]:
    """The game object from either a full envelope or a bare game dict, or None."""
    if isinstance(payload, Mapping) and "playByPlay" in payload:
        return dict(payload)
    return _game_block(payload)


def _fetch_playbook_boxscore(yahoo_game_id: str, *, live: bool = False, **kwargs: Any) -> Dict[str, Any]:
    """The shangrila playbook boxscore for one Yahoo game id (``...Poll`` variant when ``live``)."""
    from sportsdataverse.yahoo.yahoo_shangrila import yahoo_playbook_boxscore, yahoo_playbook_boxscore_poll

    fetch = yahoo_playbook_boxscore_poll if live else yahoo_playbook_boxscore
    return fetch(game_id=yahoo_game_id, is_football="true", return_parsed=False, **kwargs)


def _clock(clock: Optional[str]) -> str:
    """Yahoo ``"07:12"`` -> ESPN ``"7:12"``; anything unparseable becomes ``"0:00"``."""
    match = _CLOCK_RE.match(str(clock or "").strip())
    return f"{int(match.group(1))}:{match.group(2)}" if match else "0:00"


def _lineups(game: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """``(player id -> display name, player id -> the Yahoo team id he played for HERE)``.

    Yahoo writes play text with id placeholders (``"[nfl.p.33389] passed to ..."``) and
    resolves them from the same payload, so a game whose lineups are empty keeps the
    placeholders rather than losing the play.

    The club comes from **which lineup the entry is in**, not from ``player.teamId``: that
    field is the player's club in Yahoo's player database *today*, not in this game. On a
    January 2025 game it put Dameon Pierce (Houston) on ``nfl.t.21`` and Derek Barnett
    (Tennessee) on ``nfl.t.5``, which is what decides own-vs-opponent on a fumble recovery --
    and so flipped the ESPN play type and ~4-8 EPA on every such row.
    """
    names: Dict[str, str] = {}
    teams: Dict[str, str] = {}
    for side, team_key in (("homeTeamLineup", "homeTeamId"), ("awayTeamLineup", "awayTeamId")):
        for entry in game.get(side) or []:
            player = (entry or {}).get("player") or {}
            pid = player.get("playerId")
            if not pid:
                continue
            if player.get("displayName"):
                names[pid] = player["displayName"]
            club = game.get(team_key)
            if club:
                teams[pid] = str(club)
    return names, teams


def _text(raw: Optional[str], names: Mapping[str, str]) -> str:
    """Yahoo play text with ``[{league}.p.N]`` placeholders resolved to display names."""
    flat = _WHITESPACE_RE.sub(" ", str(raw or "").replace("\r", " ").replace("\n", " ")).strip()
    return _PLAYER_REF_RE.sub(lambda m: names.get(m.group(1), m.group(1)), flat)


def _down_distance_text(
    down: Optional[int], distance: Optional[int], to_endzone: Optional[int], spot: Optional[str]
) -> Optional[str]:
    """``"2nd & Goal at UK 3"`` -- the only ``downDistanceText`` the processors read (their goal test)."""
    if not down:
        return None
    label = ORDINAL.get(int(down), f"{int(down)}th")
    goal_to_go = to_endzone is not None and distance is not None and distance >= to_endzone
    togo = "Goal" if goal_to_go else str(int(distance or 0))
    return f"{label} & {togo}" + (f" at {spot}" if spot else "")


_STATUS_BY_YAHOO = {
    "FINAL": ("3", "STATUS_FINAL", "post", True, "Final"),
    "FINAL_OVERTIME": ("3", "STATUS_FINAL", "post", True, "Final/OT"),
    "HALFTIME": ("23", "STATUS_HALFTIME", "in", False, "Halftime"),
    "IN_PROGRESS": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "PREGAME": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
    "SCHEDULED": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
}


def _status(game: Mapping[str, Any]) -> Dict[str, Any]:
    """``header.competitions[0].status`` from Yahoo's own status word."""
    phase = str(game.get("status") or "").upper()
    if phase not in _STATUS_BY_YAHOO and game.get("isHalftime"):
        phase = "HALFTIME"
    type_id, name, state, completed, description = _STATUS_BY_YAHOO.get(
        phase, ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress")
    )
    period = ((game.get("currentPeriod") or {}) or {}).get("period") or 0
    clock = _clock(game.get("timeLeft")) if game.get("timeLeft") else None
    detail = description
    if state == "in" and name != "STATUS_HALFTIME" and period:
        detail = f"{clock or '0:00'} - {ORDINAL.get(period, str(period))}" if period <= 4 else f"{clock or '0:00'} - OT"
    return {
        "clock": 0.0,
        "displayClock": clock or "0:00",
        "period": period,
        "type": {
            "id": type_id,
            "name": name,
            "state": state,
            "completed": completed,
            "description": description,
            "detail": detail,
            "shortDetail": detail,
        },
        "yahooStatus": game.get("status"),
    }


def _odds_from_game(
    game: Mapping[str, Any], espn_by_yahoo: Mapping[str, str], home_id: str
) -> Optional[Dict[str, Any]]:
    """``odds_override`` from Yahoo's own pregame line (``"-9.5, O/U 47.5"`` + ``favoriteId``)."""
    summary = game.get("gameOddsSummary") or {}
    match = _ODDS_RE.search(str(summary.get("pregameOddsDisplay") or ""))
    if not match:
        return None
    favourite = espn_by_yahoo.get(str(summary.get("favoriteId") or ""))
    if favourite is None:
        return None
    return {
        "gameSpread": abs(float(match.group(1))),
        "overUnder": float(match.group(2)),
        "homeFavorite": str(favourite) == str(home_id),
        "gameSpreadAvailable": True,
    }


def _pickcenter(odds: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The resolved line as a one-provider ``pickcenter`` array (the offline odds path)."""
    if not odds or odds.get("gameSpread") is None or odds.get("overUnder") is None:
        return []
    return [
        {
            "provider": {"id": "0", "name": "stored closing line", "priority": 0},
            "spread": abs(float(odds["gameSpread"])),
            "overUnder": float(odds["overUnder"]),
            "homeTeamOdds": {"favorite": bool(odds.get("homeFavorite"))},
            "awayTeamOdds": {"favorite": not bool(odds.get("homeFavorite"))},
        }
    ]


def _venue(game: Mapping[str, Any]) -> Dict[str, Any]:
    """``gameInfo.venue`` -- Yahoo states ``coverType``, which is the only roof signal any feed gives."""
    venue = game.get("venue") or {}
    return {
        "id": venue.get("venueId"),
        "fullName": venue.get("displayName"),
        "address": {"city": venue.get("city"), "state": venue.get("state"), "country": venue.get("country")},
        "indoor": (str(venue.get("coverType") or "").upper() in ("DOME", "INDOOR", "RETRACTABLE")) or None,
    }
