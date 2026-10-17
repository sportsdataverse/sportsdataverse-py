"""Resolve Fox's own NFL event id for an ESPN event (private, experimental).

The Fox twin of :mod:`sportsdataverse.nfl.cbs_pbp.game_id`: Fox's week scoreboard
(``nfl/league/scores-segment/{season}-{week}-{type}``) lists every game with its Bifrost
event id and both Fox team ids, so a game is found by its **Fox team-id pair** inside the
week -- a pair that is unique there. The id is never synthesized.

It lives apart from the adapter so a batch builder (sdv-db's nightly id map) can import it
without the play-by-play projection, and it takes the same arguments as the CBS resolver.
Fox's history starts with the **2018** season: earlier segment ids answer with no games.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

#: ESPN franchise id -> Fox Bifrost team id. Fox's ids are its own (Cleveland is 7 on Fox and
#: 5 on ESPN). Derived -- not invented -- by joining the captured Fox ``scores-segment`` rows to
#: nfl-raw's ``crosswalk/teams.json`` on the **full team name** (16/16 on two 2026 weeks), and
#: stable across relocations and renames: the 2018-2026 backfill resolved every game on it.
#: Fox abbreviations are unusable for the join (``WAS``/``WSH``, ``JAC``/``JAX``).
_FOX_TEAM_BY_ESPN: Dict[str, str] = {
    "1": "25",  # ATL
    "2": "1",  # BUF
    "3": "20",  # CHI
    "4": "6",  # CIN
    "5": "7",  # CLE
    "6": "15",  # DAL
    "7": "10",  # DEN
    "8": "21",  # DET
    "9": "22",  # GB
    "10": "8",  # TEN
    "11": "2",  # IND
    "12": "11",  # KC
    "13": "12",  # LV
    "14": "26",  # LAR
    "15": "3",  # MIA
    "16": "23",  # MIN
    "17": "4",  # NE
    "18": "27",  # NO
    "19": "16",  # NYG
    "20": "5",  # NYJ
    "21": "17",  # PHI
    "22": "18",  # ARI
    "23": "9",  # PIT
    "24": "13",  # LAC
    "25": "28",  # SF
    "26": "14",  # SEA
    "27": "24",  # TB
    "28": "19",  # WSH
    "29": "32",  # CAR
    "30": "31",  # JAX
    "33": "33",  # BAL
    "34": "34",  # HOU
}

#: ESPN ``season_type`` -> Fox segment type digit (1 REG, 2 POST, 3 PRE).
_SEGMENT_TYPE = {1: 3, 2: 1, 3: 2}
#: ESPN postseason week -> Fox postseason week. ESPN numbers the Pro Bowl 4 and the Super Bowl
#: 5; Fox has no Pro Bowl and numbers the Super Bowl 4.
_POST_WEEK = {1: 1, 2: 2, 3: 3, 4: None, 5: 4}


def _segment_ids(season: Any, season_type: Any, week: Any) -> List[str]:
    """Fox ``scores-segment`` ids to try for one ESPN (season, season_type, week), best first.

    A Fox segment id is ``{season}-{week}-{type}``. Regular season and preseason are a single
    candidate; the postseason gets the rest of its weeks as fall-backs, because the segment is
    only a *search space* -- the game is identified by its Fox team-id pair, so a wrong segment
    simply yields no match rather than a wrong game.
    """
    try:
        season, season_type, week = int(season), int(season_type), int(week)
    except (TypeError, ValueError):
        return []
    digit = _SEGMENT_TYPE.get(season_type)
    if digit is None:
        return []
    if season_type != 3:
        return [f"{season}-{week}-{digit}"]
    first = _POST_WEEK.get(week)
    candidates = ([first] if first else []) + [w for w in (1, 2, 3, 4) if w != first]
    return [f"{season}-{w}-{digit}" for w in candidates]


def _resolve_fox_game_id(
    season: Any,
    season_type: Any,
    week: Any,
    home_espn_team_id: Any,
    away_espn_team_id: Any,
    *,
    transport: Optional[Callable[[str], Any]] = None,
) -> Tuple[Optional[str], str]:
    """The Fox event id for one ESPN game, from Fox's week scoreboard.

    Args:
        season: ESPN season year.
        season_type: ESPN season type (1 preseason, 2 regular, 3 postseason).
        week: ESPN week within that season type.
        home_espn_team_id: ESPN franchise id of the home club.
        away_espn_team_id: ESPN franchise id of the away club.
        transport: ``path -> decoded JSON`` (default :func:`sportsdataverse._fox_layout.fox_get`).
            A batch caller passes a rate-limited, caching one; tests pass a fake.

    Returns:
        ``(fox_event_id or None, provenance)``: ``"scores_segment:{segment}"`` for a match,
        ``"unresolved"`` otherwise. A segment that fails to fetch is a miss, never a raise.
    """
    home = _FOX_TEAM_BY_ESPN.get(str(home_espn_team_id or ""))
    away = _FOX_TEAM_BY_ESPN.get(str(away_espn_team_id or ""))
    if not (home and away):
        return None, "unresolved"
    from sportsdataverse._fox_layout import fox_get, parse_segment_events

    get = transport or fox_get
    for segment in _segment_ids(season, season_type, week):
        try:
            raw = get(f"nfl/league/scores-segment/{segment}")
        except Exception:  # noqa: BLE001 -- a 404 or a timeout is a miss on this segment, not a raise
            continue
        for event in parse_segment_events(raw, segment):
            if str(event.get("home_team_id") or "") == home and str(event.get("away_team_id") or "") == away:
                if event.get("game_id"):
                    return str(event["game_id"]), f"scores_segment:{segment}"
    return None, "unresolved"
