"""cdn.nba.com / cdn.wnba.com liveData -> the ESPN-summary dict ``helper_<league>_pbp`` parses (BB-D13).

``source="live"`` for NBA and WNBA in :mod:`sportsdataverse.basketball.sources.dispatch`. The two hosts
serve one liveData schema, so one projection serves both leagues, exactly as
:func:`~sportsdataverse.nba.nba_live.parse_nba_live_pbp` does for the tidy frames.

ESPN's NBA / WNBA play-by-play IS the league's liveData feed re-skinned: on every captured game ESPN's
``sequenceNumber`` is the liveData ``actionNumber``, so this adapter emits ESPN's own play ids
(``{espn_id}{actionNumber}``). What ESPN does to the feed, and this projection repeats (measured on 32
2025-26 NBA and 2026 WNBA games, both feeds captured the same day):

* **Folded rows.** A steal rides on its turnover row and a block or an assist on its shot row, as the
  second participant; a substitution is one row, ``[in, out]``, keyed at the ``out`` action; period
  starts are dropped.
* **Ids.** Teams and players are rewritten into ESPN's id space through the BB-D14 id map
  (:mod:`sportsdataverse.basketball.sources.idmap`); nothing is matched here. An unmapped team fails the
  game (the win-probability model needs one id space); an unmapped player becomes a null participant and
  an adapter note.
* **Shot coordinates.** liveData ``xLegacy`` / ``yLegacy`` are tenths of a foot from the rim, the same
  sign and frame as ESPN's feet-from-``(25, 0)`` pair: ``x = 25 + xLegacy / 10``, ``y = yLegacy / 10``
  (5,080 shots: within 0.6 ft of ESPN, which rounds to whole feet). Everything else carries ESPN's
  missing-coordinate sentinel.
* **Order.** liveData's array order (its ``orderNumber``), which is ESPN's except for 65 same-clock rows
  in 32 games, so a technical foul, its free throw and the substitutions after it keep ESPN's order.

What the feed cannot say is in :data:`~sportsdataverse.basketball.sources.contract.KNOWN_LOSSY` for
``("nba" | "wnba", "live")``, measured by ``tests/basketball/test_sources_live.py`` against ESPN on the
same games: liveData has no neutral-site flag (``neutralSite`` is emitted False), no team colors or logos,
and NBA tricodes where ESPN abbreviates (``GS`` / ``GSW``).
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Callable, Mapping
from typing import Any, Optional

#: ESPN NBA / WNBA play ``type.text`` -> ``type.id``, read off the published hoopR NBA (2025, 2026) and
#: wehoop WNBA (2025) play-by-play; only the types this adapter emits.
ESPN_TYPE_IDS: dict[str, str] = {
    "Not Available": "0",
    "Delay of Game": "8",
    "Defensive Goaltending": "9",
    "Lane": "10",
    "Jump Ball": "11",
    "Kicked Ball": "12",
    "Full Timeout": "16",
    "Official Timeout": "19",
    "Personal Take Foul": "22",
    "Offensive Charge": "24",
    "Delay Technical": "28",
    "Defensive 3-Seconds Technical": "29",
    "Double Technical Foul": "30",
    "Flagrant Foul Type 1": "32",
    "Technical Foul": "35",
    "Double Personal Foul": "36",
    "Away from Play Foul": "40",
    "Offensive Foul": "42",
    "Loose Ball Foul": "43",
    "Shooting Foul": "44",
    "Personal Foul": "45",
    "Flopping Technical": "48",
    "Bad Pass\nTurnover": "62",
    "Lost Ball Turnover": "63",
    "Traveling": "64",
    "Double Dribble Turnover": "65",
    "3-Second Turnover": "67",
    "5-Second Turnover": "68",
    "8-Second Turnover": "69",
    "Shot Clock Turnover": "70",
    "Inbound Turnover": "71",
    "Back Court Turnover": "72",
    "Offensive Goaltending Turnover": "73",
    "Lane Violation Turnover": "74",
    "Jumpball Violation Turnover": "75",
    "Kicked Ball Turnover": "76",
    "Palming Turnover": "78",
    "Offensive Foul Turnover": "84",
    "Out of Bounds - Step Turnover": "86",
    "Out of Bounds - Lost Ball Turnover": "87",
    "Out of Bounds - Bad Pass Turnover": "90",
    "Jump Shot": "92",
    "Hook Shot": "93",
    "Tip Shot": "94",
    "Layup Shot": "95",
    "Dunk Shot": "96",
    "Free Throw - 1 of 1": "97",
    "Free Throw - 1 of 2": "98",
    "Free Throw - 2 of 2": "99",
    "Free Throw - 1 of 3": "100",
    "Free Throw - 2 of 3": "101",
    "Free Throw - 3 of 3": "102",
    "Free Throw - Technical": "103",
    "Free Throw - Flagrant 1 of 2": "104",
    "Free Throw - Flagrant 2 of 2": "105",
    "Free Throw - Flagrant 1 of 1": "106",
    "Running Layup Shot": "109",
    "Driving Layup Shot": "110",
    "Alley Oop Layup Shot": "111",
    "Reverse Layup Shot": "112",
    "Running Jump Shot": "113",
    "Turnaround Jump Shot": "114",
    "Driving Dunk Shot": "115",
    "Running Dunk Shot": "116",
    "Alley Oop Dunk Shot": "118",
    "Driving Hook Shot": "119",
    "Turnaround Hook Shot": "120",
    "Fade Away Jump Shot": "121",
    "Jump Shot Bank": "122",
    "Hook Shot Bank": "123",
    "Finger Roll Layup": "124",
    "Layup Shot Putback": "125",
    "Layup Driving Reverse": "126",
    "Layup Running Reverse": "127",
    "Driving Finger Roll Layup": "128",
    "Running Finger Roll Layup": "129",
    "Floating Jump Shot": "130",
    "Pullup Jump Shot": "131",
    "Step Back Jump Shot": "132",
    "Pullup Bank Jump Shot": "133",
    "Driving Jump Shot Bank": "134",
    "Fade Away Bank Jump Shot": "135",
    "Turnaround Bank Jump Shot": "136",
    "Turnaround Fade Away Jump Shot": "137",
    "Putback Dunk Shot": "138",
    "Hook Driving Bank": "139",
    "Hook Turnaround Bank": "140",
    "Cutting Layup Shot": "141",
    "Cutting Finger Roll Layup Shot": "142",
    "Running Alley Oop Layup Shot": "143",
    "Driving Floating Jump Shot": "144",
    "Driving Floating Bank Jump Shot": "145",
    "Running Pullup Jump Shot": "146",
    "Step Back Bank Jump Shot": "147",
    "Turnaround Fadeaway Bank Jump Shot": "148",
    "Running Alley Oop Dunk Shot": "149",
    "Tip Dunk Shot": "150",
    "Cutting Dunk Shot": "151",
    "Defensive Rebound": "155",
    "Offensive Rebound": "156",
    "Challenge": "213",
    "Transition Take Foul": "257",
    "Heave Jump Shot": "282",
    "Reset Timeout": "283",
    "End Game": "402",
    "End Period": "412",
    "Ejection": "517",
    "Substitution": "584",
    "Jumpball": "615",
}

#: ``"{subType}|{descriptor}"`` (lower case; ``|`` alone when there is no descriptor) of a liveData field
#: goal -> ESPN's ``type.text``, every pair read off the 32-game match (ESPN's majority where it varies).
_SHOT_TYPES: dict[str, str] = {
    "jump shot|": "Jump Shot",
    "jump shot|bank": "Jump Shot Bank",
    "jump shot|driving bank": "Driving Jump Shot Bank",
    "jump shot|driving floating": "Driving Floating Jump Shot",
    "jump shot|driving floating bank": "Driving Floating Bank Jump Shot",
    "jump shot|fadeaway": "Fade Away Jump Shot",
    "jump shot|fadeaway bank": "Fade Away Bank Jump Shot",
    "jump shot|floating": "Floating Jump Shot",
    "jump shot|pullup": "Pullup Jump Shot",
    "jump shot|pullup bank": "Pullup Bank Jump Shot",
    "jump shot|running": "Running Jump Shot",
    "jump shot|running pullup": "Running Pullup Jump Shot",
    "jump shot|step back": "Step Back Jump Shot",
    "jump shot|step back bank": "Step Back Bank Jump Shot",
    "jump shot|turnaround": "Turnaround Jump Shot",
    "jump shot|turnaround bank": "Turnaround Bank Jump Shot",
    "jump shot|turnaround fadeaway": "Turnaround Fade Away Jump Shot",
    "jump shot|turnaround fadeaway bank": "Turnaround Fadeaway Bank Jump Shot",
    "layup|": "Layup Shot",
    "layup|alley-oop": "Alley Oop Layup Shot",
    "layup|cutting": "Cutting Layup Shot",
    "layup|cutting finger roll": "Cutting Finger Roll Layup Shot",
    "layup|driving": "Driving Layup Shot",
    "layup|driving finger roll": "Driving Finger Roll Layup",
    "layup|driving reverse": "Layup Driving Reverse",
    "layup|finger roll": "Finger Roll Layup",
    "layup|putback": "Layup Shot Putback",
    "layup|reverse": "Reverse Layup Shot",
    "layup|running": "Running Layup Shot",
    "layup|running alley-oop": "Running Alley Oop Layup Shot",
    "layup|running finger roll": "Running Finger Roll Layup",
    "layup|running reverse": "Layup Running Reverse",
    "layup|tip": "Tip Shot",
    "dunk|": "Dunk Shot",
    "dunk|alley-oop": "Alley Oop Dunk Shot",
    "dunk|cutting": "Cutting Dunk Shot",
    "dunk|driving": "Driving Dunk Shot",
    "dunk|putback": "Putback Dunk Shot",
    "dunk|running": "Running Dunk Shot",
    "dunk|running alley-oop": "Running Alley Oop Dunk Shot",
    "dunk|tip": "Tip Dunk Shot",
    "hook|": "Hook Shot",
    "hook|bank": "Hook Shot Bank",
    "hook|driving": "Driving Hook Shot",
    "hook|driving bank": "Hook Driving Bank",
    "hook|turnaround": "Turnaround Hook Shot",
    "hook|turnaround bank": "Hook Turnaround Bank",
}

#: Every other liveData ``"{actionType}|{subType}|{descriptor}"``, read most specific first
#: (``a|s|d``, then ``a|s``, then ``a``) -> ESPN's ``type.text``; same 32-game match.
_TYPES: dict[str, str] = {
    "ejection": "Ejection",
    "foul|offensive": "Offensive Foul",
    "foul|offensive|charge": "Offensive Charge",
    "foul|personal": "Personal Foul",
    "foul|personal|away-from-play": "Away from Play Foul",
    "foul|personal|double": "Double Personal Foul",
    "foul|personal|flagrant-type-1": "Flagrant Foul Type 1",
    "foul|personal|loose ball": "Loose Ball Foul",
    "foul|personal|shooting": "Shooting Foul",
    "foul|personal|take": "Personal Take Foul",
    "foul|personal|transition take": "Transition Take Foul",
    "foul|technical": "Technical Foul",
    "foul|technical|defensive-3-second": "Defensive 3-Seconds Technical",
    "foul|technical|delay": "Delay Technical",
    "foul|technical|double": "Double Technical Foul",
    "foul|technical|flopping": "Flopping Technical",
    "game|end": "End Game",
    "heave": "Heave Jump Shot",
    "jumpball": "Jumpball",
    "period|end": "End Period",
    "rebound|defensive": "Defensive Rebound",
    "rebound|offensive": "Offensive Rebound",
    "substitution|out": "Substitution",
    "timeout|challenge": "Challenge",
    "timeout|full": "Full Timeout",
    "timeout|officials": "Official Timeout",
    "timeout|reset": "Reset Timeout",
    "turnover|3-second-violation": "3-Second Turnover",
    "turnover|5-second-violation": "5-Second Turnover",
    "turnover|8-second-violation": "8-Second Turnover",
    "turnover|backcourt": "Back Court Turnover",
    "turnover|bad pass": "Bad Pass\nTurnover",
    "turnover|double dribble": "Double Dribble Turnover",
    "turnover|inbound": "Inbound Turnover",
    "turnover|jumpball violation": "Jumpball Violation Turnover",
    "turnover|laneviolation": "Lane Violation Turnover",
    "turnover|lost ball": "Lost Ball Turnover",
    "turnover|offensive foul": "Offensive Foul Turnover",
    "turnover|offensive goaltending": "Offensive Goaltending Turnover",
    "turnover|offensive-kicked-ball": "Kicked Ball Turnover",
    "turnover|out-of-bounds|bad pass": "Out of Bounds - Bad Pass Turnover",
    "turnover|out-of-bounds|lost ball": "Out of Bounds - Lost Ball Turnover",
    "turnover|out-of-bounds|step": "Out of Bounds - Step Turnover",
    "turnover|palming": "Palming Turnover",
    "turnover|shot clock": "Shot Clock Turnover",
    "turnover|traveling": "Traveling",
    "violation|defensive goaltending": "Defensive Goaltending",
    "violation|delay-of-game": "Delay of Game",
    "violation|jumpball": "Jump Ball",
    "violation|kicked ball": "Kicked Ball",
    "violation|lane": "Lane",
}

#: A liveData subtype the 32-game match never saw falls back to its family's generic ESPN type and is
#: named in the adapter notes; any other unseen action type reads ESPN's own ``Not Available``.
_FAMILY: dict[str, str] = {"foul": "Personal Foul", "timeout": "Full Timeout", "turnover": "Lost Ball Turnover"}

#: liveData actions ESPN folds into another row (steal, block, substitution in) or drops (period start).
_FOLDED = {"steal", "block", "substitution|in", "period|start"}

#: The third digit of a liveData game id -> ESPN ``season.type``, as measured on 2025-26 games
#: (``006`` is the NBA Cup final, which ESPN files under the regular season). Any other code (preseason,
#: All-Star) is left null, so the contract fails it and dispatch falls through to ESPN.
_SEASON_TYPES = {"2": 2, "4": 3, "5": 5, "6": 2}

#: ESPN's missing-coordinate pair, carried on every play without a located shot.
_NO_COORDINATE = {"x": -214748340, "y": -214748365}

_CLOCK = re.compile(r"^PT(\d+)M(\d+(?:\.\d+)?)S$")
_POINTS = {"3pt": 3, "2pt": 2, "freethrow": 1, "heave": 3}

#: ``(kind, liveData id) -> id-map row or None``; :func:`_idmap_lookup` builds one.
IdLookup = Callable[[str, Any], Optional[Mapping[str, Any]]]


def _espn_type(action: Mapping[str, Any]) -> tuple[str, bool]:
    """``(ESPN type.text, exact)`` for one liveData action; ``exact`` is False on a family fallback."""
    kind = action.get("actionType") or ""
    sub = str(action.get("subType") or "").lower()
    desc = str(action.get("descriptor") or "").lower()
    if kind in ("2pt", "3pt"):
        exact = _SHOT_TYPES.get(f"{sub}|{desc}")
        return (exact, True) if exact else (_SHOT_TYPES.get(f"{sub}|", "Jump Shot"), False)
    if kind == "freethrow":
        label = {"technical": "Technical", "flagrant": f"Flagrant {sub}"}.get(desc, sub)
        text = f"Free Throw - {label}"
        if text in ESPN_TYPE_IDS:
            return text, True
        return (f"Free Throw - {sub}" if f"Free Throw - {sub}" in ESPN_TYPE_IDS else "Free Throw - 1 of 1"), False
    for key in (f"{kind}|{sub}|{desc}", f"{kind}|{sub}", kind):
        if key in _TYPES:
            return _TYPES[key], True
    return _FAMILY.get(kind, "Not Available"), False


def _clock(value: Any) -> Optional[str]:
    """``PT11M36.00S`` -> ``11:36``; under a minute ESPN's ``SS.t`` (``PT00M23.10S`` -> ``23.1``)."""
    m = _CLOCK.match(str(value or ""))
    if not m:
        return None
    minutes, seconds = int(m[1]), float(m[2])
    return f"{minutes}:{int(seconds):02d}" if minutes else f"{math.floor(seconds * 10) / 10:.1f}"


def _period_label(period: int, regulation: int) -> str:
    if period <= regulation:
        return f"{period}{({1: 'st', 2: 'nd', 3: 'rd'}).get(period, 'th')} Quarter"
    extra = period - regulation
    return "OT" if extra == 1 else f"{extra}OT"


def _date(utc: Any) -> Optional[str]:
    """liveData ``2025-10-21T23:30:00Z`` -> ESPN's minute-precision ``2025-10-21T23:30Z``."""
    return f"{str(utc)[:16]}Z" if utc and len(str(utc)) >= 16 else None


class _Ids:
    """Team / player id rewriting into ESPN's space, collecting what did not map."""

    def __init__(self, lookup: IdLookup):
        self.lookup = lookup
        self.players: dict[int, tuple[Optional[str], Optional[str]]] = {}
        self.unmapped: set[int] = set()

    def team(self, team_id: Any) -> str:
        row = self.lookup("team", team_id)
        if not row or not row.get("espn_id"):
            raise ValueError(f"liveData team {team_id} is not in the id map")
        return str(row["espn_id"])

    def player(self, person_id: Any, live_name: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
        """``(ESPN athlete id or None, display name)``; ``person_id`` 0 / None is the team (no athlete)."""
        if not person_id:
            return None, live_name
        pid = int(person_id)
        if pid not in self.players:
            row = self.lookup("player", pid) or {}
            espn = str(row["espn_id"]) if row.get("espn_id") else None
            if espn is None:
                self.unmapped.add(pid)
            self.players[pid] = (espn, (row.get("name") if espn else None) or live_name)
        espn, name = self.players[pid]
        return espn, name or live_name


def _participants(action: Mapping[str, Any], ids: _Ids) -> list[Optional[str]]:
    """ESPN's participant order: actor first, then the assister / blocker / stealer (jump ball: won, lost, recovered)."""
    kind = action.get("actionType")
    keys: tuple[str, ...]
    if kind == "jumpball":
        keys = ("jumpBallWonPersonId", "jumpBallLostPersonId", "jumpBallRecoverdPersonId")
    elif kind in ("2pt", "3pt"):
        keys = ("personId", "assistPersonId" if action.get("assistPersonId") else "blockPersonId")
    elif kind == "turnover":
        keys = ("personId", "stealPersonId")
    else:
        keys = ("personId",)
    return [ids.player(action.get(k))[0] for k in keys if action.get(k)]


def _team_statistics(stats: Mapping[str, Any], opponent: Mapping[str, Any]) -> list[dict[str, str]]:
    """liveData team totals as ESPN's ``boxscore.teams[].statistics`` (the names the team box reads).

    ESPN's conventions, each checked on the 68 team rows of the 34-game match: percentages are whole
    numbers rounded half up (every exact half on the BB-D15 parity corpus: 37.5 -> 38 three times, 42.5 -> 43
    twice; Python's half-to-even ``round`` gave 42),
    ``totalRebounds`` excludes team rebounds, technicals count the bench / team ones too, and
    ``turnoverPoints`` is what the OPPONENT scored off this team's turnovers.
    """

    def pct(made: str, att: str) -> str:
        a = stats.get(att) or 0
        # integer half-up: floor(100 * made / att + 1/2) with no float rounding at the exact half
        return str((200 * (stats.get(made) or 0) + a) // (2 * a)) if a else "0"

    technicals = (stats.get("foulsTechnical") or 0) + (stats.get("foulsTeamTechnical") or 0)

    pairs = {
        "fieldGoalsMade-fieldGoalsAttempted": f"{stats.get('fieldGoalsMade', 0)}-{stats.get('fieldGoalsAttempted', 0)}",
        "fieldGoalPct": pct("fieldGoalsMade", "fieldGoalsAttempted"),
        "threePointFieldGoalsMade-threePointFieldGoalsAttempted": (
            f"{stats.get('threePointersMade', 0)}-{stats.get('threePointersAttempted', 0)}"
        ),
        "threePointFieldGoalPct": pct("threePointersMade", "threePointersAttempted"),
        "freeThrowsMade-freeThrowsAttempted": f"{stats.get('freeThrowsMade', 0)}-{stats.get('freeThrowsAttempted', 0)}",
        "freeThrowPct": pct("freeThrowsMade", "freeThrowsAttempted"),
        "totalRebounds": stats.get("reboundsPersonal"),
        "offensiveRebounds": stats.get("reboundsOffensive"),
        "defensiveRebounds": stats.get("reboundsDefensive"),
        "assists": stats.get("assists"),
        "steals": stats.get("steals"),
        "blocks": stats.get("blocks"),
        "turnovers": stats.get("turnovers"),
        "teamTurnovers": stats.get("turnoversTeam"),
        "totalTurnovers": stats.get("turnoversTotal"),
        "technicalFouls": technicals,
        "totalTechnicalFouls": technicals,
        "turnoverPoints": opponent.get("pointsFromTurnovers"),
        "fastBreakPoints": stats.get("pointsFastBreak"),
        "pointsInPaint": stats.get("pointsInThePaint"),
        "fouls": stats.get("foulsPersonal"),
        "largestLead": stats.get("biggestLead"),
        "leadChanges": stats.get("leadChanges"),
    }
    return [{"name": k, "displayValue": str(v)} for k, v in pairs.items() if v is not None]


_PLAYER_KEYS = [
    "minutes",
    "points",
    "fieldGoalsMade-fieldGoalsAttempted",
    "threePointFieldGoalsMade-threePointFieldGoalsAttempted",
    "freeThrowsMade-freeThrowsAttempted",
    "rebounds",
    "assists",
    "turnovers",
    "steals",
    "blocks",
    "offensiveRebounds",
    "defensiveRebounds",
    "fouls",
    "plusMinus",
]


def _minutes(value: Any) -> str:
    """liveData ``PT45M15.10S`` -> ESPN's whole minutes (``45``).

    ESPN rounds the seconds, then the minutes half up: ``38:29.9`` is 39 (698 of 699 players matched).
    """
    m = _CLOCK.match(str(value or ""))
    return str(math.floor((int(m[1]) * 60 + round(float(m[2]))) / 60 + 0.5)) if m else "0"


def _player_stats(s: Mapping[str, Any]) -> list[str]:
    pm = int(s.get("plusMinusPoints") or 0)
    return [
        _minutes(s.get("minutes")),
        str(s.get("points", 0)),
        f"{s.get('fieldGoalsMade', 0)}-{s.get('fieldGoalsAttempted', 0)}",
        f"{s.get('threePointersMade', 0)}-{s.get('threePointersAttempted', 0)}",
        f"{s.get('freeThrowsMade', 0)}-{s.get('freeThrowsAttempted', 0)}",
        str(s.get("reboundsTotal", 0)),
        str(s.get("assists", 0)),
        str(s.get("turnovers", 0)),
        str(s.get("steals", 0)),
        str(s.get("blocks", 0)),
        str(s.get("reboundsOffensive", 0)),
        str(s.get("reboundsDefensive", 0)),
        str(s.get("foulsPersonal", 0)),
        f"+{pm}" if pm > 0 else str(pm),
    ]


def _box_players(team: Mapping[str, Any], team_obj: dict, ids: _Ids) -> dict:
    athletes = []
    for p in team.get("players") or []:
        played = str(p.get("played")) == "1"
        if not played and p.get("status") != "ACTIVE":
            continue  # ESPN lists the dressed did-not-play bench, not the inactive list
        espn_id, name = ids.player(p.get("personId"), p.get("name"))
        athletes.append(
            {
                "active": str(p.get("oncourt")) == "1",
                "athlete": {
                    "id": espn_id,
                    "displayName": name,
                    "shortName": p.get("nameI"),
                    "jersey": p.get("jerseyNum"),
                    "position": {"abbreviation": p.get("position")},
                },
                "starter": str(p.get("starter")) == "1",
                "didNotPlay": not played,
                "reason": p.get("notPlayingReason") or "",
                "ejected": False,
                "stats": _player_stats(p.get("statistics") or {}) if played else [],
            }
        )
    return {"team": team_obj, "statistics": [{"keys": list(_PLAYER_KEYS), "athletes": athletes}]}


def _live_to_espn_summary(
    pbp: Mapping[str, Any],
    box: Mapping[str, Any],
    *,
    espn_id: int | str,
    season: int,
    lookup: IdLookup,
) -> tuple[dict, list[str]]:
    """Project one liveData game (play-by-play + boxscore payloads) onto ``espn_<lg>_pbp(raw=True)``'s dict.

    Args:
        pbp: The ``liveData/playbyplay`` payload (``nba_live_pbp(..., raw=True)``).
        box: The ``liveData/boxscore`` payload of the same game.
        espn_id: The ESPN event id the summary is served under (``header.id`` and every play id).
        season: ESPN's ``season.year`` (the id map's end year: 2026 = 2025-26).
        lookup: ``(kind, liveData id) -> id-map row`` (:func:`_idmap_lookup`).

    Returns:
        ``(summary, notes)``: the ESPN-shaped dict ``helper_<league>_pbp`` and ``game_page_frames`` read,
        and the adapter's degradations (unmapped players, unmapped play types).

    Raises:
        ValueError: The payloads carry no ``game`` object or no home / away team, or a team id is not in
            the id map.
    """
    game, bgame = (pbp or {}).get("game"), (box or {}).get("game")
    if not isinstance(game, Mapping) or not isinstance(bgame, Mapping):
        raise ValueError("liveData payload carries no game object")
    home, away = bgame.get("homeTeam"), bgame.get("awayTeam")
    if not isinstance(home, Mapping) or not isinstance(away, Mapping):
        raise ValueError("liveData boxscore carries no home / away team")
    ids, notes, eid = _Ids(lookup), [], str(espn_id)
    regulation = int(bgame.get("regulationPeriods") or 4)
    final = bgame.get("gameStatus") == 3
    teams = _teams(home, away, ids)
    header = _header(game, bgame, eid, season, teams, final)
    by_live_team = {t.get("teamId"): tid for t, tid, _ in teams.values()}
    plays, unmapped_types, unpaired = _plays(game.get("actions") or [], eid, regulation, by_live_team, ids)
    if final and plays:
        # the two payloads are fetched separately: a final box over a play list that stops short would serve
        # the final score in the header and an older one to the win-probability model. Mid-game a few
        # seconds of lag between them is expected and left alone.
        last = (plays[-1]["homeScore"], plays[-1]["awayScore"])
        boxed = (int(home.get("score") or 0), int(away.get("score") or 0))
        if last != boxed:
            raise ValueError(f"final box score {boxed} but the play-by-play ends at {last}")

    if ids.unmapped:
        notes.append(
            f"{len(ids.unmapped)} liveData people (players, or a coach charged a technical) not in the id map"
            f" (null athlete ids): {sorted(ids.unmapped)}"
        )
    if unmapped_types:
        notes.append(f"liveData play types without an ESPN match (family fallback): {sorted(unmapped_types)}")
    if unpaired:
        notes.append("substitutions left unpaired (in / out counts differ at one stoppage)")

    summary: dict[str, Any] = {
        "boxscore": {
            "teams": [
                {
                    "team": obj,
                    "statistics": _team_statistics(t.get("statistics") or {}, o.get("statistics") or {}),
                    "displayOrder": i,
                }
                for i, ((t, _, obj), (o, _, _)) in enumerate(
                    ((teams["away"], teams["home"]), (teams["home"], teams["away"])), start=1
                )
            ],
            "players": [_box_players(t, obj, ids) for t, _, obj in (teams["away"], teams["home"])],
        },
        "format": {"regulation": {"periods": regulation}},
        "header": header,
        "plays": plays,
    }
    # the rest of the keys espn_<lg>_pbp(raw=True) defaults: liveData has none of them
    for key in ("gameInfo", "predictor", "article", "standings", "season"):
        summary[key] = {}
    for key in _EMPTY_LISTS:
        summary[key] = []
    return summary, notes


_EMPTY_LISTS = (
    "leaders",
    "seasonseries",
    "broadcasts",
    "pickcenter",
    "againstTheSpread",
    "odds",
    "winprobability",
    "videos",
    "teamInfo",
    "espnWP",
    "timeouts",
)


def _teams(home: Mapping[str, Any], away: Mapping[str, Any], ids: _Ids) -> dict[str, tuple[Mapping, str, dict]]:
    """``side -> (liveData team, ESPN team id, ESPN team object)``; raises on a team the id map lacks."""
    teams = {}
    for side, t in (("home", home), ("away", away)):
        tid = ids.team(t.get("teamId"))
        city, name = t.get("teamCity"), t.get("teamName")
        obj = {
            "id": tid,
            "location": city,
            "name": name,
            "abbreviation": t.get("teamTricode"),
            "displayName": f"{city} {name}",
            "shortDisplayName": name,
        }
        teams[side] = (t, tid, obj)
    return teams


def _header(
    game: Mapping[str, Any], bgame: Mapping[str, Any], eid: str, season: int, teams: dict, final: bool
) -> dict[str, Any]:
    """ESPN's ``header``: season, date, status and both competitors (home first, as ESPN orders them)."""
    competitors = []
    for side, (t, tid, obj) in teams.items():
        other = teams["away" if side == "home" else "home"][0]
        comp: dict[str, Any] = {
            "id": tid,
            "homeAway": side,
            "score": str(t.get("score")),
            "team": obj,
            "linescores": [{"displayValue": str(p.get("score"))} for p in t.get("periods") or []],
        }
        if final:
            comp["winner"] = (t.get("score") or 0) > (other.get("score") or 0)
        competitors.append(comp)
    status: Any = bgame.get("gameStatus")
    return {
        "id": eid,
        "season": {"year": season, "type": _SEASON_TYPES.get(str(bgame.get("gameId") or game.get("gameId"))[2:3])},
        "competitions": [
            {
                "id": eid,
                "date": _date(bgame.get("gameTimeUTC")),
                # liveData has no neutral-site flag (KNOWN_LOSSY); ESPN's is True for the NBA Cup final
                "neutralSite": False,
                "playByPlaySource": "full",
                "boxscoreSource": "full",
                "status": {
                    "type": {
                        "completed": final,
                        "state": {1: "pre", 2: "in", 3: "post"}.get(status),
                        "description": bgame.get("gameStatusText"),
                    }
                },
                "competitors": competitors,
            }
        ],
    }


def _plays(
    raw_actions: Any, eid: str, regulation: int, by_live_team: dict, ids: _Ids
) -> tuple[list[dict], set[str], bool]:
    """ESPN ``plays[]`` in liveData's ``orderNumber`` order, folded rows merged.

    Returns:
        ``(plays, unmapped play types, whether a substitution was left unpaired)``.
    """
    plays: list[dict] = []
    open_subs: dict[tuple, list[dict]] = {}  # (period, clock, team) -> sub rows still missing their "in"
    waiting_ins: dict[tuple, list[Mapping]] = {}  # "in" actions logged before their "out"
    unmapped_types: set[str] = set()
    period_start: dict[Any, Any] = {}  # period -> the clock its "period start" action reads (12:00, 10:00, 5:00)
    actions = sorted((a for a in raw_actions if isinstance(a, Mapping)), key=lambda a: a.get("orderNumber") or 0)
    for a in actions:
        kind, sub = a.get("actionType") or "", str(a.get("subType") or "").lower()
        key = (a.get("period"), a.get("clock"), a.get("teamId"))
        if kind == "period" and sub == "start":
            period_start[a.get("period")] = a.get("clock")
        if kind == "substitution" and sub == "in":
            if open_subs.get(key):
                _fill_sub(open_subs[key].pop(0), a, ids)
            else:
                waiting_ins.setdefault(key, []).append(a)
            continue
        if kind in _FOLDED or f"{kind}|{sub}" in _FOLDED:
            continue
        text, exact = _espn_type(a)
        if not exact:
            unmapped_types.add("/".join(str(a.get(k) or "") for k in ("actionType", "subType", "descriptor")))
        row = _play_row(a, eid, text, regulation, period_start, by_live_team)
        if kind == "substitution":
            _open_sub(row, a, ids, waiting_ins.get(key), open_subs.setdefault(key, []))
        elif parts := _participants(a, ids):
            row["participants"] = [{"athlete": {"id": i}} for i in parts]
        plays.append(row)
    for row in plays:
        row.pop("_out_name", None)
    return plays, unmapped_types, any(open_subs.values()) or any(waiting_ins.values())


def _play_row(
    a: Mapping[str, Any], eid: str, text: str, regulation: int, period_start: dict, by_live_team: dict
) -> dict[str, Any]:
    """One ESPN play from one liveData action (participants are added by :func:`_plays`)."""
    kind = a.get("actionType") or ""
    shot = kind in _POINTS
    made = shot and a.get("shotResult") == "Made"
    clock = a.get("clock")
    if kind == "jumpball" and a.get("descriptor") == "startperiod":
        # ESPN stamps the opening tip with the period's start, not the clock at the recovery (26 of 26)
        clock = period_start.get(a.get("period"), clock)
    row: dict[str, Any] = {
        "id": f"{eid}{a.get('actionNumber')}",
        "sequenceNumber": str(a.get("actionNumber")),
        "type": {"id": ESPN_TYPE_IDS[text], "text": text},
        "text": a.get("description") or "",
        "awayScore": int(a.get("scoreAway") or 0),
        "homeScore": int(a.get("scoreHome") or 0),
        "period": {"number": a.get("period"), "displayValue": _period_label(int(a.get("period") or 0), regulation)},
        "clock": {"displayValue": _clock(clock)},
        "scoringPlay": made,
        "scoreValue": _POINTS[kind] if made else 0,
        "shootingPlay": shot,
        "pointsAttempted": _POINTS.get(kind, 0),
        # ESPN's whole-second wallclock: 2025-11-08T00:10:50.0Z -> 2025-11-08T00:10:50Z
        "wallclock": f"{a['timeActual'][:19]}Z" if a.get("timeActual") else None,
        "coordinate": dict(_NO_COORDINATE),
    }
    if kind in ("2pt", "3pt") and a.get("xLegacy") is not None and a.get("yLegacy") is not None:
        # same sign and frame as ESPN (module docstring): tenths of a foot from the rim -> feet from (25, 0)
        row["coordinate"] = {"x": 25 + a["xLegacy"] / 10, "y": a["yLegacy"] / 10}
    if a.get("teamId") in by_live_team:
        row["team"] = {"id": by_live_team[a["teamId"]]}
    return row


def _open_sub(row: dict, out: Mapping[str, Any], ids: _Ids, waiting: Optional[list], pending: list) -> None:
    """Start a substitution row at its ``out`` action; pair it with an ``in`` already logged, else wait for one."""
    out_id, out_name = ids.player(out.get("personId"), out.get("playerNameI"))
    row["participants"] = [{"athlete": {"id": None}}, {"athlete": {"id": out_id}}]
    row["_out_name"] = out_name
    if waiting:
        _fill_sub(row, waiting.pop(0), ids)
    else:
        pending.append(row)


def _fill_sub(row: dict, action_in: Mapping[str, Any], ids: _Ids) -> None:
    """Complete a substitution row with its incoming player (ESPN: ``[in, out]``, "X enters the game for Y")."""
    in_id, in_name = ids.player(action_in.get("personId"), action_in.get("playerNameI"))
    row["participants"][0] = {"athlete": {"id": in_id}}
    row["text"] = f"{in_name} enters the game for {row.get('_out_name')}"


def _idmap_lookup(league: str, idmap: Any = None) -> IdLookup:
    """``(kind, liveData id) -> id-map row`` from a local id-map frame, else the Data API (``SDV_DATA_API_URL``).

    Raises:
        ValueError: Neither an ``idmap`` frame nor ``SDV_DATA_API_URL`` is available.
    """
    from sportsdataverse.basketball.sources.idmap import _resolve
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    base_url = os.environ.get(IDMAP_BASE_URL_ENV)
    if idmap is None and not base_url:
        raise ValueError(f"no id map: pass payload['idmap'] or set {IDMAP_BASE_URL_ENV}")
    cache: dict[tuple[str, Any], Optional[Mapping[str, Any]]] = {}

    def lookup(kind: str, value: Any) -> Optional[Mapping[str, Any]]:
        if (kind, value) not in cache:
            cache[(kind, value)] = _resolve(league, value, kind=kind, frm="source", idmap=idmap, base_url=base_url)
        return cache[(kind, value)]

    return lookup


def _live_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="live"`` (NBA, WNBA). Registered in :mod:`...sources.dispatch`.

    ``ctx.payload`` (all keys optional) is ``{"playbyplay": ..., "boxscore": ..., "idmap": polars frame}``;
    a missing payload is fetched from cdn.nba.com / cdn.wnba.com, and a missing id map is read from the
    Data API (``SDV_DATA_API_URL``).

    Raises:
        SourceUnavailable: no id map, the ESPN game is not in it, a fetch failed, or the payloads do not
            project (no game object, a team the id map does not know).
    """
    from sportsdataverse.basketball.sources.dispatch import AdaptedGame
    from sportsdataverse.football.sources.dispatch import SourceUnavailable
    from sportsdataverse.nba.nba_live import _fetch_live

    payload = dict(ctx.payload or {})
    try:
        lookup = _idmap_lookup(league, payload.get("idmap"))
        row = _resolve_game(league, espn_id, payload.get("idmap"))
    except Exception as exc:  # an id-map miss or Data API error hands over to the next source
        raise SourceUnavailable(f"{league} {espn_id}: {type(exc).__name__}: {exc}") from exc
    if not row or not row.get("source_id"):
        raise SourceUnavailable(f"{league} {espn_id}: ESPN game not in the id map (no liveData game id)")
    game_id = str(row["source_id"])
    try:
        pbp = payload.get("playbyplay") or _fetch_live("playbyplay", game_id, league=league)
        box = payload.get("boxscore") or _fetch_live("boxscore", game_id, league=league)
        # an injected payload of another game would be served under this ESPN id (dispatch checks header.id only)
        for name, body in (("playbyplay", pbp), ("boxscore", box)):
            got = str(((body or {}).get("game") or {}).get("gameId") or "")
            if got != game_id:
                raise ValueError(f"{name} payload is game {got!r}, the id map says {game_id}")
        summary, notes = _live_to_espn_summary(pbp, box, espn_id=espn_id, season=int(row["season"]), lookup=lookup)
    except Exception as exc:  # network / NoDataError / projection -> the next source
        raise SourceUnavailable(f"{league} {espn_id}: liveData {game_id}: {type(exc).__name__}: {exc}") from exc
    return AdaptedGame(
        summary=summary, native_ids={"espn_event_id": str(espn_id), "live_game_id": game_id}, notes=notes
    )


def _resolve_game(league: str, espn_id: int, idmap: Any) -> Optional[Mapping[str, Any]]:
    from sportsdataverse.basketball.sources.idmap import _resolve
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV

    return _resolve(league, espn_id, kind="game", frm="espn", idmap=idmap, base_url=os.environ.get(IDMAP_BASE_URL_ENV))


def _register_live() -> None:
    """Register :func:`_live_adapter` for NBA and WNBA (called on import)."""
    from sportsdataverse.basketball.sources.dispatch import _register

    for league in ("nba", "wnba"):
        _register(league, "live")(_live_adapter)


_register_live()
