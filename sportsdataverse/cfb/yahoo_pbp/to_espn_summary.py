"""Yahoo shangrila playbook rows -> an ESPN-summary-shaped dict ``CFBPlayProcess`` consumes unchanged.

Yahoo states a play's **pre-snap situation** (down, distance, yards to the end zone, clock,
possession, the score, a typed play kind) and nothing about how it ended, so this module owns
the re-skin: ESPN's play-type vocabulary, ESPN's absolute yard line, the end state read off the
next snap, the PAT folded into its touchdown, the drive grouping and a renderable header. The
output is validated by
:func:`sportsdataverse.football.sources.contract._validate_summary` and consumed through
``espn_cfb_pbp(summary=)``; the dispatcher registers it as ``source="yahoo"`` for the CFB
(:mod:`sportsdataverse.football.sources.dispatch`).

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.cfb``:
like the Shield adapter this is a private, experimental surface, so no codegen/doc regeneration
is involved and the reference docs stay untouched.

Documented divergences from a real ESPN summary (measured, Stage 2 ``s2-yahoo-cfb`` gate):

* **No ESPN play ids.** Yahoo's ``playId`` is its own sequence, so the emitted id is
  ``{espn_event_id}{playId:04d}``: stable, unique and increasing, but it joins nothing
  ESPN-sourced. A parity comparison has to join on game state.
* **Text grammar is Yahoo's**, not ESPN's ("rushed for 5 yard gain" vs "rush middle for 5
  yards gain to the ALA48"), so every column the processor regexes out of the text degrades.
  The list is :data:`...contract.KNOWN_LOSSY` for ``("cfb", "yahoo")`` and dispatch stamps it
  into provenance; game state, EP, EPA and WP do not depend on it.
* **No FCS-hosted coverage in any era**, and no plays before 2014. Yahoo answers HTTP 200 for
  both with a real game object carrying no ``playByPlay``; the adapter detects that shape and
  hands over (:func:`...fetch._has_plays`).
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids, and Yahoo carries
  its own player-id space.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sportsdataverse.cfb.yahoo_pbp.fetch import _fetch_playbook_boxscore, _game_block, _has_plays, _resolve_game_id

#: ESPN CFB ``type.id`` -> ``(type.text, type.abbreviation)``. Enumerated from the 24 real ESPN
#: college-football summaries captured for the Stage 2 parity gate (2024-2026) plus the ESPN NFL
#: table for the scoring variants CFB shares; ESPN itself ships a null ``abbreviation`` on the
#: types mapped to ``None``. Game on Paper dereferences ``type.id`` and ``type.abbreviation``
#: unguarded, so both keys are always emitted.
ESPN_PLAY_TYPES: Dict[str, Tuple[str, Optional[str]]] = {
    "2": ("End Period", "EP"),
    "3": ("Pass Incompletion", None),
    "5": ("Rush", "RUSH"),
    "7": ("Sack", None),
    "8": ("Penalty", "PEN"),
    "9": ("Fumble Recovery (Own)", None),
    "14": ("Punt Return", None),
    "18": ("Blocked Field Goal", "BFG"),
    "20": ("Safety", "SF"),
    "21": ("Timeout", "TO"),
    "24": ("Pass Reception", "REC"),
    "26": ("Pass Interception Return", "INTR"),
    "29": ("Fumble Recovery (Opponent)", None),
    "32": ("Kickoff Return Touchdown", "TD"),
    "34": ("Punt Return Touchdown", "TD"),
    "36": ("Interception Return Touchdown", "TD"),
    "38": ("Blocked Field Goal Touchdown", "TD"),
    "39": ("Fumble Return Touchdown", "TD"),
    "52": ("Punt", "PUNT"),
    "53": ("Kickoff", "K"),
    "59": ("Field Goal Good", "FG"),
    "60": ("Field Goal Missed", "FGM"),
    "63": ("Interception", "INT"),
    "65": ("End of Half", "EH"),
    "66": ("End of Game", "EG"),
    "67": ("Passing Touchdown", "TD"),
    "68": ("Rushing Touchdown", "TD"),
    "69": ("Fumble", "F"),
}

#: Yahoo ``playTypeId`` values that ESPN folds into the touchdown they follow.
_PAT_TYPES = frozenset({"EXTRA_POINT_ATTEMPT", "TWO_POINT_PASS", "TWO_POINT_RUSH", "TWO_POINT_ATTEMPT"})
#: ESPN ``type.text`` values that stop the clock rather than describing a snap. The end state of
#: the play *before* one of these is the next real snap's spot, not the stoppage row's.
_STOPPAGE = frozenset({"Timeout", "End Period", "End of Half", "End of Game"})

_PLAYER_REF_RE = re.compile(r"\[(ncaaf\.p\.\d+)\]")
_WHITESPACE_RE = re.compile(r"\s+")
_CLOCK_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_FG_YARDS_RE = re.compile(r"(?i)\b(\d{1,2})[- ]yard field goal")
_RECOVERED_RE = re.compile(r"\[(ncaaf\.p\.\d+)\] recovered fumble")
_PAT_GOOD_RE = re.compile(r"(?i)\bmade PAT\b|\bPAT is good\b")
_TWO_POINT_GOOD_RE = re.compile(r"(?i)2pt attempt converted")
_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}


def _clock(clock: Optional[str]) -> str:
    """Yahoo ``"07:12"`` -> ESPN ``"7:12"``; anything unparseable becomes ``"0:00"``."""
    match = _CLOCK_RE.match(str(clock or "").strip())
    return f"{int(match.group(1))}:{match.group(2)}" if match else "0:00"


def _lineups(game: Mapping[str, Any]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """``(player id -> display name, player id -> Yahoo team id)`` from both lineups.

    Yahoo writes play text with id placeholders (``"[ncaaf.p.455472] rushed for 3 yard gain"``)
    and resolves them from the same payload, so a game whose lineups are empty (every season
    before 2017) keeps the placeholders rather than losing the play.
    """
    names: Dict[str, str] = {}
    teams: Dict[str, str] = {}
    for side in ("homeTeamLineup", "awayTeamLineup"):
        for entry in game.get(side) or []:
            player = (entry or {}).get("player") or {}
            pid = player.get("playerId")
            if not pid:
                continue
            if player.get("displayName"):
                names[pid] = player["displayName"]
            if player.get("teamId"):
                teams[pid] = player["teamId"]
    return names, teams


def _text(raw: Optional[str], names: Mapping[str, str]) -> str:
    """Yahoo play text with ``[ncaaf.p.N]`` placeholders resolved to display names."""
    flat = _WHITESPACE_RE.sub(" ", str(raw or "").replace("\r", " ").replace("\n", " ")).strip()
    return _PLAYER_REF_RE.sub(lambda m: names.get(m.group(1), m.group(1)), flat)


def _end_of_period_type_id(text: str, period: Optional[int]) -> str:
    """Yahoo's five end-of-period texts -> ESPN's three end types."""
    low = text.lower()
    if "game" in low:
        return "66"
    if "half" in low or period == 2:
        return "65"
    return "2"


def _fumble_type_id(play: Mapping[str, Any], text: str, player_team: Mapping[str, str], scoring: bool) -> str:
    """Own vs opponent fumble recovery, from the recovering player's team."""
    match = _RECOVERED_RE.search(str(play.get("text") or ""))
    recovered_by = player_team.get(match.group(1)) if match else None
    own = recovered_by is None or recovered_by == play.get("teamId")
    if scoring:
        return "39"
    return "9" if own else "29"


def _type_id(play: Mapping[str, Any], text: str, player_team: Mapping[str, str]) -> str:
    """The ESPN ``type.id`` for one Yahoo play row.

    Scoring rows are typed from ``scoringPlayInfo`` rather than the text: a touchdown scored by
    the team that did **not** snap the ball (a pick six, a fumble return) is a different ESPN
    type from the same play kind without the score, and typing it off possession alone would
    both mislabel it and, through :func:`_fill_end_state`, flip its end spot 100 yards.
    """
    kind = str(play.get("playTypeId") or "")
    info = play.get("scoringPlayInfo") or {}
    score_type = str(info.get("scoreTypeId") or "")
    touchdown = score_type == "TOUCHDOWN"
    low = text.lower()
    if kind == "END_OF_PERIOD":
        return _end_of_period_type_id(text, play.get("period"))
    if kind == "TIMEOUT":
        return "21"
    if score_type == "SAFETY":
        return "20"
    if kind == "PENALTY":
        return "8"
    if kind == "FIELD_GOAL_ATTEMPT":
        if touchdown:
            return "38"
        if score_type == "FIELD_GOAL":
            return "59"
        return "18" if "block" in low else "60"
    if kind == "PUNT":
        return "34" if touchdown else "52"
    if kind == "KICKOFF":
        return "32" if touchdown else "53"
    if kind == "PASS_INTERCEPTED":
        return "36" if touchdown else "63"
    if kind == "FUMBLE" or "recovered fumble" in low:
        return _fumble_type_id(play, text, player_team, touchdown)
    if kind == "SACK":
        return "7"
    if kind in ("PASS_INCOMPLETE", "SPIKE"):
        return "3"
    if kind == "PASS":
        return "67" if touchdown else "24"
    if kind == "RUSH":
        return "68" if touchdown else "5"
    # An unmapped Yahoo kind must not silently become a snap: Penalty is the processor's own
    # neutral row (no rush/pass/kick flags fire on it), which is what an unknown row deserves.
    return "8"


def _point_after(play: Mapping[str, Any], text: str) -> Dict[str, Any]:
    """ESPN's ``pointAfterAttempt`` for a Yahoo PAT row (``abbreviation`` + ``value`` are read)."""
    if str(play.get("playTypeId") or "").startswith("TWO_POINT"):
        good = bool(_TWO_POINT_GOOD_RE.search(text))
        return {
            "id": 62 if good else 63,
            "text": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "abbreviation": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "value": 2 if good else 0,
        }
    good = bool(_PAT_GOOD_RE.search(text))
    return {
        "id": 61 if good else 64,
        "text": "Extra Point Good" if good else "Extra Point Missed",
        "abbreviation": "Extra Point Good" if good else "Extra Point Missed",
        "value": 1 if good else 0,
    }


def _stat_yardage(play: Mapping[str, Any], type_id: str, text: str) -> int:
    """ESPN's ``statYardage``: Yahoo's ``yards``, except a field goal, which ESPN books as its distance."""
    if type_id in ("59", "60", "18", "38"):
        match = _FG_YARDS_RE.search(text)
        if match:
            return int(match.group(1))
    try:
        return int(play.get("yards") or 0)
    except (TypeError, ValueError):
        return 0


def _down_distance_text(
    down: Optional[int], distance: Optional[int], to_endzone: Optional[int], spot: Optional[str]
) -> Optional[str]:
    """``"2nd & Goal at UK 3"`` -- the only ``downDistanceText`` the processor reads (its goal test)."""
    if not down:
        return None
    label = _ORDINAL.get(int(down), f"{int(down)}th")
    goal_to_go = to_endzone is not None and distance is not None and distance >= to_endzone
    togo = "Goal" if goal_to_go else str(int(distance or 0))
    return f"{label} & {togo}" + (f" at {spot}" if spot else "")


def _fill_end_state(plays: List[Dict[str, Any]], home_id: str, scoring_teams: Mapping[str, str]) -> None:
    """Fill every play's ``end`` from the **next** snap's start, across drive boundaries.

    Yahoo states no end state at all, so the next snap is it -- which is how ESPN builds its
    own, and why the search must not stop at a drive boundary: a punt ends in the receiving
    team's frame, on the first row of the next drive. Clock stoppages carry no state of their
    own and are skipped, so the end spot of the play before a timeout is the ball's real spot.

    Two rows instead take ESPN's own scoring convention, measured on the captured ESPN
    summaries: a **touchdown** ends at the goal line the scoring team was attacking
    (``down -1``, ``yardsToEndzone 0``) credited to **the team the feed says scored** -- the
    defence on a pick six, where crediting the offence would flip the spot 100 yards -- and a
    **made field goal** ends at the ensuing kickoff spot (``down -1``, ``distance -1``,
    ``yardsToEndzone 65``) credited to the kicking team.
    """
    for i, play in enumerate(plays):
        nxt = next((p["start"] for p in plays[i + 1 :] if p["type"]["text"] not in _STOPPAGE), play["start"])
        type_id = play["type"]["id"]
        if play["scoringPlay"] and play["type"]["abbreviation"] == "TD":
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": 0,
                "yardLine": 100 if str(team) == str(home_id) else 0,
                "yardsToEndzone": 0,
                "team": {"id": team},
            }
            continue
        if type_id == "59":
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": -1,
                "yardLine": 35 if str(team) == str(home_id) else 65,
                "yardsToEndzone": 65,
                "team": {"id": team},
            }
            continue
        play["end"] = {
            "down": nxt["down"],
            "distance": nxt["distance"],
            "yardLine": nxt["yardLine"],
            "yardsToEndzone": nxt["yardsToEndzone"],
            "team": {"id": nxt["team"]["id"]},
        }


def _competitor(
    side: str,
    order: int,
    espn_team_id: str,
    team: Mapping[str, Any],
    score: Any,
    linescore: Sequence[Mapping[str, Any]],
    winner: bool,
) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    ``team.name`` is the mascot, and it must not be empty: the processor charges a ``Timeout``
    row to whichever club's name parts the text contains, and ``""`` is contained in every
    string, so an empty mascot charges every timeout to both clubs.
    """
    full_name = str(team.get("fullName") or "").strip()
    location = str(team.get("location") or team.get("displayName") or "").strip()
    mascot = full_name[len(location) :].strip() if full_name.startswith(location) else ""
    mascot = mascot or location or str(team.get("abbreviation") or "")
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:23~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(winner),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:23~t:{espn_team_id}",
            "location": location or mascot,
            "name": mascot,
            "nickname": team.get("displayName") or location,
            "abbreviation": team.get("abbreviation") or (mascot[:3].upper() if mascot else None),
            "displayName": full_name or location,
            "shortDisplayName": team.get("displayName") or mascot,
            "color": team.get("primaryColor"),
            "alternateColor": team.get("secondaryColor"),
            "logos": [],
        },
        "score": str(score if score is not None else 0),
        "linescores": [{"displayValue": str((q or {}).get("score") or 0)} for q in linescore or []],
        "record": [],
    }


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
        detail = (
            f"{clock or '0:00'} - {_ORDINAL.get(period, str(period))}" if period <= 4 else f"{clock or '0:00'} - OT"
        )
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


_ODDS_RE = re.compile(r"(?i)(-?\d+(?:\.\d+)?)\s*,\s*O/U\s*(\d+(?:\.\d+)?)")


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


def _drive(
    event_id: str,
    index: int,
    drive: Mapping[str, Any],
    plays: List[Dict[str, Any]],
    abbr: Optional[str],
    open_drive: bool,
) -> Dict[str, Any]:
    """One ``drives.previous[]`` / ``drives.current`` entry; ESPN's drive id is ``{event}{1-based index}``."""
    result = drive.get("result")
    if open_drive:
        # the open drive has not ended, so it states no outcome -- whatever a stored payload
        # says. ESPN's own live feed carries none either, and Game on Paper's DriveRow falls
        # back to exactly this label.
        result = "In Progress"
    return {
        "id": f"{event_id}{index}",
        "description": f"{drive.get('numPlays')} plays, {drive.get('yards')} yards, {drive.get('duration')}",
        "team": {
            "shortDisplayName": abbr,
            "displayName": abbr,
            "name": abbr,
            "abbreviation": abbr,
        },
        "start": {
            "period": {"number": (plays[0]["period"]["number"] if plays else None), "type": "quarter"},
            "clock": {"displayValue": plays[0]["clock"]["displayValue"] if plays else "0:00"},
            "yardLine": plays[0]["start"]["yardLine"] if plays else None,
            "text": drive.get("yardLine"),
        },
        "end": {
            "period": {"number": (plays[-1]["period"]["number"] if plays else None), "type": "quarter"},
            "clock": {"displayValue": plays[-1]["clock"]["displayValue"] if plays else "0:00"},
            "yardLine": plays[-1]["end"]["yardLine"] if plays else None,
        },
        "timeElapsed": {"displayValue": drive.get("duration")},
        "yards": drive.get("yards"),
        "isScore": str(result or "").upper() in ("TD", "FG", "TOUCHDOWN", "FIELD GOAL"),
        "offensivePlays": drive.get("numPlays"),
        "result": result,
        "shortDisplayResult": result,
        "displayResult": result,
        "plays": plays,
    }


def _yahoo_to_espn_summary(
    payload: Mapping[str, Any], idmap_row: Mapping[str, Any], *, odds: Optional[Mapping[str, Any]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Yahoo CFB game (final or in progress) onto an ESPN-summary-shaped dict.

    Args:
        payload: A shangrila ``playbookBoxscore`` / ``playbookBoxscorePoll`` body (the
            ``{"data": {"games": [...]}}`` envelope) or the game object itself.
        idmap_row: The game's id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`). ``espn_event_id``,
            ``home_espn_team_id`` and ``away_espn_team_id`` are required -- Yahoo's team ids are
            its own, and ``CFBPlayProcess`` casts ``team.id`` to ``int`` and uses it for logos,
            possession and the box score, so they must be ESPN's.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` (a stored closing
            line). Becomes the summary's one-provider ``pickcenter``; when None, Yahoo's own
            pregame line is used if it states one.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_cfb_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: an unresolved player-name map, a PAT with no touchdown to fold into, plays outside the drive chart, and the open drive synthesized for a live game. |

    Raises:
        KeyError: ``idmap_row`` is missing ``espn_event_id`` or a team id.
        ValueError: the payload carries no game object.

    Example:
        Adapt a stored Yahoo final and process it::

            import json
            from sportsdataverse.cfb import CFBPlayProcess
            from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import _yahoo_to_espn_summary

            with open("yahoo_ncaaf.g.202609120069.json") as fh:
                payload = json.load(fh)
            row = {"espn_event_id": "401856674", "home_espn_team_id": "96", "away_espn_team_id": "333"}
            summary, notes = _yahoo_to_espn_summary(payload, row)
            proc = CFBPlayProcess(gameId=401856674, join_participants=False)
            proc.espn_cfb_pbp(summary=summary)
            result = proc.run_processing_pipeline()
    """
    game = _game_block(payload) if "playByPlay" not in payload else dict(payload)
    if game is None:
        raise ValueError("payload carries no shangrila game object")
    notes: List[str] = []

    event_id = str(idmap_row["espn_event_id"])
    home_id, away_id = str(idmap_row["home_espn_team_id"]), str(idmap_row["away_espn_team_id"])
    espn_by_yahoo = {str(game.get("homeTeamId")): home_id, str(game.get("awayTeamId")): away_id}
    if len(espn_by_yahoo) < 2 or "None" in espn_by_yahoo:
        # A null Yahoo team id must not become a dict KEY: with both sides null the mapping
        # collapses to one entry, every play's start.team.id is the same club and the whole
        # scoreboard is credited to one side -- silently, since the contract's all-null test
        # only fires at a null rate of 1.0.
        raise ValueError("payload states no home/away team id: possession cannot be attributed")

    names, player_team = _lineups(game)
    if not names:
        notes.append("no lineups in the payload: play text keeps Yahoo's [ncaaf.p.N] placeholders")

    plays_raw = sorted(
        (p for p in (game.get("playByPlay") or []) if isinstance(p, dict) and p.get("playId") is not None),
        key=lambda p: p["playId"],
    )
    # Yahoo repeats the last regulation row as both "End of Regulation" and "End of Game"; ESPN
    # emits one. Drop the first of the pair so the play counts line up.
    texts = [str(p.get("text") or "") for p in plays_raw]
    plays_raw = [
        p
        for i, p in enumerate(plays_raw)
        if not (
            texts[i].strip().lower() == "end of regulation"
            and i + 1 < len(texts)
            and texts[i + 1].strip().lower() == "end of game"
        )
    ]

    emitted: List[Dict[str, Any]] = []
    by_play_id: Dict[Any, Dict[str, Any]] = {}
    scoring_teams: Dict[str, str] = {}
    last_touchdown: Optional[int] = None
    # Carried state. A single null in one of these turns pandas' json_normalize column into
    # float64 (the processor's incompletion UDF is declared Int64) and makes the processor
    # forward-fill possession itself, silently; carrying the last known value keeps every
    # column an integer, exactly as ESPN's own feed does on its stateless rows.
    last_team: Optional[str] = None
    last_home_score, last_away_score = 0, 0
    unmapped_team_rows = 0
    for play in plays_raw:
        text = _text(play.get("text"), names)
        info = play.get("scoringPlayInfo") or {}
        kind = str(play.get("playTypeId") or "")
        if kind in _PAT_TYPES:
            # ESPN folds the try into its touchdown: same play id, one text, one score step.
            # The try is NOT always the row right after the touchdown -- a timeout, a penalty
            # on the try or a replay review sits between them -- so this anchors on the newest
            # touchdown rather than on ``emitted[-1]``, which would hang ``pointAfterAttempt``
            # off a Timeout row and leave the touchdown stepping the scoreboard by 6.
            if last_touchdown is None:
                notes.append(f"play {play.get('playId')}: {kind} with no touchdown to fold into")
                continue
            target = emitted[last_touchdown]
            if text and text not in target["text"]:
                target["text"] = f"{target['text']} ({text})"
            target["pointAfterAttempt"] = _point_after(play, text)
            # the try's own score is the post-try one; carry it onto the touchdown and any row
            # between them, so the scoreboard never steps backwards
            for later in emitted[last_touchdown:]:
                later["homeScore"], later["awayScore"] = play.get("homeScore"), play.get("awayScore")
            continue

        type_id = _type_id(play, text, player_team)
        label, abbreviation = ESPN_PLAY_TYPES[type_id]
        team_id = espn_by_yahoo.get(str(play.get("teamId")))
        if team_id is None:
            # Yahoo ships an occasional row credited to the placeholder club ``ncaaf.t.0``
            # ("Player incomplete pass"). Carry the previous row's possession rather than
            # emitting a null: a null start.team.id is what the processor forward-fills
            # anyway, but on the way it nulls start.yardLine and floats the whole column.
            team_id = last_team
            unmapped_team_rows += 1
        last_team = team_id or last_team
        to_endzone = play.get("yardsToEndzone")
        yard_line = None
        if to_endzone is not None and team_id is not None:
            yard_line = (100 - int(to_endzone)) if team_id == home_id else int(to_endzone)
        down = int(play.get("down") or 0)
        distance = int(play.get("distance") or 0)
        scoring = bool(play.get("isScoring"))
        # The row's own score is the score BEFORE the try; scoringPlayInfo states the score
        # after the whole scoring sequence, which is what ESPN puts on the folded row.
        home_score = info.get("homeScore") if scoring and info else play.get("homeScore")
        away_score = info.get("awayScore") if scoring and info else play.get("awayScore")
        home_score = last_home_score if home_score is None else int(home_score)
        away_score = last_away_score if away_score is None else int(away_score)
        last_home_score, last_away_score = home_score, away_score
        espn_play: Dict[str, Any] = {
            "id": f"{event_id}{int(play['playId']):04d}",
            "sequenceNumber": str(play["playId"]),
            "type": {"id": type_id, "text": label, "abbreviation": abbreviation},
            "text": text,
            "awayScore": away_score,
            "homeScore": home_score,
            "period": {"number": play.get("period")},
            "clock": {"displayValue": _clock(play.get("clock"))},
            "scoringPlay": scoring,
            "priority": scoring,
            "statYardage": _stat_yardage(play, type_id, text),
            "start": {
                "down": down,
                "distance": distance,
                "yardLine": yard_line,
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(down, distance, to_endzone, play.get("yardLine")),
                "team": {"id": team_id},
            },
            "end": {},
        }
        scoring_team = espn_by_yahoo.get(str(info.get("scoringTeamId") or ""))
        if scoring_team:
            scoring_teams[espn_play["id"]] = scoring_team
        emitted.append(espn_play)
        by_play_id[play["playId"]] = espn_play
        if str(info.get("scoreTypeId") or "") == "TOUCHDOWN":
            last_touchdown = len(emitted) - 1

    if unmapped_team_rows:
        notes.append(
            f"{unmapped_team_rows} plays name a team id the header does not carry "
            "(Yahoo's ncaaf.t.0 placeholder): possession carried from the preceding play"
        )
    _fill_end_state(emitted, home_id, scoring_teams)

    # drives: Yahoo's own playList, with any row it lists nowhere (a PAT's neighbours, a
    # between-drives administrative row) attached to the drive that precedes it.
    drives_raw = [d for d in (game.get("drives") or []) if isinstance(d, dict)]
    owner: Dict[Any, Any] = {}
    for drive in drives_raw:
        for pid in drive.get("playList") or []:
            owner[pid] = drive.get("driveId")
    last_owner = drives_raw[0].get("driveId") if drives_raw else None
    orphans = 0
    for play in plays_raw:
        pid = play["playId"]
        if pid not in owner:
            owner[pid] = last_owner
            orphans += 1
        last_owner = owner[pid]
    if orphans:
        notes.append(f"{orphans} plays fell outside the drive chart and were attached to the preceding drive")

    abbr_by_espn = {
        home_id: (game.get("homeTeam") or {}).get("abbreviation"),
        away_id: (game.get("awayTeam") or {}).get("abbreviation"),
    }
    grouped: Dict[Any, List[Dict[str, Any]]] = {}
    for play in plays_raw:
        emitted_play = by_play_id.get(play["playId"])
        if emitted_play is not None:
            grouped.setdefault(owner.get(play["playId"]), []).append(emitted_play)
    final = str(game.get("status") or "").upper().startswith("FINAL")
    drives: List[Dict[str, Any]] = []
    for index, drive in enumerate(drives_raw, start=1):
        plays = grouped.get(drive.get("driveId")) or []
        if not plays:
            continue
        open_drive = not final and index == len(drives_raw)
        drives.append(
            _drive(event_id, index, drive, plays, abbr_by_espn.get(str(plays[0]["start"]["team"]["id"])), open_drive)
        )
    current: Optional[Dict[str, Any]] = None
    if drives and not final:
        # a live payload's last drive is still open: it becomes ``drives.current`` so Game on
        # Paper renders it as the current drive and the processor still sees its plays. Its
        # last play keeps its own start as its end (there is no next snap yet).
        current = drives.pop()
        notes.append(f"game is {game.get('status')}: the open drive was moved to drives.current")

    return {
        "boxscore": {"teams": [], "players": []},
        "format": {"regulation": {"periods": int(len(game.get("regulationPeriods") or []) or 4)}},
        "gameInfo": {"venue": _venue(game), "attendance": None},
        "drives": {"previous": drives, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds or _odds_from_game(game, espn_by_yahoo, home_id)),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(game, event_id, home_id, away_id),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }, notes


def _venue(game: Mapping[str, Any]) -> Dict[str, Any]:
    """``gameInfo.venue`` -- Yahoo states ``coverType``, which is the only roof signal any feed gives."""
    venue = game.get("venue") or {}
    return {
        "id": venue.get("venueId"),
        "fullName": venue.get("displayName"),
        "address": {"city": venue.get("city"), "state": venue.get("state"), "country": venue.get("country")},
        "indoor": (str(venue.get("coverType") or "").upper() in ("DOME", "INDOOR", "RETRACTABLE")) or None,
    }


def _header(game: Mapping[str, Any], event_id: str, home_id: str, away_id: str) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status, all from the payload."""
    home_score, away_score = game.get("homeScore"), game.get("awayScore")
    final = str(game.get("status") or "").upper().startswith("FINAL")
    return {
        "id": event_id,
        "uid": f"s:20~l:23~e:{event_id}",
        "season": {"year": game.get("season"), "type": 3 if str(game.get("seasonPhase") or "") == "POSTSEASON" else 2},
        "week": game.get("week"),
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:23~e:{event_id}~c:{event_id}",
                "date": game.get("startTime"),
                "neutralSite": False,
                "conferenceCompetition": False,
                "boxscoreAvailable": False,
                "commentaryAvailable": False,
                "liveAvailable": False,
                "onWatchESPN": False,
                "recent": False,
                "boxscoreSource": "none",
                "playByPlaySource": "full",
                "status": _status(game),
                "competitors": [
                    _competitor(
                        "home",
                        0,
                        home_id,
                        game.get("homeTeam") or {},
                        home_score,
                        game.get("homeLineScore") or [],
                        final and (home_score or 0) > (away_score or 0),
                    ),
                    _competitor(
                        "away",
                        1,
                        away_id,
                        game.get("awayTeam") or {},
                        away_score,
                        game.get("awayLineScore") or [],
                        final and (away_score or 0) > (home_score or 0),
                    ),
                ],
            }
        ],
    }


def _current_cfb_seasons() -> Tuple[int, ...]:
    """``(previous, current)`` CFB seasons -- the window the crosswalk asset is read over."""
    from sportsdataverse.cfb.cfb_schedule import most_recent_cfb_season

    season = int(most_recent_cfb_season())
    return (season - 1, season)


def _yahoo_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="yahoo"`` (CFB). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (:class:`...dispatch.SourceUnavailable`) when the Yahoo game
    id cannot be resolved without inventing one, when the id map states no ESPN team ids, when
    the fetch fails or comes back without the shangrila envelope (a rate limit answers with a
    23-byte ``text/html`` body), and -- the case three of the five CFB sources answer with
    HTTP 200 -- when Yahoo returns a real game object carrying **no plays**: every FCS-hosted
    game, in every era, and every game before Yahoo's 2014 play floor.
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    payload = ctx.payload
    if payload is None:
        # Resolution runs only on the fetch path: an injected payload already IS the game, and
        # resolving anyway would make an offline replay read a release asset over the network.
        game_id, resolved_by = _resolve_game_id(espn_id, ctx.idmap_row, _current_cfb_seasons())
        if not game_id:
            raise SourceUnavailable(f"cfb {espn_id}: no yahoo_game_id in the id map and none computable")
        live = str((ctx.idmap_row or {}).get("status") or "").upper() not in ("FINAL", "FINAL_OVERTIME")
        try:
            payload = _fetch_playbook_boxscore(game_id, live=live)
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"yahoo playbookBoxscore fetch failed: {type(exc).__name__}: {exc}") from exc
    else:
        game_id, resolved_by = None, "payload"
    game = _game_block(payload) if "playByPlay" not in payload else payload
    if game is None:
        # not a game payload at all: a 429's 23-byte text/html body, an error envelope, a
        # truncated response. Worth another try later; never parsed as an empty game.
        raise SourceUnavailable(f"cfb {espn_id}: yahoo returned no shangrila game object (rate limit or error body)")
    if not _has_plays(game):
        # checked BEFORE the id-map fields: "Yahoo does not cover this game" is a fact about
        # the source and is true whatever the row holds, and it is the message the fall-through
        # log should carry for every FCS-hosted game.
        raise SourceUnavailable(
            f"cfb {espn_id}: yahoo game {game_id or game.get('gameId')} carries no play-by-play "
            "(FCS-hosted, or before Yahoo's 2014 play floor)"
        )
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        raise SourceUnavailable(f"cfb {espn_id}: id-map row carries no ESPN team ids")

    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row)
    summary, notes = _yahoo_to_espn_summary(game, row, odds=odds)
    served = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in served):
        # every play fell outside the drive chart, or the chart is empty: the contract would
        # fail on ``drives[].plays[]``; say why instead. Checked over BOTH groupings, since a
        # live game's only drive is the open one, which lives under ``current``.
        raise SourceUnavailable(f"cfb {espn_id}: yahoo drive chart carries no plays yet")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=ctx.odds_override,
        native_ids={
            "espn_event_id": str(espn_id),
            "yahoo_game_id": game_id or game.get("gameId"),
            "yahoo_id_resolved_by": resolved_by,
        },
        notes=notes,
    )


def _register_yahoo() -> None:
    """Register :func:`_yahoo_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("cfb", "yahoo")(_yahoo_adapter)


_register_yahoo()
