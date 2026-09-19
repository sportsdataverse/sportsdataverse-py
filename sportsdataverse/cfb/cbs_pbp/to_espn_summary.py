"""CBS NAPI college-football rows -> an ESPN-summary-shaped dict ``CFBPlayProcess`` consumes.

CBS states a play's **pre-snap situation** plus a list of typed ``subplays`` carrying the
participants and the yardage, and nothing about how the play ended, so this module owns the
re-skin: ESPN's play-type vocabulary, ESPN's absolute yard line, the end state read off the
next snap, the PAT folded into its touchdown, the running score, the admin rows CBS never
emits, the drive grouping and a renderable header. The output is validated by
:func:`sportsdataverse.football.sources.contract._validate_summary` and consumed through
``espn_cfb_pbp(summary=)``; dispatch registers it as ``source="cbs"`` for the CFB.

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.cfb``:
like the Shield and Yahoo adapters this is a private, experimental surface, so no codegen or
reference-doc regeneration is involved.

How a spot is read, which is the whole game with CBS:

* **every subplay states its yardage in its own team's frame** and names that team
  (``team_in_possession``), so the snap's spot is ``subplays[0].yards_to_endzone`` flipped
  ``100 - x`` exactly when that subplay's team is not the offence. That is what makes an
  interception (whose first subplay is already the intercepting club's) and a punt (whose
  return subplay is the receiving club's) come out right without an era rule -- the NFL twin
  has to vote on the game because its rows do not carry the frame;
* **the offence is the drive's team, never the row's** ``team_in_possession``, which is the
  club that *ended* the play: on a turnover, a flagged snap and every kickoff it names the
  wrong side, and reading it inverts ``pos_team``, the spot and therefore EP / EPA / WP;
* **a field goal's spot is the kick spot, 8 yards behind where ESPN puts the snap**, so the
  line of scrimmage is read from the kick's own distance in the text (``distance - 18``,
  :data:`_FG_ESPN_OFFSET`), which is stated in every era CBS covers.

Documented divergences from a real ESPN summary (measured, ``s2-cbs-cfb`` gate):

* **No ESPN play ids.** A CBS CFB play ``id`` is an epoch-ms stamp (2020+) or a plain
  sequence (2019), neither of which is ESPN's, so the emitted id is ``{espn_event_id}{n:04d}``
  in play order: stable, unique and increasing, but it joins nothing ESPN-sourced. A parity
  comparison has to join on game state.
* **Text grammar is CBS's**, not ESPN's ("rushed for 5 yards. Tackled by ..." vs "rush right
  for 5 yards gain to the TEX09"), so every column the processor regexes out of the text
  degrades -- :data:`...contract.KNOWN_LOSSY` for ``("cfb", "cbs")``, stamped into provenance.
  Game state, EP, EPA and WP do not depend on it.
* **No FCS-hosted coverage, and none before 2018.** CBS answers those with a ``404`` body and
  no ``plays`` key; the adapter detects that shape and hands over.
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids, and CBS carries
  its own player-id space.

# ponytail: the fetch/scrape shape here is a CFB copy of ``nfl/cbs_pbp`` (PR #542, unmerged
# when this was written), which cannot be imported without depending on an open branch.
# TODO: once #542 lands, lift the league-neutral half into ``football/cbs_common.py``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sportsdataverse.cfb.cbs_pbp.teams import _cbs_team
from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import (
    ESPN_PLAY_TYPES,
    _clock,
    _down_distance_text,
    _pickcenter,
)

#: Subplay types that are a point-after try rather than a play of their own (ESPN folds them).
#: A **two-point** try is not among them: CBS types it as the plain ``Rush`` / ``CompletePass``
#: it was and marks it only in the text, so :data:`_TRY_TEXT_RE` is the other half of the test.
#: Missing that, a converted two-pointer is emitted as a second scoring play worth 6.
_PAT_SUBPLAYS = frozenset({"PointAfterTouchdown", "MissedPointAfterTouchdown"})
#: CBS's own marker for a two-point try, in every era measured ("TWO-POINT CONVERSION ATTEMPT.").
_TRY_TEXT_RE = re.compile(r"(?i)\btwo[- ]point conversion attempt\b|\b2[- ]?point conversion attempt\b")
#: ESPN ``type.text`` values that stop the clock rather than describing a snap. The end state
#: of the play *before* one of these is the next real snap's spot, not the stoppage's.
_STOPPAGE = frozenset({"Timeout", "End Period", "End of Half", "End of Game"})
#: ...except these two, which close a **half**. The next snap is then the other side's
#: kickoff in a new half, 65 yards from a different end zone, so a play that runs the clock
#: out takes its end from its own yardage instead. Measured: reading through the boundary put
#: the last snap of a half at the ensuing kickoff's spot on every such row.
_HALF_BOUNDARY = frozenset({"End of Half", "End of Game"})
#: ESPN type ids on which the offence keeps the ball where it left it, so a play with no next
#: snap still has a derivable end spot. Every other type changes possession or is a kick,
#: where the play's own yardage says nothing about where the next team starts.
_KEEPS_THE_BALL = frozenset({"3", "5", "7", "24"})

_WHITESPACE_RE = re.compile(r"\s+")
#: GSIS/StatCrew jersey prefix CBS keeps and ESPN does not (``"2-S.Patterson"``, 2019 and
#: earlier). ``Center-D.Riggs`` is deliberately untouched -- the prefix must be digits.
_JERSEY_RE = re.compile(r"\b\d{1,3}-(?=[A-Z])")
#: A replay reversal: CBS keeps the overturned narrative and appends the ruling after it.
_REVERSED_RE = re.compile(r"(?i)\bwas\s+REVERSED\.\s*")
#: The kick's own distance, in both CBS grammars: "26 yard field goal attempt" (2020+) and
#: "28 yards Field Goal is Good." (2019).
_FG_YARDS_RE = re.compile(r"(?i)\b(\d{1,3})\s*-?\s*yards?\s+field\s+goal")
_SAFETY_RE = re.compile(r"(?i)\bsafety\b")
#: The kicker's name at the head of a CBS try row ("C.Hawkins extra point is good.").
_TRY_KICKER_RE = re.compile(r"^([A-Z][\w'.\-]*(?:\s[A-Z][\w'.\-]+)*)\s+extra point")
_BLOCKED_RE = re.compile(r"(?i)\bblock")
_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}

#: CBS ``score_type`` -> points. ``PointAfterTouchdown`` is folded, never scored here.
_POINTS = {"Touchdown": 6, "FieldGoal": 3, "Safety": 2}

#: ESPN's college feed books a field goal at ``kick distance - 18`` yards to the end zone
#: (measured: 23 of 27 kicks matched by distance across the evidence capture, the rest split
#: 0/+2), while CBS states the **kick spot**, a flat ``kick distance - 10``. So the text's own
#: distance is the oracle, and ``kick spot - 8`` is the fallback for a payload that states none.
_FG_ESPN_OFFSET, _FG_SNAP_OFFSET = 18, 8


def _int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    """``"12"`` -> ``12``; anything unparseable -> ``default``."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _norm_text(description: Optional[str]) -> str:
    """CBS ``description`` -> ESPN ``text``: reversal tail, jersey prefixes, whitespace.

    Order matters: the reversal tail is cut **first**, so the jersey strip only ever runs on
    the ruling that actually stands.
    """
    text = _WHITESPACE_RE.sub(" ", str(description or "").replace("\r", " ").replace("\n", " ")).strip()
    match = None
    for match in _REVERSED_RE.finditer(text):  # noqa: B007 -- the LAST reversal is the ruling
        pass
    if match is not None:
        text = text[match.end() :].strip()
    return _JERSEY_RE.sub("", text).strip()


def _subplay_body(subplay: Mapping[str, Any]) -> Mapping[str, Any]:
    """The one nested object on a CBS subplay (``{"type": "Rush", "order": "1", "rush": {...}}``)."""
    return next((v for v in subplay.values() if isinstance(v, dict)), {})


def _subplays(play: Mapping[str, Any]) -> Tuple[List[Mapping[str, Any]], List[str]]:
    """``(subplays, their types)`` in CBS's own order."""
    raw = (play.get("subplays") or {}).get("subplay") or []
    subplays = [s for s in raw if isinstance(s, Mapping)]
    return subplays, [str(s.get("type") or "") for s in subplays]


def _score_type(play: Mapping[str, Any], types: Sequence[str], text: str) -> Optional[str]:
    """CBS's ``score_type``, or the same label derived from the subplays and the text.

    ``score_type`` is absent from every payload before about 2020 -- read literally those
    games score 0-0 and every EP / WP / score-differential column is wrong from the first
    touchdown on. The subplay list plus ``score_on_play`` is the era-stable signal: a scoring
    row that is not a try, a field goal or a safety is a touchdown, whatever case CBS wrote
    the word in ("TOUCHDOWN." in 2026, "runs 2 yards for a touchdown." in 2019).
    """
    stated = play.get("score_type")
    if stated:
        return str(stated)
    if play.get("score_on_play") != "Yes":
        return None
    first = types[0] if types else ""
    if first in _PAT_SUBPLAYS:
        return "PointAfterTouchdown"
    if first in ("FieldGoal", "MissedFieldGoal"):
        return "FieldGoal"
    if _SAFETY_RE.search(text):
        return "Safety"
    return "Touchdown"


def _espn_type_id(types: Sequence[str], score_type: Optional[str], own_recovery: bool, text: str) -> str:
    """ESPN ``type.id`` for one CBS play, from its subplay types and its scoring type.

    An interception arrives from CBS as ``Interception`` + ``InterceptionReturn``, and a punt
    is ``Punt`` whether or not it was returned -- see the note on the punt branch.
    A score turns the type into its touchdown variant, and a touchdown scored by the club that
    did **not** snap the ball is a different ESPN type from the same play without the score --
    the distinction is load-bearing, because :func:`_fill_end_state` keys the 100-yard end spot
    on it.
    """
    touchdown = score_type == "Touchdown"
    first = types[0] if types else ""
    if score_type == "Safety" or "Safety" in types:
        return "20"
    if first == "Penalty":
        return "8"
    if first == "Kickoff":
        return "32" if touchdown else "53"
    if first.endswith("Punt") or first.startswith("BlockedPunt"):
        if touchdown:
            return "34"
        # ESPN's college feed types a **returned** punt ``Punt`` (52), not ``Punt Return``:
        # measured over the evidence capture it emitted 52 on every punt in 2022-2025 (262
        # rows, zero 14s) and split 57/39 in 2026 only. 14 is therefore never emitted.
        return "52"
    if first.endswith("FieldGoal") or first.startswith("BlockedFieldGoal"):
        if touchdown:
            return "38"
        if first == "FieldGoal":
            return "59"
        return "18" if _BLOCKED_RE.search(text) else "60"
    if "Interception" in types or "InterceptionReturn" in types:
        return "36" if touchdown else "26"
    if "FumbleReturn" in types or "Fumble" in types:
        if touchdown:
            # the offence keeping its own fumble in the end zone is still a normal score
            if own_recovery:
                return "67" if first == "CompletePass" else "68"
            return "39"
        return "9" if own_recovery else "29"
    if first == "Sack":
        return "7"
    if first == "IncompletePass":
        return "3"
    if first == "CompletePass":
        return "67" if touchdown else "24"
    if first == "Rush":
        return "68" if touchdown else "5"
    # An unmapped CBS kind must not silently become a snap: Penalty is the processor's own
    # neutral row (no rush/pass/kick flag fires on it), which is what an unknown row deserves.
    return "8"


def _type_object(type_id: str) -> Dict[str, Any]:
    """``{"id", "text", "abbreviation"}`` -- Game on Paper bracket-reads all three."""
    label, abbreviation = ESPN_PLAY_TYPES[type_id]
    return {"id": type_id, "text": label, "abbreviation": abbreviation}


def _stat_yardage(types: Sequence[str], subplays: Sequence[Mapping[str, Any]]) -> int:
    """ESPN's ``statYardage`` for one CBS row.

    ESPN books the **return** on a kick or a punt and zero on an incompletion that stayed one;
    everything else is the first subplay's own yardage. The pick is not cosmetic: it is the
    official yardage of a plain play, and :func:`_trailing_end` derives the newest row's end
    spot from it.
    """
    first = types[0] if types else ""
    head = _int(_subplay_body(subplays[0]).get("yards_on_play")) if subplays else 0
    if first in ("Kickoff", "Punt"):
        returned = next((s for s, t in zip(subplays[1:], types[1:]) if t.endswith("Return")), None)
        return _int(_subplay_body(returned).get("yards_on_play")) or 0 if returned else 0
    if first in ("IncompletePass", "Penalty"):
        return 0
    if first in ("FieldGoal", "MissedFieldGoal"):
        return head or 0
    return head or 0


def _field_goal_line_of_scrimmage(text: str, kick_spot: Optional[int]) -> Optional[int]:
    """Yards to the end zone at the **snap** of a field goal.

    CBS states the kick spot, 8 yards behind where ESPN puts the snap (``kick distance - 10``
    against ESPN's ``- 18``, measured over the whole evidence capture), and the kick's own
    distance in the text is the unambiguous, era-stable oracle -- it is written in both CBS
    grammars, "26 yard field goal attempt" (2020+) and "28 yards Field Goal is Good." (2019).
    """
    match = _FG_YARDS_RE.search(text or "")
    if match:
        return max(0, min(100, int(match.group(1)) - _FG_ESPN_OFFSET))
    if kick_spot is None:
        return None
    return max(0, min(100, kick_spot - _FG_SNAP_OFFSET))


def _trailing_end(play: Dict[str, Any], home_id: str, away_id: str) -> Dict[str, Any]:
    """The end state of a play the feed states no next snap for, from its own yardage.

    Fires on the last row of every final and on the newest row of every live poll -- the row
    Game on Paper renders at the top of the page. Giving it its own start (which is what "the
    next snap, or myself" does) says the ball never moved, so a 19-yard gain is scored as a
    0-yard one against a spot no feed reports (#541 finding 1). On a plain scrimmage snap the
    offence keeps the ball, so the end is ``start - statYardage``; on a turnover or a kick the
    yardage says nothing about where the next team starts and the start spot is left alone
    rather than guessed at.

    **Fourth down short of the sticks is the exception**: the ball turns over where it lies,
    and ESPN writes that row's end in the DEFENCE's frame at 1st & 10 -- 50 of 50 such rows in
    the captured ESPN summaries, at exactly ``100 - (start - statYardage)``. Keeping the
    offence's frame is a ~100-yard, multi-EPA error on the highest-leverage event in football,
    on the newest row of every live poll (53 such snaps in the 35-game evidence capture).
    """
    start = play["start"]
    keeps = play["type"]["id"] in _KEEPS_THE_BALL
    gained = int(play.get("statYardage") or 0) if keeps else 0
    to_endzone = start["yardsToEndzone"]
    if to_endzone is not None and gained:
        to_endzone = max(0, min(100, int(to_endzone) - gained))
    team, down, distance = start["team"]["id"], start["down"], start["distance"]
    if keeps and (_int(start.get("down")) or 0) == 4 and gained < (_int(start.get("distance")) or 0):
        team = away_id if str(team) == str(home_id) else home_id
        down, distance = 1, 10
        if to_endzone is not None:
            to_endzone = 100 - to_endzone
    is_home = str(team) == str(home_id)
    return {
        "down": down,
        "distance": distance,
        "yardLine": start["yardLine"] if to_endzone is None else ((100 - to_endzone) if is_home else to_endzone),
        "yardsToEndzone": to_endzone,
        "team": {"id": team},
    }


def _fill_end_state(plays: List[Dict[str, Any]], home_id: str, away_id: str) -> None:
    """Fill every play's ``end`` from the **next** snap's start, across drive boundaries.

    CBS states no end state at all, so the next snap is it -- which is how ESPN builds its own,
    and why the search must not stop at a drive boundary: a punt ends in the receiving team's
    frame, on the first row of the next drive. Clock stoppages carry no state of their own and
    are skipped, so the end spot of the play before a timeout is the ball's real spot.

    The search **stops** at the end of a half: the next snap is then a kickoff in the other
    direction, and reading through the boundary gave every clock-killing kneel the ensuing
    kickoff's spot. Such a row takes its end from its own yardage, like the newest row of a
    live poll.

    Two rows take ESPN's own scoring convention instead, both read off captured ESPN college
    summaries: a **touchdown** ends at the goal line the scoring team was attacking
    (``down -1``, ``yardsToEndzone 0``) credited to **the club the feed says scored** -- the
    defence on a pick six, where crediting the offence flips the spot 100 yards -- and a **made
    field goal** ends where it was kicked from (``down -1``, ``distance -1``, its own spot),
    not at the ensuing kickoff, which is what the next snap would say.
    """
    for index, play in enumerate(plays):
        scoring_team = play.pop("_scoring_team", None)
        type_id = play["type"]["id"]
        if play["type"]["text"] in _STOPPAGE:
            play["end"] = {**play["start"], "team": {"id": play["start"]["team"]["id"]}}
            continue
        if play["scoringPlay"] and play["type"]["abbreviation"] == "TD":
            team = scoring_team or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": 0,
                "yardLine": 100 if str(team) == str(home_id) else 0,
                "yardsToEndzone": 0,
                "team": {"id": team},
            }
            continue
        if type_id == "59":
            play["end"] = {
                "down": -1,
                "distance": -1,
                "yardLine": play["start"]["yardLine"],
                "yardsToEndzone": play["start"]["yardsToEndzone"],
                "team": {"id": scoring_team or play["start"]["team"]["id"]},
            }
            continue
        nxt = None
        for later in plays[index + 1 :]:
            if later["type"]["text"] in _HALF_BOUNDARY:
                break
            if later["type"]["text"] not in _STOPPAGE:
                nxt = later["start"]
                break
        if nxt is None:
            play["end"] = _trailing_end(play, home_id, away_id)
            continue
        play["end"] = {
            "down": nxt["down"],
            "distance": nxt["distance"],
            "yardLine": nxt["yardLine"],
            "yardsToEndzone": nxt["yardsToEndzone"],
            "team": {"id": nxt["team"]["id"]},
        }


def _period_end_type_id(period: int, is_last: bool) -> str:
    """``End Period`` / ``End of Half`` / ``End of Game`` for the boundary after ``period``."""
    if is_last:
        return "66"
    return "65" if int(period) == 2 else "2"


def _synthesize_admin_rows(
    emitted: List[Dict[str, Any]],
    timeouts: List[Tuple[Optional[int], Optional[int]]],
    home_id: str,
    away_id: str,
    names: Mapping[str, str],
    final: bool,
    notes: List[str],
) -> List[Dict[str, Any]]:
    """Insert the admin rows CBS never emits: team timeouts and the end of each period.

    CBS ships **no** administrative row of any kind, but it states
    ``home_timeouts_remaining`` / ``away_timeouts_remaining`` on every play from about 2020,
    so a charged timeout is a decrement between consecutive rows. Without these
    ``posTeamTimeouts`` / ``defPosTeamTimeouts`` stay at 3 for the whole game -- an EP, WP and
    4th-down input -- and the lagged end clock at a quarter break comes from the next
    quarter's first snap.

    The boundary row after the **last** play is only emitted for a finished game: on a live
    poll there is no end of anything yet, and emitting one puts an "End of Game" row at the
    top of the page CBS is still sending plays for.

    **Only a decrement of exactly 1 is a timeout.** The counters glitch to ``0`` and back on
    the odd row, which read literally invents three timeouts in one gap and takes a club to
    zero for the rest of the half; a larger drop is recorded as a note instead. A row's text
    names the club the way ESPN writes it ("Timeout Ohio State, clock 3:30"), because
    ``cfb_pbp`` charges a timeout by matching that token against the header's own name parts.
    """
    out: List[Dict[str, Any]] = []
    previous = timeouts[0] if timeouts else (None, None)
    glitches = 0
    for index, play in enumerate(emitted):
        home_left, away_left = timeouts[index] if index < len(timeouts) else (None, None)
        for left, before, team_id in ((home_left, previous[0], home_id), (away_left, previous[1], away_id)):
            if left is None or before is None or left >= before:
                continue
            if before - left != 1:
                glitches += 1
                continue
            row = _admin_row(
                play, "21", f"Timeout {names.get(str(team_id), '')}, clock {play['clock']['displayValue']}"
            )
            row["start"]["team"] = {"id": team_id}
            out.append(row)
        if home_left is not None:
            previous = (home_left, previous[1])
        if away_left is not None:
            previous = (previous[0], away_left)
        out.append(play)
        this_period = (play.get("period") or {}).get("number")
        following = emitted[index + 1] if index + 1 < len(emitted) else None
        next_period = (following.get("period") or {}).get("number") if following else None
        if this_period and next_period != this_period and (following is not None or final):
            type_id = _period_end_type_id(this_period, following is None)
            end_row = _admin_row(play, type_id, _type_object(type_id)["text"])
            end_row["clock"] = {"displayValue": "0:00"}
            out.append(end_row)
    if glitches:
        notes.append(f"{glitches} implausible timeouts-remaining drops (>1 in one gap) ignored as feed glitches")
    return out


def _admin_row(template: Mapping[str, Any], type_id: str, text: str) -> Dict[str, Any]:
    """A stoppage row carrying the state of the play it follows; ``down 0`` keeps it a stoppage."""
    return {
        "id": None,
        "sequenceNumber": None,
        "type": _type_object(type_id),
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


_STATUS = {
    "FINAL": ("3", "STATUS_FINAL", "post", True, "Final"),
    "FINAL OT": ("3", "STATUS_FINAL", "post", True, "Final/OT"),
    "HALFTIME": ("23", "STATUS_HALFTIME", "in", False, "Halftime"),
    "INPROGRESS": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "IN PROGRESS": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "SCHEDULED": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
    "PREGAME": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
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


def _competitor(side: str, order: int, espn_team_id: str, score: Mapping[str, Any], winner: bool) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    The location and the mascot come from the committed ESPN <-> CBS snapshot, never from the
    payload, which states no team name at all. Neither may be empty: ``cfb_pbp`` charges a
    timeout to whichever club's name parts the row's text contains, and ``""`` is contained in
    every string, so an empty mascot charges every timeout to both clubs.
    """
    team = _cbs_team(espn_team_id)
    _cbs_id, abbreviation, location, mascot = team if team else (None, str(espn_team_id), str(espn_team_id), "")
    mascot = mascot or location
    quarters = [(score.get("quarter") or {}).get(str(q)) for q in (1, 2, 3, 4)]
    for extra in sorted(
        (k for k in (score.get("quarter") or {}) if (_int(k, 0) or 0) > 4), key=lambda k: int(_int(k, 0) or 0)
    ):
        quarters.append((score.get("quarter") or {})[extra])
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:23~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(winner),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:23~t:{espn_team_id}",
            "location": location,
            "name": mascot,
            "nickname": location,
            "abbreviation": abbreviation,
            "displayName": f"{location} {mascot}".strip(),
            "shortDisplayName": mascot,
            "color": None,
            "alternateColor": None,
            "logos": [],
        },
        "score": str(_int(score.get("total")) or 0),
        "linescores": [{"displayValue": str(_int(q) or 0)} for q in quarters],
        "record": [],
    }


def _header(
    event_id: str,
    scoreboard: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    home_id: str,
    away_id: str,
    period: Optional[int],
) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status."""
    status_block = scoreboard.get("game_status") or {}
    home_score = _int((status_block.get("homescore") or {}).get("total")) or 0
    away_score = _int((status_block.get("awayscore") or {}).get("total")) or 0
    final = str(status_block.get("status") or "").upper().startswith("FINAL")
    return {
        "id": event_id,
        "uid": f"s:20~l:23~e:{event_id}",
        "season": {"year": _int(idmap_row.get("season"), None), "type": _int(idmap_row.get("season_type"), None) or 2},
        "week": _int(idmap_row.get("week"), None),
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:23~e:{event_id}~c:{event_id}",
                "date": idmap_row.get("kickoff_utc"),
                "neutralSite": bool(idmap_row.get("neutral_site")),
                "conferenceCompetition": False,
                "boxscoreAvailable": False,
                "commentaryAvailable": False,
                "liveAvailable": False,
                "onWatchESPN": False,
                "recent": False,
                "boxscoreSource": "none",
                "playByPlaySource": "full",
                "status": _status(status_block, period),
                "competitors": [
                    _competitor(
                        "home", 0, home_id, status_block.get("homescore") or {}, final and home_score > away_score
                    ),
                    _competitor(
                        "away", 1, away_id, status_block.get("awayscore") or {}, final and away_score > home_score
                    ),
                ],
            }
        ],
    }


def _drive_teams(plays: Sequence[Mapping[str, Any]], drives: Optional[Sequence[Mapping[str, Any]]]) -> Dict[str, str]:
    """CBS drive id -> the CBS team id that had the ball on that drive.

    The **drive** owns possession, not the play row: CBS's top-level ``team_in_possession`` is
    the club that ended the play, so an interception, a lost fumble and every kickoff name the
    wrong side there. The drives resource states it directly; without it (CBS 404s drives
    before 2019) the drive's own non-kick plays vote, which is right as long as most of a
    drive is not a turnover.

    A play whose ``drive_id`` the resource does not list (47 of 6,090 rows in the evidence
    capture -- a try after a blocked-punt return, a dead-ball penalty) is **not** in the map,
    and the caller attaches it to the drive of the play before it rather than falling back to
    its own post-play possession, which is the club that did not have the ball.
    """
    stated = {str(d.get("id")): str(d.get("team_id")) for d in (drives or []) if d.get("team_id")}
    if stated:
        return stated
    votes: Dict[str, Dict[str, int]] = {}
    for play in plays:
        _subs, types = _subplays(play)
        if types and types[0] in ("Kickoff", "Punt"):
            continue
        team = str(play.get("team_in_possession") or "")
        if not team:
            continue
        drive = str(play.get("drive_id") or "")
        votes.setdefault(drive, {})
        votes[drive][team] = votes[drive].get(team, 0) + 1
    return {drive: max(counts, key=lambda team: counts[team]) for drive, counts in votes.items() if counts}


def _drive(
    event_id: str, index: int, cbs_drive: Mapping[str, Any], plays: List[Dict[str, Any]], abbr: Optional[str]
) -> Dict[str, Any]:
    """One ``drives.previous[]`` / ``drives.current`` entry; ESPN's drive id is ``{event}{n}``."""
    result = cbs_drive.get("result")
    first, last = (plays[0] if plays else {}), (plays[-1] if plays else {})
    return {
        "id": f"{event_id}{index}",
        "description": f"{cbs_drive.get('drive_plays')} plays, {cbs_drive.get('yards_on_drive')} yards, "
        f"{cbs_drive.get('time_of_possession')}",
        "team": {"shortDisplayName": abbr, "displayName": abbr, "name": abbr, "abbreviation": abbr},
        "start": {
            "period": {
                "number": _int(cbs_drive.get("quarter"), None) or (first.get("period") or {}).get("number"),
                "type": "quarter",
            },
            "clock": {"displayValue": _clock(str(cbs_drive.get("starting_time") or "0:00"))},
            "yardLine": (first.get("start") or {}).get("yardLine"),
            "text": cbs_drive.get("starting_yardline"),
        },
        "end": {
            "period": {"number": (last.get("period") or {}).get("number"), "type": "quarter"},
            "clock": {"displayValue": _clock(str(cbs_drive.get("ending_time") or "0:00"))},
            "yardLine": (last.get("end") or {}).get("yardLine"),
            "text": cbs_drive.get("ending_yardline"),
        },
        "timeElapsed": {"displayValue": cbs_drive.get("time_of_possession")},
        "yards": _int(cbs_drive.get("yards_on_drive")),
        "isScore": cbs_drive.get("score_on_drive") == "Yes",
        "offensivePlays": _int(cbs_drive.get("drive_plays")),
        "result": result,
        "shortDisplayResult": result,
        "displayResult": result,
        "plays": plays,
    }


def _venue(game: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """``gameInfo.venue`` from the optional ``resource/game/{id}`` body's ``meta``."""
    meta = (game or {}).get("meta") or {}
    return {
        "id": (game or {}).get("venueId"),
        "fullName": meta.get("stadiumName"),
        "address": {"city": meta.get("stadiumCity"), "state": meta.get("stadiumState"), "country": None},
        "indoor": None,
    }


def _cbs_cfb_to_espn_summary(
    plays: Sequence[Mapping[str, Any]],
    drives: Optional[Sequence[Mapping[str, Any]]],
    scoreboard: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    *,
    game: Optional[Mapping[str, Any]] = None,
    odds: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one CBS NAPI college-football game (final or in progress) onto an ESPN summary.

    Args:
        plays: the ``plays`` list of ``resource/game/scoring/plays/{cbs_game_id}``.
        drives: the ``drives`` list of ``resource/game/scoring/drives/{cbs_game_id}``, or None
            -- CBS 404s that resource before 2019, and the drives are then rebuilt from the
            plays' own ``drive_id``.
        scoreboard: the ``scoreboard`` block of ``resource/game/scoring/scoreboard/{id}``.
        idmap_row: the game's id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`). ``espn_event_id``,
            ``home_espn_team_id`` and ``away_espn_team_id`` are required -- CBS's team ids are
            its own, and ``CFBPlayProcess`` casts ``team.id`` to ``int`` and uses it for logos,
            possession and the box score, so they must be ESPN's. ``season``, ``season_type``,
            ``week``, ``kickoff_utc`` and ``neutral_site`` fill the header.
        game: the optional ``resource/game/{cbs_game_id}`` body, for the venue.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` (a stored closing
            line). Becomes the summary's one-provider ``pickcenter``.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_cfb_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a try with no touchdown to fold into, a running score that does not reach the scoreboard's own final, an absent drives resource, plays on a drive the drives resource omits, ignored timeout glitches, a payload with no timeout counters, and the open drive moved to `drives.current`. |

    Raises:
        KeyError: ``idmap_row`` is missing ``espn_event_id`` or a team id.
        ValueError: the scoreboard block states no home/away CBS team id (possession could
            not be attributed to either club).
    """
    notes: List[str] = []
    event_id = str(idmap_row["espn_event_id"])
    home_id, away_id = str(idmap_row["home_espn_team_id"]), str(idmap_row["away_espn_team_id"])
    home_cbs = str((scoreboard.get("hometeam") or {}).get("id") or "")
    away_cbs = str((scoreboard.get("awayteam") or {}).get("id") or "")
    if not home_cbs or not away_cbs or home_cbs == away_cbs:
        # A missing CBS team id must not become a dict KEY: with both sides null the mapping
        # collapses to one entry, every play's start.team.id is the same club and the whole
        # scoreboard is credited to one side -- silently, since the contract's all-null test
        # only fires at a null rate of 1.0.
        raise ValueError("CBS scoreboard states no home/away team id: possession cannot be attributed")
    espn_of = {home_cbs: home_id, away_cbs: away_id}
    names = {home_id: (_cbs_team(home_id) or (0, "", "", ""))[2], away_id: (_cbs_team(away_id) or (0, "", "", ""))[2]}

    ordered = sorted(
        (p for p in plays if isinstance(p, Mapping) and p.get("id") is not None), key=lambda p: _int(p.get("id")) or 0
    )
    drive_teams = _drive_teams(ordered, drives)
    drive_meta = {str(d.get("id")): d for d in (drives or []) if isinstance(d, Mapping)}

    emitted: List[Dict[str, Any]] = []
    timeouts: List[Tuple[Optional[int], Optional[int]]] = []
    drive_of: List[str] = []
    home_points = away_points = 0
    last_touchdown: Optional[int] = None
    last_yard_line: Optional[int] = None
    last_team: Optional[str] = None
    last_drive: Optional[str] = None
    no_counters = True

    for play in ordered:
        subplays, types = _subplays(play)
        text = _norm_text(play.get("description"))
        score_type = _score_type(play, types, text)
        drive_id = str(play.get("drive_id") or "")
        if drive_id not in drive_teams and last_drive is not None:
            # CBS files the odd row (a try after a blocked-punt return, a dead-ball penalty)
            # under a drive it never lists. It belongs to the drive it follows; reading its own
            # post-play possession instead put it in the other club's frame, 100 yards away.
            drive_id = last_drive
        last_drive = drive_id
        kickoff = bool(types) and types[0] == "Kickoff"
        owner = drive_teams.get(drive_id)
        if owner:
            # ESPN credits a kickoff to the KICKING team; CBS files it under the receiving
            # team's drive, so the kickoff row is the one place the drive's owner is wrong.
            offense_cbs = (home_cbs if owner == away_cbs else away_cbs) if kickoff else owner
        else:
            offense_cbs = str(play.get("team_in_possession") or "")
        espn_offense = espn_of.get(offense_cbs) or last_team
        last_team = espn_offense or last_team

        two_point = bool(_TRY_TEXT_RE.search(text))
        if (types and types[0] in _PAT_SUBPLAYS) or two_point:
            # ESPN folds the try into its touchdown: one row, one text, one score step. The try
            # is NOT always the row right after the touchdown -- a timeout, a penalty on the try
            # or a replay review sits between them -- so this anchors on the newest touchdown
            # rather than on ``emitted[-1]``, which would hang the try off a stoppage row and
            # leave the touchdown stepping the scoreboard by 6.
            if last_touchdown is None:
                notes.append(f"play {play.get('id')}: point-after row with no touchdown to fold into")
                continue
            target = emitted[last_touchdown]
            good = play.get("score_on_play") == "Yes"
            points, after = _point_after(two_point, text, good)
            suffix = _try_suffix(two_point, text, good)
            if suffix not in target["text"]:
                target["text"] = f"{target['text'].rstrip('.')}. {suffix}".strip()
            if after is not None:
                target["pointAfterAttempt"] = after
            if str(target.get("_scoring_team") or "") == home_id:
                home_points += points
            else:
                away_points += points
            # carry the post-try score onto the touchdown and every row between them, so the
            # scoreboard never steps backwards
            for later in emitted[last_touchdown:]:
                later["homeScore"], later["awayScore"] = home_points, away_points
            continue

        settled = [s for s, t in zip(subplays, types) if t != "Penalty"]
        ended_with = (
            str(_subplay_body(settled[-1]).get("team_in_possession") or offense_cbs) if settled else offense_cbs
        )
        own_recovery = ended_with == offense_cbs
        meta = drive_meta.get(drive_id) or {}
        if score_type == "Touchdown" and meta.get("result"):
            # CBS's subplays name the club that had the ball on a strip-sack return as the
            # OFFENCE (2 of the 35 captured games), which types a defensive touchdown as a
            # rushing one and credits it to the side that lost the ball -- a 14-EPA error on
            # the row. The drive's own outcome states it without ambiguity: a drive that ends
            # "Fumble"/"Interception" while the play scored is a score by the defence.
            own_recovery = meta.get("score_on_drive") == "Yes" and str(meta["result"]).lower() == "touchdown"
        to_endzone = _spot(subplays, types, offense_cbs, text)
        is_home = espn_offense == home_id
        yard_line = None if to_endzone is None else ((100 - to_endzone) if is_home else to_endzone)
        if yard_line is None:
            # A row that states no spot carries the ball's last known spot rather than a null:
            # one null turns pandas' json_normalize column into float64 and the processor's
            # incompletion UDF, declared Int64, raises SchemaError and loses the whole game.
            yard_line = last_yard_line
            to_endzone = None if yard_line is None else ((100 - yard_line) if is_home else yard_line)
        else:
            last_yard_line = yard_line
        down = _int(play.get("down")) or 0
        distance_raw = play.get("distance")
        goal_to_go = str(distance_raw) == "Goal"
        distance = (to_endzone or 0) if goal_to_go else (_int(distance_raw) or 0)
        if kickoff:
            # CBS writes 0/0 on a kickoff; ESPN's own college feed writes 1st & 10.
            down, distance = 1, 10
        type_id = _espn_type_id(types, score_type, own_recovery, text)
        scored = score_type in _POINTS and play.get("score_on_play") == "Yes"
        credited: Optional[str] = espn_offense
        if scored:
            if score_type == "Safety":
                credited = away_id if is_home else home_id
            elif score_type == "Touchdown" and not own_recovery:
                credited = away_id if is_home else home_id
            home_points += _POINTS[str(score_type)] if credited == home_id else 0
            away_points += _POINTS[str(score_type)] if credited != home_id else 0
        row: Dict[str, Any] = {
            "id": None,
            "sequenceNumber": None,
            "type": _type_object(type_id),
            "text": text,
            "awayScore": away_points,
            "homeScore": home_points,
            "period": {"number": _int(play.get("quarter"), None)},
            "clock": {"displayValue": _clock(str(play.get("time_remaining") or "0:00"))},
            "scoringPlay": bool(scored),
            "priority": bool(scored),
            "statYardage": _stat_yardage(types, subplays),
            "wallclock": play.get("real_clock"),
            "start": {
                "down": down,
                "distance": distance,
                "yardLine": yard_line,
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(
                    down, distance, to_endzone, f"{play.get('side')} {play.get('yardline')}"
                ),
                "team": {"id": espn_offense},
            },
            "end": {},
        }
        if scored:
            row["_scoring_team"] = credited
        emitted.append(row)
        home_left = _int(play.get("home_timeouts_remaining"), None)
        away_left = _int(play.get("away_timeouts_remaining"), None)
        no_counters = no_counters and home_left is None and away_left is None
        timeouts.append((home_left, away_left))
        drive_of.append(drive_id)
        if score_type == "Touchdown" and scored:
            last_touchdown = len(emitted) - 1

    if no_counters and emitted:
        notes.append("payload states no timeouts-remaining counters: no timeout rows were synthesized")
    status_block = scoreboard.get("game_status") or {}
    final = str(status_block.get("status") or "").upper().startswith("FINAL")
    drive_by_row = {id(row): drive for row, drive in zip(emitted, drive_of)}
    all_rows = _synthesize_admin_rows(emitted, timeouts, home_id, away_id, names, final, notes)
    _fill_end_state(all_rows, home_id, away_id)
    for number, row in enumerate(all_rows, start=1):
        row["id"] = f"{event_id}{number:04d}"
        row["sequenceNumber"] = str(number * 100)

    # a synthesized admin row belongs to the drive of the play it follows
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    current_drive = drive_of[0] if drive_of else ""
    for row in all_rows:
        current_drive = drive_by_row.get(id(row), current_drive)
        grouped.setdefault(current_drive, []).append(row)

    drives_raw = [d for d in (drives or []) if isinstance(d, Mapping)]
    if not drives_raw:
        notes.append("drives resource absent: drive meta rebuilt from the plays' own drive_id")
    meta_by_id = {str(d.get("id")): d for d in drives_raw}
    orphans = [key for key in grouped if key not in meta_by_id]
    if drives_raw and orphans:
        notes.append(f"{sum(len(grouped[k]) for k in orphans)} plays are on drives the drives resource omits")
    # every drive the PLAYS name, in play order, whether or not the drives resource lists it:
    # dropping the group would silently lose whatever it scored
    ordered_drives = [(key, meta_by_id.get(key) or {"id": key}) for key in grouped]

    stated_home = _int((status_block.get("homescore") or {}).get("total"), None)
    stated_away = _int((status_block.get("awayscore") or {}).get("total"), None)
    if (
        final
        and (stated_home, stated_away) != (None, None)
        and (stated_home, stated_away) != (home_points, away_points)
    ):
        notes.append(
            f"running score rebuilt from the plays ({home_points}-{away_points}) does not match CBS's own "
            f"final ({stated_home}-{stated_away})"
        )

    previous: List[Dict[str, Any]] = []
    for index, (key, meta) in enumerate(ordered_drives, start=1):
        rows = grouped.get(key) or []
        owner = espn_of.get(str(meta.get("team_id") or drive_teams.get(key) or ""))
        previous.append(
            _drive(event_id, index, meta, rows, (_cbs_team(owner) or (0, None, "", ""))[1] if owner else None)
        )
    current: Optional[Dict[str, Any]] = None
    if previous and not final:
        # a live payload's last drive is still open: it becomes ``drives.current`` so Game on
        # Paper renders it as the current drive and the processor still sees its plays. The
        # drive has not ended, so it states no outcome, whatever a stored payload says.
        current = previous.pop()
        current["result"] = current["shortDisplayResult"] = current["displayResult"] = "In Progress"
        current["isScore"] = False
        notes.append(f"game is {status_block.get('status')}: the open drive was moved to drives.current")
    period = next((r["period"]["number"] for r in reversed(all_rows) if (r.get("period") or {}).get("number")), None)

    return {
        "boxscore": {"teams": [], "players": []},
        "format": {"regulation": {"periods": 4}},
        "gameInfo": {"venue": _venue(game), "attendance": None},
        "drives": {"previous": previous, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(event_id, scoreboard, idmap_row, home_id, away_id, period),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }, notes


def _spot(subplays: Sequence[Mapping[str, Any]], types: Sequence[str], offense_cbs: str, text: str) -> Optional[int]:
    """Yards to the end zone at the snap, in the **offence's** frame.

    Every CBS subplay states its yardage in its own club's frame and names that club, so the
    flip is a fact from the payload rather than an era rule: an interception's first subplay
    already belongs to the intercepting club, and its ``yards_to_endzone`` is that club's.
    A field goal is corrected from the kick spot back to the line of scrimmage.
    """
    if not subplays:
        return None
    body = _subplay_body(subplays[0])
    value = _int(body.get("yards_to_endzone"), None)
    if value is None:
        return None
    first = types[0] if types else ""
    if first.endswith("FieldGoal") or first.startswith("BlockedFieldGoal"):
        # A kick is the one row whose subplay names the RECOVERING club on a block while still
        # stating the yardage in the kicking team's frame, so it is never flipped: a blocked
        # 26-yarder came out 84 yards from the end zone instead of 8, a 5-point EP swing on
        # the play before it.
        return _field_goal_line_of_scrimmage(text, value)
    if first == "Penalty":
        # A CBS penalty subplay names the **penalised** club (and on some rows the offence),
        # while its ``yards_to_endzone`` stays in the offence's frame. Reading the club as the
        # frame mirrored the pre-snap spot on every flag against the defence -- and, through
        # the next row's end state, the play before it too: the largest single class of end-spot
        # divergence in the evidence capture.
        return max(0, min(100, value))
    stated_for = str(body.get("team_in_possession") or offense_cbs)
    if stated_for and offense_cbs and stated_for != offense_cbs:
        value = 100 - value
    return max(0, min(100, value))


def _try_suffix(two_point: bool, text: str, good: bool) -> str:
    """The try, written the way ESPN's college feed writes it on the touchdown's own text.

    ``CFBPlayProcess`` reads the **text** for a try, not ``pointAfterAttempt``: ESPN folds the
    college try into the touchdown as ``(Name KICK)`` / ``(Name PAT MISSED)`` and marks a
    two-pointer with the word "conversion" plus, when it failed, "failed"
    (``__add_xp_suffix_cols``; the scoring-value branches at ``cfb_pbp.py:6310+``). CBS's own
    wording matches none of those, so a folded try in CBS's grammar left the touchdown scored
    as a bare 6 -- and a failed two-pointer scored as a converted one, 2 EPA on the row.
    """
    if two_point:
        return "TWO-POINT CONVERSION ATTEMPT SUCCEEDS." if good else "TWO-POINT CONVERSION ATTEMPT FAILED."
    match = _TRY_KICKER_RE.match(text.strip())
    kicker = match.group(1) if match else ""
    return f"({kicker} KICK)".strip() if good else f"({kicker} PAT MISSED)".strip()


def _point_after(two_point: bool, text: str, good: bool) -> Tuple[int, Optional[Dict[str, Any]]]:
    """``(points, ESPN pointAfterAttempt)`` for a folded CBS try row.

    Whether the try was good is CBS's own ``score_on_play``, not a phrase in the text: the
    wording moved between eras ("extra point is good", "TWO-POINT ATTEMPT SUCCEEDS", "extra
    point is no good") while the flag did not.
    """
    if two_point:
        return (2 if good else 0), {
            "id": 62 if good else 63,
            "text": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "abbreviation": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "value": 2 if good else 0,
        }
    blocked = "blocked" in text.lower()
    return (1 if good else 0), {
        "id": 61 if good else 64,
        "text": "Extra Point Good" if good else "Extra Point Missed",
        "abbreviation": "Extra Point Good" if good else ("Extra Point Blocked" if blocked else "Extra Point Missed"),
        "value": 1 if good else 0,
    }


def _napi_list(payload: Any, key: str) -> Optional[List[Mapping[str, Any]]]:
    """The ``key`` list out of a NAPI body, or None.

    CBS answers a game it does not carry with an ``{"errors": [{"code": 404, ...}]}`` envelope
    -- sometimes under an HTTP 404, sometimes under a 200 -- so "the key is there and is a
    list" is the only honest coverage test. Treating the status as coverage is how a
    green-but-empty game reaches the processor.
    """
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(key)
    return list(value) if isinstance(value, list) else None


def _has_coverage(bundle: Mapping[str, Any]) -> bool:
    """True when the captured bundle carries CBS play-by-play for the game.

    False is CBS saying **it does not cover this game**: every FCS-hosted game in every era,
    every non-"enhanced" FBS game, and everything before the 2018 play floor. That is a fact
    about the source, so it is checked by shape before any id-map field is read.
    """
    return bool(_napi_list(bundle.get("plays"), "plays"))


def _fetch_cbs_game(cbs_game_id: str, **kwargs: Any) -> Dict[str, Any]:
    """The NAPI bodies one college-football game needs, through the package's own getter.

    Three sequential requests on :func:`sportsdataverse.dl_utils.download`'s retry budget:
    ``plays`` and ``scoreboard`` are required, ``drives`` is optional (CBS 404s that resource
    before 2019 and the drives are rebuilt from the plays' own ``drive_id``) and so is the
    game body, which only supplies the venue. The closing line is **not** fetched: the id-map
    row already carries one for CFB (``spread_line`` / ``total_line``), and a request on Game
    on Paper's path has to earn itself.
    """
    from sportsdataverse.cbs.cbs_napi import (
        cbs_game,
        cbs_game_scoring_drives,
        cbs_game_scoring_plays,
        cbs_game_scoring_scoreboard,
    )

    out: Dict[str, Any] = {
        "cbs_game_id": str(cbs_game_id),
        "plays": cbs_game_scoring_plays(cbs_game_id, return_parsed=False, **kwargs),
        "scoreboard": cbs_game_scoring_scoreboard(cbs_game_id, return_parsed=False, **kwargs),
    }
    for key, fetch in (("drives", cbs_game_scoring_drives), ("game", cbs_game)):
        try:
            out[key] = fetch(cbs_game_id, return_parsed=False, **kwargs)
        except Exception:  # noqa: BLE001 -- an optional resource is a degradation, not a failure
            out[key] = None
    return out


def _cbs_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="cbs"`` (CFB). Registered in :mod:`...sources.dispatch`.

    Args:
        league: the dispatch league key; always ``"cfb"`` for this registration.
        espn_id: the ESPN event id the caller asked for.
        ctx: the dispatcher's :class:`...dispatch.SourceContext` -- ``payload`` (an injected
            NAPI bundle, which is what makes every test offline), ``idmap_row``,
            ``participants`` and ``odds_override``.

    Returns:
        An :class:`...dispatch.AdaptedGame` carrying the ESPN-shaped ``summary``, the
        ``native_ids`` provenance (``cbs_game_id``, how it was resolved and, on the scrape
        path, the scoreboard URL / ``data-enhanced`` flag / weeks tried) and the adapter's
        own ``notes``.

    Raises:
        SourceUnavailable: every hand-over to the next source, in the order checked --
            the payload is not a NAPI bundle; **CBS carries no play-by-play**, tested by
            shape before any id-map field, because "CBS does not cover this game" is true of
            every FCS-hosted game whatever the row says; the id-map row states no ESPN team
            ids; there is no season/week to look the game id up with; the week scoreboard
            names no card for the matchup (the id is never synthesized); the NAPI fetch
            fails or answers its own 404 envelope; the body carries no ``scoreboard`` block;
            CBS served a **different** game id than the one asked for; or the drive chart it
            did serve carries no plays yet.
    """
    from sportsdataverse.errors import NoDataError
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row

    payload = ctx.payload
    if payload is not None:
        if not isinstance(payload, Mapping):
            raise SourceUnavailable(f"cfb {espn_id}: cbs payload is {type(payload).__name__}, not a NAPI bundle")
        if not _has_coverage(payload):
            raise SourceUnavailable(
                f"cfb {espn_id}: cbs carries no play-by-play for game {payload.get('cbs_game_id')} "
                "(FCS-hosted, not enhanced, or before CBS's 2018 play floor)"
            )

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        raise SourceUnavailable(f"cfb {espn_id}: id-map row carries no ESPN team ids")

    cbs_game_id = str(payload.get("cbs_game_id")) if payload is not None and payload.get("cbs_game_id") else None
    resolved_by = "payload" if cbs_game_id else None
    if cbs_game_id is None:
        cbs_game_id, resolved_by = (row.get("cbs_game_id"), "idmap") if row.get("cbs_game_id") else (None, None)
    scoreboard_provenance: Dict[str, Any] = {}
    if cbs_game_id is None and payload is None:
        from sportsdataverse.cfb.cbs_pbp.game_id import _resolve_cbs_game_id

        if row.get("season") is None or row.get("week") is None:
            raise SourceUnavailable(f"cfb {espn_id}: no season/week to look the CBS game id up with")
        cbs_game_id, scoreboard_provenance = _resolve_cbs_game_id(
            int(row["season"]),
            int(row["week"]),
            row["home_espn_team_id"],
            row["away_espn_team_id"],
            kickoff_utc=row.get("kickoff_utc"),
        )
        resolved_by = "week_scoreboard"
        if not cbs_game_id:
            raise SourceUnavailable(f"cfb {espn_id}: no CBS game id on the week scoreboard page")

    if payload is None:
        try:
            payload = _fetch_cbs_game(str(cbs_game_id))
        except NoDataError as exc:
            # NAPI's own 404 envelope: CBS states it has no play-by-play for this game.
            raise SourceUnavailable(f"cfb {espn_id}: cbs carries no play-by-play for game {cbs_game_id}") from exc
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"cbs napi fetch failed: {type(exc).__name__}: {exc}") from exc
        if not _has_coverage(payload):
            raise SourceUnavailable(f"cfb {espn_id}: cbs carries no play-by-play for game {cbs_game_id}")

    scoreboard_body = payload.get("scoreboard")
    scoreboard = scoreboard_body.get("scoreboard") if isinstance(scoreboard_body, Mapping) else None
    if not scoreboard:
        raise SourceUnavailable(f"cfb {espn_id}: cbs carries no scoreboard block for game {cbs_game_id}")
    served = str(scoreboard.get("_id") or "")
    if cbs_game_id and served and served != str(cbs_game_id):
        # Serving it would file another game's plays under this ESPN event.
        raise SourceUnavailable(f"cfb {espn_id}: cbs served game {served}, not {cbs_game_id}")

    game_body = payload.get("game") if isinstance(payload.get("game"), Mapping) else None
    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row) or _odds_override_from_row(row)
    summary, notes = _cbs_cfb_to_espn_summary(
        _napi_list(payload.get("plays"), "plays") or [],
        _napi_list(payload.get("drives"), "drives"),
        scoreboard,
        row,
        game=game_body,
        odds=odds,
    )
    everything = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in everything):
        # the contract would fail on ``drives[].plays[]``; say why instead. Checked over BOTH
        # groupings, since a live game's only drive is the open one under ``current``.
        raise SourceUnavailable(f"cfb {espn_id}: cbs drive chart carries no plays yet")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=odds,
        native_ids={
            "espn_event_id": str(espn_id),
            "cbs_game_id": str(cbs_game_id) if cbs_game_id else served or None,
            "cbs_game_id_resolved_by": resolved_by or "payload",
            **({"cbs_scoreboard": scoreboard_provenance} if scoreboard_provenance else {}),
        },
        notes=notes,
    )


def _register_cbs() -> None:
    """Register :func:`_cbs_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("cfb", "cbs")(_cbs_adapter)


_register_cbs()
