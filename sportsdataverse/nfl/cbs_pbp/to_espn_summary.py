"""CBS NAPI NFL rows -> an ESPN-summary-shaped dict ``NFLPlayProcess`` consumes unchanged.

CBS's NFL feed is ``genius.feed.football.nfl`` -- the NFL's own GSIS feed via Genius Sports
-- so this adapter owns only the re-skin, exactly as the Shield one does:

* the play id is ``{espn_event_id}{cbs play id}``, because a CBS play ``id`` **is** the GSIS
  ``playId``; the drive id is ``{espn_event_id}{cbs drive_id}`` (both 1..N, like ESPN's);
* possession comes from the **drive** (:func:`_drive_teams`: the drives resource, else a
  per-drive majority vote of the rows), with the kickoff row credited to the kicking team --
  the row's own ``team_in_possession`` is the *post-play* team in 2019-2025 and only survives
  as the fallback and as the per-subplay settle team;
* field position is **not** a single flip rule: CBS's own ``yards_to_endzone`` frame moves
  per row, per game and per era, so a kickoff reads its spot out of the GSIS text
  (:func:`_kickoff_to_endzone`), a sack likewise (:func:`_sack_to_endzone`, because CBS
  states the spot the sack *ended* at), and a punt or a turnover picks whichever of ``x`` /
  ``100 - x`` continues its own drive (:func:`_choose_frame`);
* the PAT is its own CBS row and is folded into its touchdown, ESPN's shape;
* CBS emits **no admin rows at all** -- no timeout, no two-minute warning, no end of period
  -- so they are synthesized here from the per-play ``*_timeouts_remaining`` counters and
  the quarter boundaries. Without them the processor charges no timeout all game (an EP / WP
  / 4th-down input) and the lagged end clock is wrong at every period break.

Documented divergences from a real ESPN summary (measured; see ``s2-cbs-nfl/FINDINGS.md``):

* **No air yards, no YAC**: CBS has no such field, so ``air_yards`` / ``yards_after_catch`` /
  ``cp`` / ``cpoe`` are null and ``exp_qbr`` shifts. Declared in the contract's
  ``KNOWN_LOSSY[("nfl", "cbs")]``.
* **No formation prefix**: CBS strips ``(Shotgun)`` / ``(No Huddle)`` from the GSIS text, so
  ``shotgun`` and ``no_huddle`` degrade to whatever the rest of the text implies.
* **No ESPN athlete ids**: ``boxscore`` is empty (CBS's per-play player ids are CBS/STATS
  ids), so ``__attach_player_ids`` fills nothing. The *play* id still joins Shield and
  nflverse for free.
* **Play ids only join ESPN's from 2019**: CBS play ids are sequential ``2, 3, 4 ...`` before
  about 2019-01-01 rather than GSIS playIds. The ids stay stable and unique either way -- a
  pre-2019 game just gets a note saying they will not join an ESPN-sourced frame, and the
  parity harness pairs it on :data:`...parity.STATE_KEY` instead.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sportsdataverse.football.cbs_common import (
    _admin_row,
    _fetch_cbs_game,
    _int,
    _napi_list,
    _norm_text,
    _register_cbs,
    _status,
    _subplay_body,
    _type_object,
)
from sportsdataverse.nfl.shield_pbp.to_espn_summary import (
    ESPN_PLAY_TYPES,
    _ESPN_TEAMS,
    _clock,
    _down_distance_text,
    _drive_result,
    _quarter_end_type_id,
)

#: CBS team id -> ``(ESPN franchise id, ESPN abbreviation, location, mascot)``.
#: From NAPI ``season/teams`` for league 59 (the 32 clubs; CBS also lists AFC / NFC / HOF /
#: DRF placeholders, which never play a game). Static on purpose: it saves a request on the
#: request path and it is the same 32 rows every season.
# ponytail: a relocation keeps the CBS team id, so only the location/mascot would ever age;
# if that matters, fill the id map's TEAM_SCHEMA ``cbs_team_id`` column instead of growing this.
CBS_TEAMS: Dict[str, Tuple[str, str, str, str]] = {
    "404": ("22", "ARI", "Arizona", "Cardinals"),
    "405": ("1", "ATL", "Atlanta", "Falcons"),
    "406": ("33", "BAL", "Baltimore", "Ravens"),
    "407": ("2", "BUF", "Buffalo", "Bills"),
    "408": ("29", "CAR", "Carolina", "Panthers"),
    "409": ("3", "CHI", "Chicago", "Bears"),
    "410": ("4", "CIN", "Cincinnati", "Bengals"),
    "411": ("6", "DAL", "Dallas", "Cowboys"),
    "412": ("7", "DEN", "Denver", "Broncos"),
    "413": ("8", "DET", "Detroit", "Lions"),
    "414": ("9", "GB", "Green Bay", "Packers"),
    "415": ("11", "IND", "Indianapolis", "Colts"),
    "416": ("30", "JAX", "Jacksonville", "Jaguars"),
    "417": ("12", "KC", "Kansas City", "Chiefs"),
    "418": ("15", "MIA", "Miami", "Dolphins"),
    "419": ("16", "MIN", "Minnesota", "Vikings"),
    "420": ("17", "NE", "New England", "Patriots"),
    "421": ("18", "NO", "New Orleans", "Saints"),
    "422": ("19", "NYG", "New York", "Giants"),
    "423": ("20", "NYJ", "New York", "Jets"),
    "424": ("13", "LV", "Las Vegas", "Raiders"),
    "425": ("21", "PHI", "Philadelphia", "Eagles"),
    "426": ("23", "PIT", "Pittsburgh", "Steelers"),
    "427": ("14", "LAR", "Los Angeles", "Rams"),
    "428": ("24", "LAC", "Los Angeles", "Chargers"),
    "429": ("25", "SF", "San Francisco", "49ers"),
    "430": ("26", "SEA", "Seattle", "Seahawks"),
    "431": ("27", "TB", "Tampa Bay", "Buccaneers"),
    "432": ("10", "TEN", "Tennessee", "Titans"),
    "433": ("28", "WSH", "Washington", "Commanders"),
    "434": ("5", "CLE", "Cleveland", "Browns"),
    "247415": ("34", "HOU", "Houston", "Texans"),
}

#: CBS club codes inside the play text that ESPN spells differently. ESPN's NFL text carries
#: the legacy GSIS codes (``CLV``, ``ARZ``) which ``nfl_pbp._NFL_TEXT_TEAM_ALIASES`` already
#: folds; these two it does not, and ``JAC`` does not prefix-match ``JAX`` either, so a
#: Jacksonville game lost ``penalty_spot_side``, ``recovery_team`` and every timeout charge.
_TEXT_ABBR_FIXES = {"JAC": "JAX", "WAS": "WSH"}

#: An unresolved jersey placeholder in provisional live text (``"[14] kneels to JAC 27"``).
#: Left in the text on purpose -- no name pattern can match it, so the name column is null,
#: which is the truth until CBS re-sends the play with the name resolved.
_PLACEHOLDER_RE = re.compile(r"\[\d{1,3}\]")

#: CBS ``score_type`` -> (points, ESPN ``scoringType``).
_SCORING = {
    "Touchdown": (6, {"name": "touchdown", "displayName": "Touchdown", "abbreviation": "TD"}),
    "FieldGoal": (3, {"name": "field-goal", "displayName": "Field Goal", "abbreviation": "FG"}),
    "Safety": (2, {"name": "safety", "displayName": "Safety", "abbreviation": "SF"}),
}

#: ESPN ``type.text`` values that are clock stoppages rather than snaps (``nfl.model_vars``).
_CLOCK_STOPPAGE = frozenset(
    {"Timeout", "Official Timeout", "Two-minute warning", "End Period", "End of Half", "End of Game"}
)


#: Subplay types that are a point-after try, not a play of their own (ESPN folds them).
_PAT_SUBPLAYS = frozenset({"PointAfterTouchdown", "MissedPointAfterTouchdown"})

#: Subplay types that mean the pass was intercepted. CBS emits ``Interception`` on its own in
#: 2019-2025 and ``InterceptionReturn`` from 2026; a game can carry either or both.
_INTERCEPTION_SUBPLAYS = frozenset({"Interception", "InterceptionReturn"})

_TOUCHDOWN_RE = re.compile(r"(?i)\bTOUCHDOWN\b")
_SAFETY_RE = re.compile(r"(?i)\bSAFETY\b")
_NO_PLAY_RE = re.compile(r"(?i)-\s*No Play\b")
#: GSIS writes every sack as "... sacked at XXX 20 for -7 yards". CBS types a sack as a plain
#: ``IncompletePass`` subplay in 2019-2025 (15 rows across three sampled games), so the text is
#: the only era-stable signal that the play was a sack -- and ESPN types it 7 / 80.
_SACKED_RE = re.compile(r"(?i)\bsacked\b")
_KICK_SPOT_RE = re.compile(r"\b(?:kicks|punts)\b[^.]*?\bfrom\s+([A-Z]{2,3})\s+\d{1,2}")


def _score_type(play: Mapping[str, Any], types: Sequence[str], text: str) -> Optional[str]:
    """CBS's ``score_type``, or the same label derived from the subplays and the GSIS text.

    ``score_type`` is a **2026-only** key: across the 36-game evidence capture it is present on
    486 of 5,940 plays, all of them 2026. Read literally, every 2019-2025 game scores 0-0 and
    every EP / WP / score-differential column is wrong from the first touchdown on. The text
    is the era-stable signal -- GSIS writes ``TOUCHDOWN`` and ``SAFETY`` in capitals -- and
    ``score_on_play`` gates it, so a nullified touchdown never counts.
    """
    stated = play.get("score_type")
    if stated:
        return str(stated)
    if play.get("score_on_play") != "Yes":
        return None
    first = types[0] if types else ""
    if first in _PAT_SUBPLAYS:
        return "PointAfterTouchdown"
    if _SAFETY_RE.search(text):
        return "Safety"
    if _TOUCHDOWN_RE.search(text):
        return "Touchdown"
    if first == "FieldGoal":
        return "FieldGoal"
    return None


def _receiver_framed(plays: Sequence[Mapping[str, Any]]) -> bool:
    """True when this game states field position from the **receiving** team's side on kicks.

    CBS changed convention: through 2025 a kickoff row's ``side`` names the kicking team (the
    same club the GSIS text says the ball was kicked *from*), and from 2026 it names the
    receiving team, with ``yards_to_endzone`` flipped to match. Hard-coding either one puts
    every kick, punt and turnover 100 - x yards out of place in the other era, so the game
    votes on itself: the text's ``kicks/punts ... from {ABBR} {yd}`` is the oracle, and the
    majority of a game's own kick rows decides. A game with no parsable kick row is treated as
    not flipped, which is the convention of every season measured except 2026.
    """
    agree = disagree = 0
    for play in plays:
        subplays = (play.get("subplays") or {}).get("subplay") or []
        types = [str(s.get("type") or "") for s in subplays]
        if not types or types[0] not in ("Kickoff", "Punt"):
            continue
        match = _KICK_SPOT_RE.search(str(play.get("description") or ""))
        if not match:
            continue
        if str(play.get("side") or "") == match.group(1):
            agree += 1
        else:
            disagree += 1
    return disagree > agree


#: The kickoff spot as GSIS writes it: ``"C.Little kicks 62 yards from JAC 35 to CLE 3."``
_KICKOFF_FROM_RE = re.compile(r"\bkicks\b[^.]*?\bfrom\s+([A-Z]{2,3})\s+(\d{1,2})")

#: Club codes a GSIS play description may use for one ESPN franchise: CBS's own spellings
#: plus the gamebook legacy codes (``CLV``, ``ARZ``, ``BLT``, ``HST``, ``LA``).
_TEXT_ABBRS: Dict[str, frozenset] = {
    "5": frozenset({"CLE", "CLV"}),
    "33": frozenset({"BAL", "BLT"}),
    "22": frozenset({"ARI", "ARZ"}),
    "34": frozenset({"HOU", "HST"}),
    "14": frozenset({"LAR", "LA", "STL"}),
    "13": frozenset({"LV", "OAK"}),
    "24": frozenset({"LAC", "SD"}),
    "28": frozenset({"WAS", "WSH"}),
    "30": frozenset({"JAX", "JAC"}),
}


def _text_abbrs(espn_team_id: str, abbr: str) -> frozenset:
    return _TEXT_ABBRS.get(str(espn_team_id), frozenset({abbr}))


def _kickoff_to_endzone(text: str, kicker_abbrs: frozenset) -> Optional[int]:
    """Yards to the end zone for the **kicking** team, read off the GSIS kickoff text.

    CBS's own field position is not trustworthy on a kick: the same 2026 week measured
    ``yards_to_endzone`` for the kicking team on one game (65 from its own 35) and for the
    receiving team on another (35), with ``side``/``yardline`` flipping with it -- so a
    per-game or per-era rule is wrong on the other half. The GSIS text states the spot
    unambiguously and in every era, so a kickoff reads it instead of guessing.
    """
    match = _KICKOFF_FROM_RE.search(text or "")
    if not match:
        return None
    yards = int(match.group(2))
    return 100 - yards if match.group(1) in kicker_abbrs else yards


#: The sack spot as GSIS writes it: ``"R.Wilson sacked at NYG 14 for -9 yards"`` (``ob`` on an
#: out-of-bounds sack, ``no gain`` instead of ``0 yards``). The club code is the side of the
#: field the ball ENDED on, and the yardage is signed from the offence.
_SACK_SPOT_RE = re.compile(r"(?i)\bsacked\b(?:\s+ob)?\s+at\s+([A-Z]{2,3})\s+(\d{1,2})\s+for\s+(-?\d+|no gain)")


def _sack_to_endzone(text: str, offense_abbrs: frozenset) -> Optional[int]:
    """Pre-snap yards to the end zone on a sack, read off the GSIS text.

    CBS's own ``yards_to_endzone`` on a sack row is the spot the play **ended** at, and from
    2024 it is stated in the defence's frame as well -- measured over the 26-game capture, 24
    sack rows (15 of 16 in 2025, 8 of 30 in 2024) land 3-63 yards out of place, worth up to
    7.5 EPA on the sack and the same again on the play before it, whose end state is the
    sack's start. ``_choose_frame`` cannot repair it: the drive's own continuity picks the
    side but not the yardage. The text states both, in every era, so it is read instead:
    the ball ended at ``ABBR NN`` and the offence lost ``X``, so it was snapped ``X`` closer.
    """
    match = _SACK_SPOT_RE.search(text or "")
    if not match:
        return None
    ended = int(match.group(2))
    to_endzone = (100 - ended) if match.group(1) in offense_abbrs else ended
    gained = 0 if match.group(3).lower() == "no gain" else int(match.group(3))
    return max(min(to_endzone + gained, 100), 0)


def _choose_frame(candidate: int, expected: Optional[int]) -> int:
    """``candidate`` or ``100 - candidate``, whichever continues the drive.

    A punt or a turnover is the same offence's next snap inside its own drive, so its spot
    must be near where the previous play left the ball. CBS flips the spot on some of those
    rows and not others -- inside one season, and inside one game -- so the drive's own
    continuity picks the side rather than an era rule.
    """
    if expected is None:
        return candidate
    return candidate if abs(candidate - expected) <= abs((100 - candidate) - expected) else 100 - candidate


def _other_team(cbs_team_id: str, home_cbs_id: str, away_cbs_id: str) -> str:
    """The opposing CBS team id."""
    return away_cbs_id if str(cbs_team_id) == str(home_cbs_id) else home_cbs_id


def _drive_teams(plays: Sequence[Mapping[str, Any]], drives: Optional[Sequence[Mapping[str, Any]]]) -> Dict[str, str]:
    """CBS drive id -> the CBS team id that had the ball on that drive.

    The **drive** owns possession, not the play row: CBS's top-level ``team_in_possession`` is
    the snapping team from 2026 but the *post-play* team in 2019-2025, so an interception, a
    lost fumble and a flagged row each credit the wrong club there -- which inverts
    ``pos_team``, ``yardsToEndzone`` and therefore EP / EPA / WP on every one of them. The
    drives resource states it directly; without it (CBS 404s drives for 2018) the drive's own
    plays vote, which is right as long as most of a drive is not a turnover.
    """
    stated = {str(d.get("id")): str(d.get("team_id")) for d in (drives or []) if d.get("team_id")}
    if stated:
        return stated
    votes: Dict[str, Dict[str, int]] = {}
    for play in plays:
        subplays = (play.get("subplays") or {}).get("subplay") or []
        types = [str(s.get("type") or "") for s in subplays]
        if types and types[0] in ("Kickoff", "Punt"):
            continue
        team = str(play.get("team_in_possession") or "")
        if team:
            votes.setdefault(str(play.get("drive_id") or ""), {}).setdefault(team, 0)
            votes[str(play.get("drive_id") or "")][team] += 1
    return {drive: max(counts, key=lambda team: counts[team]) for drive, counts in votes.items() if counts}


def _espn_type_id(types: Sequence[str], score_type: Optional[str], poss_changed: bool, text: str) -> str:
    """ESPN ``type.id`` for one CBS play, from its subplay types and its scoring type.

    ESPN NFL types a punt/kickoff with a return as the kick itself (``Punt``, not ``Punt
    Return``), and an interception arrives from CBS as ``IncompletePass`` + ``InterceptionReturn``.
    A strip-sack touchdown is ESPN type 80 (``SFOP``), not a plain fumble return -- the
    distinction is load-bearing: the scoring end state keys on it.
    """
    touchdown = score_type == "Touchdown"
    first = types[0] if types else ""
    upper = text.upper()
    if "Safety" in types or score_type == "Safety":
        return "20"
    if first == "Penalty" or _NO_PLAY_RE.search(text):
        # ESPN types a play wiped out by a flag as the penalty, whatever was snapped. The
        # subplay list does not always carry the Penalty row, so the "- No Play" clause the
        # GSIS text always writes is the test.
        return "8"
    if first == "Kickoff":
        return "32" if touchdown else "53"
    if first in ("Punt", "BlockedPunt", "BlockedPuntReturn"):
        if "BLOCKED" in upper:
            return "37" if touchdown else "17"
        return "34" if touchdown else "52"
    if first in ("FieldGoal", "MissedFieldGoal", "BlockedFieldGoal", "BlockedFieldGoalReturn"):
        if "BLOCKED" in upper:
            return "38" if touchdown else "18"
        return "59" if first == "FieldGoal" else "60"
    if _INTERCEPTION_SUBPLAYS.intersection(types):
        return "36" if touchdown else "26"
    sacked = first == "Sack" or bool(_SACKED_RE.search(text))
    if "FumbleReturn" in types:
        if sacked:
            # ESPN types a strip sack 80 ("Sack Opp Fumble Recovery") only when the ball
            # actually changed hands; a sack the offence recovered itself is a plain 9.
            return "80" if (poss_changed or touchdown) else "9"
        if touchdown:
            if not poss_changed:
                return "67" if first == "CompletePass" else "68"
            return "39"
        return "29" if poss_changed else "9"
    if sacked:
        return "80" if touchdown else "7"
    if first == "IncompletePass":
        return "3"
    if first == "CompletePass":
        return "67" if touchdown else "24"
    if first == "Rush":
        return "68" if touchdown else "5"
    return "8"


def _stat_yardage(types: Sequence[str], subplays: Sequence[Mapping[str, Any]], offense: Optional[str]) -> int:
    """ESPN's ``statYardage`` for one CBS row.

    ESPN books the **return** on a kick or a punt, zero on an incompletion that stayed an
    incompletion, and a **signed** figure on a penalty-only row (CBS states an unsigned
    magnitude plus ``team_penalized``). Everything else is the first subplay's own yardage.
    The pick is not cosmetic: ``statYardage`` is the official yardage of a plain play and it
    feeds the ``downs_turnover`` test and the ``qb_epa`` re-spot.
    """
    first = types[0] if types else ""
    head = _int(_subplay_body(subplays[0]).get("yards_on_play")) if subplays else 0
    if first in ("Kickoff", "Punt"):
        if len(subplays) > 1 and "Return" in types[1]:
            return _int(_subplay_body(subplays[1]).get("yards_on_play")) or 0
        return 0
    if first == "IncompletePass" and "InterceptionReturn" not in types:
        return 0
    if first == "Penalty":
        body = _subplay_body(subplays[0])
        return -(head or 0) if str(body.get("team_penalized")) == str(offense) else (head or 0)
    return head or 0


def _point_after(pat: Optional[Mapping[str, Any]], text: str) -> Tuple[int, Optional[Dict[str, Any]]]:
    """``(points, ESPN pointAfterAttempt)`` for a folded CBS PAT row."""
    if pat is None:
        return 0, None
    lower = text.lower()
    good = "is good" in lower or "attempt succeeds" in lower
    if "two-point" in lower or "two point" in lower:
        return (2 if good else 0), {
            "id": 62 if good else 63,
            "text": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "abbreviation": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "value": 2 if good else 0,
        }
    blocked = "blocked" in lower
    return (1 if good else 0), {
        "id": 61 if good else 64,
        "text": "Extra Point Good" if good else "Extra Point Missed",
        "abbreviation": "Extra Point Good" if good else ("Extra Point Blocked" if blocked else "Extra Point Missed"),
        "value": 1 if good else 0,
    }


def _competitor(side: str, order: int, espn_team_id: str, cbs_team_id: str, score: Mapping[str, Any]) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    The mascot is never empty and the abbreviation is never invented: both come from
    :data:`CBS_TEAMS`, keyed by CBS's own team id. An empty ``team.name`` makes every Timeout
    row match both clubs, and an invented abbreviation charges none of them.
    """
    _espn, abbreviation, location, mascot = CBS_TEAMS.get(str(cbs_team_id), (espn_team_id, "", "", ""))
    abbreviation = abbreviation or _ESPN_TEAMS.get(str(espn_team_id), ("",))[0]
    _abbr, primary, alternate = _ESPN_TEAMS.get(str(espn_team_id), ("", "000000", "ffffff"))
    quarters = [(score.get("quarter") or {}).get(str(q)) for q in (1, 2, 3, 4)]
    for extra in sorted(k for k in (score.get("quarter") or {}) if (_int(k, 0) or 0) > 4):
        quarters.append((score.get("quarter") or {})[extra])
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:28~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:28~t:{espn_team_id}",
            "location": location or abbreviation,
            "name": mascot or abbreviation,
            "nickname": mascot or abbreviation,
            "abbreviation": abbreviation,
            "displayName": f"{location} {mascot}".strip() or abbreviation,
            "shortDisplayName": mascot or abbreviation,
            "color": primary,
            "alternateColor": alternate,
            "logos": [],
        },
        "score": str(_int(score.get("total")) or 0),
        "linescores": [{"displayValue": str(_int(q) or 0)} for q in quarters],
        "record": [],
    }


def _pickcenter(odds: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The closing line as a one-provider ``pickcenter`` array; ``[]`` on a partial line."""
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


def _cbs_odds_override(odds_payload: Optional[Mapping[str, Any]]) -> Optional[Dict[str, Any]]:
    """NAPI ``resource/game/odds`` consensus lines -> the processor's ``odds_override``.

    ``spread_current`` is stated for the **home** team (negative = home favoured), which is
    the opposite sign convention to nflverse's ``spread_line``; the processor wants a
    magnitude plus ``homeFavorite``. A payload with no consensus block returns None rather
    than a fabricated line -- the 2.5 / 55.5 default is then the processor's own business.
    """
    if not isinstance(odds_payload, Mapping):
        return None
    lines = next((c for c in (odds_payload.get("consensus") or []) if c.get("name") == "lines"), None)
    if not lines:
        return None
    by_name = {c.get("name"): c for c in (lines.get("consensusTypes") or [])}
    try:
        spread = float(by_name["spread_current"]["spread"])
        total = float(by_name["total_current"]["total"])
    except (KeyError, TypeError, ValueError):
        return None
    return {
        "gameSpread": abs(spread),
        "overUnder": total,
        "homeFavorite": spread < 0,
        "gameSpreadAvailable": True,
    }


def _synthesize_admin_rows(
    emitted: List[Dict[str, Any]],
    timeouts: List[Tuple[Optional[int], Optional[int]]],
    home_id: str,
    away_id: str,
    abbrs: Mapping[str, str],
    event_id: str,
    notes: List[str],
) -> List[Dict[str, Any]]:
    """Insert the admin rows CBS does not emit: team timeouts and the end of each period.

    CBS ships **no** admin row of any kind, but it states ``home_timeouts_remaining`` /
    ``away_timeouts_remaining`` on every play, so a charged timeout is a decrement between
    consecutive rows. Without these:

    * ``posTeamTimeouts`` / ``defPosTeamTimeouts`` stay at 3 for the whole game -- an EP, WP,
      xpass and 4th-down input, and the same defect the ESPN path has for ``CLV``-style codes;
    * the lagged ``end.TimeSecsRem`` at every quarter break comes from the *next* quarter's
      first snap, and the duplicate-text filter (``nfl_pbp.py:897-919``) can collapse two
      identical kneels that a period-end row would have separated. Period-end rows therefore
      carry ``down = 0``, which is what keeps them out of that filter's state comparison.

    **Only a decrement of exactly 1 is a timeout.** CBS drops both counters to ``0`` on the
    odd row and restores them on the next (2026 week 1 CLE @ JAX play 235: ``3,3 -> 0,0 ->
    3,3`` inside 39 seconds), which read literally invents six timeouts in one gap and takes
    both clubs to zero for the rest of the half. A larger drop is recorded as a note instead.

    Synthetic ids are ``previous play id + n`` and are only emitted while that number stays
    below the next play's -- an id is never allowed to collide or to sort out of order. ESPN
    renumbers its own stoppage rows too, so nothing joins on them.
    """
    out: List[Dict[str, Any]] = []
    numbers = [_int(p["id"][len(event_id) :], None) for p in emitted]
    previous = timeouts[0] if timeouts else (None, None)
    skipped = 0
    glitches = 0
    has_overtime = any(((p.get("period") or {}).get("number") or 0) > 4 for p in emitted)

    def synthetic(
        index: int, template: Dict[str, Any], type_id: str, text: str, offset: int
    ) -> Optional[Dict[str, Any]]:
        """A row numbered ``offset`` past the play before ``index``, or None if no id is free."""
        low = numbers[index - 1] if index else 0
        high = numbers[index] if index < len(numbers) else None
        number = (low or 0) + offset
        if high is not None and number >= high:
            return None
        return {
            **_admin_row(template, _type_object(ESPN_PLAY_TYPES, type_id), text),
            "id": f"{event_id}{number}",
            "sequenceNumber": str(number * 100),
        }

    for index, play in enumerate(emitted):
        home_left, away_left = timeouts[index] if index < len(timeouts) else (None, None)
        offset = 0
        for left, before, team_espn_id in ((home_left, previous[0], home_id), (away_left, previous[1], away_id)):
            if left is None or before is None or left >= before:
                continue
            if before - left != 1:
                glitches += 1
                continue
            offset += 1
            row = synthetic(
                index,
                play,
                "21",
                f"Timeout #{4 - before} by {abbrs.get(str(team_espn_id), '')} at {play['clock']['displayValue']}.",
                offset,
            )
            if row is None:
                skipped += 1
                continue
            row["homeTimeoutCalled"] = str(team_espn_id) == str(home_id)
            row["awayTimeoutCalled"] = str(team_espn_id) == str(away_id)
            out.append(row)
        if home_left is not None:
            previous = (home_left, previous[1])
        if away_left is not None:
            previous = (previous[0], away_left)
        out.append(play)
        this_period = (play.get("period") or {}).get("number")
        next_period = (emitted[index + 1].get("period") or {}).get("number") if index + 1 < len(emitted) else None
        if this_period and next_period != this_period:
            type_id = _quarter_end_type_id(this_period, has_overtime)
            end_row = synthetic(index + 1, play, type_id, _type_object(ESPN_PLAY_TYPES, type_id)["text"], 1)
            if end_row is None:
                skipped += 1
            else:
                end_row["clock"] = {"displayValue": "0:00"}
                out.append(end_row)
    if skipped:
        notes.append(f"{skipped} synthesized admin rows had no free play id and were dropped")
    if glitches:
        notes.append(f"{glitches} implausible timeouts-remaining drops (>1 in one gap) ignored as feed glitches")
    return out


def _fill_end_state(plays: List[Dict[str, Any]], home_id: str, scoring_teams: Mapping[str, str]) -> None:
    """Fill every play's ``end`` from the **next** snap, across drive boundaries.

    CBS states the pre-snap situation and nothing else, so the end state is the next snap's
    start -- which is how ESPN builds it too, and why it must not stop at a drive boundary (a
    punt ends in the receiving team's frame, on the next drive's first row). A scoring play
    takes ESPN's own convention instead (``down = -1``, the goal line, zero to the end zone)
    credited to the team the feed says **scored**, which on a return touchdown is the defence
    -- crediting the offence flips the end spot 100 yards. Clock stoppages carry no state of
    their own and are skipped when looking for the next snap; the last row of a live feed has
    no successor and advances its own start by the yardage instead.
    """
    for i, play in enumerate(plays):
        if play["type"]["text"] in _CLOCK_STOPPAGE:
            play["end"] = {**play["start"]}
            continue
        scored = play["scoringPlay"] and (
            play["type"]["abbreviation"] in ("TD", "FG", "SF") or play["id"] in scoring_teams
        )
        if scored:
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": 10,
                "yardLine": 100 if str(team) == str(home_id) else 0,
                "yardsToEndzone": 0,
                "team": {"id": team},
            }
            continue
        nxt = next((p["start"] for p in plays[i + 1 :] if p["type"]["text"] not in _CLOCK_STOPPAGE), None)
        if nxt is not None:
            play["end"] = {
                "down": nxt["down"],
                "distance": nxt["distance"],
                "yardLine": nxt["yardLine"],
                "yardsToEndzone": nxt["yardsToEndzone"],
                "team": {"id": nxt["team"]["id"]},
            }
            continue
        start = play["start"]
        gain = 0 if play["type"]["text"] == "Pass Incompletion" else int(play["statYardage"] or 0)
        to_endzone = max(min((start["yardsToEndzone"] or 0) - gain, 100), 0)
        first_down = gain >= (start["distance"] or 0)
        is_home = str(start["team"]["id"]) == str(home_id)
        play["end"] = {
            "down": 1 if first_down else min((start["down"] or 0) + 1, 4),
            "distance": 10 if first_down else max((start["distance"] or 0) - gain, 0),
            "yardLine": (100 - to_endzone) if is_home else to_endzone,
            "yardsToEndzone": to_endzone,
            "team": {"id": start["team"]["id"]},
        }


def _drive(
    event_id: str,
    cbs_drive: Mapping[str, Any],
    plays: List[Dict[str, Any]],
    abbrs: Mapping[str, str],
    espn_team_id: Optional[str],
) -> Dict[str, Any]:
    """One ``drives.previous[]`` entry: ESPN's drive id is ``{espn_event_id}{cbs drive id}``."""
    result = cbs_drive.get("result")
    abbr = abbrs.get(str(espn_team_id), "")
    first, last = (plays[0] if plays else {}), (plays[-1] if plays else {})
    return {
        "id": f"{event_id}{cbs_drive.get('id')}",
        "description": f"{cbs_drive.get('drive_plays')} plays, {cbs_drive.get('yards_on_drive')} yards, "
        f"{cbs_drive.get('time_of_possession')}",
        "team": {
            "id": str(espn_team_id) if espn_team_id else None,
            "abbreviation": abbr,
            "shortDisplayName": abbr,
            "displayName": abbr,
            "name": abbr,
        },
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
        "result": _drive_result(result),
        "shortDisplayResult": _drive_result(result),
        "displayResult": result,
        "plays": plays,
    }


def _header(
    event_id: str,
    scoreboard: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    home_cbs_id: str,
    away_cbs_id: str,
    home_id: str,
    away_id: str,
    period: Optional[int],
) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status, all from the payload."""
    status_block = scoreboard.get("game_status") or {}
    return {
        "id": event_id,
        "uid": f"s:20~l:28~e:{event_id}",
        "season": {"year": _int(idmap_row.get("season"), None), "type": _int(idmap_row.get("season_type"), None) or 2},
        "week": _int(idmap_row.get("week"), None),
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:28~e:{event_id}~c:{event_id}",
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
                    _competitor("home", 0, home_id, home_cbs_id, status_block.get("homescore") or {}),
                    _competitor("away", 1, away_id, away_cbs_id, status_block.get("awayscore") or {}),
                ],
            }
        ],
    }


def _cbs_nfl_to_espn_summary(
    plays: Sequence[Mapping[str, Any]],
    drives: Optional[Sequence[Mapping[str, Any]]],
    scoreboard: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    *,
    odds: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one CBS NAPI game (any phase) onto an ESPN-summary-shaped dict.

    Args:
        plays: the ``plays`` list of ``resource/game/scoring/plays/{cbs_game_id}``.
        drives: the ``drives`` list of ``resource/game/scoring/drives/{cbs_game_id}``, or
            None -- CBS 404s that resource for 2018 games, and the drives are then rebuilt
            from the plays' own ``drive_id``.
        scoreboard: the ``scoreboard`` block of ``resource/game/scoring/scoreboard/{id}``.
        idmap_row: the game's id-map row (:data:`...idmap.GAME_SCHEMA`): ``espn_event_id``,
            ``home_espn_team_id`` and ``away_espn_team_id`` are required; ``season``,
            ``season_type``, ``week``, ``kickoff_utc`` and ``neutral_site`` fill the header.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` -- the stored
            closing line or :func:`_cbs_odds_override`. Becomes the one-provider ``pickcenter``.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a PAT with no touchdown to fold into, plays outside the drive chart, dropped synthetic admin ids, unresolved jersey placeholders in live text, an absent drives resource, and (pre-2019) play ids that do not join ESPN's own. |

    Raises:
        KeyError: ``idmap_row`` is missing ``espn_event_id`` or a team id.
    """
    notes: List[str] = []
    event_id = str(idmap_row["espn_event_id"])
    home_id, away_id = str(idmap_row["home_espn_team_id"]), str(idmap_row["away_espn_team_id"])
    home_cbs_id = str((scoreboard.get("hometeam") or {}).get("id") or "")
    away_cbs_id = str((scoreboard.get("awayteam") or {}).get("id") or "")
    espn_team_of = {home_cbs_id: home_id, away_cbs_id: away_id}
    abbrs = {
        home_id: CBS_TEAMS.get(home_cbs_id, ("", _ESPN_TEAMS.get(home_id, ("",))[0]))[1],
        away_id: CBS_TEAMS.get(away_cbs_id, ("", _ESPN_TEAMS.get(away_id, ("",))[0]))[1],
    }

    ordered = sorted((p for p in plays if p.get("id") is not None), key=lambda p: _int(p.get("id")) or 0)
    receiver_framed = _receiver_framed(ordered)
    drive_teams = _drive_teams(ordered, drives)
    #: Where the previous play of each drive left the ball, in the drive owner's frame.
    drive_spot: Dict[str, int] = {}
    emitted: List[Dict[str, Any]] = []
    timeouts: List[Tuple[Optional[int], Optional[int]]] = []
    drive_of: List[str] = []
    scoring_teams: Dict[str, str] = {}
    home_points = away_points = 0
    last_touchdown: Optional[int] = None
    last_yard_line: Optional[int] = None
    placeholders = 0

    for play in ordered:
        subplays = list((play.get("subplays") or {}).get("subplay") or [])
        types = [str(s.get("type") or "") for s in subplays]
        text = _norm_text(play.get("description"), _TEXT_ABBR_FIXES)
        placeholders += 1 if _PLACEHOLDER_RE.search(text) else 0
        offense = str(play.get("team_in_possession") or "")
        score_type = _score_type(play, types, text)
        if types and types[0] in _PAT_SUBPLAYS:
            # ESPN folds the try into its touchdown: one row, one text, one score step. The
            # try is NOT always the row right before it -- a timeout or a penalty on the try
            # itself sits between them -- so anchor on the newest touchdown and carry the
            # post-try score onto every row in between; the scoreboard never steps back.
            if last_touchdown is None:
                notes.append(f"play {play.get('id')}: point-after row with no touchdown to fold into")
                continue
            target = emitted[last_touchdown]
            points, after = _point_after(play, text)
            target["text"] = f"{target['text'].rstrip('.')}. {text}".strip()
            if after is not None:
                target["pointAfterAttempt"] = after
            if str(scoring_teams.get(target["id"], "")) == home_id:
                home_points += points
            else:
                away_points += points
            for later in emitted[last_touchdown:]:
                later["homeScore"], later["awayScore"] = home_points, away_points
            continue

        kickoff = bool(types) and types[0] == "Kickoff"
        kick = bool(types) and types[0] in ("Kickoff", "Punt")
        drive_owner = drive_teams.get(str(play.get("drive_id") or ""))
        if drive_owner:
            # ESPN credits a kickoff to the KICKING team; CBS files it under the receiving
            # team's drive, so the kickoff row is the one place the drive's owner is wrong.
            offense = _other_team(drive_owner, home_cbs_id, away_cbs_id) if kickoff else drive_owner
        # The LAST NON-PENALTY subplay says who ended the play with the ball. Reading the last
        # subplay outright calls every flagged snap a change of possession -- a CBS penalty
        # subplay states the penalised club -- which flipped the spot 100 yards on 20+ rows a
        # game and moved EP by up to 3.5 points on each.
        settled = [s for s, t in zip(subplays, types) if t != "Penalty"]
        final_team = str(_subplay_body(settled[-1]).get("team_in_possession") or offense) if settled else offense
        possession_changed = final_team != offense
        to_endzone = _int(_subplay_body(subplays[0]).get("yards_to_endzone"), None) if subplays else None
        own_abbrs = _text_abbrs(espn_team_of.get(offense, ""), abbrs.get(espn_team_of.get(offense, ""), ""))
        if kickoff:
            to_endzone = _kickoff_to_endzone(text, own_abbrs) or (
                (100 - to_endzone) if (to_endzone is not None and receiver_framed) else to_endzone
            )
        elif _SACKED_RE.search(text):
            # CBS states the spot the SACK ended at, and from 2024 in the defence's frame; the
            # text states the pre-snap spot in every era, so it wins outright here. A goal-line
            # spot is 0, so the fallback tests for None rather than truthiness.
            from_text = _sack_to_endzone(text, own_abbrs)
            if from_text is not None:
                to_endzone = from_text
            elif to_endzone is not None:
                to_endzone = _choose_frame(to_endzone, drive_spot.get(str(play.get("drive_id") or "")))
        elif to_endzone is not None and (kick or possession_changed):
            to_endzone = _choose_frame(to_endzone, drive_spot.get(str(play.get("drive_id") or "")))
        distance_raw = play.get("distance")
        espn_offense = espn_team_of.get(offense)
        is_home = espn_offense == home_id
        yard_line = None if to_endzone is None else ((100 - to_endzone) if is_home else to_endzone)
        if yard_line is None:
            # A row that states no spot (a CBS aborted/administrative row) carries the ball's
            # last known spot instead of a null. Keeping the column an integer is load-bearing:
            # one null turns pandas' json_normalize column into float64 and the processor's
            # incompletion UDF, declared Int64, raises SchemaError and loses the whole game
            # (3 of the 26 finals in this capture failed exactly that way).
            yard_line = last_yard_line
            to_endzone = None if yard_line is None else ((100 - yard_line) if is_home else yard_line)
        else:
            last_yard_line = yard_line
        distance = to_endzone if str(distance_raw) == "Goal" else (_int(distance_raw) or 0)
        scored = play.get("score_on_play") == "Yes" and score_type in _SCORING
        type_id = _espn_type_id(types, score_type, possession_changed, text)
        credited: Optional[str] = espn_offense
        if scored:
            points, _scoring_type = _SCORING.get(str(score_type), (0, None))
            if score_type == "Safety":
                credited = away_id if is_home else home_id
            elif score_type == "FieldGoal":
                credited = espn_offense
            else:
                credited = espn_team_of.get(final_team, espn_offense)
            home_points += points if credited == home_id else 0
            away_points += points if credited != home_id else 0
        row: Dict[str, Any] = {
            "id": f"{event_id}{play.get('id')}",
            "sequenceNumber": str((_int(play.get("id")) or 0) * 100),
            "type": _type_object(ESPN_PLAY_TYPES, type_id),
            "text": text,
            "awayScore": away_points,
            "homeScore": home_points,
            "period": {"number": _int(play.get("quarter"), None)},
            "clock": {"displayValue": _clock(str(play.get("time_remaining") or "0:00"))},
            "scoringPlay": scored,
            "priority": False,
            "statYardage": _stat_yardage(types, subplays, offense),
            "wallclock": play.get("real_clock"),
            "start": {
                "down": _int(play.get("down")) or 0,
                "distance": distance or 0,
                "yardLine": yard_line,
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(
                    _int(play.get("down")) or 0, distance or 0, str(distance_raw) == "Goal", None
                ),
                "team": {"id": espn_offense},
            },
            "end": {},
        }
        if scored and _SCORING.get(str(score_type), (0, None))[1] and credited is not None:
            row["scoringType"] = _SCORING[str(score_type)][1]
            scoring_teams[row["id"]] = credited
        if to_endzone is not None and not kickoff:
            drive_spot[str(play.get("drive_id") or "")] = max(min(to_endzone - int(row["statYardage"] or 0), 100), 0)
        emitted.append(row)
        timeouts.append(
            (_int(play.get("home_timeouts_remaining"), None), _int(play.get("away_timeouts_remaining"), None))
        )
        drive_of.append(str(play.get("drive_id") or ""))
        if score_type == "Touchdown" and scored:
            last_touchdown = len(emitted) - 1

    drive_by_play = {p["id"]: d for p, d in zip(emitted, drive_of)}
    all_rows = _synthesize_admin_rows(emitted, timeouts, home_id, away_id, abbrs, event_id, notes)
    _fill_end_state(all_rows, home_id, scoring_teams)
    # A synthesized admin row belongs to the drive of the play it follows.
    current_drive = drive_of[0] if drive_of else ""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in all_rows:
        current_drive = drive_by_play.get(row["id"], current_drive)
        grouped.setdefault(current_drive, []).append(row)

    drives_raw = list(drives or [])
    if not drives_raw:
        notes.append("drives resource absent: drive meta rebuilt from the plays' own drive_id")
    # Every drive the PLAYS name, in play order, whether or not the drives resource lists it:
    # CBS's drives list skips a drive now and then (4 plays of 2020 week 7 CLE @ CIN, one of
    # them a 4th-quarter touchdown), and dropping the group silently lost the score with it.
    meta_by_id = {str(d.get("id")): d for d in drives_raw}
    orphans = [k for k in grouped if k not in meta_by_id]
    if orphans:
        notes.append(f"{sum(len(grouped[k]) for k in orphans)} plays are on drives the drives resource omits")
    drives_raw = [meta_by_id.get(key) or {"id": key} for key in grouped]
    if placeholders:
        notes.append(f"{placeholders} plays carry an unresolved jersey placeholder ([NN]); those name columns are null")
    season = _int(idmap_row.get("season"), None)
    if season is not None and season < 2019:
        notes.append(
            f"season {season} < 2019: CBS play ids are sequential, not GSIS playIds; "
            "they do not join ESPN's own play ids"
        )

    previous = [
        _drive(
            event_id,
            cbs_drive,
            grouped.get(str(cbs_drive.get("id")), []),
            abbrs,
            espn_team_of.get(str(cbs_drive.get("team_id") or drive_teams.get(str(cbs_drive.get("id"))) or "")),
        )
        for cbs_drive in drives_raw
    ]
    completed = str((scoreboard.get("game_status") or {}).get("status") or "").upper().startswith("FINAL")
    current = previous.pop() if (previous and not completed) else None
    if current is not None:
        # An open drive has no outcome yet; leave the outcome columns null rather than
        # inventing "End of Game" from a result CBS has not written.
        current["isScore"] = bool(current.get("isScore"))
        notes.append("game is in progress: the open drive is served as drives.current")
    period = next((p["period"]["number"] for p in reversed(all_rows) if (p.get("period") or {}).get("number")), None)

    summary = {
        "boxscore": {"teams": [], "players": []},
        "format": {},
        "gameInfo": {"venue": {"id": None, "fullName": None, "address": {}}, "attendance": None},
        "drives": {"previous": previous, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(event_id, scoreboard, idmap_row, home_cbs_id, away_cbs_id, home_id, away_id, period),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }
    return summary, notes


def _cbs_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="cbs"`` (NFL). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (``SourceUnavailable``) when the game has no ESPN team ids,
    when CBS's own game id cannot be resolved from the week scoreboard, when the fetch fails,
    and when CBS answers 200 with no ``plays`` list -- the shape every pre-2017 game and every
    non-enhanced game is in. It never invents a CBS game id.
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row
    from sportsdataverse.nfl.cbs_pbp.game_id import _resolve_cbs_game_id
    from sportsdataverse.nfl.shield_pbp.to_espn_summary import _idmap_row_from_schedule

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    resolved_by = "idmap" if ctx.idmap_row else "none"
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id") and row.get("season")):
        # Game on Paper calls dispatch with the ESPN event id alone, so the row can arrive
        # empty even after dispatch's own cascade. The nflverse schedule closes the gap
        # offline for everything except CBS's game id, which only the scoreboard page has.
        from_schedule = _idmap_row_from_schedule(espn_id)
        if from_schedule:
            resolved_by = "nflverse_schedule" if resolved_by == "none" else "idmap+nflverse_schedule"
            for key, value in from_schedule.items():
                if row.get(key) is None:
                    row[key] = value
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        raise SourceUnavailable(f"nfl {espn_id}: id-map row carries no ESPN team ids")

    payload = ctx.payload
    cbs_game_id = (
        str(payload.get("cbs_game_id")) if isinstance(payload, Mapping) and payload.get("cbs_game_id") else None
    )
    cbs_id_source = "payload" if cbs_game_id else None
    if cbs_game_id is None:
        cbs_game_id = row.get("cbs_game_id")
        cbs_id_source = "idmap" if cbs_game_id else None
    scoreboard_provenance: Dict[str, Any] = {}
    if cbs_game_id is None and payload is None:
        if row.get("season") is None or row.get("week") is None:
            raise SourceUnavailable(f"nfl {espn_id}: no season/week to look the CBS game id up with")
        cbs_game_id, scoreboard_provenance = _resolve_cbs_game_id(
            int(row["season"]),
            int(row.get("season_type") or 2),
            int(row["week"]),
            str(row["home_espn_team_id"]),
            str(row["away_espn_team_id"]),
            kickoff_utc=row.get("kickoff_utc"),
        )
        cbs_id_source = "week_scoreboard"
        if not cbs_game_id:
            raise SourceUnavailable(f"nfl {espn_id}: no CBS game id on the week scoreboard page")

    if payload is None:
        try:
            payload = _fetch_cbs_game(str(cbs_game_id), optional=("drives", "odds"))
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"cbs napi fetch failed: {type(exc).__name__}: {exc}") from exc

    plays = _napi_list(payload.get("plays"), "plays")
    scoreboard = (
        (payload.get("scoreboard") or {}).get("scoreboard") if isinstance(payload.get("scoreboard"), Mapping) else None
    )
    if not plays:
        raise SourceUnavailable(f"nfl {espn_id}: CBS carries no plays for game {cbs_game_id}")
    if not scoreboard:
        raise SourceUnavailable(f"nfl {espn_id}: CBS carries no scoreboard for game {cbs_game_id}")
    odds = (
        ctx.odds_override
        or _odds_override_from_row(ctx.idmap_row)
        or _cbs_odds_override((payload.get("odds") or {}) if isinstance(payload.get("odds"), Mapping) else None)
        or _odds_override_from_row(row)
    )
    summary, notes = _cbs_nfl_to_espn_summary(
        plays, _napi_list(payload.get("drives"), "drives"), scoreboard, row, odds=odds
    )
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=odds,
        native_ids={
            "espn_event_id": str(espn_id),
            "cbs_game_id": str(cbs_game_id) if cbs_game_id else None,
            "cbs_game_id_source": cbs_id_source,
            "idmap_resolved_by": resolved_by,
            **({"cbs_scoreboard": scoreboard_provenance} if scoreboard_provenance else {}),
        },
        notes=notes,
    )


_register_cbs("nfl", _cbs_adapter)
