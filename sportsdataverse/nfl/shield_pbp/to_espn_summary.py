"""Shield play rows -> an ESPN-summary-shaped dict ``NFLPlayProcess`` consumes unchanged.

The second projection of the one Shield fetch and the one Shield parse (the first is
:func:`sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp`'s nflverse-shape frame). ESPN's
NFL play-by-play *is* the GSIS feed re-skinned -- the play ids are
``{espn_event_id}{shield playId}``, the text is Shield's ``playDescription`` minus its
``(mm:ss) `` prefix -- so this module owns only the re-skin: ESPN's play-type vocabulary,
ESPN's absolute yard line, the PAT folded into its touchdown, a renderable header, and the
drive grouping. Possession, field position, the running score and the drive bounds are read
off the parsed frame and the payload's own drive chart; nothing is re-derived here.

The output is validated by :func:`sportsdataverse.football.sources.contract._validate_summary`
and consumed through ``espn_nfl_pbp(summary=)``; the dispatcher registers it as
``source="shield"`` for the NFL (:mod:`sportsdataverse.football.sources.dispatch`).

Documented divergences from a real ESPN summary (measured, see the Stage 2 Phase 3 gate):

* Shield emits no separate PAT row, so ``pointAfterAttempt`` rides on the touchdown play --
  which is exactly what ESPN does.
* Shield renumbers no timeouts, so timeout play ids are Shield's, not ESPN's (ESPN's own
  timeout ids do not line up with the GSIS ones either).
* The play id is **always** ``{espn_event_id}{shield playId}`` and is never null, but it only
  *equals* ESPN's id from the **2014** season on. The Stage 3 coverage study measured the
  id-join rate against ESPN at ~1.00 for 2014+, 0.46-0.76 for 2005-2013 (ESPN truncates its
  own play list) and ~0.04 for 2002-2004 (ESPN used its own sequence there). A pre-2014 game
  therefore gets a note saying so: the ids stay stable and unique for Game on Paper, they just
  cannot be joined to an ESPN-sourced frame.
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids, and the Shield
  path carries gsis ids instead. ``__attach_player_ids`` therefore fills nothing.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import polars as pl

from sportsdataverse.nfl.shield_pbp.live import game_phase, is_final
from sportsdataverse.nfl.shield_pbp.parse import _GAMEBOOK_TO_NFLVERSE, _SCORING_POINTS, _impute_clock

#: ESPN NFL ``type.id`` -> ``(type.text, type.abbreviation)``. Enumerated from every ESPN
#: NFL summary in ``nfl-raw/nfl/espn`` for 2025-2026 (582 crosswalked games, 33 distinct
#: types); ESPN itself ships a null ``abbreviation`` on the six types mapped to ``None``.
ESPN_PLAY_TYPES: Dict[str, Tuple[str, Optional[str]]] = {
    "2": ("End Period", "EP"),
    "3": ("Pass Incompletion", None),
    "5": ("Rush", "RUSH"),
    "7": ("Sack", None),
    "8": ("Penalty", "PEN"),
    "9": ("Fumble Recovery (Own)", None),
    "17": ("Blocked Punt", "BP"),
    "18": ("Blocked Field Goal", "BFG"),
    "20": ("Safety", "SF"),
    "21": ("Timeout", "TO"),
    "24": ("Pass Reception", "REC"),
    "26": ("Pass Interception Return", "INTR"),
    "29": ("Fumble Recovery (Opponent)", None),
    "30": ("Muffed Punt Recovery (Opponent)", None),
    "32": ("Kickoff Return Touchdown", "TD"),
    "34": ("Punt Return Touchdown", "TD"),
    "36": ("Interception Return Touchdown", "TD"),
    "37": ("Blocked Punt Touchdown", "TD"),
    "38": ("Blocked Field Goal Touchdown", "TD"),
    "39": ("Fumble Return Touchdown", "TD"),
    "52": ("Punt", "PUNT"),
    "53": ("Kickoff", "K"),
    "59": ("Field Goal Good", "FG"),
    "60": ("Field Goal Missed", "FGM"),
    "65": ("End of Half", "EH"),
    "66": ("End of Game", "EG"),
    "67": ("Passing Touchdown", "TD"),
    "68": ("Rushing Touchdown", "TD"),
    "74": ("Official Timeout", "Off TO"),
    "75": ("Two-minute warning", "2Min Warn"),
    "79": ("End of Regulation", "ER"),
    "80": ("Sack Opp Fumble Recovery", "SFOP"),
}

#: ESPN team id -> (primary, alternate) hex colour. Game on Paper styles the header with
#: these and nothing else reads them; every other competitor field is era-correct because it
#: comes from the payload (location / mascot) or the id map (abbreviation).
# ponytail: static snapshot of the 32 current franchises. If a rebrand ever moves a colour,
# add the column to the id map's TEAM_SCHEMA rather than growing this table.
_ESPN_TEAM_COLORS: Dict[str, Tuple[str, str]] = {
    "1": ("a71930", "000000"),
    "2": ("00338d", "d50a0a"),
    "3": ("0b162a", "c83803"),
    "4": ("fb4f14", "000000"),
    "5": ("311d00", "ff3c00"),
    "6": ("002a5c", "b0b7bc"),
    "7": ("0a2343", "fc4c02"),
    "8": ("0076b6", "b0b7bc"),
    "9": ("204e32", "ffb612"),
    "10": ("002c5f", "a71930"),
    "11": ("003b75", "ffffff"),
    "12": ("e31837", "ffb81c"),
    "13": ("000000", "a5acaf"),
    "14": ("003594", "ffd100"),
    "15": ("008e97", "fc4c02"),
    "16": ("4f2683", "ffc62f"),
    "17": ("002a5e", "c60c30"),
    "18": ("d3bc8d", "000000"),
    "19": ("0b2265", "a71930"),
    "20": ("115740", "ffffff"),
    "21": ("004c54", "a5acaf"),
    "22": ("97233f", "000000"),
    "23": ("ffb612", "101820"),
    "24": ("0080c6", "ffc20e"),
    "25": ("aa0000", "b3995d"),
    "26": ("002244", "69be28"),
    "27": ("d50a0a", "34302b"),
    "28": ("5a1414", "ffb612"),
    "29": ("0085ca", "101820"),
    "30": ("007487", "d7a22a"),
    "33": ("29126f", "9e7c0c"),
    "34": ("03202f", "a71930"),
}

#: Shield ``seasonType`` -> ESPN ``header.season.type``.
_SEASON_TYPE = {"PRE": 1, "REG": 2, "POST": 3, "PRO": 4}

#: Shield ``playType`` values that never become an ESPN play row: the feed's synthetic
#: game marker, a free-text note, and the PAT tries ESPN folds into the touchdown.
_SKIP_PLAY_TYPES = frozenset({"GAME_START", "COMMENT"})
_PAT_PLAY_TYPES = frozenset({"XP_KICK", "PAT2"})

_CLOCK_PREFIX_RE = re.compile(r"^\(\d{0,2}:\d{2}\)\s*")
_WHITESPACE_RE = re.compile(r"\s+")
#: ESPN ``type.text`` values that are clock stoppages, not snaps (``nfl.model_vars``).
_CLOCK_STOPPAGE = frozenset(
    {"Timeout", "Official Timeout", "Two-minute warning", "End Period", "End of Half", "End of Game"}
)
_TIMEOUT_CHARGED_RE = re.compile(r"(?i)\btimeout\s*#\s*\d+\s*(?:by\s+)?[A-Z]{2,3}\b")
_TWO_MINUTE_RE = re.compile(r"(?i)^\s*(?:two|2)[- ]minute warning\.?\s*$")
_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}
_CLOCK_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def _clock(clock: Optional[str]) -> str:
    """Shield ``"05:55"`` -> ESPN ``"5:55"`` -- ESPN never zero-pads the minutes."""
    match = _CLOCK_RE.match((clock or "").strip())
    return f"{int(match.group(1))}:{match.group(2)}" if match else "0:00"


def _text(description: Optional[str]) -> str:
    """Shield ``playDescription`` -> ESPN ``text``: drop the ``(mm:ss) `` prefix, flatten whitespace."""
    # ESPN drops the feed's newline outright ("...(N.Smith).PENALTY on PHI-..."), it does not
    # turn it into a space -- and the duplicate-text filter compares whole strings.
    flat = _WHITESPACE_RE.sub(" ", (description or "").replace("\r", "").replace("\n", "")).strip()
    return _CLOCK_PREFIX_RE.sub("", flat).strip()


def _abs_yard_line(yard_line: Optional[str], home_abbr: Optional[str]) -> Optional[int]:
    """Shield ``"JAX 28"`` -> ESPN ``yardLine``: yards from the **home** team's goal line.

    ESPN's ``yardLine`` is one absolute field coordinate shared by both teams (verified on
    every 2025 crosswalked game), while ``yardsToEndzone`` is relative to ``start.team``.
    The side abbreviation is normalised out of the feed's gamebook scheme first
    (``LAR`` -> ``LA``), exactly as the parser does for ``yardline_100``.
    """
    if not yard_line or not home_abbr:
        return None
    text = yard_line.strip()
    if text == "50":
        return 50
    parts = text.rsplit(" ", 1)
    if len(parts) != 2:
        return None
    side, number = parts
    side = _GAMEBOOK_TO_NFLVERSE.get(side, side)
    try:
        yards = int(number)
    except ValueError:
        return None
    return yards if side == home_abbr else 100 - yards


def _to_endzone(yard_line: Optional[int], team_id: Optional[str], home_id: str) -> Optional[int]:
    """ESPN ``yardsToEndzone`` for ``team_id`` from the absolute ``yardLine``."""
    if yard_line is None or team_id is None:
        return None
    return 100 - yard_line if str(team_id) == str(home_id) else yard_line


def _down_distance_text(down: int, distance: int, goal_to_go: bool, yard_line: Optional[str]) -> Optional[str]:
    """``"1st & 10 at JAX 30"`` -- the only ``downDistanceText`` the processor reads (its ``goal`` test)."""
    if not down:
        return None
    label = _ORDINAL.get(int(down), f"{int(down)}th")
    togo = "Goal" if goal_to_go else str(int(distance or 0))
    return f"{label} & {togo}" + (f" at {yard_line}" if yard_line else "")


#: ESPN ``drive.result``: the uppercase abbreviation of ``displayResult``, comma-joined.
_DRIVE_RESULT_ABBR = {
    "Touchdown": "TD",
    "Field Goal": "FG",
    "Interception": "INT",
    "Safety": "SF",
}


def _drive_result(display_result: Optional[str]) -> Optional[str]:
    """``"Interception"`` -> ``"INT"``; a comma-joined result abbreviates each part."""
    if not display_result:
        return None
    parts = [p.strip() for p in display_result.split(",")]
    return ", ".join(_DRIVE_RESULT_ABBR.get(p, p.upper()) for p in parts)


def _stat_yardage(play_type: str, play: Mapping[str, Any], row: Mapping[str, Any]) -> int:
    """ESPN's ``statYardage`` for one row, from whichever Shield field ESPN actually mirrors.

    Measured over 25 crosswalked 2025-2026 games, agreement with ESPN per play type: the
    **return** yards on a kickoff (.873) or punt (.901) -- ESPN books the return, not the
    kick; the parser's stat-summed yardage on a scrimmage play (pass/rush/sack, .98); the
    feed's own ``yardsGained`` on a field goal (.83), a penalty-only row (.97) and everything
    else. The wrong pick is not cosmetic: ``statYardage`` is the official yardage for a plain
    play, the ``downs_turnover`` test and the ``qb_epa`` re-spot.
    """
    if play_type in ("KICK_OFF", "PUNT"):
        return int(row.get("return_yards") or 0)
    if play_type in ("PASS", "RUSH", "SACK") and row.get("yards_gained") is not None:
        return int(row["yards_gained"])
    return int(play.get("yardsGained") or 0)


def _timeout_type_id(text: str) -> str:
    """``21`` (team timeout), ``75`` (two-minute warning) or ``74`` (official timeout)."""
    if _TWO_MINUTE_RE.match(text):
        return "75"
    return "21" if _TIMEOUT_CHARGED_RE.search(text) else "74"


def _quarter_end_type_id(quarter: Optional[int], has_overtime: bool) -> str:
    """``END_QUARTER`` -> ESPN's three different end-of-period types."""
    if quarter == 2:
        return "65"  # End of Half
    if quarter == 4:
        return "79" if has_overtime else "66"  # End of Regulation / End of Game
    return "2"  # End Period


def _touchdown_type_id(play_type: str, row: Mapping[str, Any], text: str) -> str:
    """ESPN ``type.id`` for a Shield row whose ``scoringPlayType`` is ``TOUCHDOWN``."""
    if play_type == "PUNT":
        return "37" if "BLOCKED" in text.upper() else "34"
    if play_type == "KICK_OFF":
        return "39" if row.get("fumble_lost") else "32"
    if play_type == "FIELD_GOAL":
        return "38"
    if play_type == "SACK":
        return "80"
    if row.get("interception"):
        return "36"
    if row.get("fumble_lost"):
        return "39"
    if row.get("rush_touchdown") or play_type == "RUSH":
        return "68"
    return "67"


def _scrimmage_type_id(play_type: str, row: Mapping[str, Any], text: str) -> Optional[str]:
    """ESPN ``type.id`` for a non-scoring scrimmage / special-teams row, or None if undecidable."""
    fumble_lost, fumble = row.get("fumble_lost"), row.get("fumble")
    if play_type == "FIELD_GOAL" or row.get("field_goal_attempt"):
        if row.get("field_goal_made"):
            return "59"
        return "18" if row.get("field_goal_blocked") else "60"
    if play_type == "PUNT" or row.get("punt_attempt"):
        if "BLOCKED" in text.upper():
            return "17"
        if fumble_lost:
            return "30" if "MUFF" in text.upper() else "29"
        return "9" if fumble else "52"
    if play_type == "KICK_OFF" or row.get("kickoff_attempt"):
        return "53"
    if play_type == "SACK" or row.get("sack"):
        if fumble_lost:
            return "80"
        return "9" if fumble else "7"
    if row.get("interception"):
        return "26"
    if row.get("pass_attempt"):
        if fumble_lost:
            return "29"
        if fumble:
            return "9"
        return "24" if row.get("complete_pass") else "3"
    if row.get("rush_attempt"):
        if fumble_lost:
            return "29"
        return "9" if fumble else "5"
    return None


def _espn_type(play: Mapping[str, Any], row: Mapping[str, Any], text: str, has_overtime: bool) -> Dict[str, Any]:
    """The ESPN ``type`` object (``id`` / ``text`` / ``abbreviation``) for one Shield play.

    Game on Paper dereferences ``type.id`` and ``type.abbreviation`` unguarded, so both keys
    are always present -- ``abbreviation`` is null on the six types ESPN itself leaves null.
    """
    play_type = play.get("playType") or "UNSPECIFIED"
    scoring = play.get("scoringPlayType") or "UNSPECIFIED"
    if play_type == "TIMEOUT":
        type_id = _timeout_type_id(text)
    elif play_type == "END_GAME":
        type_id = "66"
    elif play_type == "END_QUARTER":
        type_id = _quarter_end_type_id(play.get("quarter"), has_overtime)
    elif scoring == "SAFETY":
        type_id = "20"
    elif scoring == "TOUCHDOWN":
        type_id = _touchdown_type_id(play_type, row, text)
    else:
        type_id = _scrimmage_type_id(play_type, row, text) or "8"
    label, abbreviation = ESPN_PLAY_TYPES[type_id]
    return {"id": type_id, "text": label, "abbreviation": abbreviation}


def _point_after(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """ESPN's ``pointAfterAttempt`` for a folded PAT row (``abbreviation`` + ``value`` are what is read)."""
    if row.get("two_point_attempt"):
        good = row.get("two_point_conv_result") == "success"
        return {
            "id": 62 if good else 63,
            "text": "Two Point Pass Good" if good else "Two Point Attempt Failed",
            "abbreviation": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "value": 2 if good else 0,
        }
    result = row.get("extra_point_result")
    if result is None and not row.get("extra_point_attempt"):
        return None
    good = result == "good"
    blocked = result == "blocked"
    return {
        "id": 61 if good else 64,
        "text": "Extra Point Good" if good else "Extra Point Missed",
        "abbreviation": "Extra Point Good" if good else ("Extra Point Blocked" if blocked else "Extra Point Missed"),
        "value": 1 if good else 0,
    }


def _team_names(shield_team: Mapping[str, Any], abbreviation: Optional[str]) -> Tuple[str, str, str, str]:
    """``(abbreviation, location, mascot, name_alt)`` -- the tuple the timeout-side parser wants."""
    location, mascot = _split_name(shield_team, abbreviation)
    return (abbreviation or mascot, location, mascot, location)


def _split_name(shield_team: Mapping[str, Any], abbreviation: Optional[str]) -> Tuple[str, str]:
    """``"Jacksonville Jaguars"`` -> ``("Jacksonville", "Jaguars")``.

    Every NFL club's mascot is a single token ("49ers", "Commanders"), so the split is exact.
    A payload with no ``fullName`` degrades to the abbreviation rather than an empty mascot --
    an empty ``team.name`` makes every Timeout row match both teams (contract ``_check_values``).
    """
    full_name = str(shield_team.get("fullName") or "").strip()
    location, _, mascot = full_name.rpartition(" ")
    mascot = mascot or (abbreviation or "")
    return location or mascot, mascot


def _competitor(
    side: str,
    order: int,
    espn_team_id: str,
    shield_team: Mapping[str, Any],
    abbreviation: Optional[str],
    score: Mapping[str, Any],
    winner: Optional[bool],
    has_possession: bool,
) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first."""
    full_name = str(shield_team.get("fullName") or "").strip()
    location, mascot = _split_name(shield_team, abbreviation or espn_team_id)
    primary, alternate = _ESPN_TEAM_COLORS.get(str(espn_team_id), ("000000", "ffffff"))
    quarters = [score.get(f"q{q}") for q in (1, 2, 3, 4)]
    if score.get("ot"):
        quarters.append(score.get("ot"))
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:28~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(winner),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:28~t:{espn_team_id}",
            "location": location,
            "name": mascot,
            "nickname": mascot,
            "abbreviation": abbreviation or mascot[:3].upper(),
            "displayName": full_name or f"{location} {mascot}".strip(),
            "shortDisplayName": mascot,
            "color": primary,
            "alternateColor": alternate,
            "logos": [],
        },
        "score": str(score.get("total") if score.get("total") is not None else 0),
        "linescores": [{"displayValue": str(q if q is not None else 0)} for q in quarters],
        "possession": bool(has_possession),
        "record": [],
    }


_STATUS_BY_PHASE = {
    "FINAL": ("3", "STATUS_FINAL", "post", True, "Final"),
    "FINAL_OVERTIME": ("3", "STATUS_FINAL", "post", True, "Final/OT"),
    "HALFTIME": ("23", "STATUS_HALFTIME", "in", False, "Halftime"),
    "INGAME": ("2", "STATUS_IN_PROGRESS", "in", False, "In Progress"),
    "PREGAME": ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled"),
}


def _status(summary: Mapping[str, Any], phase: Optional[str], period: Optional[int]) -> Dict[str, Any]:
    """``header.competitions[0].status`` -- the only freshness signal Shield gives is ``summary.phase``."""
    type_id, name, state, completed, description = _STATUS_BY_PHASE.get(
        str(phase or ""), ("1", "STATUS_SCHEDULED", "pre", False, "Scheduled")
    )
    clock = _clock(summary.get("clock")) if summary.get("clock") else None
    detail = description
    if state == "in" and name != "STATUS_HALFTIME" and period:
        detail = f"{clock} - {_ORDINAL.get(period, str(period))}" if period <= 4 else f"{clock} - OT"
    return {
        "clock": 0.0,
        "displayClock": clock or "0:00",
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
        # Shield's own phase, verbatim: a live consumer needs HALFTIME and FINAL_OVERTIME,
        # neither of which survives the ESPN status vocabulary.
        "shieldPhase": phase,
    }


def _pickcenter(odds: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The stored closing line as a one-provider ``pickcenter`` array (the offline odds path)."""
    if not odds:
        return []
    return [
        {
            "provider": {"id": "0", "name": "stored closing line", "priority": 0},
            "spread": abs(float(odds["gameSpread"])),
            "overUnder": float(odds["overUnder"]),
            "homeTeamOdds": {"favorite": bool(odds["homeFavorite"])},
            "awayTeamOdds": {"favorite": not bool(odds["homeFavorite"])},
        }
    ]


def _drive_index(sequences: Sequence[float], play_seq: Optional[float]) -> int:
    """1-based ESPN drive index for a play: the last drive that started at or before it.

    Exact on every crosswalked 2025 play that exists in both feeds (6,323/6,323): ESPN's
    drive id is ``{espn_event_id}{index}`` and its grouping is this rule, which puts a
    kickoff at the head of the receiving team's drive and a PAT at the tail of the
    scoring one.
    """
    index = 1
    for i, start in enumerate(sequences):
        if play_seq is not None and start <= play_seq:
            index = i + 1
    return index


def shield_to_espn_summary(
    game_detail: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    *,
    parsed: Optional[pl.DataFrame] = None,
    odds: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Shield game (any phase) onto an ESPN-summary-shaped dict.

    Args:
        game_detail: A Shield ``experience/v2/gamedetails`` payload (raw body or a
            ``{"data": ...}`` envelope) -- the same object
            :func:`sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp` consumes.
        idmap_row: The game's pre-kickoff id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`): ``espn_event_id``,
            ``home_espn_team_id`` and ``away_espn_team_id`` are required; the optional
            ``home_team`` / ``away_team`` sub-dicts supply the era-correct ``espn_abbr``.
        parsed: The frame ``shield_nfl_pbp(game_detail, enrich=False)`` already produced.
            Built here when None -- pass it to parse the payload once for both projections.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` (the stored
            closing line, :func:`sportsdataverse.football.sources.idmap._odds_override_from_row`).
            Becomes the summary's one-provider ``pickcenter``.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header` (season/week/competitions/competitors/status), `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter`, and empty `boxscore` / passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a missing `summary.timeouts` block, a PAT with no touchdown to fold into, an unparseable drive chart. |

    Raises:
        KeyError: ``idmap_row`` is missing ``espn_event_id`` or a team id.

    Example:
        Adapt a stored final and process it::

            import json
            from sportsdataverse.nfl.shield_pbp import shield_to_espn_summary
            from sportsdataverse.nfl import NFLPlayProcess

            game = json.load(open("nfl/raw/2025/2025_07_LA_JAX.json"))
            row = {"espn_event_id": "401772635", "home_espn_team_id": "30", "away_espn_team_id": "14"}
            summary, notes = shield_to_espn_summary(game, row)
            proc = NFLPlayProcess(gameId=401772635, join_participants=False)
            proc.espn_nfl_pbp(summary=summary)
            result = proc.run_processing_pipeline()
    """
    game = game_detail.get("data") if ("driveChart" not in game_detail and "data" in game_detail) else game_detail
    game = game or {}
    notes: List[str] = []

    event_id = str(idmap_row["espn_event_id"])
    home_id, away_id = str(idmap_row["home_espn_team_id"]), str(idmap_row["away_espn_team_id"])
    abbrs = {
        home_id: ((idmap_row.get("home_team") or {}) or {}).get("espn_abbr"),
        away_id: ((idmap_row.get("away_team") or {}) or {}).get("espn_abbr"),
    }

    if parsed is None:
        from sportsdataverse.nfl.shield_pbp.build import shield_nfl_pbp

        parsed = shield_nfl_pbp(game_detail=game_detail, enrich=False)
    rows: Dict[Any, Dict[str, Any]] = {}
    home_abbr = away_abbr = None
    if parsed is not None and parsed.height:
        home_abbr, away_abbr = parsed.item(0, "home_team"), parsed.item(0, "away_team")
        rows = {r["play_id"]: r for r in parsed.to_dicts() if r.get("play_id") is not None}

    summary_block = game.get("summary") or {}
    phase = game_phase(game)
    drive_chart = game.get("driveChart") or {}
    drives_raw = [d for d in (drive_chart.get("drives") or []) if d.get("startedPlaySequenceNumber") is not None]
    drives_raw.sort(key=lambda d: d["startedPlaySequenceNumber"])
    drive_starts = [float(d["startedPlaySequenceNumber"]) for d in drives_raw]

    plays_raw = [p for p in (drive_chart.get("plays") or []) if not p.get("playDeleted")]
    plays_raw.sort(key=lambda p: p.get("playSequenceNumber") or 0)
    # ESPN stamps a scoring play with the clock the score went up at -- the *next* row's
    # snap clock -- while every other row carries its own. (Measured on the crosswalked
    # 2025 finals: the only rows whose clock disagrees are the scoring ones.)
    next_clock = [
        (plays_raw[i + 1].get("clockTime") if i + 1 < len(plays_raw) else None) for i in range(len(plays_raw))
    ]
    has_overtime = any((p.get("quarter") or 0) >= 5 for p in plays_raw)

    shield_team_ids = {
        (summary_block.get("homeTeam") or {}).get("teamId"): home_id,
        (summary_block.get("awayTeam") or {}).get("teamId"): away_id,
    }

    # (abbreviation, location, mascot, name_alt) per side -- what ``_nfl_timeout_side`` matches.
    home_names, away_names = (
        _team_names(game.get("homeTeam") or {}, abbrs.get(home_id)),
        _team_names(game.get("awayTeam") or {}, abbrs.get(away_id)),
    )
    emitted: List[Tuple[int, Dict[str, Any]]] = []  # (drive index, play)
    scoring_teams: Dict[str, str] = {}  # play id -> the ESPN team id credited with the score
    home_points = away_points = 0
    last_yard_line: Optional[int] = None
    for raw_index, play in enumerate(plays_raw):
        play_type = play.get("playType") or "UNSPECIFIED"
        if play_type in _SKIP_PLAY_TYPES:
            continue
        row = rows.get(play.get("playId")) or {}
        text = _text(play.get("playDescription"))
        points = _SCORING_POINTS.get(play.get("scoringPlayType"), 0) if play.get("playScored") else 0
        scored_home = shield_team_ids.get(play.get("scoringTeamId")) == home_id
        home_points += points if scored_home else 0
        away_points += points if points and not scored_home else 0

        if play_type in _PAT_PLAY_TYPES:
            # ESPN folds the try into its touchdown: same play id, one text, one score step.
            if not emitted:
                notes.append(f"play {play.get('playId')}: {play_type} with no touchdown to fold into")
                continue
            target = emitted[-1][1]
            target["text"] = f"{target['text']} {text}".strip()
            target["homeScore"], target["awayScore"] = home_points, away_points
            after = _point_after(row)
            if after is not None:
                target["pointAfterAttempt"] = after
            continue

        clock = play.get("clockTime")
        if (
            play.get("playScored")
            and next_clock[raw_index]
            and plays_raw[raw_index + 1].get("quarter") == play.get("quarter")
        ):
            clock = next_clock[raw_index]
        clock = _impute_clock(clock, play.get("playDescription"))
        quarter = play.get("quarter")
        admin = play_type in ("TIMEOUT", "END_QUARTER", "END_GAME")
        down = 0 if admin else int(play.get("down") or 0)
        distance = 0 if admin else int(play.get("yardsRemaining") or 0)
        # A clock-stoppage row states no yard line; ESPN carries the ball's spot onto it.
        # Carrying it keeps the column an integer -- one null turns pandas' json_normalize
        # column into float64, and the processor's incompletion UDF is declared Int64.
        yard_line = _abs_yard_line(play.get("yardLine"), home_abbr)
        if yard_line is None:
            yard_line = last_yard_line
        else:
            last_yard_line = yard_line
        team_id = shield_team_ids.get(
            _possession_team_id(play, row, shield_team_ids, home_id, away_id, drives_raw, (home_abbr, away_abbr))
        )
        goal_to_go = bool(play.get("playIsGoalToGo"))
        espn_play: Dict[str, Any] = {
            "id": f"{event_id}{play.get('playId')}",
            "sequenceNumber": str(int((play.get("playSequenceNumber") or 0) * 100)),
            "type": _espn_type(play, row, text, has_overtime),
            "text": text,
            "awayScore": away_points,
            "homeScore": home_points,
            "period": {"number": quarter},
            "clock": {"displayValue": _clock(clock)},
            "scoringPlay": bool(play.get("playScored")),
            "priority": False,
            "statYardage": _stat_yardage(play_type, play, row),
            "start": {
                "down": down,
                "distance": distance,
                "yardLine": yard_line,
                "yardsToEndzone": _to_endzone(yard_line, team_id, home_id),
                "downDistanceText": _down_distance_text(down, distance, goal_to_go, play.get("yardLine")),
                "team": {"id": team_id},
            },
            "end": {},
        }
        scoring_team = shield_team_ids.get(play.get("scoringTeamId"))
        if scoring_team:
            scoring_teams[espn_play["id"]] = scoring_team
        if play.get("scoringPlayType") not in (None, "UNSPECIFIED"):
            espn_play["scoringType"] = _SCORING_TYPES.get(play["scoringPlayType"], {})
        if espn_play["type"]["text"] == "Timeout":
            # A Shield timeout row carries no team id -- the charged club is only in the text,
            # exactly as it is in ESPN's. Reuse the processor's own parser (imported here, not
            # at module scope: it pulls nfl_pbp, which loads the EP/WP models) so the adapter
            # and the pipeline can never disagree about who was charged.
            from sportsdataverse.nfl.nfl_pbp import _nfl_timeout_side

            side = _nfl_timeout_side(text, home_names, away_names)
            espn_play["homeTimeoutCalled"] = side == "home"
            espn_play["awayTimeoutCalled"] = side == "away"
        emitted.append((_drive_index(drive_starts, play.get("playSequenceNumber")), espn_play))

    _order_stoppages(emitted, event_id)
    _fill_end_state(emitted, home_id, scoring_teams)

    season = game.get("season")
    if season is not None and int(season) < 2014:
        # Stage 3 coverage study: ESPN's own play ids only line up with the GSIS ones from
        # 2014. The id emitted here is still stable and unique -- it just will not join an
        # ESPN-sourced frame, which a shadow-mode comparison has to know.
        notes.append(
            f"season {season} < 2014: play ids are Shield-native "
            f"({{espn_event_id}}{{playId}}); they do not join ESPN's own play ids"
        )
    if not (summary_block.get("homeTeam") or {}).get("timeouts"):
        notes.append("summary.timeouts absent: live timeouts-remaining fall back to the play-derived count")

    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for index, play in emitted:
        grouped.setdefault(index, []).append(play)

    previous = [
        _drive(event_id, i + 1, drive, grouped.get(i + 1, []), shield_team_ids, home_abbr, home_id, abbrs)
        for i, drive in enumerate(drives_raw)
    ]
    orphans = sorted(k for k in grouped if k > len(drives_raw))
    if orphans:
        notes.append(f"{sum(len(grouped[k]) for k in orphans)} plays fell outside the drive chart")
    current = previous.pop() if (previous and not is_final(game) and phase is not None and phase != "PREGAME") else None

    summary = {
        "boxscore": {"teams": [], "players": []},
        "format": {},
        "gameInfo": {"venue": _venue(game), "attendance": summary_block.get("attendance")},
        "drives": {"previous": previous, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(game, summary_block, phase, event_id, home_id, away_id, abbrs, previous, current),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }
    return summary, notes


#: Shield ``scoringPlayType`` -> ESPN ``scoringType`` (``displayName`` is the only field read:
#: it relabels ``type.text`` to "Field Goal Good" / "Extra Point Good").
_SCORING_TYPES: Dict[str, Dict[str, str]] = {
    "TOUCHDOWN": {"name": "touchdown", "displayName": "Touchdown", "abbreviation": "TD"},
    "FIELD_GOAL": {"name": "field-goal", "displayName": "Field Goal", "abbreviation": "FG"},
    "SAFETY": {"name": "safety", "displayName": "Safety", "abbreviation": "SF"},
    "PAT": {"name": "extra-point", "displayName": "Extra Point", "abbreviation": "PAT"},
    "PAT2": {"name": "two-point-conversion", "displayName": "Two Point Conversion", "abbreviation": "2PT"},
}


def _possession_team_id(
    play: Mapping[str, Any],
    row: Mapping[str, Any],
    shield_team_ids: Mapping[Any, str],
    home_id: str,
    away_id: str,
    drives_raw: Sequence[Mapping[str, Any]],
    abbrs: Tuple[Optional[str], Optional[str]],
) -> Any:
    """The Shield team id ESPN would put in ``start.team`` (the kicking team on a kickoff).

    Possession comes from the parsed frame -- the drive-range assignment, including the live
    open drive's synthetic end -- so it is resolved once, in the parser. A kickoff and a PAT
    fall between drives and have no ``posteam``; ESPN credits the kickoff to the kicking team,
    which is the side its own ``yardLine`` names.
    """
    posteam = None if play.get("playType") == "KICK_OFF" else row.get("posteam")
    if posteam is not None:
        for shield_id, espn_id in shield_team_ids.items():
            if (espn_id == home_id and posteam == abbrs[0]) or (espn_id == away_id and posteam == abbrs[1]):
                return shield_id
    side = str(play.get("yardLine") or "").rsplit(" ", 1)
    if len(side) == 2:
        want = _GAMEBOOK_TO_NFLVERSE.get(side[0], side[0])
        if want == abbrs[0]:
            return next((k for k, v in shield_team_ids.items() if v == home_id), None)
        if want == abbrs[1]:
            return next((k for k, v in shield_team_ids.items() if v == away_id), None)
    seq = play.get("playSequenceNumber")
    for drive in reversed(list(drives_raw)):
        if seq is not None and drive["startedPlaySequenceNumber"] <= seq:
            return drive.get("teamId")
    return drives_raw[0].get("teamId") if drives_raw else None


_STOPPAGE_MOVES = frozenset({"Timeout", "Official Timeout", "Two-minute warning"})


def _order_stoppages(emitted: List[Tuple[int, Dict[str, Any]]], event_id: str) -> None:
    """Move a timeout row ahead of the plays that snapped at its clock, and renumber it.

    Shield emits a stoppage *after* the play it interrupted; ESPN emits it *before* -- an
    official timeout sits between the touchdown and the kickoff, not after the kickoff. The
    order is load-bearing: the processor sorts by play id and reads the next row's snap clock
    as this row's end clock, so a stoppage in the wrong slot moves ``end.TimeSecsRem`` and the
    timeouts-remaining cumsum. The row walks back past plays that share its clock and stops at
    the scoring play that put the clock there (or at another stoppage).

    Stoppage ids are the only ones renumbered, and ESPN renumbers them too -- its timeout ids
    match no GSIS play id, so nothing joins on them.
    """
    for i in range(len(emitted)):
        if emitted[i][1]["type"]["text"] not in _STOPPAGE_MOVES:
            continue
        j = i
        while j > 0:
            prev = emitted[j - 1][1]
            if (
                prev["clock"]["displayValue"] != emitted[j][1]["clock"]["displayValue"]
                or prev["scoringPlay"]
                or prev["type"]["text"] in _CLOCK_STOPPAGE
            ):
                break
            emitted[j - 1], emitted[j] = emitted[j], emitted[j - 1]
            j -= 1
    numbers = [int(p["id"][len(event_id) :]) for _, p in emitted]
    for i, (_, play) in enumerate(emitted):
        if play["type"]["text"] not in _STOPPAGE_MOVES:
            continue
        low = numbers[i - 1] if i else 0
        high = numbers[i + 1] if i + 1 < len(numbers) else low + 2
        if not low < numbers[i] < high:
            numbers[i] = high - 1 if high - 1 > low else low + 1
            play["id"] = f"{event_id}{numbers[i]}"


def _fill_end_state(emitted: List[Tuple[int, Dict[str, Any]]], home_id: str, scoring_teams: Mapping[str, str]) -> None:
    """Fill every play's ``end`` from the **next** play's start, across drive boundaries.

    Shield states a play's pre-snap situation and nothing else, so the end state is the next
    snap's start -- which is how ESPN builds it too, and why it must not stop at a drive
    boundary (a punt ends in the receiving team's frame, on the next drive's first row).
    A scoring play instead takes ESPN's own convention (``down = -1``, the goal line, zero to
    the end zone, credited to the team the feed says scored -- the defence on a return
    touchdown); the last play of a live feed has no next
    snap and keeps its own start. Clock-stoppage rows carry no state of their own, so they
    are skipped when looking for the next snap -- ESPN's end spot on the play before a
    timeout is the ball's real spot, not the timeout row's.
    """
    for i, (_, play) in enumerate(emitted):
        nxt = next(
            (p["start"] for _, p in emitted[i + 1 :] if p["type"]["text"] not in _CLOCK_STOPPAGE),
            play["start"],
        )
        if play["scoringPlay"] and play["type"]["abbreviation"] in ("TD", "FG", "SF"):
            # the team **credited with the score**, not the team that snapped it: on a return
            # touchdown they are opposite sides, and crediting the offence flips the end spot
            # 100 yards and re-types the play ("Punt Return Touchdown" became "Punt Team Fumble
            # Recovery Touchdown", worth 14 EPA on the row).
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            goal_line = 100 if str(team) == str(home_id) else 0
            play["end"] = {
                "down": -1,
                "distance": 10,
                "yardLine": goal_line,
                "yardsToEndzone": 0,
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


def _venue(game: Mapping[str, Any]) -> Dict[str, Any]:
    """``gameInfo.venue`` -- ``indoor`` is what ``_nfl_roof`` reads, and Shield never states it."""
    venue = game.get("venue") or {}
    return {
        "id": venue.get("id"),
        "fullName": venue.get("name"),
        "address": {"city": venue.get("city"), "country": venue.get("country")},
    }


def _drive(
    event_id: str,
    index: int,
    drive: Mapping[str, Any],
    plays: List[Dict[str, Any]],
    shield_team_ids: Mapping[Any, str],
    home_abbr: Optional[str],
    home_id: str,
    abbrs: Mapping[str, Optional[str]],
) -> Dict[str, Any]:
    """One ``drives.previous[]`` entry: ESPN's drive id is ``{espn_event_id}{1-based index}``."""
    result = drive.get("endedDescription")
    espn_team_id = shield_team_ids.get(drive.get("teamId"))
    return {
        "id": f"{event_id}{index}",
        "description": f"{drive.get('plays')} plays, {drive.get('yardsGainedNet')} yards, "
        f"{drive.get('timeOfPossession')}",
        "team": {
            "id": str(espn_team_id) if espn_team_id else None,
            "abbreviation": abbrs.get(str(espn_team_id)),
            "shortDisplayName": abbrs.get(str(espn_team_id)),
            "displayName": abbrs.get(str(espn_team_id)),
            "name": abbrs.get(str(espn_team_id)),
        },
        "start": {
            "period": {"number": drive.get("startedQuarter"), "type": "quarter"},
            "clock": {"displayValue": _clock(drive.get("startedClock"))},
            "yardLine": _abs_yard_line(drive.get("startedYardLine"), home_abbr),
            "text": drive.get("startedYardLine"),
        },
        "end": {
            "period": {"number": drive.get("endedQuarter"), "type": "quarter"},
            "clock": {"displayValue": _clock(drive.get("endedClock"))},
            "yardLine": _abs_yard_line(drive.get("endedYardLine"), home_abbr),
            "text": drive.get("endedYardLine"),
        },
        "timeElapsed": {"displayValue": drive.get("timeOfPossession")},
        "yards": drive.get("yardsGainedNet"),
        "isScore": bool(drive.get("endedWithScore")),
        "offensivePlays": drive.get("plays"),
        "result": _drive_result(result),
        "shortDisplayResult": _drive_result(result),
        "displayResult": result,
        "plays": plays,
    }


def _header(
    game: Mapping[str, Any],
    summary_block: Mapping[str, Any],
    phase: Optional[str],
    event_id: str,
    home_id: str,
    away_id: str,
    abbrs: Mapping[str, Optional[str]],
    previous: List[Dict[str, Any]],
    current: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status, all from the payload."""
    home_summary = summary_block.get("homeTeam") or {}
    away_summary = summary_block.get("awayTeam") or {}
    home_score = (home_summary.get("score") or {}) or {}
    away_score = (away_summary.get("score") or {}) or {}
    home_total, away_total = home_score.get("total") or 0, away_score.get("total") or 0
    final = str(phase or "") in ("FINAL", "FINAL_OVERTIME")
    last = (current or (previous[-1] if previous else None)) or {}
    period = None
    for play in reversed(last.get("plays") or []):
        period = (play.get("period") or {}).get("number")
        if period:
            break
    return {
        "id": event_id,
        "uid": f"s:20~l:28~e:{event_id}",
        "season": {"year": game.get("season"), "type": _SEASON_TYPE.get(str(game.get("seasonType") or ""), 2)},
        "week": game.get("week"),
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:28~e:{event_id}~c:{event_id}",
                "date": game.get("time") or game.get("date"),
                "neutralSite": bool(game.get("neutralSite")),
                "conferenceCompetition": False,
                "boxscoreAvailable": False,
                "commentaryAvailable": False,
                "liveAvailable": False,
                "onWatchESPN": False,
                "recent": False,
                "boxscoreSource": "none",
                "playByPlaySource": "full",
                "status": _status(summary_block, phase, period),
                "competitors": [
                    _competitor(
                        "home",
                        0,
                        home_id,
                        game.get("homeTeam") or {},
                        abbrs.get(home_id),
                        home_score,
                        final and home_total > away_total,
                        bool(home_summary.get("hasPossession")),
                    ),
                    _competitor(
                        "away",
                        1,
                        away_id,
                        game.get("awayTeam") or {},
                        abbrs.get(away_id),
                        away_score,
                        final and away_total > home_total,
                        bool(away_summary.get("hasPossession")),
                    ),
                ],
            }
        ],
    }


def _shield_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="shield"`` (NFL). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (``SourceUnavailable``) when the game is not mapped to a
    Shield uuid, when the fetch fails, or when Shield has no drive chart for it -- the state
    every pre-kickoff game is in, and the state a **cancelled** game stays in (2022 week 17
    BUF-CIN has a Shield payload and no nflverse row; ESPN then serves it).
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row
    from sportsdataverse.nfl.shield_pbp.build import shield_nfl_pbp

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    shield_game_id = row.get("shield_game_id")
    payload = ctx.payload
    if payload is None:
        if not shield_game_id:
            raise SourceUnavailable(f"nfl {espn_id}: no shield_game_id in the id map")
        from sportsdataverse.nfl.nfl_api import nfl_game_details_v2

        try:
            payload = nfl_game_details_v2(shield_game_id, include_drive_chart=True, return_parsed=False)
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"shield gamedetails fetch failed: {type(exc).__name__}: {exc}") from exc
    if row.get("home_espn_team_id") is None or row.get("away_espn_team_id") is None:
        raise SourceUnavailable(f"nfl {espn_id}: id-map row carries no ESPN team ids")

    parsed = shield_nfl_pbp(game_detail=payload, enrich=False)
    if parsed.is_empty():
        raise SourceUnavailable(f"nfl {espn_id}: shield payload has no drive chart (not started, or cancelled)")
    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row)
    summary, notes = shield_to_espn_summary(payload, row, parsed=parsed, odds=odds)
    if not any(d.get("plays") for d in summary["drives"]["previous"]):
        # a payload whose only rows are the feed's GAME_START marker: kicked off but nothing
        # snapped yet. The contract would fail on ``drives[].plays[]``; say why instead.
        raise SourceUnavailable(f"nfl {espn_id}: shield drive chart carries no plays yet")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=ctx.odds_override,
        native_ids={"espn_event_id": str(espn_id), "shield_game_id": shield_game_id},
        notes=notes,
    )


def _register_shield() -> None:
    """Register :func:`_shield_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("nfl", "shield")(_shield_adapter)


_register_shield()
