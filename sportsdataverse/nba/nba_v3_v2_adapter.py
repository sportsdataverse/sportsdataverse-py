"""v3 -> v2 play-by-play adapter: lookup tables, roster builder, name-match.

Ports hoopR's ``.v3_to_v2_format()`` (``R/nba_stats_pbp.R``) into sdv-py. This
module is the **pure, network-free** substrate for the full v3 -> v2
transformation (Phase A of the pbpstats-adaptation program): the v3
``actionType`` -> v2 ``EVENTMSGTYPE`` map, the parent-keyed ``subType`` ->
``EVENTMSGACTIONTYPE`` tables, the per-game roster builder, the 4-tier
player name-match used to resolve names embedded in v3 play descriptions
(assists, substitutions, jump balls, blocks/steals) back to a ``person_id``,
and the description-regex secondary-player extraction itself.

Task 1 scope: the tables + ``_build_roster`` + ``_lookup_player``.

Task 2 scope: ``_extract_secondary_players`` -- the v3 ``playbyplayv3``
feed drops the secondary participants (assist/block/steal/sub-in/jump) that
the older v2 feed carried as ``player2``/``player3``. This function recovers
them from the v3 ``description`` text (assist/sub/jump) plus a structural
block/steal consolidation trick (standalone rows that carry the blocker/
stealer as ``personId`` directly), validated 1-to-1 against the richer cdn
live feed on 3 committed fixtures.

Task 3 scope: ``nba_v3_to_v2_pbp`` -- the public assembly function. Turns a
``playbyplayv3`` payload + ``boxscoretraditionalv3`` payload into the full
v2-schema polars frame that Phases B/C/D (possessions, lineups, stat
tracking) build on. It drops the consolidated block/steal rows (Task 2 wires
them into their parent Missed Shot / Turnover instead), derives
``event_type``/``event_action_type`` from the lookup tables above, splits
``description`` by ``location`` into home/visitor/neutral, forward-fills the
running score, and enriches ``player2``/``player3`` fields from the roster
**by id** -- the one deliberate divergence from hoopR, which re-resolves
block/steal names in its per-row loop and can silently override the
reliable ``personId`` it already captured (see :func:`_secondary_fields`).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

# ---------------------------------------------------------------------------
# V3 description-regex extraction (Task 2)
# ---------------------------------------------------------------------------
# Compiled once at module scope. Verbatim from the design spec / proven
# ``desc_extract2.py`` scratchpad recipe -- polars/Rust regex lookaround
# restrictions do not apply here (this module uses Python's stdlib ``re``,
# not polars expressions).
_ASSIST_RE = re.compile(r"\(([^)]+?)\s+[0-9]+\s+AST\)")
_SUB_IN_RE = re.compile(r"SUB:\s+(.+?)\s+FOR\s+.+")
_JUMP_BALL_RE = re.compile(r"Jump Ball\s+.+?\s+vs\.\s+(.+?)(?::\s+Tip to\s+(.+?))?\s*$")

# Block/steal standalone-row association window: search +/- this many rows
# around the standalone row for the Missed Shot / Turnover at the same
# (period, clock). Verbatim from desc_extract2.py.
_BLOCK_STEAL_WINDOW = 6

# ---------------------------------------------------------------------------
# V3 actionType -> V2 EVENTMSGTYPE
# ---------------------------------------------------------------------------
# Verbatim port of hoopR's ``event_type_map`` (``.v3_to_v2_format``). Callers
# should look up with ``_EVENT_TYPE_MAP.get(action_type, "0")`` for unknown
# actionType strings. NOTE: ``"period"`` always maps to ``"12"`` here (period
# *start*); the period-*end* -> ``"13"`` override is applied at assembly time
# (Task 3) based on ``sub_type == "end"``, not in this table.
_EVENT_TYPE_MAP: Dict[str, str] = {
    "period": "12",
    "Jump Ball": "10",
    "Made Shot": "1",
    "Missed Shot": "2",
    "Free Throw": "3",
    "Rebound": "4",
    "Turnover": "5",
    "Foul": "6",
    "Violation": "7",
    "Substitution": "8",
    "Timeout": "9",
    "Ejection": "11",
    "Instant Replay": "18",
    "Game": "12",
    "Stoppage": "18",
}

# ---------------------------------------------------------------------------
# V3 subType -> V2 EVENTMSGACTIONTYPE (6 parent-keyed tables)
# ---------------------------------------------------------------------------
# Verbatim port of hoopR's shot_action_map / ft_action_map / to_action_map /
# foul_action_map / timeout_action_map / violation_action_map, sourced from
# the NBA API's canonical EVENTMSGACTIONTYPE catalog
# (``hoopR/data-raw/nba_eventmsg_action_types - nba_api_eventtypes.csv``).
# ``shot`` covers BOTH ``Made Shot`` and ``Missed Shot`` parents (the v2
# EVENTMSGACTIONTYPE codes for shot subtypes are shared across make/miss).
#
# A handful of entries below are *extensions* beyond the hoopR source,
# added because they appear in the committed sdv-py fixture
# (``tests/fixtures/nba_engine/0022300001/playbyplayv3.json``) but have no
# exact-string entry in the R source. Each extension reuses the
# EVENTMSGACTIONTYPE of the closest existing sibling subtype and is called
# out inline. Extend these tables as new subTypes are observed in future
# captures -- they are not claimed to be exhaustive of the NBA API surface.
_ACTION_TYPE_MAPS: Dict[str, Dict[str, str]] = {
    "shot": {
        "Jump Shot": "1",
        "Hook Shot": "3",
        "Layup": "5",
        "Layup Shot": "5",
        "Dunk": "7",
        "Dunk Shot": "7",
        "Running Layup": "41",
        "Running Layup Shot": "41",
        "Driving Layup": "42",
        "Driving Layup Shot": "42",
        "Alley Oop Layup": "43",
        "Alley Oop Layup Shot": "43",
        # Fixture extension: lowercase "shot" variant of "Alley Oop Layup
        # Shot" above -- reuses the same code (43).
        "Alley Oop Layup shot": "43",
        "Alley Oop Dunk Shot": "43",
        "Running Alley Oop Dunk Shot": "43",
        "Running Jump Shot": "46",
        "Turnaround Jump Shot": "47",
        "Driving Dunk": "49",
        "Driving Dunk Shot": "49",
        "Running Dunk": "50",
        "Running Dunk Shot": "50",
        "Driving Hook Shot": "57",
        "Turnaround Hook Shot": "58",
        "Fadeaway Jumper": "63",
        "Fadeaway Jump Shot": "63",
        "Jump Bank Shot": "66",
        "Putback Layup": "72",
        "Putback Layup Shot": "72",
        "Driving Reverse Layup": "73",
        "Driving Reverse Layup Shot": "73",
        # Fixture extension: no distinct EVENTMSGACTIONTYPE for a "running"
        # reverse layup in the NBA API catalog -- reuses the Reverse Layup
        # family code (73), same as Driving Reverse Layup.
        "Running Reverse Layup Shot": "73",
        "Running Finger Roll Layup": "76",
        "Running Finger Roll Layup Shot": "76",
        "Floating Jump Shot": "78",
        "Floating Jump shot": "78",
        "Driving Floating Jump Shot": "78",
        "Driving Floating Bank Jump Shot": "78",
        "Pullup Jump Shot": "79",
        "Pullup Jump shot": "79",
        "Running Pull-Up Jump Shot": "79",
        "Step Back Jump Shot": "80",
        "Step Back Jump shot": "80",
        "Step Back Bank Jump Shot": "80",
        "Driving Bank Shot": "82",
        "Turnaround Fadeaway": "86",
        "Turnaround Fadeaway shot": "86",
        "Tip Layup Shot": "97",
        "Cutting Layup Shot": "98",
        "Cutting Dunk Shot": "108",
        "Tip Dunk Shot": "108",
        "Cutting Finger Roll Layup Shot": "98",
        "Driving Finger Roll Layup Shot": "42",
        "Finger Roll Layup Shot": "76",
    },
    "ft": {
        "Free Throw 1 of 1": "10",
        "Free Throw 1 of 2": "11",
        "Free Throw 2 of 2": "12",
        "Free Throw 1 of 3": "13",
        "Free Throw 2 of 3": "14",
        "Free Throw 3 of 3": "15",
        "Free Throw Technical": "16",
        "Free Throw Flagrant 1 of 2": "18",
        "Free Throw Flagrant 2 of 2": "19",
        "Free Throw Flagrant 1 of 1": "20",
        "Free Throw Clear Path 1 of 2": "25",
        "Free Throw Clear Path 2 of 2": "26",
        "Free Throw Flagrant 1 of 3": "27",
        "Free Throw Flagrant 2 of 3": "28",
        "Free Throw Flagrant 3 of 3": "29",
    },
    "turnover": {
        "Bad Pass": "1",
        "Lost Ball": "2",
        "Traveling": "4",
        "Offensive Foul Turnover": "37",
        "Double Dribble": "6",
        "Discontinue Dribble": "7",
        "3 Second Violation": "8",
        "5 Second Violation": "9",
        "8 Second Violation": "10",
        "Shot Clock Violation": "11",
        "Shot Clock": "11",
        # Fixture extension: same shot-clock-violation code family as
        # "Shot Clock" / "Shot Clock Violation" above.
        "Shot Clock Turnover": "11",
        "Inbound Turnover": "12",
        "Backcourt Turnover": "13",
        "Backcourt": "13",
        "Offensive Goaltending": "15",
        "Lane Violation": "17",
        "Kicked Ball Violation": "19",
        "Palming": "21",
        "5 Second Inbound": "38",
        "Step Out of Bounds": "39",
        # Fixture extension: "Turnover"-suffixed variant of "Step Out of
        # Bounds" above -- reuses the same code (39).
        "Step Out of Bounds Turnover": "39",
        "Out of Bounds Lost Ball Turnover": "40",
        "Out of Bounds - Bad Pass Turnover": "45",
    },
    "foul": {
        "Personal": "1",
        "Shooting": "2",
        "Loose Ball": "3",
        "Offensive": "4",
        "Intentional": "5",
        "Away From Play": "6",
        "Clear Path": "9",
        "Double Technical": "10",
        "Technical": "11",
        # Fixture extension: no distinct code for a technical assessed for
        # delay of game -- reuses the plain Technical code (11).
        "Delay Technical": "11",
        "Flagrant 1": "14",
        "Flagrant Type 1": "14",
        "Flagrant 2": "15",
        "Flagrant Type 2": "15",
        "Defense 3 Second": "17",
        "Taunting": "19",
        "Excess Timeout": "25",
        "Charge": "26",
        "Block": "27",
        "Personal Take": "28",
        # Fixture extension: "Offensive Charge" is an offensive foul for a
        # charge -- reuses the Offensive code (4), same family as "Offensive".
        "Offensive Charge": "4",
        "Shooting Block": "29",
    },
    "timeout": {
        "Regular": "1",
        "Short": "2",
        "Official": "4",
    },
    "violation": {
        "Delay Of Game": "1",
        "Delay of Game": "1",
        "Defensive Goaltending": "2",
        "Lane Violation": "3",
        "Jump Ball Violation": "4",
        "Kicked Ball": "5",
        "Kicked Ball Violation": "5",
        "Double Lane Violation": "6",
    },
}


def _build_roster(box_v3: Optional[dict]) -> Dict[int, dict]:
    """Build a per-``person_id`` roster lookup from a v3 boxscore payload.

    Ports hoopR's ``.build_player_roster`` -- iterates
    ``boxScoreTraditional.{homeTeam,awayTeam}.players[]`` and attaches each
    team's ``teamId``/``teamCity``/``teamName``/``teamTricode`` to every one
    of its players.

    Args:
        box_v3: Raw dict from ``nba_stats_boxscoretraditionalv3`` (or
            ``None``/``{}``).

    Returns:
        ``{person_id: {"first", "family", "name_i", "team_id", "city",
        "nickname", "tricode", "full_name"}}``. Empty dict on malformed or
        missing input (never raises).
    """
    bst = (box_v3 or {}).get("boxScoreTraditional") or {}
    roster: Dict[int, dict] = {}
    for side in ("homeTeam", "awayTeam"):
        team = bst.get(side) or {}
        team_id = team.get("teamId")
        if team_id is None:
            continue
        city = team.get("teamCity")
        nickname = team.get("teamName")
        tricode = team.get("teamTricode")
        for player in team.get("players") or []:
            person_id = player.get("personId")
            if person_id is None:
                continue
            first = (player.get("firstName") or "").strip()
            family = (player.get("familyName") or "").strip()
            roster[int(person_id)] = {
                "first": first,
                "family": family,
                "name_i": player.get("nameI") or "",
                "team_id": int(team_id),
                "city": city,
                "nickname": nickname,
                "tricode": tricode,
                "full_name": f"{first} {family}".strip(),
            }
    return roster


def _lookup_player(name: Optional[str], roster: Dict[int, dict]) -> Optional[int]:
    """Resolve a player name extracted from a v3 description to a ``person_id``.

    4-tier resolution, ported from hoopR's ``.lookup_player``, in order:

    1. Exact match on ``family`` (case-insensitive).
    2. Exact match on ``name_i`` (e.g. ``"E. Mobley"``).
    3. ``"F. Family"`` abbreviation built from the roster entry's
       first-initial + ``". "`` + family name.
    4. Fuzzy substring: *name* found within the roster entry's ``full_name``
       or ``family`` (case-insensitive).

    Matching is case-insensitive at every tier per the project's
    name-reconciliation convention (case is not load-bearing for player
    names). On a family-name collision (2+ ids sharing a tier's match), the
    first match in roster iteration order is returned -- a later task adds
    on-court disambiguation, mirroring the sub-in resolver in
    ``nba_lineups._resolve_sub_in``.

    Args:
        name: Player name string extracted from a v3 play description
            (assist parenthetical, sub-in name, jump-ball participant, block/
            steal name). May be ``None`` or empty.
        roster: Roster dict from :func:`_build_roster`.

    Returns:
        The matching ``person_id``, or ``None`` on a total miss (unmatched
        name, empty name, or empty roster).
    """
    if not name or not roster:
        return None
    target = name.strip()
    if not target:
        return None
    target_lower = target.lower()

    # Tier 1: exact family-name match.
    for person_id, info in roster.items():
        if (info.get("family") or "").strip().lower() == target_lower:
            return person_id

    # Tier 2: exact name_i match (e.g. "E. Mobley").
    for person_id, info in roster.items():
        if (info.get("name_i") or "").strip().lower() == target_lower:
            return person_id

    # Tier 3: "F. Family" abbreviation (first-initial + ". " + family).
    for person_id, info in roster.items():
        first = (info.get("first") or "").strip()
        family = (info.get("family") or "").strip()
        if not first or not family:
            continue
        abbrev = f"{first[0]}. {family}".lower()
        if abbrev == target_lower:
            return person_id

    # Tier 4: fuzzy substring -- name found within full_name or family.
    for person_id, info in roster.items():
        full_name = (info.get("full_name") or "").lower()
        family = (info.get("family") or "").lower()
        if target_lower in full_name or target_lower in family:
            return person_id

    return None


def _extract_secondary_players(actions: List[dict], roster: Dict[int, dict]) -> Dict[int, dict]:
    """Recover v2 ``player2_id``/``player3_id`` from v3 ``playbyplayv3`` actions.

    The v3 feed drops the secondary participants (assist/block/steal/sub-in/
    jump-ball) that the v2 feed carried directly as ``player2``/``player3``.
    This ports the proven ``desc_extract2.py`` scratchpad recipe (assist
    55/55, block 14/14, steal 17/17 on game 0022300001) and adds sub + jump
    extraction per spec sections 2/3, resolving names via :func:`_lookup_player`
    instead of a family-only resolver.

    Extraction rules, in order:

    1. **Assist -> player2_id**: on a ``Made Shot``, regex
       ``\\(([^)]+?)\\s+[0-9]+\\s+AST\\)`` against ``description``; the
       captured name is resolved via :func:`_lookup_player`.
    2. **Block -> player3_id** / **Steal -> player2_id**: a standalone row
       (``actionType == ""`` with ``"BLOCK"``/``"STEAL"`` in ``description``)
       carries the blocker/stealer as ``personId`` directly (no name
       resolution -- never ``None``). It is associated with the nearest
       ``Missed Shot`` (block) / ``Turnover`` (steal) at the same
       ``(period, clock)``, searching rows ``[i-6, i+6]`` around the
       standalone row. The standalone rows themselves are never emitted as
       keys of the output.
    3. **Sub-in -> player2_id**: on a ``Substitution``, regex
       ``SUB:\\s+(.+?)\\s+FOR\\s+.+`` captures the INCOMING player's name
       (the row's own ``personId`` is the OUTGOING player = player1,
       handled elsewhere); resolved via :func:`_lookup_player`.
    4. **Jump ball -> player2_id (vs.) + player3_id (tip-to)**: regex
       ``Jump Ball\\s+.+?\\s+vs\\.\\s+(.+?)(?::\\s+Tip to\\s+(.+?))?\\s*$``;
       group 1 (the "vs." jumper) resolves to ``player2_id``, the optional
       group 2 ("Tip to" recipient, when present) resolves to
       ``player3_id``. The row's own ``personId`` is the first jumper =
       player1, handled elsewhere.

    A resolved value is recorded even when :func:`_lookup_player` misses
    (``None``) so misses stay countable against the cdn structured-truth
    oracle, mirroring the ``desc_extract2.py`` agree/miss/mismatch
    accounting; block/steal values come from ``personId`` directly and are
    therefore never ``None``.

    Args:
        actions: The v3 ``playbyplayv3`` payload's ``["game"]["actions"]``
            list. Each element is expected to carry ``actionNumber``,
            ``actionType``, ``subType``, ``description``, ``personId``,
            ``period``, and ``clock``.
        roster: Roster dict from :func:`_build_roster`.

    Returns:
        ``{actionNumber: {"player2_id": Optional[int], "player3_id":
        Optional[int]}}`` -- only ``actionNumber`` keys that had at least
        one extraction rule fire are present; each present dict carries
        only the key(s) that rule set.
    """
    result: Dict[int, dict] = {}

    def _record(action_number: Optional[int], key: str, value: Optional[int]) -> None:
        if action_number is None:
            return
        result.setdefault(action_number, {})[key] = value

    # 1. Assist -> player2_id on Made Shot.
    for action in actions:
        if action.get("actionType") != "Made Shot":
            continue
        match = _ASSIST_RE.search(action.get("description") or "")
        if match:
            _record(action.get("actionNumber"), "player2_id", _lookup_player(match.group(1), roster))

    # 2. Block -> player3_id / Steal -> player2_id: standalone rows carry
    # personId directly, associated to the nearest Missed Shot / Turnover
    # at the same (period, clock) within +/- _BLOCK_STEAL_WINDOW rows.
    n = len(actions)
    for i, action in enumerate(actions):
        if (action.get("actionType") or "") != "":
            continue
        description = action.get("description") or ""
        if "BLOCK" in description:
            target_action_type, target_key = "Missed Shot", "player3_id"
        elif "STEAL" in description:
            target_action_type, target_key = "Turnover", "player2_id"
        else:
            continue
        person_id = action.get("personId")
        if person_id is None:
            continue
        period = action.get("period")
        clock = action.get("clock")
        lo = max(0, i - _BLOCK_STEAL_WINDOW)
        hi = min(n, i + _BLOCK_STEAL_WINDOW + 1)
        for j in range(lo, hi):
            candidate = actions[j]
            if (
                candidate.get("actionType") == target_action_type
                and candidate.get("period") == period
                and candidate.get("clock") == clock
            ):
                _record(candidate.get("actionNumber"), target_key, int(person_id))
                break

    # 3. Sub-in -> player2_id.
    for action in actions:
        if action.get("actionType") != "Substitution":
            continue
        match = _SUB_IN_RE.search(action.get("description") or "")
        if match:
            _record(action.get("actionNumber"), "player2_id", _lookup_player(match.group(1), roster))

    # 4. Jump ball -> player2_id (vs.) + player3_id (tip-to, optional).
    for action in actions:
        if action.get("actionType") != "Jump Ball":
            continue
        match = _JUMP_BALL_RE.search(action.get("description") or "")
        if not match:
            continue
        action_number = action.get("actionNumber")
        _record(action_number, "player2_id", _lookup_player(match.group(1), roster))
        tip_to = match.group(2)
        if tip_to:
            _record(action_number, "player3_id", _lookup_player(tip_to, roster))

    return result


# ---------------------------------------------------------------------------
# Task 3: full v2 output schema assembly
# ---------------------------------------------------------------------------

# ISO 8601 clock pattern: "PT10M30.00S" -> minutes / seconds remaining in
# the quarter. No lookaround needed (Python's ``re`` isn't used here -- this
# constant feeds polars' ``str.extract``, whose Rust regex engine also has no
# lookaround support, but this pattern doesn't need any).
_CLOCK_PATTERN = r"PT([0-9]+)M([0-9.]+)S"

# actionType -> the _ACTION_TYPE_MAPS sub-table key used to resolve
# event_action_type (EVENTMSGACTIONTYPE) from subType. actionType values not
# present here (Substitution, Jump Ball, Rebound, period/Game/Ejection/
# Instant Replay/Stoppage, or any unrecognized value) always resolve to "0",
# mirroring hoopR's ``vapply`` switch (R 682-702).
_EVENT_ACTION_TYPE_FAMILY: Dict[str, str] = {
    "Made Shot": "shot",
    "Missed Shot": "shot",
    "Free Throw": "ft",
    "Turnover": "turnover",
    "Foul": "foul",
    "Timeout": "timeout",
    "Violation": "violation",
}

# The full v2 output schema (column order + dtype), used both to select the
# real frame's columns and to build the empty/malformed-input zero-row frame.
# Per-event columns first, then the v3 passthrough columns, matching the
# design spec's "v2 output schema" section verbatim.
_V2_SCHEMA: Dict[str, Any] = {
    "game_id": pl.Utf8,
    "event_num": pl.Utf8,
    "event_type": pl.Utf8,
    "event_action_type": pl.Utf8,
    "period": pl.Int64,
    "clock": pl.Utf8,
    "minute_game": pl.Float64,
    "time_remaining": pl.Float64,
    "wc_time_string": pl.Utf8,
    "time_quarter": pl.Utf8,
    "minute_remaining_quarter": pl.Int64,
    "seconds_remaining_quarter": pl.Int64,
    "action_type": pl.Utf8,
    "sub_type": pl.Utf8,
    "home_description": pl.Utf8,
    "neutral_description": pl.Utf8,
    "visitor_description": pl.Utf8,
    "description": pl.Utf8,
    "location": pl.Utf8,
    "score": pl.Utf8,
    "away_score": pl.Int64,
    "home_score": pl.Int64,
    "score_margin": pl.Utf8,
    "team_leading": pl.Utf8,
    "person1type": pl.Utf8,
    "player1_id": pl.Utf8,
    "player1_name": pl.Utf8,
    "player1_team_id": pl.Utf8,
    "player1_team_city": pl.Utf8,
    "player1_team_nickname": pl.Utf8,
    "player1_team_abbreviation": pl.Utf8,
    "person2type": pl.Utf8,
    "player2_id": pl.Utf8,
    "player2_name": pl.Utf8,
    "player2_team_id": pl.Utf8,
    "player2_team_city": pl.Utf8,
    "player2_team_nickname": pl.Utf8,
    "player2_team_abbreviation": pl.Utf8,
    "person3type": pl.Utf8,
    "player3_id": pl.Utf8,
    "player3_name": pl.Utf8,
    "player3_team_id": pl.Utf8,
    "player3_team_city": pl.Utf8,
    "player3_team_nickname": pl.Utf8,
    "player3_team_abbreviation": pl.Utf8,
    "video_available_flag": pl.Utf8,
    "x_legacy": pl.Int64,
    "y_legacy": pl.Int64,
    "shot_distance": pl.Int64,
    "shot_result": pl.Utf8,
    "is_field_goal": pl.Int64,
    "points_total": pl.Int64,
    "shot_value": pl.Int64,
    "action_number": pl.Int64,
    "team_id": pl.Int64,
    "team_tricode": pl.Utf8,
    "person_id": pl.Int64,
    "player_name": pl.Utf8,
    "score_home": pl.Utf8,
    "score_away": pl.Utf8,
    "action_id": pl.Int64,
}

_V2_COLUMNS: List[str] = list(_V2_SCHEMA.keys())


def _event_action_type(action_type: str, sub_type: str) -> str:
    """Resolve EVENTMSGACTIONTYPE from ``(actionType, subType)``.

    Ports hoopR's ``vapply`` switch (R 682-702): blank ``subType`` always
    resolves to ``"0"``; otherwise the ``actionType`` selects one of the 6
    :data:`_ACTION_TYPE_MAPS` sub-tables (``shot`` covers both Made Shot and
    Missed Shot), and the ``subType`` is looked up in it, defaulting to
    ``"0"`` on a miss. ``actionType`` values with no sub-table (Substitution,
    Jump Ball, Rebound, etc.) always return ``"0"``.
    """
    if not sub_type:
        return "0"
    family = _EVENT_ACTION_TYPE_FAMILY.get(action_type)
    if family is None:
        return "0"
    return _ACTION_TYPE_MAPS[family].get(sub_type, "0")


def _is_dropped_block_steal(action: dict) -> bool:
    """True for a standalone block/steal row consolidated into its parent (R 361-419).

    These are rows with ``actionType == ""`` and ``"BLOCK"``/``"STEAL"`` in
    ``description`` -- :func:`_extract_secondary_players` already folds the
    blocker/stealer into the parent Missed Shot / Turnover row, so the
    standalone row itself must be dropped from the output frame.
    """
    if (action.get("actionType") or "") != "":
        return False
    description = action.get("description") or ""
    return "BLOCK" in description or "STEAL" in description


def _secondary_fields(
    action_number: Optional[int],
    key: str,
    secondary: Dict[int, dict],
    roster: Dict[int, dict],
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Enrich a Task 2 ``player2_id``/``player3_id`` with roster name + team fields.

    **The deliberate divergence from hoopR**: hoopR re-resolves block/steal
    players by NAME inside its per-row loop (R 542-553), which can silently
    override the reliable ``personId`` it captured a few lines earlier (R
    374-416). This helper takes the id straight from
    :func:`_extract_secondary_players` -- computed once, already proven
    1-to-1 against the cdn truth -- and uses the roster **only** to attach
    display name / team metadata for that id. It never re-resolves by name.

    Args:
        action_number: The row's ``actionNumber`` (the key
            :func:`_extract_secondary_players` results are keyed by).
        key: ``"player2_id"`` or ``"player3_id"``.
        secondary: Output of :func:`_extract_secondary_players`.
        roster: Output of :func:`_build_roster`.

    Returns:
        ``(id_str, name, team_id_str, team_city, team_nickname,
        team_tricode)``. All ``None`` when there is no secondary player for
        this action/slot, or when the id has no roster entry (only
        ``id_str`` is then populated).
    """
    if action_number is None:
        return (None, None, None, None, None, None)
    person_id = secondary.get(action_number, {}).get(key)
    if person_id is None:
        return (None, None, None, None, None, None)
    id_str = str(int(person_id))
    info = roster.get(int(person_id))
    if info is None:
        return (id_str, None, None, None, None, None)
    team_id = info.get("team_id")
    team_id_str = str(int(team_id)) if team_id is not None else None
    return (id_str, info.get("full_name"), team_id_str, info.get("city"), info.get("nickname"), info.get("tricode"))


def _empty_v2_frame() -> pl.DataFrame:
    """Zero-row frame carrying the full documented v2 schema (never raises on empty input)."""
    return pl.DataFrame(schema=_V2_SCHEMA)


def _finish(df: pl.DataFrame, return_as_pandas: bool) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return ``df`` as-is, or converted to pandas when requested."""
    if return_as_pandas:
        return df.to_pandas()
    return df


def nba_v3_to_v2_pbp(
    pbp_v3: dict,
    box_v3: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Convert a v3 ``playbyplayv3`` payload into the full v2-schema pbp frame.

    Ports hoopR's ``.v3_to_v2_format()`` (``R/nba_stats_pbp.R`` lines
    210-810) to polars: the v3 feed (``stats.nba.com`` ``playbyplayv3``) is
    reshaped into the older v2 schema that the committed hoopR-nba-stats-data
    dataset carries and that ``pbpstats``' ``stats_nba`` provider consumes.
    This is a pure, network-free function -- both payloads must already be
    fetched (e.g. via ``nba_stats_playbyplayv3`` / ``nba_stats_boxscoretraditionalv3``).

    Pipeline:

    1. Build the per-``person_id`` roster from ``box_v3``
       (:func:`_build_roster`) and recover ``player2_id``/``player3_id``
       (assist/block/steal/sub-in/jump) from ``pbp_v3`` (
       :func:`_extract_secondary_players`).
    2. Drop the standalone block/steal rows consolidated into their parent
       Missed Shot / Turnover (:func:`_is_dropped_block_steal`) -- the only
       row-count change versus the raw v3 action list.
    3. Derive ``event_type``/``event_action_type`` from the module's lookup
       tables, split ``description`` by ``location`` into home/visitor/
       neutral, forward-fill the running score, and enrich ``player2``/
       ``player3`` from the roster **by id** (see :func:`_secondary_fields`
       for the deliberate divergence from hoopR's name-based re-resolution).

    Args:
        pbp_v3: Raw ``playbyplayv3`` dict (``nba_stats_playbyplayv3`` /
            ``wnba_stats_playbyplayv3`` payload shape); actions live at
            ``pbp_v3["game"]["actions"]``.
        box_v3: Raw ``boxscoretraditionalv3`` dict, passed through to
            :func:`_build_roster`.
        return_as_pandas: If ``True``, return a :class:`pandas.DataFrame`
            instead of :class:`polars.DataFrame`.

    Returns:
        Polars (or pandas) DataFrame with the full v2 schema (game/event
        identifiers, event/action type codes, home/visitor/neutral
        descriptions, forward-filled score + margin + leader, per-player
        columns for players 1-3, and the v3 passthrough columns). Empty or
        malformed input returns a zero-row frame with the same schema
        (never raises).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_v3_v2_adapter import nba_v3_to_v2_pbp
            from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3, nba_stats_boxscoretraditionalv3

            pbp_v3 = nba_stats_playbyplayv3(game_id="0022300001", return_parsed=False)
            box_v3 = nba_stats_boxscoretraditionalv3(game_id="0022300001", return_parsed=False)
            df = nba_v3_to_v2_pbp(pbp_v3, box_v3)
            print(df.shape, df.columns)

        Pandas output::

            df_pd = nba_v3_to_v2_pbp(pbp_v3, box_v3, return_as_pandas=True)
            print(type(df_pd))

        Pipeline next step (feed a pbpstats-style consumer)::

            df.filter(pl.col("event_type") == "1").select("player1_name", "player2_name")

        See Also:
            * `hoopR`_ -- the R implementation this function ports (``.v3_to_v2_format``)
            * `nba_api`_ -- reference Python client for stats.nba.com

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
    """
    game = (pbp_v3 or {}).get("game") or {}
    game_id = game.get("gameId") or (pbp_v3 or {}).get("gameId")
    actions: List[dict] = game.get("actions") or []

    roster = _build_roster(box_v3)
    secondary = _extract_secondary_players(actions, roster)

    kept = [action for action in actions if not _is_dropped_block_steal(action)]

    if not kept:
        return _finish(_empty_v2_frame(), return_as_pandas)

    df = pl.DataFrame(kept, infer_schema_length=None)
    df = df.rename({column: underscore(column) for column in df.columns})

    game_id_str: Optional[str] = str(game_id) if game_id is not None else None
    df = df.with_columns(
        pl.lit(game_id_str, dtype=pl.Utf8).alias("game_id"),
        pl.col("action_number").cast(pl.Int64),
        pl.col("period").cast(pl.Int64),
        pl.col("person_id").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("x_legacy").cast(pl.Int64),
        pl.col("y_legacy").cast(pl.Int64),
        pl.col("shot_distance").cast(pl.Int64),
        pl.col("is_field_goal").cast(pl.Int64),
        pl.col("points_total").cast(pl.Int64),
        pl.col("shot_value").cast(pl.Int64),
        pl.col("action_id").cast(pl.Int64),
        pl.col("video_available").cast(pl.Int64, strict=False),
        pl.col("action_type").cast(pl.Utf8).fill_null(""),
        pl.col("sub_type").cast(pl.Utf8).fill_null(""),
        pl.col("location").cast(pl.Utf8).fill_null(""),
        pl.col("description").cast(pl.Utf8).fill_null(""),
        pl.col("team_tricode").cast(pl.Utf8).fill_null(""),
        pl.col("player_name").cast(pl.Utf8).fill_null(""),
        pl.col("shot_result").cast(pl.Utf8).fill_null(""),
        pl.col("clock").cast(pl.Utf8).fill_null(""),
        pl.col("score_home").cast(pl.Utf8),
        pl.col("score_away").cast(pl.Utf8),
    )

    # event_num / event_type (R 671-676, 713-714)
    df = df.with_columns(
        pl.col("action_number").cast(pl.Utf8).alias("event_num"),
        pl.col("action_type").replace_strict(_EVENT_TYPE_MAP, default="0").alias("event_type"),
    )
    df = df.with_columns(
        pl.when((pl.col("action_type") == "period") & (pl.col("sub_type") == "end"))
        .then(pl.lit("13"))
        .otherwise(pl.col("event_type"))
        .alias("event_type")
    )

    # event_action_type (R 678-702) -- per-row family+subType lookup, mirroring
    # hoopR's own per-row `vapply` (not a vectorizable dplyr expression).
    event_action_types: List[str] = [
        _event_action_type(action.get("actionType") or "", action.get("subType") or "") for action in kept
    ]
    df = df.with_columns(pl.Series("event_action_type", event_action_types, dtype=pl.Utf8))

    # Descriptions split by location (R 664-669).
    df = df.with_columns(
        pl.when(pl.col("location") == "h").then(pl.col("description")).otherwise(None).alias("home_description"),
        pl.when(pl.col("location") == "v").then(pl.col("description")).otherwise(None).alias("visitor_description"),
        pl.when((pl.col("location") == "") | pl.col("location").is_null())
        .then(pl.col("description"))
        .otherwise(None)
        .alias("neutral_description"),
    )

    # person1type (R 704-709).
    df = df.with_columns(
        pl.when(pl.col("location") == "h")
        .then(pl.lit("4"))
        .when(pl.col("location") == "v")
        .then(pl.lit("5"))
        .otherwise(pl.lit("0"))
        .alias("person1type")
    )

    # player1 (R 737-743) + person2type/person3type -- hoopR never assigns
    # these in its per-row loop (R 517-525), so they stay null (Task brief #8).
    df = df.with_columns(
        pl.col("person_id").cast(pl.Utf8).alias("player1_id"),
        pl.col("player_name").alias("player1_name"),
        pl.col("team_id").cast(pl.Utf8).alias("player1_team_id"),
        pl.lit(None, dtype=pl.Utf8).alias("player1_team_city"),
        pl.lit(None, dtype=pl.Utf8).alias("player1_team_nickname"),
        pl.col("team_tricode").alias("player1_team_abbreviation"),
        pl.lit(None, dtype=pl.Utf8).alias("person2type"),
        pl.lit(None, dtype=pl.Utf8).alias("person3type"),
    )

    # player2 / player3: id straight from Task 2's extraction, enriched via
    # roster BY ID -- never re-resolved by name (the deliberate divergence).
    p2_id: List[Optional[str]] = []
    p2_name: List[Optional[str]] = []
    p2_team_id: List[Optional[str]] = []
    p2_city: List[Optional[str]] = []
    p2_nick: List[Optional[str]] = []
    p2_abbr: List[Optional[str]] = []
    p3_id: List[Optional[str]] = []
    p3_name: List[Optional[str]] = []
    p3_team_id: List[Optional[str]] = []
    p3_city: List[Optional[str]] = []
    p3_nick: List[Optional[str]] = []
    p3_abbr: List[Optional[str]] = []
    for action in kept:
        action_number = action.get("actionNumber")
        i2, n2, t2, c2, k2, a2 = _secondary_fields(action_number, "player2_id", secondary, roster)
        p2_id.append(i2)
        p2_name.append(n2)
        p2_team_id.append(t2)
        p2_city.append(c2)
        p2_nick.append(k2)
        p2_abbr.append(a2)
        i3, n3, t3, c3, k3, a3 = _secondary_fields(action_number, "player3_id", secondary, roster)
        p3_id.append(i3)
        p3_name.append(n3)
        p3_team_id.append(t3)
        p3_city.append(c3)
        p3_nick.append(k3)
        p3_abbr.append(a3)

    df = df.with_columns(
        pl.Series("player2_id", p2_id, dtype=pl.Utf8),
        pl.Series("player2_name", p2_name, dtype=pl.Utf8),
        pl.Series("player2_team_id", p2_team_id, dtype=pl.Utf8),
        pl.Series("player2_team_city", p2_city, dtype=pl.Utf8),
        pl.Series("player2_team_nickname", p2_nick, dtype=pl.Utf8),
        pl.Series("player2_team_abbreviation", p2_abbr, dtype=pl.Utf8),
        pl.Series("player3_id", p3_id, dtype=pl.Utf8),
        pl.Series("player3_name", p3_name, dtype=pl.Utf8),
        pl.Series("player3_team_id", p3_team_id, dtype=pl.Utf8),
        pl.Series("player3_team_city", p3_city, dtype=pl.Utf8),
        pl.Series("player3_team_nickname", p3_nick, dtype=pl.Utf8),
        pl.Series("player3_team_abbreviation", p3_abbr, dtype=pl.Utf8),
    )

    # video_available_flag = str(video_available).
    df = df.with_columns(pl.col("video_available").cast(pl.Utf8).alias("video_available_flag"))

    # Time columns (R 606-637): parse ISO clock -> minute/second remaining in
    # the quarter, then derive minute_game / time_remaining.
    df = df.with_columns(
        pl.col("clock").str.extract(_CLOCK_PATTERN, 1).cast(pl.Float64, strict=False).alias("_mins"),
        pl.col("clock").str.extract(_CLOCK_PATTERN, 2).cast(pl.Float64, strict=False).alias("_secs"),
    )
    df = df.with_columns(
        pl.col("_mins").floor().cast(pl.Int64).alias("minute_remaining_quarter"),
        pl.col("_secs").floor().cast(pl.Int64).alias("seconds_remaining_quarter"),
    )
    df = df.with_columns(
        (
            pl.col("minute_remaining_quarter").cast(pl.Utf8).str.zfill(2)
            + pl.lit(":")
            + pl.col("seconds_remaining_quarter").cast(pl.Utf8).str.zfill(2)
        ).alias("time_quarter")
    )
    quarter_len = pl.when(pl.col("period") <= 4).then(pl.lit(12.0)).otherwise(pl.lit(5.0))
    elapsed_in_period = quarter_len - (
        pl.col("minute_remaining_quarter").cast(pl.Float64)
        + pl.col("seconds_remaining_quarter").cast(pl.Float64) / 60.0
    )
    df = df.with_columns(elapsed_in_period.alias("_elapsed"))
    minute_game = (
        pl.when(pl.col("period") <= 4)
        .then((pl.col("period") - 1) * 12 + pl.col("_elapsed"))
        .otherwise(48 + (pl.col("period") - 5) * 5 + pl.col("_elapsed"))
    )
    time_remaining = (
        pl.when(pl.col("period") <= 4)
        .then(
            (4 - pl.col("period")) * 12
            + pl.col("minute_remaining_quarter").cast(pl.Float64)
            + pl.col("seconds_remaining_quarter").cast(pl.Float64) / 60.0
        )
        .otherwise(
            pl.col("minute_remaining_quarter").cast(pl.Float64)
            + pl.col("seconds_remaining_quarter").cast(pl.Float64) / 60.0
        )
    )
    df = df.with_columns(
        minute_game.round(2).alias("minute_game"),
        time_remaining.round(2).alias("time_remaining"),
        pl.lit(None, dtype=pl.Utf8).alias("wc_time_string"),
    )

    # Score columns (R 639-662): cast raw string scores to nullable Int64,
    # forward-fill (polars .forward_fill()) with an initial 0 before the
    # first score, then derive score / score_margin / team_leading.
    df = df.with_columns(
        pl.col("score_home").cast(pl.Int64, strict=False).alias("_home_num"),
        pl.col("score_away").cast(pl.Int64, strict=False).alias("_away_num"),
    )
    df = df.with_columns(
        pl.col("_home_num").forward_fill().fill_null(0).alias("home_score"),
        pl.col("_away_num").forward_fill().fill_null(0).alias("away_score"),
    )
    df = df.with_columns(
        (pl.col("away_score").cast(pl.Utf8) + pl.lit(" - ") + pl.col("home_score").cast(pl.Utf8)).alias("score"),
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Utf8).alias("score_margin"),
        pl.when(pl.col("home_score") == pl.col("away_score"))
        .then(pl.lit("Tie"))
        .when(pl.col("home_score") > pl.col("away_score"))
        .then(pl.lit("Home"))
        .otherwise(pl.lit("Away"))
        .alias("team_leading"),
    )

    df = df.select(_V2_COLUMNS)
    return _finish(df, return_as_pandas)
