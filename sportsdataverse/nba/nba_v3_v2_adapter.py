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
live feed on 3 committed fixtures. Event assembly and score/description
derivation land in later tasks of Phase A.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

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
