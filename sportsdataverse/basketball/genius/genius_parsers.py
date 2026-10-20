"""Parsers for the generated ``genius`` wrappers (Genius Sports / FIBA LiveStats, 20+ basketball competitions).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_cased columns, and a zero-row frame (never an exception) for an empty or malformed body. The column set
and dtypes of every frame are pinned in this module (:data:`SCHEMAS`), not inferred from a sample, so an empty
frame carries the documented schema and stacks onto a populated one. The generated wrappers also pass
``columns=`` (the endpoint's returns-schema names); the pinned schema is the source of truth, so it is accepted
and ignored.

**Two body kinds.** ``data.json`` and ``data/competition/{compId}.json`` are JSON. The hosted schedule pages,
the ``/u/`` shell and the site widget are HTML or JavaScript; ``genius_runtime`` reduces them with
:func:`_page_record` to the small ``dict`` records that the refs captures hold, and the frame parsers read
those records.

**Id key spaces** (never mix them in a join).

* ``match_id``: one global sequence across every Genius customer (``Int64``). It is the ``{matchId}`` of
  ``data.json`` and ``/u/``, and a hosted ``extfix_<id>``. ``data.json`` does not carry it, so the runtime stamps
  it under ``_request`` and the data frames read it from there.
* ``comp_id``: one per org competition-season (``Int64``). A ``data.json`` game has none; map it through
  ``genius_match_competition`` or a hosted schedule.
* ``team_id`` (``home_team_id`` / ``away_team_id`` in schedule, widget and competition-window frames): the
  global Genius team id (one key space across orgs and seasons). ``data.json`` carries NO team id: its teams are
  ``tno`` (``1`` or ``2``), in-game only; team 1 is the team listed first.
* ``pno``: the in-game player number, the key of ``tm.<tno>.pl``. It is neither a shirt number nor a person id,
  and it restarts every game. ``data.json`` has no global player id; join players across games on name, team
  code and ``shirt_number``. Shirt numbers are strings (``"00"`` and ``"0"`` differ).
* ``action_number``: unique within a game; a shot joins one play-by-play row on it. Absent in the 2014-15 feed.

**Season.** Genius years are START years for two-year orgs and calendar years otherwise. ``season`` in
:func:`parse_genius_competitions` is the sdv END year (:func:`season_end`): a spelled span in the label wins
(``"2019 - 20"`` is 2020), else the Genius year plus one for a ``split`` org, else the year itself.

**Eras and types.** Percentages arrive as integers in player rows and as floats in some team rows, so every
percentage column is ``Float64`` (0-100). ``s1``/``s2`` of a play-by-play row are strings in most games and
integers in others, and are always cast to ``Int64`` (``score_1``/``score_2``). Minutes are an ``mm:ss`` string,
sometimes an integer; ``minutes`` keeps the text and ``seconds_played`` parses the ``mm:ss`` form (an integer
value has no known unit, so its seconds are null). Court coordinates are percentages of the full court, NOT
normalised to one basket (a team shoots at one end in periods 1-2 and the other in 3-4); the origin corner is
unverified.
"""

from __future__ import annotations

import html as htmllib
import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from urllib.parse import urlsplit

import pandas as pd
import polars as pl

from sportsdataverse.errors import _data_only

__all__ = [
    "parse_genius_competition_games",
    "parse_genius_competitions",
    "parse_genius_game",
    "parse_genius_match_competition",
    "parse_genius_pbp",
    "parse_genius_player_box",
    "parse_genius_schedule",
    "parse_genius_shots",
    "parse_genius_team_box",
    "parse_genius_widget_games",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Where the runtime stamps the request (``match_id``, ``org``, ``comp_id``).
_REQUEST = "_request"

#: ORG code -> season shape: ``split`` = a season over two calendar years (Genius year = START year),
#: ``calendar`` = one calendar year (Genius year = END year). From the refs org registry.
ORG_SPAN: Dict[str, str] = {
    "ESAKE": "split",
    "POL": "split",
    "HKS": "split",
    "FRB": "split",
    "CBFFE": "split",
    "BNXT": "split",
    "CEBL": "calendar",
    "BSN": "calendar",
    "LNBP": "calendar",
    "NZNBL": "calendar",
    "NZN": "calendar",
    "BAL": "calendar",
    "IBBA": "split",
    "FFBB": "split",
    "WBBL": "split",
    "SLB": "split",
    "FUBB": "split",
    "NBL": "split",
    "SPB": "calendar",
}

# ------------------------------------------------------------------------------ schemas
INT, FLT, STR = pl.Int64, pl.Float64, pl.String

#: (wire key, dtype) of the per-player box-score stats, ``tm.<tno>.pl.<pno>.<key>``.
_PLAYER_STATS: Tuple[Tuple[str, Any], ...] = (
    ("sFieldGoalsMade", INT),
    ("sFieldGoalsAttempted", INT),
    ("sFieldGoalsPercentage", FLT),
    ("sThreePointersMade", INT),
    ("sThreePointersAttempted", INT),
    ("sThreePointersPercentage", FLT),
    ("sTwoPointersMade", INT),
    ("sTwoPointersAttempted", INT),
    ("sTwoPointersPercentage", FLT),
    ("sFreeThrowsMade", INT),
    ("sFreeThrowsAttempted", INT),
    ("sFreeThrowsPercentage", FLT),
    ("sReboundsDefensive", INT),
    ("sReboundsOffensive", INT),
    ("sReboundsTotal", INT),
    ("sAssists", INT),
    ("sTurnovers", INT),
    ("sSteals", INT),
    ("sBlocks", INT),
    ("sBlocksReceived", INT),
    ("sFoulsPersonal", INT),
    ("sFoulsOn", INT),
    ("sPoints", INT),
    ("sPointsFromTurnovers", INT),
    ("sPointsSecondChance", INT),
    ("sPointsFastBreak", INT),
    ("sPointsInThePaint", INT),
    ("sPlusMinusPoints", INT),
    ("eff_1", INT),
    ("eff_2", INT),
    ("eff_3", FLT),
    ("eff_4", FLT),
    ("eff_5", INT),
    ("eff_6", INT),
    ("eff_7", INT),
)
#: Team totals ``tm.<tno>.tot_<key>``: the player stats the team also totals, plus team-only counters.
_TEAM_ONLY: Tuple[Tuple[str, Any], ...] = (
    ("sBenchPoints", INT),
    ("sBiggestLead", INT),
    ("sBiggestScoringRun", INT),
    ("sLeadChanges", INT),
    ("sTimesScoresLevel", INT),
    ("sTimeLeading", FLT),
    ("sFoulsTeam", INT),
    ("sFoulsTotal", INT),
    ("sReboundsTeam", INT),
    ("sReboundsTeamDefensive", INT),
    ("sReboundsTeamOffensive", INT),
    ("sTurnoversTeam", INT),
)
_TEAM_STATS = tuple((k, t) for k, t in _PLAYER_STATS if k != "sPlusMinusPoints") + _TEAM_ONLY


def _stat_col(key: str) -> str:
    """``sFieldGoalsMade`` -> ``field_goals_made``; ``eff_3`` stays."""
    return key if key.startswith("eff_") else _snake(key[1:])


def _snake(name: str) -> str:
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    s = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", s)
    return s.lower()


def _team_wire(col: str) -> str:
    """The ``data.json`` path (``tm.<tno>.<key>``) a team_box stat column reads."""
    for key, _ in _TEAM_STATS:
        if _stat_col(key) == col:
            return f"tm.<tno>.tot_{key}"
    raise KeyError(col)


def _player_wire(col: str) -> str:
    for key, _ in _PLAYER_STATS:
        if _stat_col(key) == col:
            return f"tm.<tno>.pl.<pno>.{key}"
    raise KeyError(col)


SCHEMAS: Dict[str, Dict[str, Any]] = {
    "game": {
        "match_id": INT,
        "period": INT,
        "period_type": STR,
        "in_ot": INT,
        "clock": STR,
        "periods_max": INT,
        "period_length_regular": INT,
        "period_length_overtime": INT,
        "attendance": INT,
        "disable_match": INT,
        "total_time_added": INT,
        "team1_name": STR,
        "team1_code": STR,
        "team1_score": INT,
        "team2_name": STR,
        "team2_code": STR,
        "team2_score": INT,
        "referee1": STR,
        "referee2": STR,
        "referee3": STR,
        "commissioner": STR,
    },
    "team_box": {
        "match_id": INT,
        "tno": INT,
        "name": STR,
        "name_international": STR,
        "short_name": STR,
        "code": STR,
        "code_international": STR,
        "coach": STR,
        "score": INT,
        "full_score": INT,
        "p1_score": INT,
        "p2_score": INT,
        "p3_score": INT,
        "p4_score": INT,
        "ot_score": INT,
        "fouls": INT,
        "timeouts": INT,
        "minutes": STR,
        "seconds_played": INT,
        **{_stat_col(k): t for k, t in _TEAM_STATS},
    },
    "player_box": {
        "match_id": INT,
        "tno": INT,
        "pno": INT,
        "name": STR,
        "first_name": STR,
        "family_name": STR,
        "international_first_name": STR,
        "international_family_name": STR,
        "scoreboard_name": STR,
        "shirt_number": STR,
        "playing_position": STR,
        "starter": INT,
        "captain": INT,
        "active": INT,
        "minutes": STR,
        "seconds_played": INT,
        **{_stat_col(k): t for k, t in _PLAYER_STATS},
    },
    "pbp": {
        "match_id": INT,
        "row_index": INT,
        "action_number": INT,
        "previous_action": INT,
        "period": INT,
        "period_type": STR,
        "gt": STR,
        "clock": STR,
        "tno": INT,
        "pno": INT,
        "player": STR,
        "shirt_number": STR,
        "first_name": STR,
        "family_name": STR,
        "international_first_name": STR,
        "international_family_name": STR,
        "scoreboard_name": STR,
        "action_type": STR,
        "sub_type": STR,
        "qualifier": STR,
        "success": INT,
        "scoring": INT,
        "score_1": INT,
        "score_2": INT,
        "lead": INT,
    },
    "shots": {
        "match_id": INT,
        "tno": INT,
        "pno": INT,
        "action_number": INT,
        "period": INT,
        "period_type": STR,
        "action_type": STR,
        "sub_type": STR,
        "result": INT,
        "x": FLT,
        "y": FLT,
        "player": STR,
        "shirt_number": STR,
    },
    "competition_games": {
        "league_id": INT,
        "competition_id": INT,
        "match_id": INT,
        "match_status": STR,
        "match_time": STR,
        "match_time_utc": STR,
        "live": INT,
        "period": INT,
        "period_type": STR,
        "clock": STR,
        "home_team_id": INT,
        "home_name": STR,
        "home_name_international": STR,
        "home_code": STR,
        "home_score": INT,
        "away_team_id": INT,
        "away_name": STR,
        "away_name_international": STR,
        "away_code": STR,
        "away_score": INT,
    },
    "match_competition": {
        "match_id": INT,
        "comp_id": INT,
        "competition_name": STR,
        "org_label": STR,
        "og_description": STR,
        "org": STR,
    },
    "competitions": {
        "org": STR,
        "comp_id": INT,
        "label": STR,
        "league_id": INT,
        "year": INT,
        "season": INT,
        "season_rule": STR,
        "selected": pl.Boolean,
    },
    "schedule": {
        "match_id": INT,
        "link_match_id": INT,
        "comp_id": INT,
        "org": STR,
        "status": STR,
        "date_time": STR,
        "venue_id": INT,
        "venue_name": STR,
        "home_team_id": INT,
        "home_team_name": STR,
        "home_team_code": STR,
        "home_score": INT,
        "away_team_id": INT,
        "away_team_name": STR,
        "away_team_code": STR,
        "away_score": INT,
    },
    "widget_games": {
        "match_id": INT,
        "webcast_org": STR,
        "competition_name": STR,
        "status": STR,
        "date": STR,
        "home_team_id": INT,
        "home_team_code": STR,
        "home_score": INT,
        "away_team_id": INT,
        "away_team_code": STR,
        "away_score": INT,
    },
}

# ------------------------------------------------------------------------------ scalars


def _int(v: Any) -> Optional[int]:
    """An integer from an int, an integral float or a digit string; ``None`` for anything else."""
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v == int(v) else None
    if isinstance(v, str) and re.fullmatch(r"\s*-?\d+\s*", v):
        return int(v)
    return None


def _num(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v.strip())
        except ValueError:
            return None
    return None


def _str(v: Any) -> Optional[str]:
    if v is None or isinstance(v, (dict, list)):
        return None
    return str(v)


def _seconds(v: Any) -> Optional[int]:
    """``'35:08'`` -> 2108; ``'199:60'`` -> 12000 (as printed); an integer or other text -> ``None``."""
    m = re.fullmatch(r"\s*(\d+):(\d{1,2})\s*", v) if isinstance(v, str) else None
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _cast(value: Any, dtype: Any) -> Any:
    if dtype == INT:
        return _int(value)
    if dtype == FLT:
        return _num(value)
    if dtype == pl.Boolean:
        return value if isinstance(value, bool) else None
    return _str(value)


def _frame(rows: List[Dict[str, Any]], name: str, return_as_pandas: bool) -> Frame:
    """``rows`` as the pinned frame ``name`` (missing keys are nulls, extra keys are dropped)."""
    schema = SCHEMAS[name]
    if rows:
        df = pl.DataFrame({c: [_cast(r.get(c), t) for r in rows] for c, t in schema.items()}, schema=schema)
    else:
        df = pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _req(raw: Any) -> Dict[str, Any]:
    r = raw.get(_REQUEST) if isinstance(raw, dict) else None
    return r if isinstance(r, dict) else {}


def _game_ok(raw: Any) -> bool:
    tm = raw.get("tm") if isinstance(raw, dict) else None
    return isinstance(tm, dict) and isinstance(raw.get("pbp"), list)


# ------------------------------------------------------------------------------ data.json frames


def parse_genius_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One row for a ``data.json`` game: clock, period state, the two teams' scores and the officials.

    Args:
        raw: the ``data.json`` body (with the runtime's ``_request`` stamp for ``match_id``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        A one-row frame (zero rows for an empty or malformed body). ``team1`` is the team listed first.
        ``referee1``..``commissioner`` read ``officials.<role>.name`` (current feeds) or the flat
        ``officials_refereeN`` strings (older feeds); null when absent.

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Parse a fetched game::

            from sportsdataverse.basketball.genius import genius_game
            genius_game(match_id=2902690)
    """
    if not _game_ok(raw):
        return _frame([], "game", return_as_pandas)
    tm = raw["tm"]
    off = raw.get("officials") if isinstance(raw.get("officials"), dict) else {}

    def person(role: str, flat: str) -> Optional[str]:
        node = off.get(role)
        if isinstance(node, dict):
            return _str(
                node.get("internationalFirstName")
                and f"{node.get('internationalFirstName')} {node.get('internationalFamilyName')}"
            ) or _str(node.get("name"))
        return _str(raw.get(flat))

    row: Dict[str, Any] = {
        "match_id": _req(raw).get("match_id"),
        "period": raw.get("period"),
        "period_type": raw.get("periodType"),
        "in_ot": raw.get("inOT"),
        "clock": raw.get("clock"),
        "periods_max": raw.get("periodsMax"),
        "period_length_regular": raw.get("periodLengthREGULAR"),
        "period_length_overtime": raw.get("periodLengthOVERTIME"),
        "attendance": raw.get("attendance"),
        "disable_match": raw.get("disableMatch"),
        "total_time_added": raw.get("totalTimeAdded"),
        "referee1": person("referee1", "officials_referee1"),
        "referee2": person("referee2", "officials_referee2"),
        "referee3": person("referee3", "officials_referee3"),
        "commissioner": person("commissioner", "officials_commissioner"),
    }
    for tno in ("1", "2"):
        t = tm.get(tno) if isinstance(tm.get(tno), dict) else {}
        row[f"team{tno}_name"] = t.get("name")
        row[f"team{tno}_code"] = t.get("code")
        row[f"team{tno}_score"] = t.get("score")
    return _frame([row], "game", return_as_pandas)


def parse_genius_team_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team box score of a ``data.json`` game: one row per team (``tno`` 1 and 2).

    Args:
        raw: the ``data.json`` body (with the runtime's ``_request`` stamp for ``match_id``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        Team names and codes, score by period (``p1_score``..``p4_score``, ``ot_score``), fouls, timeouts and
        the ``tot_s*`` totals (``field_goals_made`` ... ``points``) plus ``eff_1``..``eff_7``. Percentages are
        ``Float64`` 0-100. ``ot_score`` is the points in overtime (null without overtime; whether it sums several
        overtimes is unverified). The feed has no team id: ``tno`` is in-game only.

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Both teams of a game::

            from sportsdataverse.basketball.genius import genius_team_box
            genius_team_box(match_id=2902690)
    """
    if not _game_ok(raw):
        return _frame([], "team_box", return_as_pandas)
    rows = []
    for tno in sorted(raw["tm"], key=lambda k: (not str(k).isdigit(), str(k))):
        t = raw["tm"][tno]
        if not isinstance(t, dict) or _int(tno) is None:
            continue
        row = {
            "match_id": _req(raw).get("match_id"),
            "tno": _int(tno),
            "name": t.get("name"),
            "name_international": t.get("nameInternational"),
            "short_name": t.get("shortName"),
            "code": t.get("code"),
            "code_international": t.get("codeInternational"),
            "coach": t.get("coach"),
            "score": t.get("score"),
            "full_score": t.get("full_score"),
            "p1_score": t.get("p1_score"),
            "p2_score": t.get("p2_score"),
            "p3_score": t.get("p3_score"),
            "p4_score": t.get("p4_score"),
            "ot_score": t.get("ot_score"),
            "fouls": t.get("fouls"),
            "timeouts": t.get("timeouts"),
            "minutes": t.get("tot_sMinutes"),
            "seconds_played": _seconds(t.get("tot_sMinutes")),
        }
        for key, _ in _TEAM_STATS:
            row[_stat_col(key)] = t.get(f"tot_{key}")
        rows.append(row)
    return _frame(rows, "team_box", return_as_pandas)


def parse_genius_player_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Player box score of a ``data.json`` game: one row per player of both teams.

    Args:
        raw: the ``data.json`` body (with the runtime's ``_request`` stamp for ``match_id``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        Rows keyed by (``tno``, ``pno``), both in-game numbers; ``pno`` is not the shirt number and the feed
        has no global player id (join across games on name, team code and ``shirt_number``). ``name`` is the
        display name in the competition's script; ``international_*`` are the Latin forms. Stat columns drop the
        wire ``s`` prefix (``field_goals_made``, ``points``, ``plus_minus_points``); percentages are 0-100
        ``Float64``. ``minutes`` is the printed ``mm:ss`` and ``seconds_played`` its seconds.

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Every player line of a game::

            from sportsdataverse.basketball.genius import genius_player_box
            genius_player_box(match_id=2902690)
    """
    if not _game_ok(raw):
        return _frame([], "player_box", return_as_pandas)
    rows = []
    for tno, t in raw["tm"].items():
        pls = t.get("pl") if isinstance(t, dict) else None
        if not isinstance(pls, dict) or _int(tno) is None:
            continue
        for pno, p in pls.items():
            if not isinstance(p, dict) or _int(pno) is None:
                continue
            row = {
                "match_id": _req(raw).get("match_id"),
                "tno": _int(tno),
                "pno": _int(pno),
                "name": p.get("name"),
                "first_name": p.get("firstName"),
                "family_name": p.get("familyName"),
                "international_first_name": p.get("internationalFirstName"),
                "international_family_name": p.get("internationalFamilyName"),
                "scoreboard_name": p.get("scoreboardName"),
                "shirt_number": p.get("shirtNumber"),
                "playing_position": p.get("playingPosition"),
                "starter": p.get("starter"),
                "captain": p.get("captain"),
                "active": p.get("active"),
                "minutes": p.get("sMinutes"),
                "seconds_played": _seconds(p.get("sMinutes")),
            }
            for key, _ in _PLAYER_STATS:
                row[_stat_col(key)] = p.get(key)
            rows.append(row)
    return _frame(rows, "player_box", return_as_pandas)


def parse_genius_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of a ``data.json`` game, one row per action, newest first as the feed sends it.

    Args:
        raw: the ``data.json`` body (with the runtime's ``_request`` stamp for ``match_id``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        Rows in the feed's order: newest first by game time, so ``row_index`` 0 is the last action, normally
        the ``game`` ``end`` row. The order is NOT strictly descending ``action_number`` (60 of the 663 rows of
        match 2902690 sit after a lower number, substitutions entered out of order): sort on ``action_number``
        for entry order. ``score_1``/``score_2`` are the scores after the action, always ``Int64``
        (the feed sends strings in most games); ``lead`` is team 1 minus team 2. ``tno`` and ``pno`` are
        in-game numbers (0 = none). ``qualifier`` joins the qualifier codes with commas (null when none).
        ``clock`` (``mm:ss:cc``) and ``action_number`` / ``previous_action`` are absent in the 2014-15 feed, so
        they are null there. Overtime restarts ``period`` at 1: read it with ``period_type``.

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Fetch and sort oldest first::

            from sportsdataverse.basketball.genius import genius_pbp
            genius_pbp(match_id=2902690).sort("row_index", descending=True)
    """
    if not _game_ok(raw):
        return _frame([], "pbp", return_as_pandas)
    rows = []
    for i, r in enumerate(raw["pbp"]):
        if not isinstance(r, dict):
            continue
        q = r.get("qualifier")
        qual = ",".join(str(x) for x in q) if isinstance(q, list) else (q or None)
        rows.append(
            {
                "match_id": _req(raw).get("match_id"),
                "row_index": i,
                "action_number": r.get("actionNumber"),
                "previous_action": r.get("previousAction"),
                "period": r.get("period"),
                "period_type": r.get("periodType"),
                "gt": r.get("gt"),
                "clock": r.get("clock"),
                "tno": r.get("tno"),
                "pno": r.get("pno"),
                "player": r.get("player"),
                "shirt_number": r.get("shirtNumber"),
                "first_name": r.get("firstName"),
                "family_name": r.get("familyName"),
                "international_first_name": r.get("internationalFirstName"),
                "international_family_name": r.get("internationalFamilyName"),
                "scoreboard_name": r.get("scoreboardName"),
                "action_type": r.get("actionType"),
                "sub_type": r.get("subType"),
                "qualifier": qual,
                "success": r.get("success"),
                "scoring": r.get("scoring"),
                "score_1": r.get("s1"),
                "score_2": r.get("s2"),
                "lead": r.get("lead"),
            }
        )
    return _frame(rows, "pbp", return_as_pandas)


def parse_genius_shots(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Shot chart of a ``data.json`` game: one row per field-goal attempt of both teams.

    Args:
        raw: the ``data.json`` body (with the runtime's ``_request`` stamp for ``match_id``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        ``x`` and ``y`` are percentages (0-100) of the full court length and width, NOT normalised to one
        basket: a team attacks one end in periods 1-2 and the other in 3-4, so flip x per period before
        plotting one basket. The origin corner is unverified. ``result`` is 1 for a made shot and 0 for a miss.
        ``action_number`` joins the play-by-play row (null in the 2014-15 feed).

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Made shots only::

            from sportsdataverse.basketball.genius import genius_shots
            shots = genius_shots(match_id=2902690)
            shots.filter(shots["result"] == 1)
    """
    if not _game_ok(raw):
        return _frame([], "shots", return_as_pandas)
    rows = []
    for tno, t in raw["tm"].items():
        for s in (t.get("shot") if isinstance(t, dict) else None) or []:
            if not isinstance(s, dict):
                continue
            rows.append(
                {
                    "match_id": _req(raw).get("match_id"),
                    "tno": s.get("tno", _int(tno)),
                    "pno": s.get("pno"),
                    "action_number": s.get("actionNumber"),
                    "period": s.get("per"),
                    "period_type": s.get("perType"),
                    "action_type": s.get("actionType"),
                    "sub_type": s.get("subType"),
                    "result": s.get("r"),
                    "x": s.get("x"),
                    "y": s.get("y"),
                    "player": s.get("player"),
                    "shirt_number": s.get("shirtNumber"),
                }
            )
    return _frame(rows, "shots", return_as_pandas)


# ------------------------------------------------------------------------------ other JSON route


_COMPETITION_MAP = {
    "league_id": "leagueId",
    "competition_id": "competitionId",
    "match_id": "matchId",
    "match_status": "matchStatus",
    "match_time": "matchTime",
    "match_time_utc": "matchTimeUTC",
    "live": "live",
    "period": "period",
    "period_type": "periodType",
    "clock": "clock",
    "home_team_id": "hometeamId",
    "home_name": "homename",
    "home_name_international": "homenameInternational",
    "home_code": "homecode",
    "home_score": "homescore",
    "away_team_id": "awayteamId",
    "away_name": "awayname",
    "away_name_international": "awaynameInternational",
    "away_code": "awaycode",
    "away_score": "awayscore",
}


@_data_only
def parse_genius_competition_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The recent-games window of a competition (``data/competition/{compId}.json``).

    Args:
        raw: the JSON list the route returns.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        One row per game of the window (a few recent or upcoming games, not the season). ``home_team_id`` /
        ``away_team_id`` are the global Genius team ids (the key space of the hosted schedule and the widget;
        ``data.json`` has none). Scores are strings on most rows and are cast to ``Int64`` (null before
        tip-off). Times are the feed's text: ``match_time`` is local (zone not stated), ``match_time_utc`` UTC.

    Raises:
        None: a malformed body gives a zero-row frame.

    Example:
        Games of the Greek league window::

            from sportsdataverse.basketball.genius import genius_competition_games
            genius_competition_games(comp_id=49915)
    """
    rows = [
        {col: r.get(wire) for col, wire in _COMPETITION_MAP.items()}
        for r in (raw if isinstance(raw, list) else [])
        if isinstance(r, dict)
    ]
    return _frame(rows, "competition_games", return_as_pandas)


# ------------------------------------------------------------------------------ HTML records


class _NotParsed(ValueError):
    """A page that is not the page asked for; ``empty`` marks a legitimately empty answer."""

    def __init__(self, msg: str, *, empty: bool = False) -> None:
        super().__init__(msg)
        self.empty = empty


def _text(fragment: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))).strip()


def _parse_u(page: str) -> Dict[str, Any]:
    mid = re.search(r'id\s*=\s*"matchId"\s+value\s*=\s*"(\d+)"', page)
    cid = re.search(r'id\s*=\s*"compId"\s+value\s*=\s*"(\d+)"', page)
    name = re.search(r'id="competitionName"[^>]*>([^<]*)<', page)
    desc = re.search(r'og:description"\s+content\s*=\s*"([^"]*)"', page)
    parts = [p.strip() for p in htmllib.unescape(desc.group(1)).split(",")] if desc else []
    return {
        "match_id": int(mid.group(1)) if mid else None,
        "comp_id": int(cid.group(1)) if cid else None,
        "competition_name": _text(name.group(1)) if name else None,
        "org_label": parts[0] if parts else None,
        "og_description": htmllib.unescape(desc.group(1)).strip() if desc else None,
    }


_COMPDATA = re.compile(
    r"compdata\.push\(\['(https://[^']*/competition/(\d+)/schedule)','((?:[^'\\]|\\.)*)',\s*(\d+),\s*'(\d*)',\s*(\d+)\]\)"
)


def _parse_chooser(page: str) -> Dict[str, Any]:
    rows, seen = [], set()
    for url, cid, label, league, year, cid2 in _COMPDATA.findall(page):
        if cid != cid2 or cid in seen:
            continue
        seen.add(cid)
        path = urlsplit(url).path.split("/")
        rows.append(
            {
                "comp_id": int(cid),
                "label": htmllib.unescape(label.replace("\\'", "'")).strip(),
                "league_id": int(league),
                "year": int(year) if year else None,
                "org": path[1] if len(path) > 1 else None,
            }
        )
    sel = re.search(r'/competition/(\d+)/schedule"\s+SELECTED', page)
    return {"selected_comp_id": int(sel.group(1)) if sel else None, "competitions": rows}


def _team(block: str, side: str) -> Dict[str, Any]:
    part = re.search(
        rf'<div class="{side}-team">(.*?)<div class="team-score {side}score">\s*<div class="fake-cell">([^<]*)</div>',
        block,
        re.S,
    )
    if not part:
        return {"team_id": None, "team_name": None, "team_code": None, "score": None}
    body, score = part.groups()
    tid = re.search(r"/team/(\d+)", body)
    full = re.search(r'team-name-full">([^<]*)<', body)
    code = re.search(r'team-name-code">([^<]*)<', body)
    return {
        "team_id": int(tid.group(1)) if tid else None,
        "team_name": _text(full.group(1)) if full else None,
        "team_code": _text(code.group(1)) if code else None,
        "score": _int(htmllib.unescape(score).replace("\xa0", " ")),
    }


def _parse_schedule(page: str) -> Dict[str, Any]:
    matches = []
    for b in re.split(r'(?=<div class="match-wrap )', page)[1:]:
        head = re.match(r'<div class="match-wrap STATUS_(\w+)"\s+id\s*=\s*"extfix_(\d+)"', b)
        if not head:
            continue
        link = re.search(r"/([A-Za-z0-9]+)/en/competition/(\d+)/match/(\d+)/", b)
        ctx = link or re.search(r"/([A-Za-z0-9]+)/en/competition/(\d+)/team/\d+", b)
        when = re.search(r'class="match-time">.*?<span>([^<]*)</span>', b, re.S)
        venue = re.search(r'(?:/venue/(\d+)\?"\s+class="venuename"|class\s*=\s*"venuename")>([^<]*)<', b)
        home, away = _team(b, "home"), _team(b, "away")
        matches.append(
            {
                "match_id": int(head.group(2)),
                "link_match_id": int(link.group(3)) if link else None,
                "comp_id": int(ctx.group(2)) if ctx else None,
                "org": ctx.group(1) if ctx else None,
                "status": head.group(1),
                "date_time": _text(when.group(1)) if when else None,
                "venue_id": int(venue.group(1)) if venue and venue.group(1) else None,
                "venue_name": _text(venue.group(2)) if venue else None,
                **{f"home_{k}": v for k, v in home.items()},
                **{f"away_{k}": v for k, v in away.items()},
            }
        )
    return {"matches": matches}


def _parse_embednf(text: str) -> Dict[str, Any]:
    try:
        env = json.loads(text)
    except ValueError:
        raise _NotParsed("the embed body is not JSON") from None
    if not isinstance(env, dict) or not isinstance(env.get("html"), str):
        raise _NotParsed("the embed body is not a {css, js, html} envelope")
    return _parse_schedule(env["html"])


def _parse_widget(js: str) -> Dict[str, Any]:
    m = re.search(r'var html = ("(?:[^"\\]|\\.)*")\s*;', js, re.S)
    if not m:
        raise _NotParsed("the widget script has no 'var html' payload")
    page = json.loads(m.group(1))
    rows = []
    for li in re.findall(r'<li class="spls_lsmatch[^"]*">(.*?)</li>', page, re.S):
        link = re.search(r"/webcast/(\w+)/(\d+)/", li)
        teams = re.findall(
            r'<span class="spteam tid(\d+)">.*?<span class="teamname">([^<]*)</span><span class="score[^"]*">([^<]*)</span>',
            li,
            re.S,
        )
        comp = re.search(r'spls_matchcomp">([^<]*)<', li)
        status = re.search(r'spls_matchstatus">([^<]*)<', li)
        date = re.search(r'spls_datefield">([^<]*)<', li)
        (h_id, h_code, h_sc), (a_id, a_code, a_sc) = (teams + [(None, None, None)] * 2)[:2]
        rows.append(
            {
                "match_id": int(link.group(2)) if link else None,
                "webcast_org": link.group(1) if link else None,
                "competition_name": _text(comp.group(1)) if comp else None,
                "status": _text(status.group(1)) if status else None,
                "date": _text(date.group(1)) if date else None,
                "home_team_id": _int(h_id),
                "home_team_code": _text(h_code) if h_code else None,
                "home_score": _int(h_sc),
                "away_team_id": _int(a_id),
                "away_team_code": _text(a_code) if a_code else None,
                "away_score": _int(a_sc),
            }
        )
    return {"matches": rows}


def _page_record(kind: str, text: str) -> Dict[str, Any]:
    """Reduce a fetched page to its record (the sdv-internal-refs capture shape).

    Args:
        kind: ``u``, ``chooser``, ``schedule``, ``embednf`` or ``widget``.
        text: the response text.

    Returns:
        The record ``dict``.

    Raises:
        _NotParsed: the page is empty, truncated or not the expected kind. ``empty`` is true when the answer
            is a legitimate "nothing here" (an org without hosted pages answers an empty 200).
    """
    if not (text or "").strip():
        raise _NotParsed("empty body (an org without hosted pages answers an empty 200)", empty=True)
    if kind in ("u", "chooser", "schedule") and not text.rstrip().endswith("</html>"):
        raise _NotParsed("the page is cut off (no closing </html>)")
    if kind == "u":
        rec = _parse_u(text)
        if rec["match_id"] is None or rec["comp_id"] is None:
            raise _NotParsed("the page has no matchId / compId inputs")
        return rec
    if kind == "chooser":
        rec = _parse_chooser(text)
        if not rec["competitions"]:
            raise _NotParsed("the page lists no competitions", empty="competitionChooser" not in text)
        return rec
    if kind in ("schedule", "embednf"):
        rec = _parse_schedule(text) if kind == "schedule" else _parse_embednf(text)
        if not rec["matches"]:
            raise _NotParsed("the page lists no matches", empty=True)
        return rec
    if kind == "widget":
        rec = _parse_widget(text)
        if not rec["matches"]:
            raise _NotParsed("the widget lists no matches", empty=True)
        return rec
    raise ValueError(f"unknown page kind {kind!r}")


# ------------------------------------------------------------------------------ HTML-record frames


def parse_genius_match_competition(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The competition of a game, from its ``/u/{org}/{matchId}/`` LiveStats shell.

    Args:
        raw: the page record (``match_id``, ``comp_id``, ``competition_name``, ``org_label``,
            ``og_description``), as the runtime builds it from the HTML.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        A one-row frame: the only way to map a ``data.json`` game to its competition (``comp_id``) and, through
        ``genius_competitions``, to a season. ``org`` is the ORG the request used.

    Raises:
        None: a malformed record gives a zero-row frame.

    Example:
        Which competition is a game in::

            from sportsdataverse.basketball.genius import genius_match_competition
            genius_match_competition(org="ESAKE", match_id=2902690)
    """
    if not isinstance(raw, dict) or raw.get("match_id") is None:
        return _frame([], "match_competition", return_as_pandas)
    return _frame([{**raw, "org": _req(raw).get("org")}], "match_competition", return_as_pandas)


def season_end(label: Optional[str], year: Optional[int], span: Optional[str]) -> Tuple[Optional[int], str]:
    """The sdv END-year season of a competition, and the rule that produced it.

    1. A two-year span in the label (``"2019 - 20"``, ``"2023-2024"``, ``"26-27"``) gives its second year.
    2. Else the Genius year: START year plus one for a ``split`` org, the year itself for a ``calendar`` org.
    3. Else a lone four-digit year in the label of a ``calendar`` org.

    Args:
        label: the competition label as the org typed it.
        year: the chooser's Genius year (or ``None``).
        span: ``"split"``, ``"calendar"`` or ``None`` for an unknown org.

    Returns:
        ``(season, rule)``; ``(None, "unknown")`` when none of the rules applies.

    Raises:
        None: does not raise; an unreadable label gives ``(None, "unknown")``.

    Example:
        Season of a Greek competition::

            from sportsdataverse.basketball.genius.genius_parsers import season_end
            season_end("BASKET LEAGUE 2019 - 20", 2019, "split")   # (2020, "label span")
    """
    m = re.search(r"(?<!\d)((?:19|20)\d{2}|\d{2})\s*[-/–]\s*((?:19|20)\d{2}|\d{2})(?!\d)", label or "")
    if m:
        a, b = m.groups()
        start = int(a) if len(a) == 4 else 2000 + int(a)
        end = int(b) if len(b) == 4 else (start // 100) * 100 + int(b)
        if end < start:
            end += 100
        if end == start + 1:
            return end, "label span"
    if year and span:
        return (year + 1, "genius year + 1 (split)") if span == "split" else (year, "genius year (calendar)")
    m = re.search(r"(?<!\d)((?:19|20)\d{2})(?!\d)", label or "")
    if m and span == "calendar":
        return int(m.group(1)), "label year (calendar)"
    return None, "unknown"


def parse_genius_competitions(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every competition of an org (the hosted WebStats competition chooser), with its END-year season.

    Args:
        raw: the page record (``selected_comp_id``, ``competitions``) with the runtime's ``_request`` ``org``.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        One row per competition-season. ``comp_id`` is the key of the other hosted routes. ``year`` is the
        Genius year (START year for a two-year org, the year itself for a calendar org) and ``season`` the sdv
        END year (see :func:`season_end`; ``season_rule`` says which rule produced it, null season when none).
        ``selected`` marks the competition the chooser pre-selects (the org default, not always the newest).
        The registry the refs keep also reads game dates to fix a season the label leaves ambiguous; this
        frame uses the label and the year only.

    Raises:
        None: a malformed record gives a zero-row frame.

    Example:
        The seasons of the Greek league::

            from sportsdataverse.basketball.genius import genius_competitions
            genius_competitions(org="ESAKE")
    """
    comps = raw.get("competitions") if isinstance(raw, dict) else None
    org = _req(raw).get("org") if isinstance(raw, dict) else None
    rows = []
    for c in comps or []:
        if not isinstance(c, dict):
            continue
        span = ORG_SPAN.get(str(c.get("org") or org or "").upper())
        season, rule = season_end(c.get("label"), c.get("year"), span)
        rows.append(
            {
                "org": c.get("org") or org,
                "comp_id": c.get("comp_id"),
                "label": c.get("label"),
                "league_id": c.get("league_id"),
                "year": c.get("year"),
                "season": season,
                "season_rule": rule,
                "selected": c.get("comp_id") == raw.get("selected_comp_id"),
            }
        )
    return _frame(rows, "competitions", return_as_pandas)


def parse_genius_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every match of a hosted competition schedule (plain or ``embednf`` variant).

    Args:
        raw: the page record (``matches``), as the runtime builds it from the HTML.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        One row per match in page order. ``match_id`` is the ``data.json`` match id; ``home_team_id`` /
        ``away_team_id`` are the global Genius team ids. Scores are null before tip-off and for some completed games whose score the page does not print. ``date_time`` is the
        page's text (``'Oct 10, 2026, 6:00 PM'``, local time, zone not stated). ``link_match_id`` is null for
        orgs that hide match pages (FFBB); ``venue_id`` is null when the venue is plain text.

    Raises:
        None: a malformed record gives a zero-row frame.

    Example:
        All games of a competition::

            from sportsdataverse.basketball.genius import genius_schedule
            genius_schedule(org="CEBL", comp_id=47852)
    """
    rows = [m for m in (raw.get("matches") if isinstance(raw, dict) else None) or [] if isinstance(m, dict)]
    return _frame(rows, "schedule", return_as_pandas)


def parse_genius_widget_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The latest games a league site's results widget shows (JavaScript widget, no proxy needed).

    Args:
        raw: the page record (``matches``), as the runtime builds it from the script.
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: accepted from the generated wrapper and ignored; the schema is pinned here.

    Returns:
        One row per game (about 5 to 50, newest competition first). ``match_id`` is the ``data.json`` id;
        ``webcast_org`` is the LiveStats ORG of the webcast link (``NZN`` for the hosted ``NZNBL``);
        ``home_team_id`` / ``away_team_id`` are the global Genius team ids. ``date`` is the widget's text.

    Raises:
        None: a malformed record gives a zero-row frame.

    Example:
        The Greek league widget::

            from sportsdataverse.basketball.genius import genius_widget_games
            genius_widget_games(widget_key="RFJIN037I07QV520B7WEXKV1GYNA31")
    """
    rows = [m for m in (raw.get("matches") if isinstance(raw, dict) else None) or [] if isinstance(m, dict)]
    return _frame(rows, "widget_games", return_as_pandas)
