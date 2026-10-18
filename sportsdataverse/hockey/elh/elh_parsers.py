"""Parsers for the Czech Extraliga (``elh``) flat-API family: hokej.cz's S3 bucket + the scoreboard.

Every parser follows the package contract: polars by default (pandas with
``return_as_pandas=True``), snake_case columns, and a zero-row frame with the documented
schema for an empty or malformed payload -- never an exception. Each frame's schema is
fixed (``ELH_SCHEMAS``), so an id column is the same dtype whatever the body sent.

**Two id spaces that never join directly.** The play-by-play (onlajny, the commentary
system) and the shots / scoreboard (hokej.cz) number games, teams and players separately:

=============================  ==========================================================
Column                         Key space
=============================  ==========================================================
``hokejcz_game_id``            hokej.cz game id: the S3 file name, shots ``match.id``,
                               scoreboard ``hokejcz_id``
``onlajny_game_id``            onlajny game id (scoreboard only)
``hokejcz_*team_id``           hokej.cz team id (Sparta = 8): shots, scoreboard
``onlajny_*team_id``           onlajny team id (Sparta = 156): play-by-play, scoreboard
``hokejcz_player_id``          hokej.cz player id (shots; Hradec = 10093378)
``onlajny_player*_id``         onlajny player id (play-by-play; Hradec = 48287)
``onlajny_player*_global_id``  onlajny GLOBAL player id (Hradec = 76559); can differ from
                               ``onlajny_player*_id`` for the same player
``comment_id`` / ``event_id``  onlajny commentary entry / event ids
=============================  ==========================================================

Every id is ``Int64``, cast from the body's string or integer at this boundary. The
scoreboard maps games and teams between the two spaces (:func:`parse_elh_scoreboard`),
but only for days still in its rolling window; no player crosswalk is published (it has
to be built per game from jersey + name, a follow-up).

**XML converted to JSON.** The full / short play-by-play is an XML feed run through a
converter: every attribute is a string (numbers too), an empty element is ``[]``, and a
lone child element is an object while repeated ones become an array. So ``time`` is a
``"mm:ss"`` string on a clocked entry and an object naming only the phase on a clockless
one, and ``details`` is ``[]`` except on goals and penalties. These parsers cast numbers
and normalise both shapes here.

**Inferred codes.** The clockless phases ``0`` (pre-game), ``1INT`` / ``2INT``
(intermissions), ``3PER`` (third period) and ``ERT`` (after the game) and the strength
``EQ`` (even strength) are read from the entries' content; the provider documents
neither, and no other strength value has been observed. Shot result code ``4`` is a goal
(verified against the play-by-play); ``1`` / ``2`` / ``3`` / ``5`` are unknown.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import polars as pl

__all__ = [
    "ELH_LEAGUE_ID",
    "ELH_SCHEMAS",
    "parse_elh_events",
    "parse_elh_game_ids",
    "parse_elh_pbp",
    "parse_elh_pbp_goals",
    "parse_elh_pbp_mobile",
    "parse_elh_scoreboard",
    "parse_elh_shots",
]

Frame = Union[pl.DataFrame, pd.DataFrame]

#: Scoreboard league key of the Tipsport extraliga.
ELH_LEAGUE_ID = 101
#: ``match_shot_resutl_id`` of a goal (the only verified code).
GOAL_RESULT_ID = 4
#: Seconds in a regulation period.
PERIOD_SECONDS = 1200

_S, _I, _F, _B = pl.String, pl.Int64, pl.Float64, pl.Boolean


def _players_schema(n: int) -> Dict[str, Any]:
    return {
        f"onlajny_player{n}_id": _I,
        f"onlajny_player{n}_global_id": _I,
        f"player{n}_name": _S,
        f"player{n}_first_name": _S,
        f"player{n}_last_name": _S,
    }


_PBP: Dict[str, Any] = {
    "comment_id": _I,
    "sort_order": _I,
    "event_type": _S,
    "label2": _S,
    "time_type": _S,
    "phase": _S,
    "clock": _S,
    "game_seconds": _I,
    "period": _I,
    "home_score": _I,
    "away_score": _I,
    "home_shootout_score": _I,
    "away_shootout_score": _I,
    "event_id": _I,
    "onlajny_team_id": _I,
    "onlajny_team_global_id": _I,
    "team_code": _S,
    **_players_schema(1),
    **_players_schema(2),
    **_players_schema(3),
    "no_assist": _I,
    "penalty_minutes": _I,
    "penalty_offense": _S,
    "strength": _S,
    "message": _S,
    "written": _S,
    "last_change": _S,
}
_PBP_GOALS: Dict[str, Any] = {
    "comment_id": _I,
    "event_id": _I,
    "clock": _S,
    "game_seconds": _I,
    "period": _I,
    "strength": _S,
    "home_score": _I,
    "away_score": _I,
    "scoring_onlajny_team_id": _I,
    "scoring_team_code": _S,
    "on_ice_onlajny_team_id": _I,
    "on_ice_team_code": _S,
    "is_scoring_team": _B,
    "slot": _I,
    "onlajny_player_id": _I,
    "onlajny_player_global_id": _I,
    "player_name": _S,
}
_PBP_MOBILE: Dict[str, Any] = {"comment_id": _I, "event_type": _S, "game_seconds": _I, "content": _S}
_SHOTS: Dict[str, Any] = {
    "hokejcz_game_id": _I,
    "game_date": _S,
    "hokejcz_home_team_id": _I,
    "home_team_name": _S,
    "home_team_code": _S,
    "hokejcz_away_team_id": _I,
    "away_team_name": _S,
    "away_team_code": _S,
    "shot_id": _I,
    "hokejcz_player_id": _I,
    "jersey": _I,
    "first_name": _S,
    "last_name": _S,
    "hokejcz_team_id": _I,
    "team_side": _S,
    "match_shot_resutl_id": _I,
    "shot_result_id": _I,
    "is_goal": _B,
    "time": _I,
    "period": _I,
    "coordinate_x": _F,
    "coordinate_y": _F,
    "real_date": _S,
}
_EVENTS: Dict[str, Any] = {
    "start_date": _S,
    "team_side": _S,
    "team_name": _S,
    "team_score": _I,
    "minute": _I,
    "shots": _I,
    "shots_on_goal": _I,
}
_SCOREBOARD: Dict[str, Any] = {
    "league_id": _I,
    "league_name": _S,
    "hokejcz_game_id": _I,
    "onlajny_game_id": _I,
    "game_date": pl.Date,
    "start_time": _S,
    "status_code": _S,
    "status_name": _S,
    "match_status": _S,
    "home_score": _I,
    "away_score": _I,
    "score_periods": _S,
    "hokejcz_home_team_id": _I,
    "onlajny_home_team_id": _I,
    "home_team_name": _S,
    "home_team_short_name": _S,
    "home_team_code": _S,
    "hokejcz_away_team_id": _I,
    "onlajny_away_team_id": _I,
    "away_team_name": _S,
    "away_team_short_name": _S,
    "away_team_code": _S,
    "stream_url": _S,
}
_GAME_IDS: Dict[str, Any] = {"hokejcz_game_id": _I, "key": _S, "last_modified": _S, "size": _I}

#: Every shaper's documented schema, keyed by its parser's name.
ELH_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "parse_elh_pbp": _PBP,
    "parse_elh_pbp_goals": _PBP_GOALS,
    "parse_elh_pbp_mobile": _PBP_MOBILE,
    "parse_elh_shots": _SHOTS,
    "parse_elh_events": _EVENTS,
    "parse_elh_scoreboard": _SCOREBOARD,
    "parse_elh_game_ids": _GAME_IDS,
}

_INT = re.compile(r"-?\d+")
_CLOCK = re.compile(r"(\d+):(\d{2})")
_DMY = re.compile(r"(\d{2})-(\d{2})-(\d{4})")
_GAME_KEY = re.compile(r"^match/\d+/full/(\d+)\.json$")
_SIDES = (("home", "home"), ("visitor", "away"))


# ---------------------------------------------------------------- cell helpers


def _int(value: Any) -> Optional[int]:
    """An integer from an int, an integral float or a numeric string; else None (never raises)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str) and _INT.fullmatch(value.strip()):
        return int(value.strip())
    return None


def _id(value: Any) -> Optional[int]:
    """An id; the feed's ``0`` placeholder (an unused player slot) is null."""
    out = _int(value)
    return out or None


def _float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _str(value: Any) -> Optional[str]:
    """A non-empty string, else None (the XML converter writes an absent value as ``""`` or ``[]``)."""
    return value if isinstance(value, str) and value != "" else None


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _attrs(node: Any) -> Dict[str, Any]:
    return _dict(_dict(node).get("@attributes"))


def _many(value: Any) -> List[Any]:
    """XML-to-JSON children: a lone element is an object, repeated ones an array."""
    if isinstance(value, dict):
        return [value] if value else []
    return value if isinstance(value, list) else []


def _clock_seconds(clock: Optional[str]) -> Optional[int]:
    match = _CLOCK.fullmatch(clock or "")
    return int(match.group(1)) * 60 + int(match.group(2)) if match else None


def _period(seconds: Optional[int]) -> Optional[int]:
    """Period of an elapsed-clock second (20-minute periods; ``20:00`` closes the first)."""
    return None if seconds is None else max(seconds - 1, 0) // PERIOD_SECONDS + 1


def _frame(rows: List[Dict[str, Any]], schema: Dict[str, Any], return_as_pandas: bool) -> Frame:
    df = pl.DataFrame(rows, schema=schema, orient="row") if rows else pl.DataFrame(schema=schema)
    return df.to_pandas() if return_as_pandas else df


def _numbered(node: Dict[str, Any], stem: str) -> List[tuple]:
    """``[(n, child)]`` for the numbered children ``<stem>1``, ``<stem>2``, ... in number order."""
    out = []
    for key, child in node.items():
        if key.startswith(stem) and key[len(stem) :].isdigit() and isinstance(child, dict):
            out.append((int(key[len(stem) :]), child))
    return sorted(out, key=lambda kv: kv[0])


# ---------------------------------------------------------------- play-by-play


def _comments(raw: Any) -> List[Dict[str, Any]]:
    return [c for c in _many(_dict(_dict(raw).get("comments")).get("comment")) if isinstance(c, dict)]


def _clock_and_phase(entry: Dict[str, Any]) -> tuple:
    time = entry.get("time")
    if isinstance(time, str):
        return _str(time), None
    return None, _str(_attrs(time).get("period"))


def _penalty(player: Dict[str, Any]) -> tuple:
    """``(minutes, offense)`` of a penalised player; two penalties are summed / comma-joined."""
    pens = [_attrs(p) for p in _many(_dict(player.get("penalties")).get("penalty"))]
    if not pens:
        return None, None
    minutes = [_int(p.get("length")) for p in pens]
    offenses = [o for o in (_str(p.get("offense")) for p in pens) if o]
    total = sum(m for m in minutes if m is not None) if any(m is not None for m in minutes) else None
    return total, ",".join(offenses) or None


def _pbp_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    a = _attrs(entry)
    clock, phase = _clock_and_phase(entry)
    seconds = _clock_seconds(clock)
    details = _dict(entry.get("details"))
    detail = _dict(details.get("detail"))
    team = _attrs(detail.get("opponent"))
    row: Dict[str, Any] = {
        "comment_id": _id(a.get("id")),
        "sort_order": _int(a.get("order")),
        "event_type": _str(a.get("label")),
        "label2": _str(a.get("label2")),
        "time_type": _str(a.get("type")),
        "phase": phase,
        "clock": clock,
        "game_seconds": seconds,
        "period": _period(seconds),
        "home_score": _int(a.get("score1")),
        "away_score": _int(a.get("score2")),
        "home_shootout_score": _int(a.get("shootout_score1")),
        "away_shootout_score": _int(a.get("shootout_score2")),
        "event_id": _id(_attrs(detail).get("id")),
        "onlajny_team_id": _id(team.get("id")),
        "onlajny_team_global_id": _id(team.get("global_id")),
        "team_code": _str(team.get("code")),
        "no_assist": None,
        "penalty_minutes": None,
        "penalty_offense": None,
        "strength": _str(_attrs(details.get("on_field")).get("balance")),
        "message": _str(entry.get("message")),
        "written": _str(a.get("written")),
        "last_change": _str(a.get("last_change")),
    }
    for n in (1, 2, 3):
        player = _dict(detail.get(f"player{n}"))
        p = _attrs(player)
        row.update(
            {
                f"onlajny_player{n}_id": _id(p.get("id")),
                f"onlajny_player{n}_global_id": _id(p.get("global_id")),
                f"player{n}_name": _str(p.get("name")),
                f"player{n}_first_name": _str(p.get("firstname")),
                f"player{n}_last_name": _str(p.get("surname")),
            }
        )
        if n == 1:
            row["no_assist"] = _int(p.get("no_assist"))
            row["penalty_minutes"], row["penalty_offense"] = _penalty(player)
    return row


def _sorted(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Chronological: ascending ``sort_order`` (the file is newest first); unsortable rows last, in file order."""
    keyed = [(r["sort_order"] is None, r["sort_order"] or 0, i, r) for i, r in enumerate(rows)]
    return [r for *_k, r in sorted(keyed, key=lambda t: t[:3])]


def parse_elh_pbp(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per play-by-play commentary entry (``match/{season}/full|short/{id}.json``), oldest first.

    The onlajny feed's goals and penalties carry a team and up to three players: a goal's
    ``player1`` is the scorer and ``player2`` / ``player3`` the assists; a penalty's
    ``player1`` is the penalised player (``penalty_minutes``, ``penalty_offense``). Ids are
    ONLAJNY ids (``onlajny_player{n}_id``, ``onlajny_team_id``), not the hokej.cz ids of
    :func:`parse_elh_shots`. ``clock`` is the elapsed game clock ``mm:ss``; a clockless
    entry names only its ``phase`` (``0`` pre-game, ``1INT`` / ``2INT`` intermissions,
    ``3PER``, ``ERT`` after the game -- meanings inferred). ``period`` is derived from the
    clock (20-minute periods). ``strength`` (``EQ`` = even strength, inferred) is set only
    on goals that carry the on-ice block (not in 2016-17).

    Args:
        raw: the play-by-play body (``{"comments": {"comment": [...]}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_pbp"]``; zero rows for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_pbp
            pbp = elh_pbp(season=2027, match_id=2928321)
            pbp.filter(pl.col("event_type") == "goal")

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_pbp` - the route this works on.
        * :func:`sportsdataverse.hockey.elh.elh_pbp_latest` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = [_pbp_row(c) for c in _comments(raw)]
    return _frame(_sorted(rows), _PBP, return_as_pandas)


def parse_elh_pbp_goals(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per player on the ice at each goal, from the full play-by-play.

    A goal entry's ``details.on_field`` lists both teams' skaters and goalies
    (``opponent1`` / ``opponent2``, up to six players each) and the strength. Every
    player is a row tagged with the scoring team, so plus/minus and on-ice counts fall out
    of a group-by. A goal without that block (every 2016-17 goal) is ONE row with null
    on-ice columns, so no goal is dropped. Ids are ONLAJNY ids.

    Args:
        raw: the play-by-play body (``{"comments": {"comment": [...]}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_pbp_goals"]`` (goals oldest first,
        players in team then slot order); zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_pbp_goals
            on_ice = elh_pbp_goals(season=2027, match_id=2928321)
            on_ice.group_by("on_ice_team_code").len()

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_pbp_goals` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    out: List[Dict[str, Any]] = []
    for entry in _sorted([dict(_pbp_row(c), _entry=c) for c in _comments(raw)]):
        if entry["event_type"] != "goal":
            continue
        details = _dict(entry["_entry"].get("details"))
        base = {
            "comment_id": entry["comment_id"],
            "event_id": entry["event_id"],
            "clock": entry["clock"],
            "game_seconds": entry["game_seconds"],
            "period": entry["period"],
            "strength": entry["strength"],
            "home_score": entry["home_score"],
            "away_score": entry["away_score"],
            "scoring_onlajny_team_id": entry["onlajny_team_id"],
            "scoring_team_code": entry["team_code"],
        }
        empty = dict.fromkeys(list(_PBP_GOALS)[len(base) :])
        players = []
        for _n, team in _numbered(_dict(details.get("on_field")), "opponent"):
            t = _attrs(team)
            team_id = _id(t.get("id"))
            for slot, player in _numbered(team, "player"):
                p = _attrs(player)
                players.append(
                    {
                        **base,
                        "on_ice_onlajny_team_id": team_id,
                        "on_ice_team_code": _str(t.get("code")),
                        "is_scoring_team": None if team_id is None else team_id == base["scoring_onlajny_team_id"],
                        "slot": slot,
                        "onlajny_player_id": _id(p.get("id")),
                        "onlajny_player_global_id": _id(p.get("global_id")),
                        "player_name": _str(p.get("name")),
                    }
                )
        out.extend(players or [{**base, **empty}])
    return _frame(out, _PBP_GOALS, return_as_pandas)


def parse_elh_pbp_mobile(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per entry of the mobile play-by-play (``match/{season}/mobile/{id}.json``), oldest first.

    The same entries as the full file, flattened to text: no team, player or game ids.
    ``comment_id`` equals the full file's ``comment_id``; ``game_seconds`` counts from the
    start of the game (``0`` before it).

    Args:
        raw: the mobile body (``{"commentEvents": [...]}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_pbp_mobile"]``; zero rows for an
        empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_pbp_mobile
            text = elh_pbp_mobile(season=2027, match_id=2928321)

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_pbp_mobile` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    events = _dict(raw).get("commentEvents")
    rows = [
        {
            "comment_id": _id(e.get("id")),
            "event_type": _str(e.get("type")),
            "game_seconds": _int(e.get("time")),
            "content": _str(e.get("content")),
        }
        for e in (events if isinstance(events, list) else [])
        if isinstance(e, dict)
    ]
    return _frame(rows, _PBP_MOBILE, return_as_pandas)


# ---------------------------------------------------------------- shots / events


def parse_elh_shots(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per shot attempt (``visualization/shots/{id}.json``) with rink coordinates.

    Ids are HOKEJ.CZ ids (``hokejcz_player_id``, ``hokejcz_team_id``), not the onlajny ids of
    :func:`parse_elh_pbp`. ``coordinate_x`` runs along the rink LENGTH (goal to goal), centred
    at 0, about 1 unit = 1 ft with the goal lines near +-89; the home team shoots at +x and the
    away team at -x in every period. ``coordinate_y`` runs across the WIDTH, normalised so +-100
    are the boards (1 unit is about 0.425 ft); which board is +y is not verified. Scale and
    orientation are measured on the committed fixtures; the provider does not document them. The provider's
    ``match_shot_resutl_id`` (sic) is kept and aliased ``shot_result_id``: ``4`` = goal
    (``is_goal``); ``1`` / ``2`` / ``3`` / ``5`` are unknown. ``time`` is seconds from the
    start of the game, EXCEPT in 2016-17, where it is only the period's start offset
    (0 / 1200 / 2400; use ``real_date``). ``period`` is derived from ``time`` and holds in
    both eras. The faceoff win % block is left out (``return_parsed=False`` keeps it).

    Args:
        raw: the shots body (``{"match": {..., "shots": [...]}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_shots"]``; zero rows for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_shots
            shots = elh_shots(match_id=2928321)
            shots.filter(pl.col("is_goal") == True)

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_shots` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    match = _dict(_dict(raw).get("match"))
    home_id, away_id = _id(match.get("home_id")), _id(match.get("visitor_id"))
    game = {
        "hokejcz_game_id": _id(match.get("id")),
        "game_date": _str(match.get("date")),
        "hokejcz_home_team_id": home_id,
        "home_team_name": _str(match.get("home_name")),
        "home_team_code": _str(match.get("home_shortcut")),
        "hokejcz_away_team_id": away_id,
        "away_team_name": _str(match.get("visitor_name")),
        "away_team_code": _str(match.get("visitor_shortcut")),
    }
    rows = []
    shots = match.get("shots")
    for s in shots if isinstance(shots, list) else []:
        if not isinstance(s, dict):
            continue
        team_id, result, time = _id(s.get("team_id")), _int(s.get("match_shot_resutl_id")), _int(s.get("time"))
        side = "home" if team_id is not None and team_id == home_id else None
        side = "away" if team_id is not None and team_id == away_id else side
        rows.append(
            {
                **game,
                "shot_id": _id(s.get("id")),
                "hokejcz_player_id": _id(s.get("player_id")),
                "jersey": _int(s.get("jersey")),
                "first_name": _str(s.get("first_name")),
                "last_name": _str(s.get("last_name")),
                "hokejcz_team_id": team_id,
                "team_side": side,
                "match_shot_resutl_id": result,
                "shot_result_id": result,
                "is_goal": None if result is None else result == GOAL_RESULT_ID,
                "time": time,
                "period": None if time is None or time < 0 else time // PERIOD_SECONDS + 1,
                "coordinate_x": _float(s.get("coordinate_x")),
                "coordinate_y": _float(s.get("coordinate_y")),
                "real_date": _str(s.get("real_date")),
            }
        )
    return _frame(rows, _SHOTS, return_as_pandas)


def parse_elh_events(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Per-minute cumulative shots (``visualization/events/{id}.json``): one row per team and game minute.

    ``shots`` (attempts) and ``shots_on_goal`` are running totals up to the end of the
    minute; ``team_score`` is the team's final score. The file's goal list and power plays
    are left out (``return_parsed=False`` keeps them; goals are in :func:`parse_elh_pbp`).
    The body names no game id: it is the requested ``match_id``.

    Args:
        raw: the events body (``{"startDate", "home": {...}, "visitor": {...}}``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_events"]`` (home rows, then away,
        by minute); zero rows for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_events
            minutes = elh_events(match_id=2928321)
            minutes.filter(pl.col("minute") == 60)

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_events` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    body = _dict(raw)
    rows = []
    for key, side in _SIDES:
        team = _dict(body.get(key))
        shots, on_goal = _dict(team.get("shots")), _dict(team.get("shotsOnGoal"))
        minutes = sorted({m for m in (_int(k) for k in (*shots, *on_goal)) if m is not None})
        for minute in minutes:
            rows.append(
                {
                    "start_date": _str(body.get("startDate")),
                    "team_side": side,
                    "team_name": _str(team.get("teamName")),
                    "team_score": _int(team.get("score")),
                    "minute": minute,
                    "shots": _int(shots.get(str(minute))),
                    "shots_on_goal": _int(on_goal.get(str(minute))),
                }
            )
    return _frame(rows, _EVENTS, return_as_pandas)


# ---------------------------------------------------------------- scoreboard / listing


def _iso_date(dmy: Any) -> Optional[Any]:
    import datetime as _dt

    match = _DMY.fullmatch(dmy) if isinstance(dmy, str) else None
    if not match:
        return None
    try:
        return _dt.date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    except ValueError:
        return None


def parse_elh_scoreboard(
    raw: Any, *, league_id: Optional[int] = ELH_LEAGUE_ID, return_as_pandas: bool = False
) -> Frame:
    """One row per game on a scoreboard day (``scoreboard/{date}.json``), with both id spaces.

    The scoreboard is the only place hokej.cz ids meet onlajny ids: each game carries
    ``hokejcz_game_id`` + ``onlajny_game_id`` and each team ``hokejcz_*_team_id`` +
    ``onlajny_*_team_id``, which is how the play-by-play joins the shots. The window is
    ROLLING (recent days only), so it cannot map past seasons. Betting odds are left out
    (``return_parsed=False`` keeps them).

    Args:
        raw: the scoreboard body, keyed by league id.
        league_id: keep only this league (``101`` = Tipsport extraliga, the default; ``82``
            Maxa liga, ``130`` para hockey); ``None`` keeps every league.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_scoreboard"]``; zero rows for an
        empty or malformed body, or a day without a game in ``league_id``.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_scoreboard
            games = elh_scoreboard(date="2026-10-11")
            games.select("hokejcz_game_id", "onlajny_game_id")

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_scoreboard` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows = []
    for key, league in _dict(raw).items():
        lid = _int(key)
        if lid is None or (league_id is not None and lid != league_id):
            continue
        for g in _many(_dict(league).get("matches")):
            if not isinstance(g, dict):
                continue
            row: Dict[str, Any] = {
                "league_id": lid,
                "league_name": _str(_dict(league).get("league_name")),
                "hokejcz_game_id": _id(g.get("hokejcz_id")),
                "onlajny_game_id": _id(g.get("onlajny_id")),
                "game_date": _iso_date(g.get("date")),
                "start_time": _str(g.get("time")),
                "status_code": _str(g.get("match_actual_time_alias")),
                "status_name": _str(g.get("match_actual_time_name")),
                "match_status": _str(g.get("match_status")),
                "home_score": _int(g.get("score_home")),
                "away_score": _int(g.get("score_visitor")),
                "score_periods": ",".join(p for p in _many(g.get("score_periods")) if isinstance(p, str)) or None,
                "stream_url": _str(g.get("stream_url")),
            }
            for wire, side in _SIDES:
                team = _dict(g.get(wire))
                row.update(
                    {
                        f"hokejcz_{side}_team_id": _id(team.get("hokejcz_id")),
                        f"onlajny_{side}_team_id": _id(team.get("onlajny_id")),
                        f"{side}_team_name": _str(team.get("name")),
                        f"{side}_team_short_name": _str(team.get("short_name")),
                        f"{side}_team_code": _str(team.get("shortcut")),
                    }
                )
            rows.append({k: row[k] for k in _SCOREBOARD})
    return _frame(rows, _SCOREBOARD, return_as_pandas)


def _listing_contents(text: str) -> List[Dict[str, Any]]:
    try:
        from defusedxml.ElementTree import fromstring  # entity-expansion safe, when installed
    except ImportError:  # pragma: no cover - defusedxml ships with the dev environment
        from xml.etree.ElementTree import fromstring  # noqa: S405
    try:
        root = fromstring(text)
    except Exception:  # ParseError, defusedxml's DefusedXmlException: malformed -> no rows
        return []
    out = []
    for el in root:
        if el.tag.rsplit("}", 1)[-1] != "Contents":
            continue
        out.append({kid.tag.rsplit("}", 1)[-1]: kid.text for kid in el})
    return out


def parse_elh_game_ids(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """One row per key of an S3 ListObjects page (the bucket root ``/``); match keys carry the game id.

    ``hokejcz_game_id`` is the file name of a ``match/{season}/full/{id}.json`` key (null for
    any other key). The bucket is NOT Extraliga-only: it holds every competition hokej.cz
    shows, and the listing has no league field, so these ids are unfiltered (see
    :func:`~sportsdataverse.hockey.elh.elh_game_ids`, which pages a whole season). Keys of a
    few hundred bytes are files written ahead of a game.

    Args:
        raw: one ListObjects V1 XML page (text), or a list of pages.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        A frame with the schema ``ELH_SCHEMAS["parse_elh_game_ids"]``; zero rows for an empty
        or malformed body.

    Example:
        Quick start::

            from sportsdataverse.hockey.elh import elh_list_objects
            page = elh_list_objects(prefix="match/2026/full/")

    See Also:
        * :func:`sportsdataverse.hockey.elh.elh_list_objects` - the route this works on.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    pages: Iterable[Any] = raw if isinstance(raw, list) else [raw]
    rows = []
    for page in pages:
        if not isinstance(page, str):
            continue
        for c in _listing_contents(page):
            key = _str(c.get("Key"))
            match = _GAME_KEY.fullmatch(key or "")
            rows.append(
                {
                    "hokejcz_game_id": int(match.group(1)) if match else None,
                    "key": key,
                    "last_modified": _str(c.get("LastModified")),
                    "size": _int(c.get("Size")),
                }
            )
    return _frame(rows, _GAME_IDS, return_as_pandas)
