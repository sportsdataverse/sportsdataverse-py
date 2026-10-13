"""Parsers for the generated ``pleague_preciser`` wrappers (``api.preciser.io``, the analytics vendor behind the
P. LEAGUE+ game page).

Four keyless JSON routes keyed by the Preciser game uuid (``uuid`` of ``pleague_game``): the shot list (the only
court x / y outside the play-by-play fragment), the post-game summary, the rotation chart and the injury report. Every
body is a one-element list; the runtime unwraps it and returns ``{"data": {...}, "_request": {...}}`` (the bodies echo
at most a ``game_id``, so the request is stamped under ``_request``). **The vendor is a third party and the clip urls
carry an access key**: ``pleague_preciser_runtime`` scrubs ``key=`` values from every body, and no parser keeps a url.

Each public ``parse_pleague_preciser_*`` follows the package parser contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns, and a zero-row frame with the documented columns and dtypes, never an
exception, for an empty or unrecognised input.

**Id key spaces.** ``game_uuid``: the Preciser game (a version-1 uuid), found only on the site's game page (and empty
there before 2022-23, so these routes have no data for 2020-21 and 2021-22). ``team_uuid``: the Preciser team, the
file name of the club logo; it joins the site ``team_id`` through ``pleague_standings.team_uuid`` (the clubs table is
in the returns reference). ``person_id``: the Preciser person, the same value as the site box ``person_id`` (the join
between a shot and a box row).

**Shot frame.** ``x`` / ``y`` are in the VENDOR frame: percent (0-100) of the court length / width, the home team
always attacking the basket at x = 95. The site's play-by-play ``x`` / ``y`` are in the physical court frame (teams
swap ends at half-time): ``x_vendor = 100 - x_pbp`` and ``y_vendor = 100 - y_pbp`` in the halves where the home team
attacks x < 50, identical elsewhere. The court is 28.75 x 15.25 m (``distance = hypot((x - 5) * 0.2875,
(y - 50) * 0.1525)`` metres to the nearer basket, mean error 0.010 m on 1,893 shots). Which sideline is y = 0 is
unverified.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

from sportsdataverse.basketball.plg import pleague_parsers as SITE

__all__ = [
    "parse_pleague_preciser_injury",
    "parse_pleague_preciser_lead_flow",
    "parse_pleague_preciser_rotation",
    "parse_pleague_preciser_score_log",
    "parse_pleague_preciser_shots",
    "parse_pleague_preciser_summary_teams",
    "parse_pleague_preciser_win_probability",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
_REQUEST = "_request"
INT, F, S, B = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean
Spec = Dict[str, pl.DataType]

#: The clip vendor's access key rides on every video url (``?key=<hex>``): it is never kept.
SECRET = re.compile(r"[?&]key=[0-9a-fA-F]{16,}")
_KEY_QS = re.compile(r"([?&]key=)[0-9a-fA-F]{16,}")
#: Stint totals vs the box's mm:ss: 1,626 live rows differ by 1 s (median), 4 s (p95), 24 s (max).
ROTATION_TOLERANCE = 30


def scrub(obj: Any) -> Any:
    """``obj`` with the vendor access key replaced by ``<redacted>`` in every string (clip urls)."""
    if isinstance(obj, str):
        return _KEY_QS.sub(r"\1<redacted>", obj)
    if isinstance(obj, list):
        return [scrub(v) for v in obj]
    if isinstance(obj, dict):
        return {k: scrub(v) for k, v in obj.items()}
    return obj


def has_secret(obj: Any) -> bool:
    """True when a vendor access key survives anywhere in ``obj`` (a final scan before a body is returned)."""
    return bool(SECRET.search(json.dumps(obj, ensure_ascii=False)))


# --------------------------------------------------------------------------------------- helpers
def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _data(raw: Any) -> Tuple[Dict[str, Any], Optional[str]]:
    """``(the vendor's one game object, the game uuid asked for)``; ``({}, None)`` for anything else."""
    if not isinstance(raw, dict):
        return {}, None
    d = raw.get("data")
    req = raw.get(_REQUEST)
    uuid = req.get("uuid") if isinstance(req, dict) else None
    return (d if isinstance(d, dict) else {}), (uuid if isinstance(uuid, str) and uuid else None)


def _period_label(v: Any) -> Optional[str]:
    """``'q1'`` -> ``'Q1'``, ``'ot2'`` -> ``'OT2'``; also an integer period (1-4, 5+ = overtime)."""
    if isinstance(v, int) and not isinstance(v, bool):
        return f"Q{v}" if 1 <= v <= 4 else f"OT{v - 4}" if v > 4 else None
    return v.strip().upper() or None if isinstance(v, str) else None


def _sides(d: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        "home": SITE._s(SITE._obj(d.get("home_team")).get("id")),
        "away": SITE._s(SITE._obj(d.get("away_team")).get("id")),
    }


# --------------------------------------------------------------------------------------- shots
_SHOT_SPEC: Spec = {
    "game_uuid": S,
    "shot_no": INT,
    "period": INT,
    "period_label": S,
    "clock": S,
    "seconds_left": INT,
    "side": S,
    "team_uuid": S,
    "person_id": S,
    "player_name": S,
    "type": S,
    "subtype": S,
    "area": S,
    "success": B,
    "x": F,
    "y": F,
    "message_zh": S,
    "has_video": B,
    "home_team_uuid": S,
    "away_team_uuid": S,
}


def parse_pleague_preciser_shots(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every field-goal attempt of one finished P. LEAGUE+ game with court x / y, zone and style.

    Args:
        raw: the record a ``pleague_preciser_shots`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per shot (no free throws), in the order the vendor sent them (NOT guaranteed game order: sort on
        ``period`` and ``seconds_left``). ``person_id`` joins the site box score (``pleague_box_players``); the
        player name and team come from the body's own player list. ``x`` / ``y`` are percent of the court in the
        VENDOR frame, in which the home team always attacks x = 95: the play-by-play frame is rotated 180 degrees
        in the halves where the home team attacks x < 50 (``x_vendor = 100 - x_pbp``; see the module docstring).
        ``has_video`` flags a clip; the clip url (it carries a vendor access key) is never kept. Zero rows for an empty
        or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_shots, parse_pleague_preciser_shots

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_shots(uuid=uuid, return_parsed=False)
            shots = parse_pleague_preciser_shots(raw)
    """
    d, uuid = _data(raw)
    sides = _sides({"home_team": {"id": d.get("home_id")}, "away_team": {"id": d.get("away_id")}})
    players = {p.get("id"): p for p in SITE._dicts(d.get("players"))}
    rows = []
    for n, s in enumerate(SITE._dicts(d.get("shot")), 1):
        p = players.get(s.get("player")) or {}
        team = SITE._s(p.get("team_id"))
        rows.append(
            {
                "game_uuid": uuid,
                "shot_no": n,
                "period": SITE._i(s.get("period")),
                "period_label": _period_label(s.get("period")),
                "clock": SITE._s(s.get("clock")),
                "seconds_left": SITE._seconds(s.get("clock")),
                "side": "home"
                if team and team == sides["home"]
                else "away"
                if team and team == sides["away"]
                else None,
                "team_uuid": team,
                "person_id": SITE._s(s.get("player")),
                "player_name": SITE._s(p.get("name")),
                "type": SITE._s(s.get("type")),
                "subtype": SITE._s(s.get("subtype")),
                "area": SITE._s(s.get("area")),
                "success": s.get("success") if isinstance(s.get("success"), bool) else None,
                "x": SITE._f(s.get("x")),
                "y": SITE._f(s.get("y")),
                "message_zh": SITE._s(s.get("message")),
                "has_video": bool(s.get("video_url")),
                "home_team_uuid": sides["home"],
                "away_team_uuid": sides["away"],
            }
        )
    return _frame(rows, _SHOT_SPEC, columns, as_pandas=return_as_pandas)


# --------------------------------------------------------------------------------------- summary
_COMPARE = {
    "points": "points",
    "rebounds": "rebounds",
    "assists": "assists",
    "steals": "steals",
    "blocks": "blocks",
    "turnovers": "turnovers",
    "field_goal_percentage": "fg_made",  # the key says percentage; the value is a count of makes
    "two_points_percentage": "fg2_pct",
    "three_points_percentage": "fg3_pct",
    "free_throw_percentage": "ft_pct",
}
_SUMMARY_TEAM_SPEC: Spec = {
    "game_uuid": S,
    "side": S,
    "team_uuid": S,
    "team_name_zh": S,
    "color": S,
    "logo_url": S,
    "points": INT,
    "rebounds": INT,
    "assists": INT,
    "steals": INT,
    "blocks": INT,
    "turnovers": INT,
    "fg_made": INT,
    "fg2_pct": F,
    "fg3_pct": F,
    "ft_pct": F,
    "fast_break_points": F,
    "paint_points": F,
    "second_chance_points": F,
    "biggest_lead": INT,
    "biggest_scoring_run": INT,
    "time_leading_seconds": INT,
    "lead_changes": INT,
    "top_player_name": S,
    "top_player_jersey": S,
    "top_player_points": INT,
    "top_player_rebounds": INT,
    "top_player_assists": INT,
}


def parse_pleague_preciser_summary_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The team comparison of the post-game summary of one finished P. LEAGUE+ game: one row per team.

    Args:
        raw: the record a ``pleague_preciser_summary_teams`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows (``home``, ``away``): team identity, the comparison block (points, rebounds, assists, steals,
        blocks, turnovers, field goals made, 2-point / 3-point / free-throw percentage), the three situational
        point totals, the lead tracker and the team's top performer. The vendor's ``field_goal_percentage`` is a
        COUNT of field goals made (``fg_made`` here). The bar-length ``*_progress`` fields are not kept. Zero rows
        for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_summary_teams, parse_pleague_preciser_summary_teams

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_summary_teams(uuid=uuid, return_parsed=False)
            teams = parse_pleague_preciser_summary_teams(raw)
    """
    d, uuid = _data(raw)
    rows = []
    if isinstance(d.get("home_team"), dict) or isinstance(d.get("away_team"), dict):
        lead = SITE._obj(d.get("leadStats"))
        for side in ("home", "away"):
            team = SITE._obj(d.get(f"{side}_team"))
            top = SITE._obj(SITE._obj(d.get("top_performers")).get(f"{side}_player"))
            row: Dict[str, Any] = {
                "game_uuid": uuid or SITE._s(d.get("game_id")),
                "side": side,
                "team_uuid": SITE._s(team.get("id")),
                "team_name_zh": SITE._s(team.get("name")),
                "color": SITE._s(team.get("color")),
                "logo_url": SITE._s(team.get("logo_url")),
                "fast_break_points": SITE._f(SITE._obj(d.get("fast_break_points")).get(f"{side}_value")),
                "paint_points": SITE._f(SITE._obj(d.get("paint_points")).get(f"{side}_value")),
                "second_chance_points": SITE._f(SITE._obj(d.get("second_chance_points")).get(f"{side}_value")),
                "biggest_lead": SITE._i(SITE._obj(lead.get("biggestLead")).get(side)),
                "biggest_scoring_run": SITE._i(SITE._obj(lead.get("biggestScoringRun")).get(side)),
                "time_leading_seconds": SITE._i(SITE._obj(lead.get("timeLeadingSeconds")).get(side)),
                "lead_changes": SITE._i(lead.get("leadChanges")),
                "top_player_name": SITE._s(top.get("name")),
                "top_player_jersey": SITE._s(top.get("jersey_number")),
                "top_player_points": SITE._i(top.get("points")),
                "top_player_rebounds": SITE._i(top.get("rebound")),
                "top_player_assists": SITE._i(top.get("assists")),
            }
            cmp_ = SITE._obj(d.get("team_comparison"))
            for key, col in _COMPARE.items():
                v = SITE._obj(cmp_.get(key)).get(f"{side}_value")
                row[col] = SITE._f(v) if _SUMMARY_TEAM_SPEC[col] == F else SITE._i(v)
            rows.append(row)
    return _frame(rows, _SUMMARY_TEAM_SPEC, columns, return_as_pandas)


_SCORE_LOG_SPEC: Spec = {
    "game_uuid": S,
    "seq": INT,
    "period": S,
    "period_no": INT,
    "clock": S,
    "seconds_left": INT,
    "home_score": INT,
    "away_score": INT,
}


def _score_rows(d: Dict[str, Any], uuid: Optional[str], items: Any) -> List[Dict[str, Any]]:
    rows = []
    for n, r in enumerate(SITE._dicts(items), 1):
        label = _period_label(r.get("quarter"))
        rows.append(
            {
                "game_uuid": uuid or SITE._s(d.get("game_id")),
                "seq": n,
                "period": label,
                "period_no": SITE._period_no(label),
                "clock": SITE._s(r.get("time")),
                "seconds_left": SITE._seconds(r.get("time")),
                "home_score": SITE._i(r.get("home_score")),
                "away_score": SITE._i(r.get("away_score")),
                "score_difference": SITE._i(r.get("score_difference")),
            }
        )
    return rows


def parse_pleague_preciser_score_log(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The scoring log of the post-game summary of one finished P. LEAGUE+ game.

    Args:
        raw: the record a ``pleague_preciser_score_log`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per SCORING event in game order (not the full play-by-play; use ``pleague_pbp`` for that) with the
        running score after it. The last row is the final score. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_score_log, parse_pleague_preciser_score_log

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_score_log(uuid=uuid, return_parsed=False)
            log = parse_pleague_preciser_score_log(raw)
    """
    d, uuid = _data(raw)
    spec = {k: v for k, v in _SCORE_LOG_SPEC.items()}
    return _frame(_score_rows(d, uuid, d.get("pbp")), spec, columns, return_as_pandas)


_LEAD_FLOW_SPEC: Spec = {**_SCORE_LOG_SPEC, "score_difference": INT}


def parse_pleague_preciser_lead_flow(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The score at each minute mark of one finished P. LEAGUE+ game (the summary's lead tracker).

    Args:
        raw: the record a ``pleague_preciser_lead_flow`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per minute mark in game order with both scores and ``score_difference`` (home minus away; positive
        = home leads). Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_lead_flow, parse_pleague_preciser_lead_flow

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_lead_flow(uuid=uuid, return_parsed=False)
            flow = parse_pleague_preciser_lead_flow(raw)
    """
    d, uuid = _data(raw)
    lead = SITE._obj(d.get("leadStats"))
    return _frame(_score_rows(d, uuid, lead.get("leadPerMinutes")), _LEAD_FLOW_SPEC, columns, return_as_pandas)


_WIN_PROB_SPEC: Spec = {"game_uuid": S, "minute": INT, "home_win_pct": F}


def parse_pleague_preciser_win_probability(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The vendor's home-team win probability by minute for one finished P. LEAGUE+ game.

    Args:
        raw: the record a ``pleague_preciser_win_probability`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per minute mark: ``minute`` 0 is the tip-off (50 percent) and the list has 49 entries for a
        48-minute game. ``home_win_pct`` is the home team's win probability in percent (0-100). The vendor does
        not document its method. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_win_probability, parse_pleague_preciser_win_probability

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_win_probability(uuid=uuid, return_parsed=False)
            wp = parse_pleague_preciser_win_probability(raw)
    """
    d, uuid = _data(raw)
    probs = d.get("home_team_probability_over_time")
    rows = [
        {"game_uuid": uuid or SITE._s(d.get("game_id")), "minute": n, "home_win_pct": SITE._f(p)}
        for n, p in enumerate(probs if isinstance(probs, list) else [])
    ]
    return _frame(rows, _WIN_PROB_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------------------- rotation
_ROTATION_SPEC: Spec = {
    "game_uuid": S,
    "side": S,
    "name_zh": S,
    "jersey": INT,
    "started": B,
    "stint_no": INT,
    "start_period": S,
    "start_clock": S,
    "start_seconds": INT,
    "end_period": S,
    "end_clock": S,
    "end_seconds": INT,
}


def parse_pleague_preciser_rotation(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The on-court stints of every player of one finished P. LEAGUE+ game (the rotation chart).

    Args:
        raw: the record a ``pleague_preciser_rotation`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        One row per stint, home team first, starters first within a team. ``start_seconds`` / ``end_seconds`` count
        seconds from the start of play on the game clock, cumulative across periods (``end_seconds -
        start_seconds`` is the stint's length). The chart carries names and jersey numbers, no ids: ``name_zh``
        equals the site box ``name_zh``. A player who never entered has no row. Zero rows for an empty or malformed
        input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_rotation, parse_pleague_preciser_rotation

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_rotation(uuid=uuid, return_parsed=False)
            stints = parse_pleague_preciser_rotation(raw)
    """
    d, uuid = _data(raw)
    rows = []
    for side, key in (("home", "homeTeam"), ("away", "awayTeam")):
        for p in SITE._dicts(d.get(key)):
            for n, st in enumerate(SITE._dicts(p.get("periods")), 1):
                rows.append(
                    {
                        "game_uuid": uuid,
                        "side": side,
                        "name_zh": SITE._s(p.get("name")),
                        "jersey": SITE._i(p.get("jerseyNumber")),
                        "started": p.get("started") if isinstance(p.get("started"), bool) else None,
                        "stint_no": n,
                        "start_period": _period_label(st.get("startQuarter")),
                        "start_clock": SITE._s(st.get("startTime")),
                        "start_seconds": SITE._i(st.get("startSeconds")),
                        "end_period": _period_label(st.get("endQuarter")),
                        "end_clock": SITE._s(st.get("endTime")),
                        "end_seconds": SITE._i(st.get("endSeconds")),
                    }
                )
    return _frame(rows, _ROTATION_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------------------- injury
_INJURY_SPEC: Spec = {
    "game_uuid": S,
    "side": S,
    "team_uuid": S,
    "team_name_zh": S,
    "team_name_en": S,
    "color": S,
    "logo_url": S,
    "injury_report": S,
}


def parse_pleague_preciser_injury(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The injury report of one finished P. LEAGUE+ game: one row per team.

    Args:
        raw: the record a ``pleague_preciser_injury`` wrapper returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as nulls when missing).

    Returns:
        Two rows (``home``, ``away``) with the team identity. ``injury_report`` is the team's report as a JSON
        string, null when the vendor sent null: it was null for every one of 16 sampled games, so its shape is
        unverified. Zero rows for an empty or malformed input.

    Raises:
        This parser does not raise on a malformed input; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.plg import pleague_game
            from sportsdataverse.basketball.plg import pleague_preciser_injury, parse_pleague_preciser_injury

            uuid = pleague_game(gid=197)["uuid"][0]
            raw = pleague_preciser_injury(uuid=uuid, return_parsed=False)
            report = parse_pleague_preciser_injury(raw)
    """
    d, uuid = _data(raw)
    rows = []
    if isinstance(d.get("home_team"), dict) or isinstance(d.get("away_team"), dict):
        report = SITE._obj(d.get("injury_report"))
        for side in ("home", "away"):
            team = SITE._obj(d.get(f"{side}_team"))
            got = report.get(side)
            rows.append(
                {
                    "game_uuid": uuid or SITE._s(d.get("game_id")),
                    "side": side,
                    "team_uuid": SITE._s(team.get("id")),
                    "team_name_zh": SITE._s(team.get("name")),
                    "team_name_en": SITE._s(team.get("english_name")),
                    "color": SITE._s(team.get("color")),
                    "logo_url": SITE._s(team.get("logo_url")),
                    "injury_report": None if got is None else json.dumps(got, ensure_ascii=False),
                }
            )
    return _frame(rows, _INJURY_SPEC, columns, return_as_pandas)


# --------------------------------------------------------------------------------------- scope checks
def _made(cell: Any) -> int:
    m = re.fullmatch(r"(\d+)-(\d+)", cell or "") if isinstance(cell, str) else None
    return int(m.group(1)) if m else 0


def box_field_goals(box: Dict[str, Any]) -> Tuple[int, int]:
    """(2-point makes, 3-point makes) over both sides' player rows of a box body's ``data``."""
    two = three = 0
    for side in ("home", "away"):
        for p in box.get(side) or []:
            two += _made(p.get("two_m_two"))
            three += _made(p.get("trey_m_trey"))
    return two, three


def box_seconds(box: Dict[str, Any]) -> Dict[str, int]:
    """``{name: seconds on court}`` over both sides from the box's ``mm:ss`` minutes (did-not-play rows omitted)."""
    out = {}
    for side in ("home", "away"):
        for p in box.get(side) or []:
            s = SITE._seconds(p.get("mins"))
            if s is not None:
                out[p.get("name_alt")] = s
    return out


def vendor_error(kind: str, body: Any, uuid: str, ctx: Dict[str, Any]) -> Optional[str]:
    """Why ``body`` is not the ``kind`` body of game ``uuid`` (None when it is).

    ``ctx`` may hold ``game`` (the site's game record), ``box`` (the site box ``data``) and ``team_ids`` (the
    verified summary's ``(home, away)`` team uuids); a check whose context is absent is skipped, which is why the
    runtime verifies against the site only when the caller passes a ``gid``.
    """
    if not isinstance(body, list) or len(body) != 1 or not isinstance(body[0], dict):
        return f"{kind}: expected a one-element list"
    b = body[0]
    if kind in ("summary", "injury"):
        if not SITE._same(b.get("game_id"), uuid):
            return f"{kind}: game_id {b.get('game_id')!r}, asked for {uuid!r}"
        ids = tuple(SITE._obj(b.get(n)).get("id") for n in ("home_team", "away_team"))
        if not all(ids):
            return f"{kind}: a team id is missing"
        if kind == "summary":
            ps = b.get("pbp") or []
            if not ps:
                return "summary: the play log is empty"
            game = ctx.get("game")
            if game and (ps[-1].get("away_score"), ps[-1].get("home_score")) != (
                game["away_score"],
                game["home_score"],
            ):
                return "summary: the play log does not end on the game page's final score"
            ctx["team_ids"] = ids  # only after every summary check passed
        elif ctx.get("team_ids") and ids != ctx["team_ids"]:
            return "injury: team ids differ from the summary's"
    elif kind == "shots":
        if b.get("game_status") != "post":
            return f"shots: game_status {b.get('game_status')!r}, not post"
        if ctx.get("team_ids") and (b.get("home_id"), b.get("away_id")) != ctx["team_ids"]:
            return "shots: home_id / away_id differ from the summary's teams"
        known = {p.get("id") for p in b.get("players") or []}
        if any(s.get("player") not in known for s in b.get("shot") or []):
            return "shots: a shot names a player the body does not list"
        if ctx.get("box"):
            made = {"2pt": 0, "3pt": 0}
            for s in b.get("shot") or []:
                if s.get("success") is True and s.get("type") in made:
                    made[s["type"]] += 1
            if (made["2pt"], made["3pt"]) != box_field_goals(ctx["box"]):
                return f"shots: made 2pt/3pt {made['2pt']}/{made['3pt']} != the box's {box_field_goals(ctx['box'])}"
    elif kind == "rotation":
        if ctx.get("box"):
            for side, key in (("home", "homeTeam"), ("away", "awayTeam")):
                roster = {p.get("name_alt") for p in ctx["box"][side]}
                names = {p.get("name") for p in b.get(key) or []}
                if not names or not names <= roster:
                    return f"rotation: {key} names {sorted(n for n in names - roster if n)[:3]} are not on the {side} box roster"
            secs = box_seconds(ctx["box"])
            for key in ("homeTeam", "awayTeam"):
                for p in b.get(key) or []:
                    on = sum(
                        (st.get("endSeconds") or 0) - (st.get("startSeconds") or 0) for st in p.get("periods") or []
                    )
                    if abs(on - secs.get(p.get("name"), -999)) > ROTATION_TOLERANCE:
                        return f"rotation: {p.get('name')} on court {on}s != the box's {secs.get(p.get('name'))}s"
    return None


#: Each parser's column dtypes, keyed by wrapper short, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "shots": _SHOT_SPEC,
    "summary_teams": _SUMMARY_TEAM_SPEC,
    "score_log": _SCORE_LOG_SPEC,
    "lead_flow": _LEAD_FLOW_SPEC,
    "win_probability": _WIN_PROB_SPEC,
    "rotation": _ROTATION_SPEC,
    "injury": _INJURY_SPEC,
}
