"""Parsers for the generated ``ibba_segev`` wrappers (Segev Stats, the live-stats vendor behind basket.co.il).

Three JSON-RPC methods of ``stats.segevstats.com/realtimestat_heb/api/`` (``getGameInfo``, ``getBoxScore``,
``getActions``) feed six frames. ``ibba_segev_runtime`` refuses error envelopes, unfinished games and bodies for
another game, and stamps the request under ``_request``; each public ``parse_ibba_segev_*`` takes the envelope
and follows the package parser contract: a polars frame by default, pandas with ``return_as_pandas=True``,
snake_case columns, and a zero-row frame with the documented columns and dtypes (never an untyped
``pl.DataFrame()``, never an exception) for an empty or unrecognised body. A box-score body parsed by a
game-info or play-by-play parser (and the reverse) is "unrecognised": a zero-row frame.

**Dtypes** are pinned by column name in :data:`SPECS`. Segev sends every box-score number as a STRING and ids
as strings in the header and box score but INTEGERS in the actions; the parsers cast all ids to ``Int64`` at the
boundary (a non-digit string is null, never ``"123.0"``). Clock times stay ``'mm:ss'`` text; box-score minutes
also get ``seconds_played``.

**Id key spaces** (``Int64``, none interchangeable with the ASMX ids of ``ibba_*``):

* ``game_id``: the Segev game. One flat space across seasons AND competitions (men's Winner League, Winner Cup,
  women's, Leumit, youth, school, pre-season); ``competition`` says which. For a league game from 2025-26 it is
  ``ibba_games.external_id`` (``segev_game_id``).
* ``team_id``: a Segev team, stable across seasons and competitions (Maccabi Tel Aviv = 2). Not an IBBA
  team-season id.
* ``player_id``: a Segev player, stable across seasons (Jaylen Hoard = 1000 in games 50 and 419): the only
  cross-season player key of the Israeli sources. Linking it to the IBBA player-season id needs a name or jersey
  match inside one game.
* ``action_id``: a play-by-play action, unique and increasing within one game (500001 ... for game 50).
* Actions name the acting player by Segev ``player_id`` and, in ``jersey``, by JERSEY number (a string, not an
  id); ``team_side`` is ``home`` (action ``parameters.team`` 1) or ``away`` (2).

**Renames.** Segev's made / MISSED pairs are kept as ``fg2m`` / ``fg2_missed`` (``fg_2m`` / ``fg_2mis``), with
``fg2a`` = made + missed derived; ``f`` -> ``fouls`` (committed), ``f_drawn`` -> ``fouls_drawn``, ``rate`` ->
``val`` (the efficiency rating, equal to the ASMX ``VAL``), ``to`` -> ``turnovers``. The team game-stat keys
Segev renamed between 2025-26 and 2026-27 (``Points_in_the_paint`` -> ``Pts_In_The_Paint``) land in one column.

**Scores** read away-home in an action's ``score``; the parser splits them into ``score_away`` / ``score_home``.
Header ``Score`` is always 0 on the feed, so the header frame carries no score (see ``ibba_segev_team_box``).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Union

import pandas as pd
import polars as pl

__all__ = [
    "parse_ibba_segev_game_info",
    "parse_ibba_segev_pbp",
    "parse_ibba_segev_period_scores",
    "parse_ibba_segev_player_box",
    "parse_ibba_segev_rosters",
    "parse_ibba_segev_team_box",
]

Frame = Union[pl.DataFrame, pd.DataFrame]
Spec = Dict[str, pl.DataType]

INT, F, S, B = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean
_DT = pl.Datetime("us")
_DIGITS = re.compile(r"-?\d+")
_MMSS = re.compile(r"(\d+):(\d{2})")
_SIDES = {1: "home", 2: "away"}


# ---------------------------------------------------------------------------------------- cells
def _s(v: Any) -> Optional[str]:
    if v is None or isinstance(v, (dict, list)):
        return None
    s = str(v).strip()
    return s or None


def _i(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    s = _s(v)
    return int(s) if s is not None and _DIGITS.fullmatch(s) else None


def _nz(v: Any) -> Optional[int]:
    """An id where 0 means "none" (actions carry ``playerId`` 0 and ``teamId`` 0 on game-level events)."""
    return _i(v) or None


def _f(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = _s(v)
    try:
        return float(s.replace(",", ".")) if s else None
    except ValueError:
        return None


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _seconds(v: Any) -> Optional[int]:
    m = _MMSS.fullmatch(_s(v) or "")
    return int(m.group(1)) * 60 + int(m.group(2)) if m else None


def _when(v: Any) -> Optional[datetime]:
    s = _s(v)
    try:
        return datetime.fromisoformat(s) if s else None
    except ValueError:
        return None


def _frame(rows: List[Dict[str, Any]], spec: Spec, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``rows`` as a frame with exactly ``spec``'s columns and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name the spec lacks is added as a null string column.
    """
    full: Dict[str, pl.DataType] = dict(spec)
    for c in columns or ():
        full.setdefault(c, S)
    df = pl.DataFrame({c: [r.get(c) for r in rows] for c in full}, schema=full)
    return df.to_pandas() if as_pandas else df


def _d(v: Any) -> Dict[str, Any]:
    """``v`` when it is a dict, else an empty dict."""
    return v if isinstance(v, dict) else {}


def _result(raw: Any) -> Dict[str, Any]:
    r = raw.get("result") if isinstance(raw, dict) else None
    return r if isinstance(r, dict) else {}


def _box(raw: Any) -> Dict[str, Any]:
    b = _result(raw).get("boxscore")
    return b if isinstance(b, dict) else {}


def _box_info(raw: Any) -> Dict[str, Any]:
    g = _box(raw).get("gameInfo")
    return g if isinstance(g, dict) else {}


def _header(raw: Any) -> Dict[str, Any]:
    """``result.gameInfo`` of a getGameInfo / getActions body (the box score keeps its own under ``boxscore``)."""
    g = _result(raw).get("gameInfo")
    return g if isinstance(g, dict) else {}


def _team(info: Dict[str, Any], side: str) -> Dict[str, Any]:
    t = info.get(f"{side}Team")
    return t if isinstance(t, dict) else {}


def _dicts(v: Any) -> List[Dict[str, Any]]:
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


# ----------------------------------------------------------------------------- game header
_INFO_SPEC: Spec = {
    "game_id": INT,
    "start_time": _DT,
    "competition": S,
    "home_team_id": INT,
    "home_team": S,
    "home_team_local": S,
    "home_short_name": S,
    "away_team_id": INT,
    "away_team": S,
    "away_team_local": S,
    "away_short_name": S,
    "periods": INT,
    "finished": B,
    "current_period": INT,
    "clock": S,
}


def parse_ibba_segev_game_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row: the game header (tip-off, competition, both teams) of a Segev game.

    Args:
        raw: the dict ``ibba_segev_game_info`` (or ``ibba_segev_pbp``, which carries the same header) returns
            with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row. ``start_time`` is the scheduled tip-off in Israel local time as a naive datetime (the feed
        has no offset; it can differ from the IBBA ``tip_off``). ``competition`` is the reliable men's / women's
        discriminator (``Winner League``, ``Winner Cup``, ``Women``, ``Leumit``, ...). Scores are not here: the
        feed's header ``Score`` is always 0. A body without a header gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_game_info, parse_ibba_segev_game_info

            raw = ibba_segev_game_info(game_id=50, return_parsed=False)
            df = parse_ibba_segev_game_info(raw)
    """
    g = _header(raw)
    rows: List[Dict[str, Any]] = []
    if g:
        home, away = _team(g, "home"), _team(g, "away")
        q = _d(g.get("currentQuarterTime"))
        m, sec = _i(q.get("m")), _i(q.get("s"))
        comp = _d(g.get("competition"))
        rows.append(
            {
                "game_id": _i(g.get("id")) if _i(g.get("id")) is not None else _i(g.get("gameId")),
                "start_time": _when(g.get("time")),
                "competition": _s(comp.get("name")),
                "home_team_id": _i(home.get("id")),
                "home_team": _s(home.get("name")),
                "home_team_local": _s(home.get("nameLocal")),
                "home_short_name": _s(home.get("shortName")),
                "away_team_id": _i(away.get("id")),
                "away_team": _s(away.get("name")),
                "away_team_local": _s(away.get("nameLocal")),
                "away_short_name": _s(away.get("shortName")),
                "periods": _i(g.get("numberOfQuarters")),
                "finished": _b(g.get("gameFinished")),
                "current_period": _i(g.get("currentQuarter")),
                "clock": None if m is None or sec is None else f"{m:02d}:{sec:02d}",
            }
        )
    return _frame(rows, _INFO_SPEC, columns, return_as_pandas)


_ROSTER_SPEC: Spec = {
    "game_id": INT,
    "team_side": S,
    "team_id": INT,
    "player_id": INT,
    "jersey_number": INT,
    "first_name": S,
    "last_name": S,
    "first_name_local": S,
    "last_name_local": S,
    "in_roster": INT,
    "api_id": INT,
}


def parse_ibba_segev_rosters(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One row per rostered player of a Segev game, both teams, with English and Hebrew names.

    Args:
        raw: the dict ``ibba_segev_game_info`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per player listed in the header, home side first. ``player_id`` is the Segev player id (stable
        across seasons); ``in_roster`` is 0 or 2 in the samples (meaning unverified). A body without rosters
        gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_game_info, parse_ibba_segev_rosters

            raw = ibba_segev_game_info(game_id=50, return_parsed=False)
            df = parse_ibba_segev_rosters(raw)
    """
    g = _header(raw)
    gid = _i(g.get("id")) if _i(g.get("id")) is not None else _i(g.get("gameId"))
    rows: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        team = _team(g, side)
        for p in _dicts(team.get("players")):
            rows.append(
                {
                    "game_id": gid,
                    "team_side": side,
                    "team_id": _i(team.get("id")),
                    "player_id": _i(p.get("id")),
                    "jersey_number": _i(p.get("jerseyNumber")),
                    "first_name": _s(p.get("firstName")),
                    "last_name": _s(p.get("lastName")),
                    "first_name_local": _s(p.get("firstNameLocal")),
                    "last_name_local": _s(p.get("lastNameLocal")),
                    "in_roster": _i(p.get("inRoster")),
                    "api_id": _i(p.get("apiId")),
                }
            )
    return _frame(rows, _ROSTER_SPEC, columns, return_as_pandas)


# ------------------------------------------------------------------------------------- box score
#: wire key -> output name for the counting stats shared by player lines, team lines and totals.
_COUNT_KEYS = {
    "fg_2m": "fg2m",
    "fg_2mis": "fg2_missed",
    "fg_3m": "fg3m",
    "fg_3mis": "fg3_missed",
    "ft_m": "ftm",
    "ft_mis": "ft_missed",
    "reb_d": "dreb",
    "reb_o": "oreb",
    "f": "fouls",
    "f_drawn": "fouls_drawn",
    "stl": "steals",
    "to": "turnovers",
    "ast": "assists",
    "blk": "blocks",
    "blk_against": "blocks_against",
    "fb": "fast_break_pts",
    "sc": "second_chance_pts",
    "points": "pts",
    "rate": "val",
    "dfl": "deflections",
    "rec": "recoveries",
    "dunk": "dunks",
    "fd_off": "offensive_fouls_drawn",
    "and_one": "and_ones",
}
_COUNT_SPEC: Spec = {**{name: INT for name in _COUNT_KEYS.values()}, "vps": F}
_ATTEMPTS = {"fg2a": ("fg2m", "fg2_missed"), "fg3a": ("fg3m", "fg3_missed"), "fta": ("ftm", "ft_missed")}


def _counts(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {name: _i(d.get(key)) for key, name in _COUNT_KEYS.items()}
    out["vps"] = _f(d.get("vps"))
    for att, (made, missed) in _ATTEMPTS.items():
        m, x = out[made], out[missed]
        out[att] = None if m is None or x is None else m + x
    return out


_COUNT_SPEC_FULL: Spec = {**_COUNT_SPEC, "fg2a": INT, "fg3a": INT, "fta": INT}

_PLAYER_SPEC: Spec = {
    "game_id": INT,
    "team_side": S,
    "team_id": INT,
    "row_type": S,
    "player_id": INT,
    "jersey_number": INT,
    "starter": B,
    "on_court": B,
    "minutes": S,
    "seconds_played": INT,
    "plus_minus": INT,
    **_COUNT_SPEC_FULL,
}


def parse_ibba_segev_player_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-player box score of a finished Segev game, plus each team's team-credited line.

    Args:
        raw: the dict ``ibba_segev_player_box`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per player (``row_type`` ``player``) and one ``team`` row per side holding the actions credited
        to the team and no player (team rebounds and turnovers); a team's totals are the sum of its rows.
        Counts are ``Int64``; the feed gives made / MISSED pairs, so ``fg2a`` / ``fg3a`` / ``fta`` are derived
        (made + missed). ``minutes`` is the feed's ``'mm:ss'`` and ``seconds_played`` its seconds. A body
        without a box score gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_player_box, parse_ibba_segev_player_box

            raw = ibba_segev_player_box(game_id=50, return_parsed=False)
            df = parse_ibba_segev_player_box(raw)
    """
    box, info = _box(raw), _box_info(raw)
    gid = _i(info.get("gameId"))
    rows: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        team = box.get(f"{side}Team")
        if not isinstance(team, dict):
            continue
        ctx = {"game_id": gid, "team_side": side, "team_id": _i(info.get(f"{side}TeamId"))}
        for p in _dicts(team.get("players")):
            rows.append(
                {
                    **ctx,
                    "row_type": "player",
                    "player_id": _i(p.get("playerId")),
                    "jersey_number": _i(p.get("jerseyNumber")),
                    "starter": _b(p.get("starter")),
                    "on_court": _b(p.get("onCourt")),
                    "minutes": _s(p.get("minutes")),
                    "seconds_played": _seconds(p.get("minutes")),
                    "plus_minus": _i(p.get("plusMinus")),
                    **_counts(p),
                }
            )
        if isinstance(team.get("teamActions"), dict):
            rows.append({**ctx, "row_type": "team", **_counts(team["teamActions"])})
    return _frame(rows, _PLAYER_SPEC, columns, return_as_pandas)


#: gameStats keys by feed era (2025-26 first, 2026-27 second) -> output column.
_GAME_STAT_KEYS = {
    "pts_from_turnovers": ("Points_from_turnovers", "Pts_From_Turnovers"),
    "pts_in_paint": ("Points_in_the_paint", "Pts_In_The_Paint"),
    "starting_points": ("starting_points", "starting_points"),
    "bench_points": ("bench_points", "bench_points"),
}
_TEAM_SPEC: Spec = {
    "game_id": INT,
    "team_side": S,
    "team_id": INT,
    "score": INT,
    "timeouts": INT,
    "time_leading": S,
    "biggest_lead": INT,
    "times_tied": INT,
    "lead_changes": INT,
    **_COUNT_SPEC_FULL,
    "pts_from_turnovers": INT,
    "pts_in_paint": INT,
    "starting_points": INT,
    "bench_points": INT,
    "fast_break_possessions": INT,
    "defensive_stops": INT,
    "biggest_scoring_run": S,
}


def _first(d: Dict[str, Any], *keys: str) -> Any:
    return next((d[k] for k in keys if d.get(k) is not None), None)


def parse_ibba_segev_team_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team totals of both sides of a finished Segev game, with the final score and the team game stats.

    Args:
        raw: the dict ``ibba_segev_team_box`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        Two rows (home, away). Counts are the feed's team totals (players plus team-credited actions); the
        shooting attempts are derived from the made / MISSED pairs. The team game stats the feed renamed between
        2025-26 and 2026-27 land in one column each; ``fast_break_possessions`` (the N of the feed's "P of N"
        fast-break stat, its meaning unverified), ``defensive_stops`` and ``biggest_scoring_run`` exist only
        from 2026-27 and are null before. A body without a box score gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_team_box, parse_ibba_segev_team_box

            raw = ibba_segev_team_box(game_id=50, return_parsed=False)
            df = parse_ibba_segev_team_box(raw)
    """
    box, info = _box(raw), _box_info(raw)
    gid = _i(info.get("gameId"))
    rows: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        team = box.get(f"{side}Team")
        total = team.get("teamActionsTotal") if isinstance(team, dict) else None
        if not isinstance(total, dict):
            continue
        stats = _d(info.get(f"{side}GameStats"))
        fb = _s(_first(stats, "Pts_of_total_FBs", "Pts_Of_Total_FB_Possessions"))
        m = re.fullmatch(r"\d+ of (\d+)", fb or "")
        row: Dict[str, Any] = {
            "game_id": gid,
            "team_side": side,
            "team_id": _i(info.get(f"{side}TeamId")),
            "score": _i(info.get(f"{side}Score")),
            "timeouts": _i(info.get(f"{side}TimeOut")),
            "time_leading": _s(total.get("TimeLeading")),
            "biggest_lead": _i(total.get("BLead")),
            "times_tied": _i(total.get("TimesTied")),
            "lead_changes": _i(total.get("LeadChange")),
            **_counts(total),
            "fast_break_possessions": int(m.group(1)) if m else None,
            "defensive_stops": _i(stats.get("Defensive_Stops")),
            "biggest_scoring_run": _s(stats.get("Biggest_Scoring_Run")),
        }
        for name, keys in _GAME_STAT_KEYS.items():
            row[name] = _i(_first(stats, *keys))
        rows.append(row)
    return _frame(rows, _TEAM_SPEC, columns, return_as_pandas)


_PERIOD_SPEC: Spec = {
    "game_id": INT,
    "period": INT,
    "home_points": INT,
    "away_points": INT,
    "home_fouls": INT,
    "away_fouls": INT,
}


def parse_ibba_segev_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points and team fouls in each period of a finished Segev game.

    Args:
        raw: the dict ``ibba_segev_period_scores`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per period played (1 to the last period; the feed pads its lists with zeros to seven
        periods, which are dropped). The points are scored IN the period, not cumulative. A body without
        period lists gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_period_scores, parse_ibba_segev_period_scores

            raw = ibba_segev_period_scores(game_id=50, return_parsed=False)
            df = parse_ibba_segev_period_scores(raw)
    """
    info = _box_info(raw)
    gid = _i(info.get("gameId"))
    last = _i(info.get("currentQuarter")) or 0
    lists: Dict[str, List[Any]] = {k: v if isinstance(v := info.get(k), list) else [] for k in _PERIOD_LISTS}
    rows = [
        {
            "game_id": gid,
            "period": n,
            "home_points": _at(lists["homeQuarterScores"], n),
            "away_points": _at(lists["awayQuarterScores"], n),
            "home_fouls": _at(lists["homeQuarterFouls"], n),
            "away_fouls": _at(lists["awayQuarterFouls"], n),
        }
        for n in range(1, last + 1)
    ]
    return _frame(rows, _PERIOD_SPEC, columns, return_as_pandas)


_PERIOD_LISTS = ("homeQuarterScores", "awayQuarterScores", "homeQuarterFouls", "awayQuarterFouls")


def _at(seq: List[Any], n: int) -> Optional[int]:
    return _i(seq[n - 1]) if 0 < n <= len(seq) else None


# ------------------------------------------------------------------------------- play-by-play
_PBP_SPEC: Spec = {
    "game_id": INT,
    "action_id": INT,
    "parent_action_id": INT,
    "period": INT,
    "clock": S,
    "user_time_utc": S,
    "type": S,
    "subtype": S,
    "team_id": INT,
    "player_id": INT,
    "team_side": S,
    "jersey": S,
    "score_away": INT,
    "score_home": INT,
    "player_points": INT,
    "points": INT,
    "made": S,
    "x": F,
    "y": F,
    "fast_break": B,
    "second_chance": B,
    "points_from_turnover": B,
    "free_throw_number": INT,
    "free_throws_awarded": INT,
    "foul_kind": S,
    "fouled_on_jersey": S,
    "foul_free_throws": INT,
    "is_coach_foul": B,
    "player_in_jersey": S,
    "player_out_jersey": S,
}
_SCORE = re.compile(r"(\d+)-(\d+)")


def parse_ibba_segev_pbp(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """Play-by-play of a finished Segev game: every action, with shot coordinates and running scores.

    Args:
        raw: the dict ``ibba_segev_pbp`` returns with ``return_parsed=False``.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per action in game order (sorted by ``action_id``), clock ticks included: filter ``type`` for
        ``shot``, ``freeThrow``, ``rebound``, ``assist``, ``steal``, ``turnover``, ``block``, ``deflection``,
        ``foul``, ``foul-drawn``, ``timeout``, ``substitution``, ``quarter``, ``game`` or ``clock``. ``score_away``
        / ``score_home`` are filled on scoring actions only (the feed's ``score`` reads away-home). ``x`` / ``y``
        are shot coordinates in a half-court frame (x across 0-1470, basket near 735; y out from the baseline,
        rim near 112-140); which sideline is x = 0 is unverified and the metric scale is unverified. The
        ``jersey`` columns are jersey numbers, not ids. A body without actions gives a zero-row frame.
        Bad input is a zero-row frame. The fetch errors (``NoDataError``, ``AssetFetchError``) come
        from the wrapper itself.

    Example:
        Quick start::

            from sportsdataverse.basketball.ibba import ibba_segev_pbp, parse_ibba_segev_pbp

            raw = ibba_segev_pbp(game_id=50, return_parsed=False)
            df = parse_ibba_segev_pbp(raw)
    """
    g = _header(raw)
    gid = _i(g.get("id")) if _i(g.get("id")) is not None else _i(g.get("gameId"))
    rows: List[Dict[str, Any]] = []
    for a in _dicts(_result(raw).get("actions")):
        p = _d(a.get("parameters"))
        score = _SCORE.fullmatch(_s(a.get("score")) or "")
        rows.append(
            {
                "game_id": gid,
                "action_id": _i(a.get("id")),
                "parent_action_id": _nz(a.get("parentActionId")),
                "period": _i(a.get("quarter")),
                "clock": _s(a.get("quarterTime")),
                "user_time_utc": _s(a.get("userTime")),
                "type": _s(a.get("type")),
                "subtype": _s(p.get("type")),
                "team_id": _nz(a.get("teamId")),
                "player_id": _nz(a.get("playerId")),
                "team_side": _SIDES.get(_i(p.get("team")) or 0),
                "jersey": _s(p.get("player")),
                "score_away": int(score.group(1)) if score else None,
                "score_home": int(score.group(2)) if score else None,
                "player_points": _i(a.get("totalPlayerPoints")),
                "points": _i(p.get("points")),
                "made": _s(p.get("made")),
                "x": _f(p.get("coordX")),
                "y": _f(p.get("coordY")),
                "fast_break": _b(p.get("fastBreak")),
                "second_chance": _b(p.get("secondChancePoints")),
                "points_from_turnover": _b(p.get("pointsFromTurnover")),
                "free_throw_number": _i(p.get("freeThrowNumber")),
                "free_throws_awarded": _i(p.get("freeThrowsAwarded")),
                "foul_kind": _s(p.get("kind")),
                "fouled_on_jersey": _s(p.get("fouledOn")),
                "foul_free_throws": _i(p.get("freeThrows")) if a.get("type") == "foul" else None,
                "is_coach_foul": True if _i(p.get("isCoachFoul")) == 1 else None,
                "player_in_jersey": _s(p.get("playerIn")),
                "player_out_jersey": _s(p.get("playerOut")),
            }
        )
    rows.sort(key=lambda r: (r["action_id"] is None, r["action_id"] or 0))
    return _frame(rows, _PBP_SPEC, columns, return_as_pandas)


#: Each parser's column dtypes, for the generator and the tests.
SPECS: Dict[str, Spec] = {
    "game_info": _INFO_SPEC,
    "rosters": _ROSTER_SPEC,
    "player_box": _PLAYER_SPEC,
    "team_box": _TEAM_SPEC,
    "period_scores": _PERIOD_SPEC,
    "pbp": _PBP_SPEC,
}
