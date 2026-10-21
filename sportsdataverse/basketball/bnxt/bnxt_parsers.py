"""Parsers for the generated ``sportpress`` wrappers (BNXT League, Belgium and the Netherlands).

The data comes from the Sportpress CMS behind bnxtleague.com (``bnxt.sportpress.info/api/v1``). Each public
``parse_sportpress_*`` takes the dict a wrapper returns with ``return_parsed=False`` (the body, with the
request stamped under ``_request`` by ``bnxt_runtime``) and follows the package parser contract: a polars
frame by default, pandas with ``return_as_pandas=True``, snake_case columns, and a zero-row frame with the
documented columns and dtypes, never an exception, for an empty or unrecognised input.

**Dtypes** are pinned by column name, never sampled: the 3-row refs captures type a rate as an integer
whenever the sample happens to hold whole numbers, and the full bodies mix ``42`` and ``41.7`` in one
column. Counts in a game line are ``Int64``; every percentage (0-100) and every per-game mean is
``Float64``; flags are ``Boolean``; ``game_time`` is a naive ``Datetime`` (the source carries no offset;
the zone is assumed to be the league's local time and is unverified); ``birth_date`` is a ``Date``.

**Id key spaces.** Every id is ``Int64``. None joins another directly except where noted.

* ``season``: the END year (2026 = 2025-26). Only the season routes take it.
* ``competition_id``: ONE season of the league (1 = 2021-22, 2 = 2020-21, 3 = 2022-23, 12 = 2023-24,
  22 = 2024-25, 24 = 2025-26, 25 = 2026-27). ``sportpress_phases(season)`` maps a season to it.
* ``phase_id``: a stage of a competition (regular season, playoffs); new every season.
* ``competition_team_id``: a team in ONE competition, so a club has a new id every season (Antwerp = 3 in
  2021-22, 506 in 2025-26, 541 in 2026-27). ``uu_team_id`` is the stable club key. ``club_id`` is the
  COUNTRY (1 = Belgium, 2 = Netherlands), the provider's "club".
* ``roster_id``: one player on one competition team in one season (new for every team and season);
  ``player_id`` is the person, stable across seasons and teams.
* ``game_id``: unique across seasons. ``genius_id`` (``sportpress_game_slider`` only) is the Genius
  Sports LiveStats match id of organisation ``BNXT``: the join to the ``genius`` family.
* Names differ by language and sponsor ("Okapi Aalst Playoff"): never join on names.

**Sides.** ``team_side`` is ``home`` / ``away`` (the provider's ``is_home`` or ``side`` 1 / 2).

**Personal data.** The runtime drops contact and social fields; a player's ``birth_date`` is public on
the site and is kept. ``nationality_code`` is dirty (lower-case variants, ``USA/BEL`` pairs, the empty
string, ``HOL``): normalise before joining.

**Provider names are kept.** ``throw_*`` are field goals (two- plus three-pointers), ``free_throw_*``
free throws, ``defensive_foul`` fouls DRAWN, ``value`` the efficiency rating (PIR with fouls drawn),
``block_against`` shots of the player blocked by an opponent.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd
import polars as pl

Frame = Union[pl.DataFrame, pd.DataFrame]
#: column name -> (dtype, dotted path into the item the extractor built)
Cols = Dict[str, Tuple[Any, str]]

INT, F, S, B, D = pl.Int64, pl.Float64, pl.Utf8, pl.Boolean, pl.Date
DT = pl.Datetime("us")
_REQUEST = "_request"


# ----------------------------------------------------------------------------------------- casts
def _i(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    return int(v) if isinstance(v, float) and v == int(v) else None


def _f(v: Any) -> Optional[float]:
    return None if isinstance(v, bool) or not isinstance(v, (int, float)) else float(v)


def _s(v: Any) -> Optional[str]:
    return None if v is None or isinstance(v, (dict, list)) else str(v)


def _b(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _d(v: Any) -> Optional[date]:
    try:
        return datetime.strptime(v[:10], "%Y-%m-%d").date() if isinstance(v, str) else None
    except ValueError:
        return None


def _dt(v: Any) -> Optional[datetime]:
    try:
        return datetime.strptime(v.strip(), "%Y-%m-%d %H:%M:%S") if isinstance(v, str) else None
    except ValueError:
        return None


_CAST: Dict[Any, Callable[[Any], Any]] = {INT: _i, F: _f, S: _s, B: _b, D: _d, DT: _dt}


def _at(item: Any, path: str) -> Any:
    """The value at dotted ``path`` in nested dicts, or ``None`` when any step is missing or not a dict."""
    for key in path.split("."):
        if not isinstance(item, dict):
            return None
        item = item.get(key)
    return item


def _frame(items: Iterable[Dict[str, Any]], cols: Cols, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    """``items`` as a frame with exactly ``cols``' names and dtypes (a zero-row frame when there are none).

    ``columns`` is the wrapper's documented column list; a name ``cols`` lacks is added as a null string column.
    """
    rows = list(items)
    schema: Dict[str, Any] = {name: dtype for name, (dtype, _) in cols.items()}
    data: Dict[str, List[Any]] = {
        name: [_CAST[dtype](_at(r, path)) for r in rows] for name, (dtype, path) in cols.items()
    }
    for extra in columns or ():
        if extra not in schema:
            schema[extra] = S
            data[extra] = [None] * len(rows)
    df = pl.DataFrame(data, schema=schema)
    return df.to_pandas() if as_pandas else df


# ----------------------------------------------------------------------------------------- body access
def _ctx(raw: Any) -> Dict[str, Any]:
    req = raw.get(_REQUEST) if isinstance(raw, dict) else None
    return dict(req) if isinstance(req, dict) else {}


def _rows(raw: Any) -> List[Dict[str, Any]]:
    d = raw.get("data") if isinstance(raw, dict) else None
    return [r for r in d if isinstance(r, dict)] if isinstance(d, list) else []


def _obj(raw: Any) -> Dict[str, Any]:
    d = raw.get("data") if isinstance(raw, dict) else None
    return d if isinstance(d, dict) else {}


def _list(v: Any) -> List[Dict[str, Any]]:
    return [x for x in v if isinstance(x, dict)] if isinstance(v, list) else []


def _side_name(is_home: Any) -> Optional[str]:
    return {True: "home", False: "away"}.get(is_home) if isinstance(is_home, bool) else None


def _game_item(g: Dict[str, Any]) -> Dict[str, Any]:
    """A game object plus ``home`` / ``away`` = its competitors by ``side`` (1 / 2)."""
    by_side = {c.get("side"): c for c in _list(g.get("competitors"))}
    return {**g, "home": by_side.get(1) or {}, "away": by_side.get(2) or {}}


# ----------------------------------------------------------------------------------------- column blocks
_STATS = (
    "minute", "foul", "defensive_foul", "dunk_made", "dunk_all", "dunk_percent",
    "two_point_made", "two_point_all", "two_point_percent",
    "three_point_made", "three_point_all", "three_point_percent",
    "throw_made", "throw_all", "throw_percent",
    "free_throw_made", "free_throw_all", "free_throw_percent",
    "offensive_rebound", "defensive_rebound", "total_rebound",
    "block", "block_against", "turnover", "steal", "assist", "points", "value",
)  # fmt: skip


def _stat_cols(prefix: str = "", *, avg: bool = False, plusminus: bool = False) -> Cols:
    """The 28 (29 with ``plusminus``) statistic columns: counts ``Int64``, percentages and means ``Float64``."""
    names = _STATS + (("plusminus",) if plusminus else ())
    return {n: (F if avg or n.endswith("_percent") else INT, prefix + n) for n in names}


def _player_cols(prefix: str) -> Cols:
    return {
        "player_id": (INT, prefix + "id"),
        "first_name": (S, prefix + "first_name"),
        "last_name": (S, prefix + "last_name"),
        "nationality_code": (S, prefix + "nationality_code"),
        "birth_date": (D, prefix + "birthdate"),
    }


def _team_cols(prefix: str, *, club: bool = True) -> Cols:
    cols: Cols = {
        "competition_team_id": (INT, prefix + "id"),
        "team": (S, prefix + "name"),
        "team_short_name": (S, prefix + "short_name"),
    }
    if club:
        cols["club_id"] = (INT, prefix + "club.id")
        cols["club_name"] = (S, prefix + "club.name")
    return cols


def _game_cols(prefix: str = "") -> Cols:
    p = prefix
    cols: Cols = {
        "game_id": (INT, p + "id"),
        "game_time": (DT, p + "game_time"),
        "status": (S, p + "status"),
        "competition_id": (INT, p + "competition.id"),
        "season": (INT, p + "competition.season"),
        "phase_id": (INT, p + "phase.id"),
        "phase_name": (S, p + "phase.name"),
        "round_name": (S, p + "round.name"),
        "round_priority": (INT, p + "round.priority"),
        "period_name": (S, p + "period.name"),
        "arena": (S, p + "arena.name"),
    }
    for side in ("home", "away"):
        cols[f"{side}_competition_team_id"] = (INT, f"{p}{side}.competition_team.id")
        cols[f"{side}_team"] = (S, f"{p}{side}.competition_team.name")
        cols[f"{side}_team_short_name"] = (S, f"{p}{side}.competition_team.short_name")
        cols[f"{side}_score"] = (INT, f"{p}{side}.finalScore")
    return cols


def _roster_cols() -> Cols:
    return {
        "roster_id": (INT, "id"),
        "competition_team_id": (INT, "competition_team_id"),
        "jersey": (S, "jersey"),
        "weight": (INT, "weight"),
        "height": (INT, "height"),
        "active": (B, "active"),
        "position_id": (INT, "player_position.id"),
        "position": (S, "player_position.name"),
        **_player_cols("player."),
    }


def _ctx_cols(*names: str) -> Cols:
    return {n: (INT, n) for n in names}


_GAME_ROUTE = {"game_id": (INT, "game_id"), "competition_id": (INT, "competition_id")}
_SIDE = {"team_side": (S, "team_side")}

SPECS: Dict[str, Cols] = {
    "ui_strings": {"language": (S, "language"), "key": (S, "key"), "text": (S, "text")},
    "club_seasons": {"season": (INT, "season")},
    "referees": {
        "staff_id": (INT, "id"),
        "name": (S, "name"),
        "title_id": (INT, "title.id"),
        "title_name": (S, "title.name"),
    },
    "phases": {
        "season": (INT, "competition.season"),
        "competition_id": (INT, "competition.id"),
        "competition_name": (S, "competition.name"),
        "phase_id": (INT, "phase.id"),
        "phase_name": (S, "phase.name"),
        "standings_visible": (B, "phase.standings_visible"),
    },
    "competition_teams": {
        "competition_id": (INT, "competition_id"),
        "season": (INT, "competition.season"),
        "uu_team_id": (INT, "uu_team_id"),
        **_team_cols(""),
    },
    "competition_staff": {
        "competition_id": (INT, "team.competition_id"),
        "competition_team_id": (INT, "competition_team_id"),
        "staff_id": (INT, "staff.id"),
        "name": (S, "staff.name"),
        "title_id": (INT, "title.id"),
        "title_name": (S, "title.name"),
    },
    "standings": {
        **_ctx_cols("competition_id", "phase_id"),
        **_team_cols("team."),
        "position": (INT, "position"),
        "played": (INT, "played"),
        "win": (INT, "win"),
        "loss": (INT, "loss"),
        "even": (INT, "even"),
        "score": (INT, "score"),
        "against": (INT, "against"),
        "score_difference": (INT, "score_difference"),
        "percent": (F, "percent"),
        "point": (INT, "point"),
        "point_percent": (F, "point_percent"),
        "home_win": (INT, "win_loss.home.win"),
        "home_loss": (INT, "win_loss.home.loss"),
        "away_win": (INT, "win_loss.away.win"),
        "away_loss": (INT, "win_loss.away.loss"),
        "last5_win": (INT, "last5.win"),
        "last5_loss": (INT, "last5.loss"),
        "streak_is_win": (B, "streak.is_win"),
        "streak_count": (INT, "streak.count"),
        "group": (S, "group"),
    },
    "schedule": _game_cols(),
    "game_slider": {
        "game_id": (INT, "id"),
        "genius_id": (INT, "genius_id"),
        "game_time": (DT, "game_time"),
        "status": (S, "status"),
        "competition_id": (INT, "competition.id"),
        "season": (INT, "competition.season"),
        "phase_id": (INT, "phase.id"),
        "phase_name": (S, "phase.name"),
        **{k: v for k, v in _game_cols().items() if k.startswith(("home_", "away_"))},
    },
    "team_rankings": {
        **_ctx_cols("competition_id"),
        "category": (S, "category"),
        "rank": (INT, "rank"),
        **_team_cols("team.", club=False),
        "value": (F, "value"),
    },
    "player_rankings": {
        **_ctx_cols("competition_id"),
        "category": (S, "category"),
        "rank": (INT, "rank"),
        "roster_id": (INT, "roster_id"),
        **_player_cols("player."),
        **_team_cols("team.", club=False),
        "value": (F, "value"),
        "throw_made": (F, "throw_made"),
        "throw_attempted": (F, "throw_attempted"),
    },
    "season_roster": {"season": (INT, "season"), **_roster_cols()},
    "nationality_codes": {"season": (INT, "season"), "nationality_code": (S, "nationality_code")},
    "team_roster": _roster_cols(),
    "team_find": {
        "competition_id": (INT, "competition_id"),
        "season": (INT, "competition.season"),
        "competition_name": (S, "competition.name"),
        "uu_team_id": (INT, "uu_team_id"),
        **_team_cols(""),
        "average_score_for": (F, "statistics.average_score_for"),
        "average_score_against": (F, "statistics.average_score_against"),
        "field_goal_percent": (F, "statistics.field_goal_percent"),
    },
    "team_player_averages": {
        **_ctx_cols("competition_team_id"),
        "roster_id": (INT, "roster_id"),
        **_player_cols("player."),
        "played_games": (INT, "played_games"),
        "games_started": (INT, "starter"),
        **_stat_cols(avg=True),
    },
    "team_games": {
        **_ctx_cols("competition_team_id"),
        **_game_cols(),
        **_stat_cols("total.", plusminus=True),
    },
    "roster_find": {
        **_roster_cols(),
        "uu_team_id": (INT, "competition_team.uu_team_id"),
        "team": (S, "competition_team.name"),
        "team_short_name": (S, "competition_team.short_name"),
        "competition_id": (INT, "competition_team.competition_id"),
        "season": (INT, "competition_team.competition.season"),
    },
    "player_career": {
        **_ctx_cols("roster_id"),
        "competition_id": (INT, "competition.id"),
        "season": (INT, "competition.season"),
        "competition_name": (S, "competition.name"),
        **_team_cols("team."),
        "position": (S, "position.name"),
        "jersey": (S, "jersey"),
        "played_games": (INT, "played_games"),
        "games_started": (INT, "starter"),
        **_stat_cols(avg=True),
    },
    "player_games": {
        **_ctx_cols("roster_id"),
        **_game_cols("game."),
        "competition_team_id": (INT, "competition_team_id"),
        "starter": (B, "starter"),
        "jersey": (S, "jersey"),
        **_stat_cols(),
    },
    "game_find": {
        **_game_cols(),
        "last_game_id": (INT, "last_game.id"),
        "next_game_id": (INT, "next_game.id"),
    },
    "game_periods": {
        "game_id": (INT, "game_id"),
        **_SIDE,
        "competition_team_id": (INT, "competition_team_id"),
        "period": (INT, "period_id"),
        "score": (INT, "score"),
    },
    "game_overview": {
        "game_id": (INT, "game_id"),
        **_SIDE,
        **_team_cols("competitor.competition_team.", club=False),
        "score": (INT, "competitor.finalScore"),
        **_stat_cols("total.", plusminus=True),
    },
    "game_top_performers": {
        "game_id": (INT, "game_id"),
        **_SIDE,
        **_team_cols("competitor.competition_team.", club=False),
        **_player_cols("player.player."),
        "jersey": (S, "player.jersey"),
        "starter": (B, "player.starter"),
        **_stat_cols("player."),
    },
    "game_leaders": {
        "game_id": (INT, "game_id"),
        **_SIDE,
        **_team_cols("competitor.competition_team.", club=False),
        "stat": (S, "stat"),
        **_player_cols("leader.player."),
        "value": (F, "leader.value"),
    },
    "game_box": {
        **_GAME_ROUTE,
        **_SIDE,
        **_team_cols("team.", club=False),
        "roster_id": (INT, "roster_id"),
        **_player_cols("player."),
        "position_id": (INT, "position.id"),
        "position": (S, "position.name"),
        "starter": (B, "starter"),
        "jersey": (S, "jersey"),
        **_stat_cols(plusminus=True),
    },
    "game_team_box": {
        **_GAME_ROUTE,
        **_SIDE,
        **_team_cols("team.", club=False),
        **_stat_cols("total.", plusminus=True),
    },
    "game_film": {
        **_GAME_ROUTE,
        "order": (INT, "order"),
        "period": (INT, "period"),
        "minute": (INT, "minute"),
        "second": (INT, "second"),
        **_SIDE,
        **_team_cols("team.", club=False),
        "event_code": (INT, "event_code"),
        "event_subcode": (INT, "event_subcode"),
        "x": (INT, "x"),
        "y": (INT, "y"),
        "home_score": (INT, "current_home_score"),
        "away_score": (INT, "current_away_score"),
        **_player_cols("player."),
        "player2_id": (INT, "player2.id"),
        "player2_first_name": (S, "player2.first_name"),
        "player2_last_name": (S, "player2.last_name"),
    },
}


# ----------------------------------------------------------------------------------------- item extractors
Items = Callable[[Any], List[Dict[str, Any]]]


def _plain(raw: Any) -> List[Dict[str, Any]]:
    return [{**_ctx(raw), **r} for r in _rows(raw)]


def _it_ui_strings(raw: Any) -> List[Dict[str, Any]]:
    langs = raw.get("available_lang_array") if isinstance(raw, dict) else None
    return [
        {"language": lg.get("alpha2"), "key": k, "text": v}
        for lg in _list(langs)
        if isinstance(lg.get("resources"), dict)
        for k, v in lg["resources"].items()
    ]


def _it_phases(raw: Any) -> List[Dict[str, Any]]:
    return [{**r, "phase": p} for r in _rows(raw) for p in _list(r.get("phases"))]


def _it_competition_staff(raw: Any) -> List[Dict[str, Any]]:
    return [{"team": t, **s} for t in _rows(raw) for s in _list(t.get("staffs"))]


def _it_schedule(raw: Any) -> List[Dict[str, Any]]:
    return [_game_item(g) for g in _rows(raw)]


def _it_rankings(raw: Any) -> List[Dict[str, Any]]:
    ctx = _ctx(raw)
    cats = {k: v for k, v in raw.items() if k != _REQUEST} if isinstance(raw, dict) else {}
    return [
        {**ctx, "category": cat, "rank": n, **r}
        for cat, rows in cats.items()
        for n, r in enumerate(_list(rows), start=1)
    ]


def _it_nationality_codes(raw: Any) -> List[Dict[str, Any]]:
    codes = raw.get("data") if isinstance(raw, dict) else None
    return (
        [{**_ctx(raw), "nationality_code": c} for c in codes if isinstance(c, str)] if isinstance(codes, list) else []
    )


def _it_obj(raw: Any) -> List[Dict[str, Any]]:
    o = _obj(raw)
    return [{**_ctx(raw), **o}] if o else []


def _it_team_games(raw: Any) -> List[Dict[str, Any]]:
    return [{**_ctx(raw), **_game_item(g)} for g in _rows(raw)]


def _it_player_games(raw: Any) -> List[Dict[str, Any]]:
    return [{**_ctx(raw), **r, "game": _game_item(r["game"])} for r in _rows(raw) if isinstance(r.get("game"), dict)]


def _it_game_find(raw: Any) -> List[Dict[str, Any]]:
    g = _obj(raw)
    return [_game_item(g)] if g else []


def _it_game_periods(raw: Any) -> List[Dict[str, Any]]:
    g = _obj(raw)
    return [
        {
            "game_id": g.get("id"),
            "team_side": "home" if c.get("side") == 1 else "away" if c.get("side") == 2 else None,
            "competition_team_id": _at(c, "competition_team.id"),
            **ps,
        }
        for c in _list(g.get("competitors"))
        for ps in _list(c.get("period_scores"))
    ]


def _overview(raw: Any, key: str) -> List[Dict[str, Any]]:
    d = _obj(raw)
    ctx = _ctx(raw)
    return [{"game_id": ctx.get("game_id"), "team_side": _side_name(r.get("is_home")), **r} for r in _list(d.get(key))]


def _it_game_overview(raw: Any) -> List[Dict[str, Any]]:
    return _overview(raw, "team_totals")


def _it_game_top_performers(raw: Any) -> List[Dict[str, Any]]:
    return _overview(raw, "top_performer")


def _leaders(row: Dict[str, Any]) -> Dict[str, Any]:
    leaders = row.get("leaders")
    return leaders if isinstance(leaders, dict) else {}


def _it_game_leaders(raw: Any) -> List[Dict[str, Any]]:
    return [
        {**r, "stat": stat, "leader": leader}
        for r in _overview(raw, "game_leaders")
        for stat, leader in _leaders(r).items()
        if isinstance(leader, dict)
    ]


def _it_game_box(raw: Any) -> List[Dict[str, Any]]:
    ctx = _ctx(raw)
    return [
        {**ctx, "team_side": _side_name(t.get("is_home")), "team": t.get("team"), **p}
        for t in _rows(raw)
        for p in _list(t.get("players"))
    ]


def _it_game_team_box(raw: Any) -> List[Dict[str, Any]]:
    ctx = _ctx(raw)
    return [
        {**ctx, "team_side": _side_name(t.get("is_home")), **t} for t in _rows(raw) if isinstance(t.get("total"), dict)
    ]


def _it_game_film(raw: Any) -> List[Dict[str, Any]]:
    ctx = _ctx(raw)
    return [{**ctx, "team_side": _side_name(e.get("is_home")), **e} for e in _rows(raw)]


ITEMS: Dict[str, Items] = {
    "ui_strings": _it_ui_strings,
    "club_seasons": _plain,
    "referees": _plain,
    "phases": _it_phases,
    "competition_teams": _plain,
    "competition_staff": _it_competition_staff,
    "standings": _plain,
    "schedule": _it_schedule,
    "game_slider": _it_schedule,
    "team_rankings": _it_rankings,
    "player_rankings": _it_rankings,
    "season_roster": _plain,
    "nationality_codes": _it_nationality_codes,
    "team_roster": _plain,
    "team_find": _it_obj,
    "team_player_averages": _plain,
    "team_games": _it_team_games,
    "roster_find": _it_obj,
    "player_career": _plain,
    "player_games": _it_player_games,
    "game_find": _it_game_find,
    "game_periods": _it_game_periods,
    "game_overview": _it_game_overview,
    "game_top_performers": _it_game_top_performers,
    "game_leaders": _it_game_leaders,
    "game_box": _it_game_box,
    "game_team_box": _it_game_team_box,
    "game_film": _it_game_film,
}


def _parse(short: str, raw: Any, columns: Optional[Sequence[str]], as_pandas: bool) -> Frame:
    try:
        items = ITEMS[short](raw)
    except (AttributeError, KeyError, TypeError):  # a body of the wrong shape parses to the typed empty frame
        items = []
    return _frame(items, SPECS[short], columns, as_pandas)


def parse_sportpress_ui_strings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """UI string tables of the BNXT site, one row per language and key.

    Args:
        raw: the dict ``sportpress_ui_strings(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per language (``en``, ``fr``, ``nl``) and UI key (302 keys each). Only 11 keys label
        data columns (the statistics labels); the rest are menus and buttons.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_ui_strings

            ui = sportpress_ui_strings()

        Pipeline next step::

            import polars as pl

            ui.filter(pl.col("key").str.starts_with("statistics.individual"))
    """
    return _parse("ui_strings", raw, columns, return_as_pandas)


def parse_sportpress_club_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Seasons (END years) in which the given clubs (countries) have teams.

    Args:
        raw: the dict ``sportpress_club_seasons(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per season, END year (2021 = 2020-21).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_club_seasons

            seasons = sportpress_club_seasons()

        Pipeline next step::

            import polars as pl

            seasons["season"].to_list()
    """
    return _parse("club_seasons", raw, columns, return_as_pandas)


def parse_sportpress_referees(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Referees of the league (staff with title 2).

    Args:
        raw: the dict ``sportpress_referees(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per referee: ``staff_id``, ``name``, ``title_id``, ``title_name``. Photos and any
        contact data are not kept.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_referees

            refs = sportpress_referees()

        Pipeline next step::

            import polars as pl

            refs.sort("name")
    """
    return _parse("referees", raw, columns, return_as_pandas)


def parse_sportpress_phases(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The competition of a season with its phases (regular season, playoffs).

    Args:
        raw: the dict ``sportpress_phases(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per phase of the season's competition. ``competition_id`` is the key of every per-
        competition route; ``standings_visible`` is false for playoff phases (their standings answer
        empty).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_phases

            phases = sportpress_phases(season=2026)

        Pipeline next step::

            import polars as pl

            phases.filter(pl.col("standings_visible") == True)
    """
    return _parse("phases", raw, columns, return_as_pandas)


def parse_sportpress_competition_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every competition team of one competition.

    Args:
        raw: the dict ``sportpress_competition_teams(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per team: ``competition_team_id`` (new every season), ``uu_team_id`` (the stable club
        key), name, short name and country (``club_id``).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_competition_teams

            teams = sportpress_competition_teams(competition_id=24)

        Pipeline next step::

            import polars as pl

            teams.select("competition_team_id", "team", "uu_team_id")
    """
    return _parse("competition_teams", raw, columns, return_as_pandas)


def parse_sportpress_competition_staff(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Coaching staff of every competition team of one competition.

    Args:
        raw: the dict ``sportpress_competition_staff(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per team and staff member (head coach, assistant coach); the staff list of
        ``sportpress_competition_teams``.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_competition_staff

            staff = sportpress_competition_staff(competition_id=24)

        Pipeline next step::

            import polars as pl

            staff.filter(pl.col("title_name") == "Head coach")
    """
    return _parse("competition_staff", raw, columns, return_as_pandas)


def parse_sportpress_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Standings of one phase of a competition.

    Args:
        raw: the dict ``sportpress_standings(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per team: record, points for and against, home and away split, last five and the current
        streak. Empty (``NoDataError``) for 2022-23 and 2023-24 and for phases without standings.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_standings

            table = sportpress_standings(competition_id=24, phase_id=169)

        Pipeline next step::

            import polars as pl

            table.sort("position")
    """
    return _parse("standings", raw, columns, return_as_pandas)


def parse_sportpress_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every game of a season for Belgium and the Netherlands.

    Args:
        raw: the dict ``sportpress_schedule(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per game with the home and away team and score. ``status`` is ``finished`` or ``ready``
        (upcoming). ``game_time`` is a naive local datetime. Carries no ``genius_id`` (see
        ``sportpress_game_slider``).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_schedule

            games = sportpress_schedule(season=2026)

        Pipeline next step::

            import polars as pl

            games.filter(pl.col("status") == "finished")
    """
    return _parse("schedule", raw, columns, return_as_pandas)


def parse_sportpress_game_slider(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The latest 14 finished games of a competition plus every upcoming game, with the Genius id.

    Args:
        raw: the dict ``sportpress_game_slider(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per game. ``genius_id`` is the Genius Sports LiveStats match id (organisation ``BNXT``),
        the join to the ``genius`` family; it is null for the 2020-21 competition. A window, not a list:
        use ``sportpress_schedule`` for a complete season.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_slider

            games = sportpress_game_slider(competition_id=25)

        Pipeline next step::

            import polars as pl

            games.filter(pl.col("genius_id").is_not_null())
    """
    return _parse("game_slider", raw, columns, return_as_pandas)


def parse_sportpress_team_rankings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team per-game leaders in nine categories.

    Args:
        raw: the dict ``sportpress_team_rankings(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per team and category (``efficiency``, ``points``, ``total_rebound``, ``block``,
        ``assist``, ``steal``, ``two_point_percent``, ``three_point_percent``, ``free_throw_percent``),
        ``rank`` 1 = best.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_team_rankings

            ranks = sportpress_team_rankings(competition_id=24)

        Pipeline next step::

            import polars as pl

            ranks.filter(pl.col("category") == "points")
    """
    return _parse("team_rankings", raw, columns, return_as_pandas)


def parse_sportpress_player_rankings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Qualified-player per-game leaders in nine categories.

    Args:
        raw: the dict ``sportpress_player_rankings(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per player and category, ``rank`` 1 = best. Only players who played more than half of
        their team's games appear. ``throw_made`` / ``throw_attempted`` (field goals per game) are
        filled in the three percentage categories only.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_player_rankings

            ranks = sportpress_player_rankings(competition_id=24)

        Pipeline next step::

            import polars as pl

            ranks.filter(pl.col("category") == "points").head(10)
    """
    return _parse("player_rankings", raw, columns, return_as_pandas)


def parse_sportpress_season_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every roster entry of a season.

    Args:
        raw: the dict ``sportpress_season_roster(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per roster entry (a player on one team in the season). Current entries only: a player
        who left mid-season is absent. ``nationality_code`` is dirty.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_season_roster

            roster = sportpress_season_roster(season=2026)

        Pipeline next step::

            import polars as pl

            roster.group_by("competition_team_id").len()
    """
    return _parse("season_roster", raw, columns, return_as_pandas)


def parse_sportpress_nationality_codes(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Distinct player nationality codes of a season.

    Args:
        raw: the dict ``sportpress_nationality_codes(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per code, in first-seen order, including the empty string and lower-case variants.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_nationality_codes

            codes = sportpress_nationality_codes(season=2026)

        Pipeline next step::

            import polars as pl

            codes["nationality_code"].unique()
    """
    return _parse("nationality_codes", raw, columns, return_as_pandas)


def parse_sportpress_team_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Roster of one competition team.

    Args:
        raw: the dict ``sportpress_team_roster(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per roster entry of the team (current entries only).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_team_roster

            roster = sportpress_team_roster(competition_team_id=541)

        Pipeline next step::

            import polars as pl

            roster.select("roster_id", "first_name", "last_name", "jersey")
    """
    return _parse("team_roster", raw, columns, return_as_pandas)


def parse_sportpress_team_find(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One competition team with its season averages.

    Args:
        raw: the dict ``sportpress_team_find(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row. ``average_score_for``, ``average_score_against`` and ``field_goal_percent`` are null
        for a team with no statistics.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_team_find

            team = sportpress_team_find(competition_team_id=541)

        Pipeline next step::

            import polars as pl

            team.row(0, named=True)
    """
    return _parse("team_find", raw, columns, return_as_pandas)


def parse_sportpress_team_player_averages(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-game averages of each player of a competition team.

    Args:
        raw: the dict ``sportpress_team_player_averages(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per roster entry. Statistics are per-game MEANS (Float64); ``games_started`` is a count.
        Also lists departed roster entries, so it is not a subset of the team roster.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_team_player_averages

            avgs = sportpress_team_player_averages(competition_team_id=541)

        Pipeline next step::

            import polars as pl

            avgs.sort("points", descending=True)
    """
    return _parse("team_player_averages", raw, columns, return_as_pandas)


def parse_sportpress_team_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every finished game of a competition team with that team's totals.

    Args:
        raw: the dict ``sportpress_team_games(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per finished game with both sides and the team's own totals in the statistic columns
        (counts, ``Int64``).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_team_games

            games = sportpress_team_games(competition_team_id=541)

        Pipeline next step::

            import polars as pl

            games.select("game_id", "points")
    """
    return _parse("team_games", raw, columns, return_as_pandas)


def parse_sportpress_roster_find(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One roster entry with its player and team.

    Args:
        raw: the dict ``sportpress_roster_find(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row: the roster entry, the player and the team and season. The player's latest-game line is
        in the raw body (``return_parsed=False``).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_roster_find

            entry = sportpress_roster_find(roster_id=11193)

        Pipeline next step::

            import polars as pl

            entry.row(0, named=True)
    """
    return _parse("roster_find", raw, columns, return_as_pandas)


def parse_sportpress_player_career(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Season-by-season per-game averages of the player of a roster entry.

    Args:
        raw: the dict ``sportpress_player_career(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per season and team of the person, as per-game means (Float64).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_player_career

            career = sportpress_player_career(roster_id=11193)

        Pipeline next step::

            import polars as pl

            career.select("season", "team", "points")
    """
    return _parse("player_career", raw, columns, return_as_pandas)


def parse_sportpress_player_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Game-by-game box lines of a roster entry.

    Args:
        raw: the dict ``sportpress_player_games(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per game the roster entry has a line in (including games with 0 minutes), with the
        game's teams and scores and the player's line (counts, ``Int64``).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_player_games

            lines = sportpress_player_games(roster_id=11193)

        Pipeline next step::

            import polars as pl

            lines.select("game_id", "minute", "points")
    """
    return _parse("player_games", raw, columns, return_as_pandas)


def parse_sportpress_game_find(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One game: teams, scores, phase, venue and neighbouring games.

    Args:
        raw: the dict ``sportpress_game_find(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row. ``period_name`` is the period the game ended in (``Extra2`` = second overtime).
        ``last_game_id`` is the latest finished meeting of the same two teams in the competition, which
        can be LATER than this game. Referees are in the raw body.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_find

            game = sportpress_game_find(game_id=11295)

        Pipeline next step::

            import polars as pl

            game.row(0, named=True)
    """
    return _parse("game_find", raw, columns, return_as_pandas)


def parse_sportpress_game_periods(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points each side scored in each period of a game.

    Args:
        raw: the dict ``sportpress_game_periods(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per side and period: ``period`` 1-4 are the quarters, 5 and up overtimes. The sum of
        ``score`` per side is the final score.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_periods

            periods = sportpress_game_periods(game_id=11295)

        Pipeline next step::

            import polars as pl

            periods.pivot(on="team_side", index="period", values="score")
    """
    return _parse("game_periods", raw, columns, return_as_pandas)


def parse_sportpress_game_overview(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team totals of both sides of a finished game.

    Args:
        raw: the dict ``sportpress_game_overview(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per side with the final score and the team's statistic totals. A finished game with no
        statistics (some 2022-23 games) raises ``NoDataError``.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_overview

            totals = sportpress_game_overview(game_id=11295)

        Pipeline next step::

            import polars as pl

            totals.select("team_side", "points")
    """
    return _parse("game_overview", raw, columns, return_as_pandas)


def parse_sportpress_game_top_performers(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The top performer of each side of a finished game.

    Args:
        raw: the dict ``sportpress_game_top_performers(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per side: the player and his full line.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_top_performers

            top = sportpress_game_top_performers(game_id=11295)

        Pipeline next step::

            import polars as pl

            top.select("team_side", "first_name", "last_name", "points")
    """
    return _parse("game_top_performers", raw, columns, return_as_pandas)


def parse_sportpress_game_leaders(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """The game leader of each side in every statistic.

    Args:
        raw: the dict ``sportpress_game_leaders(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per side and statistic (``stat`` is the provider's column name, e.g. ``points``,
        ``total_rebound``): the leading player and the ``value``.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_leaders

            leaders = sportpress_game_leaders(game_id=11295)

        Pipeline next step::

            import polars as pl

            leaders.filter(pl.col("stat") == "points")
    """
    return _parse("game_leaders", raw, columns, return_as_pandas)


def parse_sportpress_game_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-player box score of a finished game.

    Args:
        raw: the dict ``sportpress_game_box(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per player line and team. Present for 2021-22, 2024-25, 2025-26 and 2026-27; 2022-23 and
        2023-24 games answer empty (``NoDataError``). 15 of 14,142 sampled lines have a null player. A
        team's box total can differ from the schedule final score on about 18 of 1,246 sampled team
        boxes (documented, not refused).
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_box

            box = sportpress_game_box(competition_id=25, game_id=11295)

        Pipeline next step::

            import polars as pl

            box.filter(pl.col("team_side") == "home")
    """
    return _parse("game_box", raw, columns, return_as_pandas)


def parse_sportpress_game_team_box(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team totals of both sides from the box score of a finished game.

    Args:
        raw: the dict ``sportpress_game_team_box(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per side. ``points`` can differ from the schedule final score on about 18 of 1,246
        sampled team boxes (mostly 2024-25, by 2-21 points); the frame reports the box total as served.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_team_box

            totals = sportpress_game_team_box(competition_id=25, game_id=11295)

        Pipeline next step::

            import polars as pl

            totals.select("team_side", "points")
    """
    return _parse("game_team_box", raw, columns, return_as_pandas)


def parse_sportpress_game_film(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Play-by-play ('film') of a finished game with court x/y.

    Args:
        raw: the dict ``sportpress_game_film(..., return_parsed=False)`` returns (the body with ``_request``).
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns to guarantee (added as null strings when missing).

    Returns:
        One row per event, in order. Populated for 2021-22 and 2026-27 only; 2022-23 to 2025-26 answer
        empty (``NoDataError``): use Genius play-by-play (``genius_id``). ``minute`` / ``second`` count
        ELAPSED time in the period. ``event_code`` is the provider's code: 1000 made shot, 1001 missed
        shot (``event_subcode`` 1, 2, 5 two-point, 3 three-point, 4 free throw), 1002 defensive rebound,
        1003 offensive rebound, 1004 steal, 1005 turnover, 1006 foul drawn, 1007 foul committed, 1008
        assist, 1009 block, 1011 substitution (the only event with a second player), 1101 period marker;
        the 2xxx codes are team events (meaning unverified). x / y are court coordinates in an
        unverified system.
        A malformed or empty body parses to a zero-row frame with the documented columns.

    Example:
        Quick start::

            from sportsdataverse.basketball.bnxt import sportpress_game_film

            pbp = sportpress_game_film(competition_id=25, game_id=11295)

        Pipeline next step::

            import polars as pl

            pbp.filter(pl.col("event_code") == 1000)
    """
    return _parse("game_film", raw, columns, return_as_pandas)


__all__ = [
    "parse_sportpress_club_seasons",
    "parse_sportpress_competition_staff",
    "parse_sportpress_competition_teams",
    "parse_sportpress_game_box",
    "parse_sportpress_game_film",
    "parse_sportpress_game_find",
    "parse_sportpress_game_leaders",
    "parse_sportpress_game_overview",
    "parse_sportpress_game_periods",
    "parse_sportpress_game_slider",
    "parse_sportpress_game_team_box",
    "parse_sportpress_game_top_performers",
    "parse_sportpress_nationality_codes",
    "parse_sportpress_phases",
    "parse_sportpress_player_career",
    "parse_sportpress_player_games",
    "parse_sportpress_player_rankings",
    "parse_sportpress_referees",
    "parse_sportpress_roster_find",
    "parse_sportpress_schedule",
    "parse_sportpress_season_roster",
    "parse_sportpress_standings",
    "parse_sportpress_team_find",
    "parse_sportpress_team_games",
    "parse_sportpress_team_player_averages",
    "parse_sportpress_team_rankings",
    "parse_sportpress_team_roster",
    "parse_sportpress_ui_strings",
]
