"""Parsers for the generated ``espn_fantasy`` wrappers (ESPN Fantasy read API, public data only).

ESPN runs five fantasy games on one platform: ``ffl`` (football), ``fba`` (men's basketball),
``flb`` (baseball), ``fhl`` (hockey) and ``wfba`` (women's basketball). Their bodies share one
shape, so one parser per table serves every game.

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_cased columns, and a **typed** zero-row frame (the documented
schema, :data:`SCHEMAS`) for an empty or malformed body, so frames from several calls stack.

**Ids.** Every id is ``Int64``: ``league_id``, ``team_id`` (unique only inside one
league), ``player_id`` (ESPN's athlete id, the same one ESPN's site and core APIs use),
``pro_team_id`` (ESPN's team id for the pro league; ``0`` is the free-agent pool),
``pro_game_id`` (ESPN's event id) and the per-game enums (``stat_id``, ``lineup_slot_id``,
``default_position_id``). ``transaction_id`` is a UUID and stays ``String``. ``season_id`` is ESPN's
season id: the start year for football (2026 = fall 2026), the end year for basketball and hockey
(2026-27 = 2027).

**Members are never emitted.** League bodies name each team's owners by their ESPN account id;
no parser returns an owner, member or account column.

**Times** arrive as epoch milliseconds (news as ISO 8601 strings) and become UTC ``Datetime``.
List cells (``eligible_slots``, ``scoring_item_stat_ids``, ...) and id-keyed maps
(``points_overrides``) are JSON-encoded strings.
"""

from __future__ import annotations

import json
import re
import warnings
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
import polars as pl

from sportsdataverse._codegen_runtime import _pandas_ids
from sportsdataverse.errors import SchemaDriftWarning

Frame = Union[pl.DataFrame, pd.DataFrame]

__all__ = [
    "SCHEMAS",
    "parse_espn_fantasy_games",
    "parse_espn_fantasy_league_boxscores",
    "parse_espn_fantasy_league_directory",
    "parse_espn_fantasy_league_draft",
    "parse_espn_fantasy_league_matchups",
    "parse_espn_fantasy_league_rosters",
    "parse_espn_fantasy_league_teams",
    "parse_espn_fantasy_league_transactions",
    "parse_espn_fantasy_player_news",
    "parse_espn_fantasy_player_pool",
    "parse_espn_fantasy_players",
    "parse_espn_fantasy_pro_schedule",
    "parse_espn_fantasy_pro_teams",
    "parse_espn_fantasy_scoring",
    "parse_espn_fantasy_seasons",
]

N, F, S, B = pl.Int64, pl.Float64, pl.String, pl.Boolean
DT = pl.Datetime("ms", time_zone="UTC")

_LEAGUE = {"league_id": N, "season_id": N}
_PLAYER = {"player_id": N, "full_name": S, "pro_team_id": N, "default_position_id": N}

#: The documented columns of every parser, keyed by the parser's table name.
SCHEMAS: Dict[str, Dict[str, Any]] = {
    "games": {
        "game_id": N,
        "game_abbrev": S,
        "game_name": S,
        "pro_sport_abbrev": S,
        "pro_sport_name": S,
        "active": B,
        "display": B,
        "current_season_id": N,
        "current_scoring_period_id": N,
        "season_start_date": DT,
        "season_end_date": DT,
    },
    "seasons": {
        "game_id": N,
        "season_id": N,
        "season_abbrev": S,
        "season_name": S,
        "active": B,
        "display": B,
        "current_scoring_period_id": N,
        "start_date": DT,
        "end_date": DT,
    },
    "pro_teams": {
        "pro_team_id": N,
        "pro_team_abbrev": S,
        "pro_team_location": S,
        "pro_team_name": S,
        "bye_week": N,
        "universe_id": N,
    },
    "pro_schedule": {
        "pro_game_id": N,
        "scoring_period_id": N,
        "game_date": DT,
        "home_pro_team_id": N,
        "away_pro_team_id": N,
        "start_time_tbd": B,
        "stats_official": B,
        "valid_for_locking": B,
    },
    "players": {
        "player_id": N,
        "full_name": S,
        "first_name": S,
        "last_name": S,
        "pro_team_id": N,
        "default_position_id": N,
        "eligible_slots": S,
        "droppable": B,
        "percent_owned": F,
        "last_news_date": DT,
        "universe_id": N,
    },
    "league_directory": {
        "league_id": N,
        "league_name": S,
        "league_sub_type": S,
        "league_default_id": N,
        "league_size": N,
        "teams_joined": N,
        "full": B,
        "public": B,
        "standard_league": B,
        "customizable": B,
        "draft_type": S,
        "draft_date": DT,
        "draft_available_date": DT,
        "draft_in_progress": B,
        "scoring_type": S,
        "experience_type": S,
        "rank_type": S,
        "restriction_type": S,
        "roster_locktime_type": S,
        "scoring_item_stat_ids": S,
        "universe_ids": S,
        "season_id": N,
        "game_id": N,
    },
    "scoring": {
        **_LEAGUE,
        "game_id": N,
        "scoring_type": S,
        "stat_id": N,
        "points": F,
        "is_reverse_item": B,
        "points_overrides": S,
    },
    "player_news": {
        "news_id": N,
        "now_id": S,
        "player_id": N,
        "type": S,
        "headline": S,
        "description": S,
        "story": S,
        "published": DT,
        "last_modified": DT,
        "premium": B,
    },
    "league_teams": {
        **_LEAGUE,
        "team_id": N,
        "team_name": S,
        "team_abbrev": S,
        "division_id": N,
        "wins": N,
        "losses": N,
        "ties": N,
        "win_percentage": F,
        "points_for": F,
        "points_against": F,
        "games_back": F,
        "streak_type": S,
        "streak_length": N,
        "playoff_seed": N,
        "points": F,
        "current_projected_rank": N,
        "draft_day_projected_rank": N,
        "rank_calculated_final": N,
        "waiver_rank": N,
        "acquisitions": N,
        "drops": N,
        "trades": N,
        "move_to_active": N,
        "move_to_ir": N,
        "acquisition_budget_spent": N,
    },
    "league_rosters": {
        **_LEAGUE,
        "team_id": N,
        **_PLAYER,
        "lineup_slot_id": N,
        "acquisition_type": S,
        "acquisition_date": DT,
        "injury_status": S,
        "player_status": S,
        "applied_stat_total": F,
        "percent_owned": F,
        "percent_started": F,
    },
    "league_matchups": {
        **_LEAGUE,
        "matchup_id": N,
        "matchup_period_id": N,
        "playoff_tier_type": S,
        "winner": S,
        "home_team_id": N,
        "away_team_id": N,
        "home_total_points": F,
        "away_total_points": F,
    },
    "league_boxscores": {
        **_LEAGUE,
        "scoring_period_id": N,
        "matchup_id": N,
        "matchup_period_id": N,
        "side": S,
        "team_id": N,
        **_PLAYER,
        "lineup_slot_id": N,
        "applied_stat_total": F,
    },
    "league_draft": {
        **_LEAGUE,
        "pick_id": N,
        "overall_pick_number": N,
        "round_id": N,
        "round_pick_number": N,
        "team_id": N,
        "player_id": N,
        "bid_amount": N,
        "keeper": B,
        "reserved_for_keeper": B,
        "auto_draft_type_id": N,
        "lineup_slot_id": N,
        "nominating_team_id": N,
    },
    "league_transactions": {
        **_LEAGUE,
        "transaction_id": S,
        "transaction_type": S,
        "status": S,
        "execution_type": S,
        "scoring_period_id": N,
        "proposed_date": DT,
        "team_id": N,
        "bid_amount": N,
        "is_pending": B,
        "item_type": S,
        "player_id": N,
        "from_team_id": N,
        "to_team_id": N,
        "from_lineup_slot_id": N,
        "to_lineup_slot_id": N,
    },
    "player_pool": {
        **_PLAYER,
        "first_name": S,
        "last_name": S,
        "eligible_slots": S,
        "jersey": S,
        "injured": B,
        "injury_status": S,
        "status": S,
        "on_team_id": N,
        "keeper_value": N,
        "draft_auction_value": N,
        "percent_owned": F,
        "percent_started": F,
        "percent_change": F,
        "average_draft_position": F,
        "auction_value_average": F,
        "last_news_date": DT,
    },
}


# -- value coercion: a value of the wrong type becomes null, never an exception ---------------------


def _d(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _l(v: Any) -> List[Any]:
    return v if isinstance(v, list) else []


_INT_TEXT = re.compile(r"-?[0-9]+", re.ASCII)
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def _rows(v: Any) -> List[Dict[str, Any]]:
    """The object members of a list; anything else (a scalar row, a non-list) contributes no row."""
    return [x for x in _l(v) if isinstance(x, dict)]


def _int(v: Any) -> Optional[int]:
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v if _INT64_MIN <= v <= _INT64_MAX else None
    if isinstance(v, float) and v.is_integer() and _INT64_MIN <= v <= _INT64_MAX:
        return int(v)
    if isinstance(v, str) and _INT_TEXT.fullmatch(v.strip()):
        v = int(v.strip())
    else:
        return None
    return v if _INT64_MIN <= v <= _INT64_MAX else None


def _num(v: Any) -> Optional[float]:
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _str(v: Any) -> Optional[str]:
    return v if isinstance(v, str) else None


def _bool(v: Any) -> Optional[bool]:
    return v if isinstance(v, bool) else None


def _json(v: Any) -> Optional[str]:
    return json.dumps(v, separators=(",", ":"), sort_keys=True) if isinstance(v, (list, dict)) else None


def _ms(v: Any) -> Optional[datetime]:
    """Epoch milliseconds -> an aware UTC datetime (``0`` and out-of-range values are null)."""
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        return None
    try:
        return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _iso(v: Any) -> Optional[datetime]:
    if not isinstance(v, str):
        return None
    try:
        out = datetime.fromisoformat(v.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return out.astimezone(timezone.utc) if out.tzinfo else out.replace(tzinfo=timezone.utc)


_COERCE = {N: _int, F: _num, S: _str, B: _bool}


#: The column a row must carry to count: a list member without it (``{}``, a stub) is not a row.
_ROW_KEY = {
    "games": "game_id",
    "seasons": "season_id",
    "pro_teams": "pro_team_id",
    "pro_schedule": "pro_game_id",
    "players": "player_id",
    "league_directory": "league_id",
    "scoring": "stat_id",
    "player_news": "news_id",
    "league_teams": "team_id",
    "league_rosters": "player_id",
    "league_matchups": "matchup_id",
    "league_boxscores": "player_id",
    "league_draft": "pick_id",
    "league_transactions": "transaction_id",
    "player_pool": "player_id",
}


def _frame(name: str, rows: Iterable[Dict[str, Any]], return_as_pandas: bool) -> Frame:
    """Rows -> exactly the documented columns of table ``name``, typed (a zero-row frame when empty).

    A row without its :data:`_ROW_KEY` is dropped. A value of the wrong type becomes null, and the
    column is named in a :class:`~sportsdataverse.errors.SchemaDriftWarning` (the body changed shape).
    """
    schema = SCHEMAS[name]
    key = _ROW_KEY[name]
    coerced = []
    drifted: List[str] = []
    for r in rows:
        out = {}
        for col, dtype in schema.items():
            v = r.get(col)
            coerce = _COERCE.get(dtype)
            out[col] = coerce(v) if coerce is not None else v
            if out[col] is None and v is not None and col not in drifted:
                drifted.append(col)
        if out[key] is not None:
            coerced.append(out)
    if drifted and coerced:
        warnings.warn(
            f"espn_fantasy {name}: values of an unexpected type were set to null in {drifted}",
            SchemaDriftWarning,
            stacklevel=3,
        )
    data = {col: [r[col] for r in coerced] for col in schema}
    df = pl.DataFrame(data, schema=schema)
    return _pandas_ids(df) if return_as_pandas else df


def _league_keys(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {"league_id": raw.get("id"), "season_id": raw.get("seasonId")}


def _player_cols(player: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "player_id": player.get("id"),
        "full_name": player.get("fullName"),
        "first_name": player.get("firstName"),
        "last_name": player.get("lastName"),
        "pro_team_id": player.get("proTeamId"),
        "default_position_id": player.get("defaultPositionId"),
        "eligible_slots": _json(player.get("eligibleSlots")),
        "jersey": player.get("jersey"),
        "injured": player.get("injured"),
        "injury_status": player.get("injuryStatus"),
        "last_news_date": _ms(player.get("lastNewsDate")),
    }


# -- league-agnostic tables ----------------------------------------------------------------------


def parse_espn_fantasy_games(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games`` into one row per fantasy game.

    Args:
        raw: The decoded ``/games`` body, a list of game objects.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``games`` table of :data:`SCHEMAS`: the game id and abbreviation, its pro sport, and the
        current season and scoring period. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_games

            espn_fantasy_games()

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_games` -- the wrapper that fetches this body and parses it
    """
    rows = []
    for g in _rows(raw):
        cur = _d(g.get("currentSeason"))
        rows.append(
            {
                "game_id": g.get("id"),
                "game_abbrev": g.get("abbrev"),
                "game_name": g.get("name"),
                "pro_sport_abbrev": g.get("proSportAbbrev"),
                "pro_sport_name": g.get("proSportName"),
                "active": g.get("active"),
                "display": g.get("display"),
                "current_season_id": g.get("currentSeasonId"),
                "current_scoring_period_id": _d(cur.get("currentScoringPeriod")).get("id"),
                "season_start_date": _ms(cur.get("startDate")),
                "season_end_date": _ms(cur.get("endDate")),
            }
        )
    return _frame("games", rows, return_as_pandas)


def parse_espn_fantasy_seasons(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ``/games/{game}/seasons`` into one row per season.

    Args:
        raw: The decoded body, a list of season objects (newest first).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``seasons`` table of :data:`SCHEMAS`. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_seasons

            espn_fantasy_seasons(game="ffl")

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_seasons` -- the wrapper that fetches this body and parses it
    """
    rows = []
    for s in _rows(raw):
        rows.append(
            {
                "game_id": s.get("gameId"),
                "season_id": s.get("id"),
                "season_abbrev": s.get("abbrev"),
                "season_name": s.get("name"),
                "active": s.get("active"),
                "display": s.get("display"),
                "current_scoring_period_id": _d(s.get("currentScoringPeriod")).get("id"),
                "start_date": _ms(s.get("startDate")),
                "end_date": _ms(s.get("endDate")),
            }
        )
    return _frame("seasons", rows, return_as_pandas)


def _pro_teams(raw: Any) -> List[Dict[str, Any]]:
    return _rows(_d(_d(raw).get("settings")).get("proTeams"))


def parse_espn_fantasy_pro_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a ``view=proTeamSchedules_wl`` season body into one row per pro team.

    Args:
        raw: The decoded season body (``settings.proTeams[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``pro_teams`` table of :data:`SCHEMAS`, including the free-agent pool (``pro_team_id`` 0).
        A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_pro_teams

            espn_fantasy_pro_teams(game="ffl", season=2026)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_pro_teams` -- the wrapper that fetches this body and parses it
    """
    rows = [
        {
            "pro_team_id": t.get("id"),
            "pro_team_abbrev": t.get("abbrev"),
            "pro_team_location": t.get("location"),
            "pro_team_name": t.get("name"),
            "bye_week": t.get("byeWeek"),
            "universe_id": t.get("universeId"),
        }
        for t in _pro_teams(raw)
    ]
    return _frame("pro_teams", rows, return_as_pandas)


def parse_espn_fantasy_pro_schedule(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a ``view=proTeamSchedules_wl`` season body into one row per pro game.

    Each pro game appears under both of its teams; it is kept once (by ``pro_game_id``).

    Args:
        raw: The decoded season body (``settings.proTeams[].proGamesByScoringPeriod``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``pro_schedule`` table of :data:`SCHEMAS`, sorted by game time then id. A zero-row frame for an
        empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_pro_schedule

            espn_fantasy_pro_schedule(game="ffl", season=2026)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_pro_schedule` -- the wrapper that fetches this body and parses it
    """
    seen: Dict[Any, Dict[str, Any]] = {}
    for t in _pro_teams(raw):
        for games in _d(t.get("proGamesByScoringPeriod")).values():
            for g in _rows(games):
                gid = _int(g.get("id"))
                if gid is None or gid in seen:
                    continue
                seen[gid] = {
                    "pro_game_id": gid,
                    "scoring_period_id": g.get("scoringPeriodId"),
                    "game_date": _ms(g.get("date")),
                    "home_pro_team_id": g.get("homeProTeamId"),
                    "away_pro_team_id": g.get("awayProTeamId"),
                    "start_time_tbd": g.get("startTimeTBD"),
                    "stats_official": g.get("statsOfficial"),
                    "valid_for_locking": g.get("validForLocking"),
                }
    rows = sorted(seen.values(), key=lambda r: (r["game_date"] is None, r["game_date"] or 0, r["pro_game_id"]))
    return _frame("pro_schedule", rows, return_as_pandas)


def parse_espn_fantasy_players(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a season's ``players`` (``view=players_wl``) body into one row per player.

    Args:
        raw: The decoded body, a bare list of player objects.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``players`` table of :data:`SCHEMAS`. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_players

            espn_fantasy_players(game="fba", season=2027)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_players` -- the wrapper that fetches this body and parses it
    """
    rows = []
    for p in _rows(raw):
        rows.append(
            {
                **_player_cols(p),
                "droppable": p.get("droppable"),
                "percent_owned": _d(p.get("ownership")).get("percentOwned"),
                "universe_id": p.get("universeId"),
            }
        )
    return _frame("players", rows, return_as_pandas)


def parse_espn_fantasy_league_directory(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse ESPN's public-league directory into one row per listed league.

    Args:
        raw: The decoded ``leaguedirectory/{league_sub_type}`` body, a list of league entries.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_directory`` table of :data:`SCHEMAS`. ``league_id`` is readable by the
        ``espn_fantasy_league_*`` wrappers. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_directory

            espn_fantasy_league_directory(game="fba", season=2027)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_directory` -- the wrapper that fetches this body and parses it
    """
    rows = []
    for r in _rows(raw):
        rows.append(
            {
                "league_id": r.get("leagueId"),
                "league_name": r.get("leagueName"),
                "league_sub_type": r.get("leagueSubType"),
                "league_default_id": r.get("leagueDefaultId"),
                "league_size": r.get("leagueSize"),
                "teams_joined": r.get("teamsJoined"),
                "full": r.get("full"),
                "public": r.get("public"),
                "standard_league": r.get("standardLeague"),
                "customizable": r.get("customizable"),
                "draft_type": r.get("draftType"),
                "draft_date": _ms(r.get("draftDate")),
                "draft_available_date": _ms(r.get("draftAvailableDate")),
                "draft_in_progress": r.get("draftInProgress"),
                "scoring_type": r.get("scoringType"),
                "experience_type": r.get("experienceType"),
                "rank_type": r.get("rankType"),
                "restriction_type": r.get("restrictionType"),
                "roster_locktime_type": r.get("rosterLocktimeType"),
                "scoring_item_stat_ids": _json(r.get("scoringItemStatIds")),
                "universe_ids": _json(r.get("universeIds")),
                "season_id": r.get("seasonId"),
                "game_id": r.get("gameId"),
            }
        )
    return _frame("league_directory", rows, return_as_pandas)


def parse_espn_fantasy_scoring(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a ``view=mSettings`` body (a league, or a default-settings template) into one row per scoring item.

    Args:
        raw: The decoded body (``settings.scoringSettings.scoringItems[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``scoring`` table of :data:`SCHEMAS`: the points each stat is worth. A zero-row frame for an
        empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_default_scoring

            espn_fantasy_default_scoring(game="ffl", season=2026, league_default_id=1)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_default_scoring` -- the wrapper that fetches this body and parses it
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_scoring` -- the same table for one public league
    """
    raw = _d(raw)
    scoring = _d(_d(raw.get("settings")).get("scoringSettings"))
    base = {**_league_keys(raw), "game_id": raw.get("gameId"), "scoring_type": scoring.get("scoringType")}
    rows = [
        {
            **base,
            "stat_id": item.get("statId"),
            "points": item.get("points"),
            "is_reverse_item": item.get("isReverseItem"),
            "points_overrides": _json(item.get("pointsOverrides")),
        }
        for item in _rows(scoring.get("scoringItems"))
    ]
    return _frame("scoring", rows, return_as_pandas)


def parse_espn_fantasy_player_news(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a player-news body into one row per news item.

    Args:
        raw: The decoded ``news/players`` body (``news.feed[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``player_news`` table of :data:`SCHEMAS`. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_player_news

            espn_fantasy_player_news(game="fba", player_id=5104157)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_player_news` -- the wrapper that fetches this body and parses it
    """
    rows = [
        {
            "news_id": n.get("id"),
            "now_id": n.get("nowId"),
            "player_id": n.get("playerId"),
            "type": n.get("type"),
            "headline": n.get("headline"),
            "description": n.get("description"),
            "story": n.get("story"),
            "published": _iso(n.get("published")),
            "last_modified": _iso(n.get("lastModified")),
            "premium": n.get("premium"),
        }
        for n in _rows(_d(_d(raw).get("news")).get("feed"))
    ]
    return _frame("player_news", rows, return_as_pandas)


# -- league tables -------------------------------------------------------------------------------


def parse_espn_fantasy_league_teams(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mTeam`` body into one row per team (overall record and counters).

    Args:
        raw: The decoded league body (``teams[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_teams`` table of :data:`SCHEMAS`. Owner account ids are not emitted. A zero-row
        frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_teams

            espn_fantasy_league_teams(game="fba", season=2027, league_id=1535462463)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_teams` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = _league_keys(raw)
    rows = []
    for t in _rows(raw.get("teams")):
        rec = _d(_d(t.get("record")).get("overall"))
        cnt = _d(t.get("transactionCounter"))
        rows.append(
            {
                **keys,
                "team_id": t.get("id"),
                "team_name": t.get("name"),
                "team_abbrev": t.get("abbrev"),
                "division_id": t.get("divisionId"),
                "wins": rec.get("wins"),
                "losses": rec.get("losses"),
                "ties": rec.get("ties"),
                "win_percentage": rec.get("percentage"),
                "points_for": rec.get("pointsFor"),
                "points_against": rec.get("pointsAgainst"),
                "games_back": rec.get("gamesBack"),
                "streak_type": rec.get("streakType"),
                "streak_length": rec.get("streakLength"),
                "playoff_seed": t.get("playoffSeed"),
                "points": t.get("points"),
                "current_projected_rank": t.get("currentProjectedRank"),
                "draft_day_projected_rank": t.get("draftDayProjectedRank"),
                "rank_calculated_final": t.get("rankCalculatedFinal"),
                "waiver_rank": t.get("waiverRank"),
                "acquisitions": cnt.get("acquisitions"),
                "drops": cnt.get("drops"),
                "trades": cnt.get("trades"),
                "move_to_active": cnt.get("moveToActive"),
                "move_to_ir": cnt.get("moveToIR"),
                "acquisition_budget_spent": cnt.get("acquisitionBudgetSpent"),
            }
        )
    return _frame("league_teams", rows, return_as_pandas)


def parse_espn_fantasy_league_rosters(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mRoster`` body into one row per rostered player.

    Args:
        raw: The decoded league body (``teams[].roster.entries[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_rosters`` table of :data:`SCHEMAS`. A zero-row frame for an empty or malformed body
        (and for a league whose draft has not happened, whose rosters are empty).

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_rosters

            espn_fantasy_league_rosters(game="fba", season=2027, league_id=1535462463)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_rosters` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = _league_keys(raw)
    rows = []
    for t in _rows(raw.get("teams")):
        for e in _rows(_d(t.get("roster")).get("entries")):
            pool = _d(e.get("playerPoolEntry"))
            player = _d(pool.get("player"))
            own = _d(player.get("ownership"))
            rows.append(
                {
                    **keys,
                    "team_id": t.get("id"),
                    **_player_cols(player),
                    "player_id": e.get("playerId") if e.get("playerId") is not None else player.get("id"),
                    "lineup_slot_id": e.get("lineupSlotId"),
                    "acquisition_type": e.get("acquisitionType"),
                    "acquisition_date": _ms(e.get("acquisitionDate")),
                    "injury_status": e.get("injuryStatus"),
                    "player_status": pool.get("status"),
                    "applied_stat_total": pool.get("appliedStatTotal"),
                    "percent_owned": own.get("percentOwned"),
                    "percent_started": own.get("percentStarted"),
                }
            )
    return _frame("league_rosters", rows, return_as_pandas)


def _sides(m: Dict[str, Any]) -> Iterable[tuple]:
    for side in ("home", "away"):
        if isinstance(m.get(side), dict):
            yield side, m[side]


def parse_espn_fantasy_league_matchups(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mMatchupScore`` body into one row per matchup of the season.

    Args:
        raw: The decoded league body (``schedule[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_matchups`` table of :data:`SCHEMAS`. Unplayed matchups are included
        (``winner`` ``"UNDECIDED"``); a bye has a null ``away_team_id``. A zero-row frame for an empty or
        malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_matchups

            espn_fantasy_league_matchups(game="fba", season=2027, league_id=1535462463)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_matchups` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = _league_keys(raw)
    rows = []
    for m in _rows(raw.get("schedule")):
        home, away = _d(m.get("home")), _d(m.get("away"))
        rows.append(
            {
                **keys,
                "matchup_id": m.get("id"),
                "matchup_period_id": m.get("matchupPeriodId"),
                "playoff_tier_type": m.get("playoffTierType"),
                "winner": m.get("winner"),
                "home_team_id": home.get("teamId"),
                "away_team_id": away.get("teamId"),
                "home_total_points": home.get("totalPoints"),
                "away_total_points": away.get("totalPoints"),
            }
        )
    return _frame("league_matchups", rows, return_as_pandas)


def parse_espn_fantasy_league_boxscores(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mBoxscore`` body into one row per player per matchup side.

    Only matchups in the requested scoring period's matchup period carry player rows; the others are
    skipped (their totals are in :func:`parse_espn_fantasy_league_matchups`).

    Args:
        raw: The decoded league body (``schedule[].home/away.rosterForCurrentScoringPeriod.entries[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_boxscores`` table of :data:`SCHEMAS`. ``applied_stat_total`` is the player's fantasy
        points for ``scoring_period_id``. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_boxscores

            espn_fantasy_league_boxscores(game="fba", season=2027, league_id=1535462463, scoring_period_id=37)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_boxscores` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = {**_league_keys(raw), "scoring_period_id": raw.get("scoringPeriodId")}
    rows = []
    for m in _rows(raw.get("schedule")):
        for side, s in _sides(m):
            roster = _d(s.get("rosterForCurrentScoringPeriod"))
            for e in _rows(roster.get("entries")):
                pool = _d(e.get("playerPoolEntry"))
                player = _d(pool.get("player"))
                if e.get("playerId") is None and player.get("id") is None:
                    continue  # an empty lineup slot placeholder
                rows.append(
                    {
                        **keys,
                        "matchup_id": m.get("id"),
                        "matchup_period_id": m.get("matchupPeriodId"),
                        "side": side,
                        "team_id": s.get("teamId"),
                        **_player_cols(player),
                        "player_id": e.get("playerId") if e.get("playerId") is not None else player.get("id"),
                        "lineup_slot_id": e.get("lineupSlotId"),
                        "applied_stat_total": pool.get("appliedStatTotal"),
                    }
                )
    return _frame("league_boxscores", rows, return_as_pandas)


def parse_espn_fantasy_league_draft(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mDraftDetail`` body into one row per pick.

    Args:
        raw: The decoded league body (``draftDetail.picks[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_draft`` table of :data:`SCHEMAS`, in pick order. The picking member's account id is
        not emitted. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_draft

            espn_fantasy_league_draft(game="fba", season=2027, league_id=1535462463)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_draft` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = _league_keys(raw)
    rows = [
        {
            **keys,
            "pick_id": p.get("id"),
            "overall_pick_number": p.get("overallPickNumber"),
            "round_id": p.get("roundId"),
            "round_pick_number": p.get("roundPickNumber"),
            "team_id": p.get("teamId"),
            "player_id": p.get("playerId"),
            "bid_amount": p.get("bidAmount"),
            "keeper": p.get("keeper"),
            "reserved_for_keeper": p.get("reservedForKeeper"),
            "auto_draft_type_id": p.get("autoDraftTypeId"),
            "lineup_slot_id": p.get("lineupSlotId"),
            "nominating_team_id": p.get("nominatingTeamId"),
        }
        for p in _rows(_d(raw.get("draftDetail")).get("picks"))
    ]
    return _frame("league_draft", rows, return_as_pandas)


def parse_espn_fantasy_league_transactions(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=mTransactions2`` body into one row per transaction item.

    A transaction with no items (a lineup-only move) is one row with null item columns.

    Args:
        raw: The decoded league body (``transactions[].items[]``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``league_transactions`` table of :data:`SCHEMAS`. The proposing member's account id is not
        emitted. A zero-row frame for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_league_transactions

            espn_fantasy_league_transactions(game="fba", season=2027, league_id=1535462463, scoring_period_id=30)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_league_transactions` -- the wrapper that fetches this body and parses it
    """
    raw = _d(raw)
    keys = _league_keys(raw)
    rows = []
    for t in _rows(raw.get("transactions")):
        base = {
            **keys,
            "transaction_id": t.get("id"),
            "transaction_type": t.get("type"),
            "status": t.get("status"),
            "execution_type": t.get("executionType"),
            "scoring_period_id": t.get("scoringPeriodId"),
            "proposed_date": _ms(t.get("proposedDate")),
            "team_id": t.get("teamId"),
            "bid_amount": t.get("bidAmount"),
            "is_pending": t.get("isPending"),
        }
        items = _rows(t.get("items")) or [{}]
        for i in items:
            rows.append(
                {
                    **base,
                    "item_type": i.get("type"),
                    "player_id": i.get("playerId"),
                    "from_team_id": i.get("fromTeamId"),
                    "to_team_id": i.get("toTeamId"),
                    "from_lineup_slot_id": i.get("fromLineupSlotId"),
                    "to_lineup_slot_id": i.get("toLineupSlotId"),
                }
            )
    return _frame("league_transactions", rows, return_as_pandas)


def parse_espn_fantasy_player_pool(raw: Any, *, return_as_pandas: bool = False) -> Frame:
    """Parse a league's ``view=kona_player_info`` body into one row per player-pool entry.

    Args:
        raw: The decoded body, ``{"players": [{..., "player": {...}}]}``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The ``player_pool`` table of :data:`SCHEMAS`: who is a free agent, on waivers or on a team
        (``status``, ``on_team_id``), with ownership and draft-market values. A zero-row frame for an
        empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.providers.espn_fantasy import espn_fantasy_player_pool

            espn_fantasy_player_pool(game="fba", season=2027, league_id=1535462463)

        See Also:
            * :func:`~sportsdataverse.providers.espn_fantasy.espn_fantasy_player_pool` -- the wrapper that fetches this body and parses it
    """
    rows = []
    for e in _rows(_d(raw).get("players")):
        player = _d(e.get("player"))
        own = _d(player.get("ownership"))
        rows.append(
            {
                **_player_cols(player),
                "player_id": e.get("id") if e.get("id") is not None else player.get("id"),
                "status": e.get("status"),
                "on_team_id": e.get("onTeamId"),
                "keeper_value": e.get("keeperValue"),
                "draft_auction_value": e.get("draftAuctionValue"),
                "percent_owned": own.get("percentOwned"),
                "percent_started": own.get("percentStarted"),
                "percent_change": own.get("percentChange"),
                "average_draft_position": own.get("averageDraftPosition"),
                "auction_value_average": own.get("auctionValueAverage"),
            }
        )
    return _frame("player_pool", rows, return_as_pandas)
