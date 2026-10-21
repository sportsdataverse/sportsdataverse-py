"""Parsers for the generated ``fiba_gdap`` wrappers (FIBA digital API, ``digital-api.fiba.basketball/hapi``).

Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, snake_case columns (``dl_utils.underscore``), and a zero-row frame typed
from the documented returns-schema (never an exception, never an untyped ``pl.DataFrame()``) for an
empty or malformed body, so two empty frames stack and an empty frame stacks onto a populated one.
Dtypes are pinned (see :mod:`sportsdataverse.basketball.fiba_hub.fiba_frames`).

**Row rules.** A route is parsed by the shaper that fits its body, not by one generic rule:

* a list body is its rows (schedules, teams, awards, national teams, medal table, editions);
* an object body is one row (a game header, a competition, a player's season line);
* a body that nests rows under a list key is exploded with its parent's ids (a roster is its
  players, with the team and competition ids; a team's statistics are one row per team and one
  per player; a round's group standings are one row per team in a group);
* nested objects flatten into ``<parent>_<child>`` columns (``team_a_organisation_id``) and
  remaining list cells become JSON strings.

**Ids** (one dtype per id, ``Int64``): ``player_id`` is global across competitions and roles;
``team_id`` is per team PER COMPETITION (Germany: 279569 at the 2023 World Cup, 281545 at Paris
2024), so key a team across seasons on ``organisation_id`` (64 = Germany, stable); ``club_id`` on a
roster row is a club's organisation id; ``game_id`` is global; ``competition_id`` is one per
edition; ``competition_marketing_name_id`` names the competition family (201 = World Cup, 112 = BCL).
``group_id`` is a string (``"A"``-style group code, not an integer id).

**Seasons.** ``season`` is the provider's key, which equals the sdv key: the END year for club
competitions (BCL 2024-25 = 2025), the event year for national-team events (World Cup 2023 =
2023), and the TARGET tournament year for qualifiers.

**Personal data.** Contact details and coaches' / officials' dates of birth never leave the
parsers: :func:`~sportsdataverse.basketball.fiba_hub.fiba_runtime._scrub` drops them from the raw body
and every parser here drops them again from a body that was fetched elsewhere.

Percentages are 0-100. ``game_date_time`` is the venue-local wall-clock time without an offset and
``game_date_time_utc`` is UTC without a ``Z``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from sportsdataverse.basketball.fiba_hub.fiba_frames import Frame, _finish, _flatten, _frame, _id, _rows, _snake
from sportsdataverse.basketball.fiba_hub.fiba_runtime import _scrub

__all__ = [
    "parse_fiba_gdap_schedule",
    "parse_fiba_gdap_games_between",
    "parse_fiba_gdap_game",
    "parse_fiba_gdap_game_periods",
    "parse_fiba_gdap_game_officials",
    "parse_fiba_gdap_competition",
    "parse_fiba_gdap_editions",
    "parse_fiba_gdap_teams",
    "parse_fiba_gdap_rounds",
    "parse_fiba_gdap_round_standings",
    "parse_fiba_gdap_final_standings",
    "parse_fiba_gdap_round_gamedays",
    "parse_fiba_gdap_awards",
    "parse_fiba_gdap_nominations",
    "parse_fiba_gdap_team_staff",
    "parse_fiba_gdap_roster",
    "parse_fiba_gdap_roster_coaches",
    "parse_fiba_gdap_roster_latest",
    "parse_fiba_gdap_team_stats",
    "parse_fiba_gdap_team_player_stats",
    "parse_fiba_gdap_team_games",
    "parse_fiba_gdap_player_info",
    "parse_fiba_gdap_player_stats",
    "parse_fiba_gdap_player_game_stats",
    "parse_fiba_gdap_federation_games",
    "parse_fiba_gdap_national_teams",
    "parse_fiba_gdap_medal_count",
    "parse_fiba_gdap_ranking_zones",
    "parse_fiba_gdap_federation_ranking",
    "parse_fiba_gdap_gateway_federation_ranking",
    "parse_fiba_gdap_gateway_competition_ids",
]


def _clean(raw: Any, root_staff: bool = False) -> Any:
    return _scrub(raw, root_staff=root_staff)


def _row(obj: Any, **head: Any) -> Dict[str, Any]:
    """One snake_case row: ``head`` values first, then ``obj`` flattened."""
    return _snake({**head, **_flatten(obj)})


def _list(obj: Any) -> List[Any]:
    return obj if isinstance(obj, list) else []


def _dict(obj: Any) -> Dict[str, Any]:
    return obj if isinstance(obj, dict) else {}


def _without(obj: Any, *keys: str) -> Dict[str, Any]:
    return {k: v for k, v in _dict(obj).items() if k not in keys}


# --- shapers: body -> list of rows -----------------------------------------------------------------------------


def _list_rows(raw: Any) -> List[Dict[str, Any]]:
    """The object rows of a list body, or of the one list an envelope object holds; any other object has none."""
    if isinstance(raw, dict):
        lists = [k for k, v in raw.items() if isinstance(v, list)]
        if len(lists) != 1 or any(isinstance(v, dict) for k, v in raw.items() if k != lists[0]):
            return []
        raw = raw[lists[0]]
    return [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []


def _sh_flat(raw: Any) -> List[Dict[str, Any]]:
    return [_row(r) for r in _list_rows(_clean(raw))]


def _sh_obj(raw: Any, key: str) -> List[Dict[str, Any]]:
    """One row for an object body that names its own ``key`` (a competition, a player's season line)."""
    body = _dict(_clean(raw))
    return [_row(body)] if body.get(key) is not None else []


def _sh_nominations(raw: Any) -> List[Dict[str, Any]]:
    """Officials of a competition: every top-level row is a person, so dates of birth go from the row itself."""
    return [_row(r) for r in _list_rows(_clean(raw, root_staff=True))]


def _sh_game_header(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    return [_row(_without(body, "nominations", "periods"))] if body.get("gameId") is not None else []


def _sh_game_periods(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    return [_row(p, game_id=body.get("gameId")) for p in _list(body.get("periods")) if isinstance(p, dict)]


def _sh_game_officials(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    return [_row(n, game_id=body.get("gameId")) for n in _list(body.get("nominations")) if isinstance(n, dict)]


def _sh_rounds(raw: Any) -> List[Dict[str, Any]]:
    return [_row(_without(r, "groupStandings")) for r in _rows(_clean(raw))]


def _sh_round_standings(raw: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rnd in _rows(_clean(raw)):
        head = {
            "round_id": rnd.get("roundId"),
            "round_name": rnd.get("roundName"),
            "round_code": rnd.get("roundCode"),
        }
        for grp in _list(_dict(rnd.get("groupStandings")).get("groups")):
            grp = _dict(grp)
            ghead = {**head, "group_name": grp.get("groupName"), "group_status_code": grp.get("statusCode")}
            for team in _list(grp.get("teams")):
                if isinstance(team, dict):
                    out.append(_row(team, **ghead))
    return out


def _sh_final_standings(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    cid = _dict(body.get("competition")).get("competitionId")
    return [_row(s, competition_id=cid) for s in _list(body.get("standings")) if isinstance(s, dict)]


def _sh_round_gamedays(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    cid = _dict(body.get("competition")).get("competitionId")
    out = []
    for item in _list(body.get("rounds")):
        item = _dict(item)
        out.append(
            _row(
                {"round": _without(item.get("round"), "groupStandings"), "gamedays": item.get("gamedays")},
                competition_id=cid,
            )
        )
    return out


def _sh_team_staff(raw: Any) -> List[Dict[str, Any]]:
    out = []
    for team in _rows(_clean(raw)):
        head = {"team_id": team.get("teamId"), "team_name": team.get("teamName"), "team_code": team.get("teamCode")}
        out += [_row(c, **head) for c in _list(team.get("coaches")) if isinstance(c, dict)]
    return out


def _team_head(body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "team_id": body.get("teamId"),
        "team_code": body.get("code"),
        "team_name": body.get("name"),
        "organisation_id": body.get("teamOrganisationId"),
        "competition_id": body.get("competitionId"),
    }


def _sh_roster_players(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    return [_row(p, **_team_head(body)) for p in _list(body.get("players")) if isinstance(p, dict)]


def _sh_roster_coaches(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    return [_row(c, **_team_head(body)) for c in _list(body.get("coaches")) if isinstance(c, dict)]


def _sh_team_stats(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    if body.get("teamId") is None:
        return []
    return [
        _row(
            {
                **_without(body, "playerInCompetitionTeamStatistics", "benchStatistics"),
                "bench": body.get("benchStatistics"),
            }
        )
    ]


def _sh_team_player_stats(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    head = {"competition_id": body.get("competitionId")}
    return [_row(p, **head) for p in _list(body.get("playerInCompetitionTeamStatistics")) if isinstance(p, dict)]


def _sh_player_game_stats(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    head = {"player_id": body.get("playerId"), "competition_id": body.get("competitionId")}
    return [_row(g, **head) for g in _list(body.get("gameStatistics")) if isinstance(g, dict)]


def _sh_ranking(raw: Any) -> List[Dict[str, Any]]:
    body = _dict(_clean(raw))
    head = {
        "as_of_date": body.get("asOfDate"),
        "zone_ranking_code": body.get("zoneRankingCode"),
        "zone_ranking_official_name": body.get("zoneRankingOfficialName"),
    }
    return [_row(i, **head) for i in _list(body.get("items")) if isinstance(i, dict)]


def _sh_competition_ids(raw: Any) -> List[Dict[str, Any]]:
    ids = [_id(i) for i in _list(_dict(_clean(raw)).get("competitionIds"))]  # the API sends numeric strings
    return [{"competition_id": i} for i in ids if i is not None]


# --- public parsers -----------------------------------------------------------------------------------------------


def parse_fiba_gdap_schedule(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Schedule and results of one competition (edition): one row per game.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_schedule(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per game, with both teams, the round and the competition flattened. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_schedule, parse_fiba_gdap_schedule
            raw = fiba_gdap_schedule(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_schedule(raw)

        Pandas::

            df_pd = parse_fiba_gdap_schedule(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_schedule", columns, return_as_pandas)


def parse_fiba_gdap_games_between(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Games of EVERY FIBA competition in a local-time window (how competitions and games are discovered).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_games_between(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per game; the window compares the venue-local game_date_time and dateTo is exclusive. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_games_between, parse_fiba_gdap_games_between
            raw = fiba_gdap_games_between(date_from="2023-09-08", date_to="2023-09-11", return_parsed=False)
            df = parse_fiba_gdap_games_between(raw)

        Pandas::

            df_pd = parse_fiba_gdap_games_between(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_games_between", columns, return_as_pandas)


def parse_fiba_gdap_game(raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None) -> Frame:
    """One game's header: teams, score, venue, attendance and status codes (no box score or play-by-play: see fiba_gamepage).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_game(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_game, parse_fiba_gdap_game
            raw = fiba_gdap_game(game_id=100720, return_parsed=False)
            df = parse_fiba_gdap_game(raw)

        Pandas::

            df_pd = parse_fiba_gdap_game(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_game_header(raw)), "parse_fiba_gdap_game", columns, return_as_pandas)


def parse_fiba_gdap_game_periods(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Period scores of one game.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_game_periods(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per period (Q1..Q4, OT1...). A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_game_periods, parse_fiba_gdap_game_periods
            raw = fiba_gdap_game_periods(game_id=100720, return_parsed=False)
            df = parse_fiba_gdap_game_periods(raw)

        Pandas::

            df_pd = parse_fiba_gdap_game_periods(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_game_periods(raw)), "parse_fiba_gdap_game_periods", columns, return_as_pandas)


def parse_fiba_gdap_game_officials(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Officials nominated to one game (referees, table officials, commissioner).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_game_officials(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per official; contact data and dates of birth are dropped. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_game_officials, parse_fiba_gdap_game_officials
            raw = fiba_gdap_game_officials(game_id=100720, return_parsed=False)
            df = parse_fiba_gdap_game_officials(raw)

        Pandas::

            df_pd = parse_fiba_gdap_game_officials(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_game_officials(raw)), "parse_fiba_gdap_game_officials", columns, return_as_pandas)


def parse_fiba_gdap_competition(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """One competition (edition): season, dates, hosts, zone, category and competition family.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_competition(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_competition, parse_fiba_gdap_competition
            raw = fiba_gdap_competition(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_competition(raw)

        Pandas::

            df_pd = parse_fiba_gdap_competition(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_obj(raw, "competitionId")), "parse_fiba_gdap_competition", columns, return_as_pandas)


def parse_fiba_gdap_editions(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every edition of a competition family between two seasons.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_editions(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per edition, shaped like fiba_gdap_competition. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_editions, parse_fiba_gdap_editions
            raw = fiba_gdap_editions(family_id=201, season=2023, return_parsed=False)
            df = parse_fiba_gdap_editions(raw)

        Pandas::

            df_pd = parse_fiba_gdap_editions(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_editions", columns, return_as_pandas)


def parse_fiba_gdap_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Teams of a competition with their profile (final ranking, per-game averages).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_teams(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per team; the team's e-mail address is dropped. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_teams, parse_fiba_gdap_teams
            raw = fiba_gdap_teams(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_teams(raw)

        Pandas::

            df_pd = parse_fiba_gdap_teams(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_teams", columns, return_as_pandas)


def parse_fiba_gdap_rounds(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Rounds of a competition (group stage, knockout rounds).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_rounds(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per round; the standings are in fiba_gdap_round_standings. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_rounds, parse_fiba_gdap_rounds
            raw = fiba_gdap_rounds(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_rounds(raw)

        Pandas::

            df_pd = parse_fiba_gdap_rounds(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_rounds(raw)), "parse_fiba_gdap_rounds", columns, return_as_pandas)


def parse_fiba_gdap_round_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Group standings of every round of a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_round_standings(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per team in a group of a round. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_round_standings, parse_fiba_gdap_round_standings
            raw = fiba_gdap_round_standings(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_round_standings(raw)

        Pandas::

            df_pd = parse_fiba_gdap_round_standings(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_round_standings(raw)), "parse_fiba_gdap_round_standings", columns, return_as_pandas)


def parse_fiba_gdap_final_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Final ranking of a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_final_standings(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per team. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_final_standings, parse_fiba_gdap_final_standings
            raw = fiba_gdap_final_standings(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_final_standings(raw)

        Pandas::

            df_pd = parse_fiba_gdap_final_standings(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_final_standings(raw)), "parse_fiba_gdap_final_standings", columns, return_as_pandas)


def parse_fiba_gdap_round_gamedays(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Rounds of a competition with their game days.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_round_gamedays(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per round; the game days are a JSON string. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_round_gamedays, parse_fiba_gdap_round_gamedays
            raw = fiba_gdap_round_gamedays(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_round_gamedays(raw)

        Pandas::

            df_pd = parse_fiba_gdap_round_gamedays(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_round_gamedays(raw)), "parse_fiba_gdap_round_gamedays", columns, return_as_pandas)


def parse_fiba_gdap_awards(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Awards of a competition (MVP, All-Star Five...).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_awards(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per award winner. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_awards, parse_fiba_gdap_awards
            raw = fiba_gdap_awards(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_awards(raw)

        Pandas::

            df_pd = parse_fiba_gdap_awards(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_awards", columns, return_as_pandas)


def parse_fiba_gdap_nominations(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Officials nominated for a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_nominations(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per official; contact data and dates of birth are dropped. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_nominations, parse_fiba_gdap_nominations
            raw = fiba_gdap_nominations(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_nominations(raw)

        Pandas::

            df_pd = parse_fiba_gdap_nominations(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_nominations(raw)), "parse_fiba_gdap_nominations", columns, return_as_pandas)


def parse_fiba_gdap_team_staff(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Coaches of every team of a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_team_staff(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per coach, with the team ids; dates of birth are dropped. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_team_staff, parse_fiba_gdap_team_staff
            raw = fiba_gdap_team_staff(comp=208182, return_parsed=False)
            df = parse_fiba_gdap_team_staff(raw)

        Pandas::

            df_pd = parse_fiba_gdap_team_staff(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_team_staff(raw)), "parse_fiba_gdap_team_staff", columns, return_as_pandas)


def parse_fiba_gdap_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A team's roster at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_roster(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per player, with the team and competition ids. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_roster, parse_fiba_gdap_roster
            raw = fiba_gdap_roster(team_id=279569, return_parsed=False)
            df = parse_fiba_gdap_roster(raw)

        Pandas::

            df_pd = parse_fiba_gdap_roster(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_roster_players(raw)), "parse_fiba_gdap_roster", columns, return_as_pandas)


def parse_fiba_gdap_roster_coaches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A team's coaches on its roster at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_roster_coaches(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per coach; dates of birth are dropped. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_roster_coaches, parse_fiba_gdap_roster_coaches
            raw = fiba_gdap_roster_coaches(team_id=279569, return_parsed=False)
            df = parse_fiba_gdap_roster_coaches(raw)

        Pandas::

            df_pd = parse_fiba_gdap_roster_coaches(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_roster_coaches(raw)), "parse_fiba_gdap_roster_coaches", columns, return_as_pandas)


def parse_fiba_gdap_roster_latest(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A team's latest roster (same shape as the roster).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_roster_latest(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per player, with the team and competition ids. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_roster_latest, parse_fiba_gdap_roster_latest
            raw = fiba_gdap_roster_latest(team_id=282183, return_parsed=False)
            df = parse_fiba_gdap_roster_latest(raw)

        Pandas::

            df_pd = parse_fiba_gdap_roster_latest(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_roster_players(raw)), "parse_fiba_gdap_roster_latest", columns, return_as_pandas)


def parse_fiba_gdap_team_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A team's per-game averages and totals at a competition, with the bench totals.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_team_stats(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_team_stats, parse_fiba_gdap_team_stats
            raw = fiba_gdap_team_stats(team_id=279569, return_parsed=False)
            df = parse_fiba_gdap_team_stats(raw)

        Pandas::

            df_pd = parse_fiba_gdap_team_stats(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_team_stats(raw)), "parse_fiba_gdap_team_stats", columns, return_as_pandas)


def parse_fiba_gdap_team_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Per-game averages of every player of a team at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_team_player_stats(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per player. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_team_player_stats, parse_fiba_gdap_team_player_stats
            raw = fiba_gdap_team_player_stats(team_id=279569, return_parsed=False)
            df = parse_fiba_gdap_team_player_stats(raw)

        Pandas::

            df_pd = parse_fiba_gdap_team_player_stats(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_team_player_stats(raw)), "parse_fiba_gdap_team_player_stats", columns, return_as_pandas)


def parse_fiba_gdap_team_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A team's games with each team's leaders.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_team_games(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per game; leaders are flattened. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_team_games, parse_fiba_gdap_team_games
            raw = fiba_gdap_team_games(team_id=279569, return_parsed=False)
            df = parse_fiba_gdap_team_games(raw)

        Pandas::

            df_pd = parse_fiba_gdap_team_games(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_team_games", columns, return_as_pandas)


def parse_fiba_gdap_player_info(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's bio and per-game averages with ranks at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_player_info(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_player_info, parse_fiba_gdap_player_info
            raw = fiba_gdap_player_info(player_id=211501, comp=208182, return_parsed=False)
            df = parse_fiba_gdap_player_info(raw)

        Pandas::

            df_pd = parse_fiba_gdap_player_info(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_obj(raw, "playerId")), "parse_fiba_gdap_player_info", columns, return_as_pandas)


def parse_fiba_gdap_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's per-game averages, totals and highs at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_player_stats(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_player_stats, parse_fiba_gdap_player_stats
            raw = fiba_gdap_player_stats(player_id=211501, comp=208182, return_parsed=False)
            df = parse_fiba_gdap_player_stats(raw)

        Pandas::

            df_pd = parse_fiba_gdap_player_stats(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_obj(raw, "playerId")), "parse_fiba_gdap_player_stats", columns, return_as_pandas)


def parse_fiba_gdap_player_game_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A player's game log (full box lines) at a competition.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_player_game_stats(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per game. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_player_game_stats, parse_fiba_gdap_player_game_stats
            raw = fiba_gdap_player_game_stats(player_id=211501, comp=208182, return_parsed=False)
            df = parse_fiba_gdap_player_game_stats(raw)

        Pandas::

            df_pd = parse_fiba_gdap_player_game_stats(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_player_game_stats(raw)), "parse_fiba_gdap_player_game_stats", columns, return_as_pandas)


def parse_fiba_gdap_federation_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """A federation's recent and upcoming national-team games.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_federation_games(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per game. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_federation_games, parse_fiba_gdap_federation_games
            raw = fiba_gdap_federation_games(federation_id=64, return_parsed=False)
            df = parse_fiba_gdap_federation_games(raw)

        Pandas::

            df_pd = parse_fiba_gdap_federation_games(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_federation_games", columns, return_as_pandas)


def parse_fiba_gdap_national_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Every national federation FIBA lists.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_national_teams(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per federation. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_national_teams, parse_fiba_gdap_national_teams
            raw = fiba_gdap_national_teams(return_parsed=False)
            df = parse_fiba_gdap_national_teams(raw)

        Pandas::

            df_pd = parse_fiba_gdap_national_teams(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_national_teams", columns, return_as_pandas)


def parse_fiba_gdap_medal_count(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """All-time medal table of a competition family.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_medal_count(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per federation. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_medal_count, parse_fiba_gdap_medal_count
            raw = fiba_gdap_medal_count(family_id=201, return_parsed=False)
            df = parse_fiba_gdap_medal_count(raw)

        Pandas::

            df_pd = parse_fiba_gdap_medal_count(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_medal_count", columns, return_as_pandas)


def parse_fiba_gdap_ranking_zones(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """FIBA world-ranking zones.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_ranking_zones(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per zone. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_ranking_zones, parse_fiba_gdap_ranking_zones
            raw = fiba_gdap_ranking_zones(return_parsed=False)
            df = parse_fiba_gdap_ranking_zones(raw)

        Pandas::

            df_pd = parse_fiba_gdap_ranking_zones(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_flat(raw)), "parse_fiba_gdap_ranking_zones", columns, return_as_pandas)


def parse_fiba_gdap_federation_ranking(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """FIBA world ranking of national teams.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_federation_ranking(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per federation, with the ranking date. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_federation_ranking, parse_fiba_gdap_federation_ranking
            raw = fiba_gdap_federation_ranking(category="men", zone_id=1, as_of_date="2025-09-15", return_parsed=False)
            df = parse_fiba_gdap_federation_ranking(raw)

        Pandas::

            df_pd = parse_fiba_gdap_federation_ranking(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_ranking(raw)), "parse_fiba_gdap_federation_ranking", columns, return_as_pandas)


def parse_fiba_gdap_gateway_federation_ranking(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """FIBA world ranking from the server-side gateway (same items as fiba_gdap_federation_ranking).

    Args:
        raw: the decoded GDAP body (``fiba_gdap_gateway_federation_ranking(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per federation, with the ranking date. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_gateway_federation_ranking, parse_fiba_gdap_gateway_federation_ranking
            raw = fiba_gdap_gateway_federation_ranking(gender="men", zone_id=1, as_of_date="2025-09-15", return_parsed=False)
            df = parse_fiba_gdap_gateway_federation_ranking(raw)

        Pandas::

            df_pd = parse_fiba_gdap_gateway_federation_ranking(raw, return_as_pandas=True)
    """
    return _finish(_frame(_sh_ranking(raw)), "parse_fiba_gdap_gateway_federation_ranking", columns, return_as_pandas)


def parse_fiba_gdap_gateway_competition_ids(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Ids of the competitions that run in a date window.

    Args:
        raw: the decoded GDAP body (``fiba_gdap_gateway_competition_ids(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            body keeps exactly these, typed.

    Returns:
        One row per competition id. A zero-row frame, typed from the returns schema, for an empty or malformed body.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gdap_gateway_competition_ids, parse_fiba_gdap_gateway_competition_ids
            raw = fiba_gdap_gateway_competition_ids(start_date="2025-01-14", end_date="2025-01-16", return_parsed=False)
            df = parse_fiba_gdap_gateway_competition_ids(raw)

        Pandas::

            df_pd = parse_fiba_gdap_gateway_competition_ids(raw, return_as_pandas=True)
    """
    return _finish(
        _frame(_sh_competition_ids(raw)), "parse_fiba_gdap_gateway_competition_ids", columns, return_as_pandas
    )
