"""Parsers for the generated ``vtb_infobasket`` wrappers (Infobasket, ``org.infobasket.su``).

Every parser follows the package contract: a polars frame by default, pandas with ``return_as_pandas=True``, snake_cased
columns (``dl_utils.underscore``), and a zero-row frame (never an exception) for an empty or malformed body. The zero-row
frame carries the parser's documented returns schema (``vtb_dtypes.DTYPES``, generated with the schemas), so two empty
frames, or an empty one and a populated one, stack.

**Input.** ``vtb_infobasket_runtime._get`` returns the body (a bare list as ``{"items": [...]}``) with the request stamped
under ``_request`` (``game_id``, ``person_id``, ``team_id``, ``comp_id``, ``season``, ``season_comp_id``, ``tag``); the parsers
also accept the bare body. One body can feed several frames (``GetOnline`` feeds ``game``, ``box_score``,
``team_box_score``, ``pbp`` and ``game_staff``; ``GetTeamStats`` feeds ``team_stats``, ``team_stats_players`` and
``team_stats_coaches``; ``PlayOff`` feeds ``playoff`` and ``playoff_games``; ``PlayerSeasonStats`` feeds
``player_season_stats`` and ``player_career_stats``), so fetch it once with ``return_parsed=False`` and parse it as many
ways as needed.

**Row rule.** Each route has its own shaper. Nested objects flatten into ``<parent>_<child>`` columns, empty lists and the
platform's bookkeeping (``Sys*`` fields, GUIDs, empty relation lists) are left out, and club names become
``<prefix>name_ru`` / ``name_en`` / ``short_name_*`` / ``region_name_*``. See :mod:`sportsdataverse.basketball.vtb.vtb_frames`
for the value conversions (dates, comma decimals, dropped paint and made/attempted strings).

**Season.** The sdv season is the **END year** (2026 = 2025-26). Infobasket's season-level ``CompID`` (50714 for 2026)
is output as ``comp_id`` / ``season_comp_id`` next to ``season``. ``CompID`` means three things: a season (50714), a
stage below it (50720 Regular Season, 54281 Playoff) and, in the player season stats, an END year.

**Id key spaces.** Every id column is ``Int64``.

* ``game_id`` is the Infobasket ``GameID``; it is also the v2 API's ``match_id``. ``GetOnline`` does not echo it at the top
  level (``Online.GameID`` does); ``OnlineTeams[].GameID`` and ``OnlineStarts[].GameID`` hold another, internal protocol id
  and ``OnlinePlays[].GameID`` is 0, so they are not output.
* ``team_id`` is the Infobasket ``TeamID`` (2650 = Lokomotiv Kuban) on every team route; it is also the v2 ``teamId``.
  ``/Widget/TeamPage`` additionally returns a club-record id (``team_record_id``), a different key space. ``comp_team_id``
  is a team's registration in one stage, never a team id.
* ``person_id`` is one person across clubs and seasons (players, coaches and officials share the space). ``start_id`` is a
  line-up slot within one game (0 = placeholder), not a person; the play-by-play carries the ``person_id`` / ``team_id`` it
  resolves to.
* ``comp_id`` / ``season_comp_id`` / ``parent_comp_id`` are competition ids (see Season).

**Personal data.** The runtime scrubs licence numbers, contact and operator fields for everyone and coaches', referees' and
officials' birth data; these parsers never output them either. Players' public bio (birth date, height, weight,
nationality) is kept.

**Box-score nulls.** Infobasket sends null instead of 0 for counting statistics; the stat parsers fill them with 0
(``plus_minus`` stays null where the host sends none). The ``paint_*`` statistics are broken (every player row of both teams
carries the same values) and the ``made/attempted`` strings duplicate the integer columns: both are dropped.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from sportsdataverse.basketball.vtb import vtb_frames as F
from sportsdataverse.basketball.vtb.vtb_codes import PLAY_TYPES, staff_role
from sportsdataverse.basketball.vtb.vtb_frames import Frame

__all__ = [
    "parse_vtb_infobasket_best_player_parameters",
    "parse_vtb_infobasket_best_players",
    "parse_vtb_infobasket_best_team_parameters",
    "parse_vtb_infobasket_best_teams",
    "parse_vtb_infobasket_box_score",
    "parse_vtb_infobasket_calendar",
    "parse_vtb_infobasket_coach",
    "parse_vtb_infobasket_comp_issue",
    "parse_vtb_infobasket_cross_table",
    "parse_vtb_infobasket_game",
    "parse_vtb_infobasket_game_page",
    "parse_vtb_infobasket_game_staff",
    "parse_vtb_infobasket_pbp",
    "parse_vtb_infobasket_player",
    "parse_vtb_infobasket_player_career_stats",
    "parse_vtb_infobasket_player_game_log",
    "parse_vtb_infobasket_player_season_stats",
    "parse_vtb_infobasket_playoff",
    "parse_vtb_infobasket_playoff_games",
    "parse_vtb_infobasket_roster",
    "parse_vtb_infobasket_roster_coaches",
    "parse_vtb_infobasket_seasons",
    "parse_vtb_infobasket_standings",
    "parse_vtb_infobasket_team",
    "parse_vtb_infobasket_team_box_score",
    "parse_vtb_infobasket_team_coach_stats",
    "parse_vtb_infobasket_team_comps",
    "parse_vtb_infobasket_team_games",
    "parse_vtb_infobasket_team_player_stats",
    "parse_vtb_infobasket_team_preview",
    "parse_vtb_infobasket_team_season_stats",
    "parse_vtb_infobasket_team_seasons",
    "parse_vtb_infobasket_team_stats",
    "parse_vtb_infobasket_team_stats_coaches",
    "parse_vtb_infobasket_team_stats_players",
]

_NAME_SUFFIX = {
    "CompTeamShortNameRu": "short_name_ru",
    "CompTeamShortNameEn": "short_name_en",
    "CompTeamNameRu": "name_ru",
    "CompTeamNameEn": "name_en",
    "CompTeamRegionNameRu": "region_name_ru",
    "CompTeamRegionNameEn": "region_name_en",
}
_PERSON_NAMES = {
    "PersonLastNameRu": "last_name_ru",
    "PersonLastNameEn": "last_name_en",
    "PersonFirstNameRu": "first_name_ru",
    "PersonFirstNameEn": "first_name_en",
    "PersonFullNameRu": "full_name_ru",
    "PersonFullNameEn": "full_name_en",
}
#: Wire keys of a person record that are always null or never useful (see ``vtb-returns.md``).
_PERSON_SKIP = frozenset(
    {
        "PersonNickNameRu", "PersonNickNameEn", "PersonSecondNameRu", "PersonSecondNameEn",
        "PersonBornIn", "PersonBornInRegion", "PersonCountry", "PersonRegion", "Coaches", "Players", "Position",
        "PersonRank", "PersonFullNameRu", "PersonFullNameEn", "Country",
    }
)  # fmt: skip
_STAT_SKIP = frozenset({"Players", "Coaches", "Staff", "TeamName", "PersonInfo"})
_GAME_SKIP = frozenset(
    {
        "IsToday", "DaysFromToday", "GameDateInt", "GameLocalDate", "HasVideo", "VideoID", "LiveID", "Periods",
        "TeamLogoA", "TeamLogoB", "RegionId",
        "DisplayDateTimeLocal", "DisplayDateTimeMsk", "DisplayDateTimeLocalEn", "DisplayDateTimeMskEn",
    }
)  # fmt: skip


def _names(team_name: Any, prefix: str) -> Dict[str, Any]:
    """The six name columns of a ``CompTeamName`` record under ``prefix`` (``team_``, ``team_a_`` ...)."""
    d = F.obj(team_name)
    return {f"{prefix}{col}": d.get(wire) for wire, col in _NAME_SUFFIX.items()}


def _person(info: Any, *, bio: bool) -> Dict[str, Any]:
    """The person columns of a ``PersonInfo`` record: names, gender and (players only) birth date, height, weight, position."""
    d = F.obj(info)
    out: Dict[str, Any] = {f"person_{col}": d.get(wire) for wire, col in _PERSON_NAMES.items()}
    out["person_gender"] = d.get("PersonGender")
    if bio:
        out["birth_date"] = F.conv(d.get("PersonBirth"))
        out["height"] = d.get("PersonHeight")
        out["weight"] = d.get("PersonWeight")
        first = next(iter(F.rows(d.get("Players"))), {})
        out["player_position"] = first.get("PlayerPosition")
    return out


def _fin(data: Any, columns: Optional[Sequence[str]], pandas: bool, parser: str) -> Frame:
    return F.finish(F.frame(data), columns, pandas, parser)


# --- seasons, calendar, games lists -------------------------------------------------------------------------------------


def parse_vtb_infobasket_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetSeasonsForTag`` -> one row per season of a competition tag: END year -> season-level ``CompID``.

    Args:
        raw: the runtime's body (``items`` list) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per season: ``season`` (END year), ``comp_id`` (the season-level CompID every other route is keyed by),
        the season name in Russian and English, ``db`` and the ``tag`` asked for. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_seasons
            parse_vtb_infobasket_seasons([{"SeasonYear": 2026, "NameEn": "Season 2025/2026", "CompID": 50714, "Db": "org"}])
    """
    tag = F.request(raw).get("tag")
    data = [
        {
            "season": r.get("SeasonYear"),
            "comp_id": r.get("CompID"),
            "name_ru": r.get("NameRu"),
            "name_en": r.get("NameEn"),
            "db": r.get("Db"),
            "tag": tag,
        }
        for r in F.items(raw)
    ]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_seasons")


def _game_row(r: Mapping[str, Any], ctx: Mapping[str, Any]) -> Dict[str, Any]:
    return F.tidy({**ctx, **F.flat(r, skip=_GAME_SKIP)})


def parse_vtb_infobasket_calendar(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetCalendar/`` -> one row per game of a season competition (finished, live and planned).

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game: ``game_id``, ``game_number``, date and time (``game_date`` ISO date, ``game_time`` and
        ``game_time_msk`` as ``HH:MM`` strings, ``game_date_time`` / ``game_date_time_moscow`` ISO strings with the
        +03:00 offset), ``game_status`` (0 planned, 1 completed, 2 live), the two teams (``team_a_id`` = home, ``team_b_id``
        = away; names and short names in both languages), ``score_a`` / ``score_b``, the venue, attendance and stage
        name, plus the stamped ``season`` and season ``comp_id``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_calendar
            parse_vtb_infobasket_calendar([{"GameID": 921391, "GameDate": "28.09.2025", "TeamAid": 2650, "TeamBid": 1390}])
    """
    req = F.request(raw)
    ctx = {"season": req.get("season"), "comp_id": req.get("season_comp_id")}
    data = [_game_row(r, ctx) for r in F.items(raw)]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_calendar")


def parse_vtb_infobasket_team_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamGames/{teamId}`` -> one row per game of a club in a season competition (the calendar's row shape).

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        The columns of :func:`parse_vtb_infobasket_calendar` plus the requested ``team_id``. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_games
            parse_vtb_infobasket_team_games([{"GameID": 921391, "TeamAid": 2650, "TeamBid": 1390}])
    """
    req = F.request(raw)
    ctx = {"team_id": req.get("team_id"), "season": req.get("season"), "comp_id": req.get("season_comp_id")}
    data = [_game_row(r, ctx) for r in F.items(raw)]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_games")


# --- season totals ------------------------------------------------------------------------------------------------------


def _totals_row(node: Mapping[str, Any], ctx: Mapping[str, Any]) -> Dict[str, Any]:
    """A team-totals record (team stats page, preview, team season stats): scalars plus the team names."""
    return F.tidy({**ctx, **F.flat(node, skip=_STAT_SKIP), **_names(node.get("TeamName"), "team_")}, counts=True)


def parse_vtb_infobasket_team_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetTeamStats/{compId}`` -> one row per team: the season totals of a season competition (all stages).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``{"Stats": [...]}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team: ``team_id``, names, ``game_count``, the shooting totals (``shot*`` attempts, ``goal*`` made;
        ``shot23`` / ``goal23`` field goals), rebounds, assists, steals, blocks, turnovers, fouls, ``kpi``, ``seconds``,
        ``plus_minus`` and the per-game ``avg_*`` columns as floats, plus the stamped ``season``. The team's players and
        coaches are ``team_stats_players`` / ``team_stats_coaches``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_stats
            parse_vtb_infobasket_team_stats({"Stats": [{"TeamID": 2650, "GameCount": 50, "Points": 4000}]})
    """
    ctx = {"season": F.request(raw).get("season")}
    data = [_totals_row(t, ctx) for t in F.rows(F.obj(raw).get("Stats"))]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_stats")


def _people_rows(
    teams: List[Mapping[str, Any]], key: str, ctx: Mapping[str, Any], *, player: bool
) -> List[Dict[str, Any]]:
    out = []
    for t in teams:
        for p in F.rows(t.get(key)):
            row = {**ctx, "team_id": t.get("TeamID"), **F.flat(p, skip=_STAT_SKIP | _PERSON_SKIP)}
            row.update(_person(p.get("PersonInfo"), bio=player))
            out.append(F.tidy(row, counts=True))
    return out


def parse_vtb_infobasket_team_stats_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetTeamStats/{compId}`` -> one row per player: the season totals of every team's players.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``{"Stats": [...]}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player and team: ``team_id``, ``person_id``, ``player_number``, ``is_active``, the person's names, birth
        date, height, weight and position, and the same statistics as :func:`parse_vtb_infobasket_team_stats` (null counts
        filled with 0). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_stats_players
            parse_vtb_infobasket_team_stats_players({"Stats": [{"TeamID": 2650, "Players": [{"PersonID": 423011}]}]})
    """
    ctx = {"season": F.request(raw).get("season")}
    data = _people_rows(F.rows(F.obj(raw).get("Stats")), "Players", ctx, player=True)
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_stats_players")


def parse_vtb_infobasket_team_stats_coaches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetTeamStats/{compId}`` -> one row per coach: the technical fouls and games of every team's coaches.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``{"Stats": [...]}`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per coach and team: ``team_id``, ``person_id``, ``display_number`` (1 = head coach), ``is_active``, the
        coach's names, ``game_count`` and the technical-foul counts ``foul_c`` / ``foul_b`` / ``foul_d`` / ``foul``. Coaches'
        birth data is never output. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_stats_coaches
            parse_vtb_infobasket_team_stats_coaches({"Stats": [{"TeamID": 2650, "Coaches": [{"PersonID": 131047}]}]})
    """
    ctx = {"season": F.request(raw).get("season")}
    data = _people_rows(F.rows(F.obj(raw).get("Stats")), "Coaches", ctx, player=False)
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_stats_coaches")


def _leader_rows(raw: Any, team: bool) -> List[Dict[str, Any]]:
    ctx = {"season": F.request(raw).get("season")}
    out = []
    for item in F.items(raw):
        param = F.obj(item.get("Parameter"))
        base = {
            **ctx,
            "param_id": param.get("ParamID"),
            "param_name": param.get("ParamName"),
            "param_short_name": param.get("ShortName"),
            "param_label": param.get("Name"),
            "param_group": param.get("ParamGroup"),
        }
        for r in F.rows(item.get("Results")):
            row = {**base, "rank": r.get("Rank")}
            tn = F.obj(r.get("TeamName"))
            row["team_id"] = tn.get("TeamID")
            row.update(_names(tn, "team_"))
            if not team:
                person = F.obj(r.get("Person"))
                row["person_id"] = person.get("PersonID")
                row.update(_person(person, bio=False))
            game = F.obj(r.get("Game"))
            row.update(
                game_id=game.get("GameID"),
                game_count=r.get("GameCount"),
                res=r.get("Res"),
                pre_res=r.get("PreRes"),
                is_same=r.get("IsSame"),
            )
            out.append(F.tidy(row))
    return out


def parse_vtb_infobasket_best_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetBestPlayers/{compId}`` -> one row per leader of each leader category (player leaders).

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per leader: the category (``param_id``, ``param_name``, ``param_short_name``, ``param_label``), ``rank``,
        ``person_id`` and names, the club (``team_id`` and names), ``game_id`` (set for single-game categories),
        ``game_count``, ``res`` (the leading value as a float), ``pre_res`` and ``is_same`` (tied with the row above). Birth
        data is not output. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_best_players
            parse_vtb_infobasket_best_players([{"Parameter": {"ParamID": 2}, "Results": [{"Rank": 1, "Res": 17.58}]}])
    """
    return _fin(_leader_rows(raw, False), columns, return_as_pandas, "parse_vtb_infobasket_best_players")


def parse_vtb_infobasket_best_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetBestTeams/{compId}`` -> one row per leader of each leader category (team leaders).

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per leading team: the category (``param_id``, ``param_name``, ``param_short_name``, ``param_label``),
        ``rank``, the team (``team_id`` and names), ``game_id``, ``game_count``, ``res`` (float), ``pre_res`` and
        ``is_same``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_best_teams
            parse_vtb_infobasket_best_teams([{"Parameter": {"ParamID": 2}, "Results": [{"Rank": 1, "Res": 93.2}]}])
    """
    return _fin(_leader_rows(raw, True), columns, return_as_pandas, "parse_vtb_infobasket_best_teams")


def parse_vtb_infobasket_team_preview(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Comp/GetTeamStatsForPreview/{gameId}`` -> the season totals of a game's two teams (pre-game preview).

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        Two rows (home, then away) with the columns of :func:`parse_vtb_infobasket_team_stats` (without the team's
        players) plus the requested ``game_id``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_preview
            parse_vtb_infobasket_team_preview([{"TeamID": 2650, "GameCount": 50}, {"TeamID": 1390, "GameCount": 50}])
    """
    ctx = {"game_id": F.request(raw).get("game_id")}
    data = [_totals_row(t, ctx) for t in F.items(raw)]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_preview")


# --- one game (GetOnline) -----------------------------------------------------------------------------------------------


def _online_game_id(raw: Any) -> Optional[int]:
    gid = F.int_or_none(F.obj(F.obj(raw).get("Online")).get("GameID"))
    return gid if gid is not None else F.int_or_none(F.request(raw).get("game_id"))


def _teams(raw: Any) -> List[Mapping[str, Any]]:
    return F.rows(F.obj(raw).get("GameTeams"))


def parse_vtb_infobasket_game(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetOnline/{gameId}`` -> the game header: one row with the stage, venue, rules and the two teams' scores.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GetOnline`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``game_id`` (from ``Online.GameID``), ``game_status`` (1 completed), the stage ``comp_id`` and names, the
        league names, ``season_comp_id`` (the season-level CompID the body calls ``LeagueID``), date and time,
        ``game_number``, venue, attendance, the rules (period count and lengths, breaks, foul limits, shot values), the
        two teams (``team_a_id`` / ``team_b_id``, names, ``score_a`` / ``score_b``) and the TV text. Zero rows for an empty
        or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_game
            parse_vtb_infobasket_game({"GameStatus": 1, "Online": {"GameID": 921391}, "GameTeams": []})
    """
    body = F.obj(raw)
    on = F.obj(body.get("Online"))
    if not body or not on:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_game")
    skip = {"GameTeams", "Online", "OnlineTeams", "OnlineStarts", "OnlinePlays", "OnlinePeriods", "VideoID", "LiveID"}
    row = F.flat(body, skip=skip)
    row["season_comp_id"] = row.pop("league_id", None)
    row.update(
        game_id=_online_game_id(raw),
        game_number=on.get("GameNumber"),
        game_date_time=F.conv(on.get("GameTime")),
        region_id=on.get("RegionID"),
        arena_id=on.get("ArenaID"),
        venue_en=on.get("Venue1"),
        venue_ru=on.get("Venue2"),
        attendance=on.get("Attendance"),
        period_count=on.get("PeriodCount"),
        period_extra_seconds=on.get("PeriodExtraSeconds"),
        period_minutes=on.get("Periods"),
        intervals_minutes=on.get("Intervals"),
        personal_fouls=on.get("PersonalFouls"),
        team_fouls=on.get("TeamFouls"),
        players_on_court=on.get("PlayersOnCourt"),
        free_throw_value=on.get("FreeThrowValue"),
        field_goal_value=on.get("FieldGoalValue"),
        long_shot_value=on.get("LongShotValue"),
        left_team_number=on.get("LeftTeamNumber"),
    )
    for t, side in zip(_teams(raw)[:2], ("a", "b")):
        row[f"team_{side}_id"] = t.get("TeamID")
        row[f"score_{side}"] = t.get("Score")
        row.update(_names(t.get("TeamName"), f"team_{side}_"))
    return _fin([F.tidy(row)], columns, return_as_pandas, "parse_vtb_infobasket_game")


def parse_vtb_infobasket_box_score(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetOnline/{gameId}`` -> the player box score: one row per player of each team (DNPs included).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GetOnline`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: ``game_id``, ``team_id``, ``team_number`` (1 = the team listed first, the home side in the
        calendar; the body has no explicit home flag, unverified), ``is_home``, ``person_id``, names, ``birth_date`` (a
        player's public bio), ``height``, ``weight``, nationality, the shooting line (``shot*`` attempts, ``goal*`` made),
        rebounds, assists, steals, blocks, turnovers, fouls, ``plus_minus``, ``seconds``, ``played_time`` and the
        ``*_percent`` columns as floats. Null counts are 0; a player who did not play has ``seconds`` 0. The paint
        statistics are dropped (broken in the source). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_box_score
            parse_vtb_infobasket_box_score({"Online": {"GameID": 921391}, "GameTeams": [{"TeamID": 2650, "TeamNumber": 1, "Players": [{"PersonID": 423011}]}]})
    """
    gid = _online_game_id(raw)
    data = []
    for t in _teams(raw):
        for p in F.rows(t.get("Players")):
            row = {
                "game_id": gid,
                "team_id": t.get("TeamID"),
                "is_home": t.get("TeamNumber") == 1,
                **F.flat(p, birth=True),
            }
            data.append(F.tidy(row, counts=True))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_box_score")


def parse_vtb_infobasket_team_box_score(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetOnline/{gameId}`` -> the team box score: one row per team, with the head coach's technical fouls.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GetOnline`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        Two rows: ``game_id``, ``team_id``, ``team_number``, ``is_home``, names, ``score`` / ``points``, the team totals (as
        in the player box score, plus the team-credited ``team_*`` rebounds, steals and turnovers), ``foul`` committed and
        ``opponent_foul`` drawn, ``seconds`` and ``played_time``, and the head coach (``coach_person_id``, names and the
        ``coach_foul*`` technical-foul counts; no birth data). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_box_score
            parse_vtb_infobasket_team_box_score({"Online": {"GameID": 921391}, "GameTeams": [{"TeamID": 2650, "TeamNumber": 1, "Score": 85}]})
    """
    gid = _online_game_id(raw)
    data = []
    for t in _teams(raw):
        coach = F.obj(t.get("Coach"))
        row = {
            "game_id": gid,
            "is_home": t.get("TeamNumber") == 1,
            **F.flat(t, skip={"Players", "Coaches", "Coach", "TeamName"}),
            **_names(t.get("TeamName"), "team_"),
            "coach_person_id": coach.get("PersonID"),
            "coach_name_en": coach.get("PersonNameEn"),
            "coach_name_ru": coach.get("PersonNameRu"),
            "coach_foul_c": coach.get("FoulC"),
            "coach_foul_b": coach.get("FoulB"),
            "coach_foul_d": coach.get("FoulD"),
            "coach_foul": coach.get("Foul"),
        }
        data.append(F.tidy(row, counts=True))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_box_score")


def parse_vtb_infobasket_pbp(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetOnline/{gameId}`` -> the play-by-play, one row per play (a tree: join ``parent_play_id`` to ``play_id``).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GetOnline`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per play: ``game_id``, ``play_id``, ``parent_play_id`` (a made shot has children for the shot type, the
        assist and the possession change; a miss has the rebound; a foul has the free throws awarded), ``play_type_id`` with
        its widget label ``play_type``, ``play_period``, ``play_second`` (tenths of a second ELAPSED in the period; 6000 =
        the end of a 10-minute quarter), ``play_sort_order``, ``start_id`` and the ``person_id`` / ``team_id`` /
        ``team_number`` it resolves to in the line-up, the shot ``play_zone``, ``play_x`` / ``play_y`` (decimetres from the
        basket centre; see ``vtb-returns.md``; orientation unverified) and the video clip window. Play-by-play exists from
        2022-23 only: earlier games give zero rows. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_pbp
            parse_vtb_infobasket_pbp({"Online": {"GameID": 921391}, "OnlinePlays": [{"PlayID": 1, "PlayTypeID": 2}]})
    """
    body = F.obj(raw)
    gid = _online_game_id(raw)
    team_ids = {t.get("TeamNumber"): t.get("TeamID") for t in F.rows(body.get("OnlineTeams")) if t.get("TeamNumber")}
    slots = {s.get("StartID"): s for s in F.rows(body.get("OnlineStarts")) if s.get("StartID")}
    data = []
    for p in F.rows(body.get("OnlinePlays")):
        slot = slots.get(p.get("StartID"), {})
        tn = slot.get("TeamNumber") if slot.get("StartType") in (0, 1) else None
        ptype = F.int_or_none(p.get("PlayTypeID"))
        known = PLAY_TYPES.get(ptype) if ptype is not None else None
        label = known[0] if known else None
        data.append(
            {
                "game_id": gid,
                "play_id": p.get("PlayID"),
                "parent_play_id": p.get("ParentPlayID"),
                "play_type_id": p.get("PlayTypeID"),
                "play_type": label,
                "play_period": p.get("PlayPeriod"),
                "play_second": p.get("PlaySecond"),
                "play_sort_order": p.get("PlaySortOrder"),
                "play_flag": p.get("PlayFlag"),
                "start_id": p.get("StartID"),
                "team_number": tn if tn else None,
                "team_id": team_ids.get(tn) if tn else None,
                "person_id": slot.get("PersonID") if slot.get("StartType") == 1 and slot.get("PersonID") else None,
                "play_zone": p.get("PlayZone"),
                "play_x": p.get("PlayX"),
                "play_y": p.get("PlayY"),
                "video_from": p.get("VideoFrom"),
                "video_to": p.get("VideoTo"),
            }
        )
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_pbp")


def parse_vtb_infobasket_game_staff(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetOnline/{gameId}`` -> the game's coaches and officials, one row each (names and roles only).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GetOnline`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per coach (``start_type`` 3) and official (4) of the line-up: ``game_id``, ``team_number`` / ``team_id``
        (0 / null for the neutral officials), ``person_id``, ``start_id``, ``role`` (head coach, assistant coach, referee,
        commissioner, table official), ``pos_id``, names and nationality. Birth dates, ages and licence numbers are never
        output (personal data). Officials appear from 2022-23 in the sampled games. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_game_staff
            parse_vtb_infobasket_game_staff({"Online": {"GameID": 921391}, "OnlineStarts": [{"StartType": 3, "PosID": 1000, "PersonID": 131047}]})
    """
    body = F.obj(raw)
    gid = _online_game_id(raw)
    team_ids = {t.get("TeamNumber"): t.get("TeamID") for t in F.rows(body.get("OnlineTeams")) if t.get("TeamNumber")}
    data = []
    for s in F.rows(body.get("OnlineStarts")):
        if s.get("StartType") not in (3, 4):
            continue
        tn = s.get("TeamNumber")
        data.append(
            {
                "game_id": gid,
                "team_number": tn,
                "team_id": team_ids.get(tn) if tn else None,
                "start_id": s.get("StartID"),
                "start_type": s.get("StartType"),
                "role": staff_role(s.get("StartType"), s.get("PosID")),
                "pos_id": s.get("PosID"),
                "person_id": s.get("PersonID"),
                "person_name_en": s.get("PersonName1"),
                "person_name_ru": s.get("PersonName2"),
                "country_code_ioc": s.get("CountryCodeIOC"),
            }
        )
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_game_staff")


def parse_vtb_infobasket_game_page(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GamePage/{gameId}`` -> the thin game header (use ``vtb_infobasket_game`` for teams, box score and plays).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``GamePage`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``game_id``, ``game_number``, date and time, ``game_status``, the region and arena ids with their names,
        the rules code, attendance and forfeit flag. ``comp_id`` is 0 and every list (teams, plays, referees) is empty on
        this route. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_game_page
            parse_vtb_infobasket_game_page({"GameID": 921391, "GameStatus": 1, "GameDate": "/Date(1759006800000)/"})
    """
    body = F.obj(raw)
    if not body or "GameID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_game_page")
    region, arena = F.obj(body.get("Region")), F.obj(body.get("Arena"))
    row = F.flat(
        body,
        skip={
            "Region",
            "Arena",
            "VideoID",
            "LiveID",
            "GameIDnew",
            "GameDateTimeDB",
            "CompTeamA",
            "CompTeamB",
            "TeamAid",
            "TeamBid",
        },
    )
    row.update(
        region_name_ru=region.get("RegionNameRu"),
        region_name_en=region.get("RegionNameEn"),
        arena_name_ru=arena.get("ArenaNameRu"),
        arena_name_en=arena.get("ArenaNameEn"),
        arena_full_name_ru=arena.get("ArenaFullNameRu"),
        arena_full_name_en=arena.get("ArenaFullNameEn"),
    )
    return _fin([F.tidy(row)], columns, return_as_pandas, "parse_vtb_infobasket_game_page")


# --- stages, standings, brackets ----------------------------------------------------------------------------------------


def _stage_rows(comps: List[Mapping[str, Any]], level: int, base: Mapping[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for c in comps:
        row = F.tidy({**base, "level": level, **F.flat(c, skip={"Children"})})
        out.append(row)
        out.extend(_stage_rows(F.rows(c.get("Children")), level + 1, base))
    return out


def parse_vtb_infobasket_comp_issue(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/CompIssue/{compId}`` -> the stage tree of a season competition, one row per stage.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``CompIssue`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per stage at any depth: ``comp_id`` (the stage id the standings, cross-table and playoff wrappers take),
        ``parent_comp_id``, ``level`` (1 = a stage of the season, 2 = a series or group), type, sort order, round, names
        in both languages, state and ``is_public``, plus the season's ``season_comp_id`` and END-year ``season``. Zero rows
        for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_comp_issue
            parse_vtb_infobasket_comp_issue({"ParentCompID": 50714, "Comps": [{"CompID": 50720, "CompIDparent": 50714}]})
    """
    body = F.obj(raw)
    req = F.request(raw)
    base = {"season": req.get("season"), "season_comp_id": F.int_or_none(body.get("ParentCompID"))}
    return _fin(
        _stage_rows(F.rows(body.get("Comps")), 1, base), columns, return_as_pandas, "parse_vtb_infobasket_comp_issue"
    )


def parse_vtb_infobasket_standings(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/CompTeamResults/{compId}`` -> the standings of a stage, one row per team.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per team: the stage ``comp_id``, ``comp_team_id`` (the team's registration in this stage, not a team id),
        ``team_id``, ``place``, ``start_place``, names, and the table (``standing_game``, ``standing_win``,
        ``standing_lose``, ``standing_points``, ``standing_goal_plus`` / ``_minus`` = points for / against,
        ``standing_goal_difference``, ``victory_percent`` and the average points for and against as floats). The per-team
        game list is not output (it is ``vtb_infobasket_team_games``). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_standings
            parse_vtb_infobasket_standings([{"CompID": 50720, "TeamID": 15, "Place": 1, "Standings": {"StandingGame": 40}}])
    """
    data = []
    for r in F.items(raw):
        row = {
            **F.flat(r, skip={"Results", "CompTeamName", "Standings"}),
            **F.flat(F.obj(r.get("Standings")), skip={"CompTeamStandingID", "CompTeamID", "CompTeam"}),
            **_names(r.get("CompTeamName"), "team_"),
        }
        data.append(F.tidy(row))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_standings")


def parse_vtb_infobasket_cross_table(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/CrossTable/{compId}`` -> the head-to-head grid of a round-robin stage, one row per game and side.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game side: ``game_id``, ``game_status`` (21 = completed in this code set), ``score`` and its two parts
        ``score1`` / ``score2``, ``plus_minus``, ``game_date`` (day and month only, no year), ``team1_id`` / ``team2_id``, the
        places, ``home_team`` (1 or 2) and ``win_team``. Every game appears twice, once from each side's view. Zero rows for
        an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_cross_table
            parse_vtb_infobasket_cross_table([{"GameID": 921391, "Score": "85:86", "Team1id": 2650, "Team2id": 1390}])
    """
    comp = F.request(raw).get("comp_id")
    data = [_cross_row(r, {"comp_id": comp}) for r in F.items(raw)]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_cross_table")


def _cross_row(r: Mapping[str, Any], ctx: Mapping[str, Any]) -> Dict[str, Any]:
    row = F.tidy({**ctx, **F.flat(r)})
    score = r.get("Score")
    parts = score.split(":") if isinstance(score, str) else []
    if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
        row["score1"], row["score2"] = int(parts[0]), int(parts[1])
    return row


def parse_vtb_infobasket_playoff(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayOff/{compId}`` -> the playoff bracket of a stage, one row per series.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per series: the stage ``comp_id``, ``tree``, ``round`` (4 = quarter-final), ``sort``, the round name,
        ``is_current_round``, the two teams (``team1_id`` / ``team2_id`` and names), the series score
        (``score1`` / ``score2``), ``is_final`` and ``winner`` (1 or 2). The games are ``playoff_games``. Zero rows for an
        empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_playoff
            parse_vtb_infobasket_playoff([{"Tree": 1, "Round": 4, "Score1": 3, "Score2": 0, "Scores": []}])
    """
    comp = F.request(raw).get("comp_id")
    data = []
    for r in F.items(raw):
        row = {
            "comp_id": comp,
            **F.flat(r, skip={"Scores", "TeamName1", "TeamName2", "Score1String", "Score2String"}),
            "team1_id": F.obj(r.get("TeamName1")).get("TeamID"),
            **_names(r.get("TeamName1"), "team1_"),
            "team2_id": F.obj(r.get("TeamName2")).get("TeamID"),
            **_names(r.get("TeamName2"), "team2_"),
        }
        data.append(F.tidy(row))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_playoff")


def parse_vtb_infobasket_playoff_games(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayOff/{compId}`` -> the games of every series of a playoff stage, one row per game.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game: the series key (``tree``, ``round``, ``sort``), then the columns of
        :func:`parse_vtb_infobasket_cross_table` (``game_id``, ``score`` with ``score1`` / ``score2``, ``plus_minus``,
        ``game_date`` ISO date, ``team1_id`` / ``team2_id``, places, ``home_team``, ``win_team``). Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_playoff_games
            parse_vtb_infobasket_playoff_games([{"Tree": 1, "Round": 4, "Sort": 1, "Scores": [{"GameID": 1058377, "Score": "104:66"}]}])
    """
    comp = F.request(raw).get("comp_id")
    data: List[Dict[str, Any]] = []
    for r in F.items(raw):
        key = {"comp_id": comp, "tree": r.get("Tree"), "round": r.get("Round"), "sort": r.get("Sort")}
        data.extend(_cross_row(g, key) for g in F.rows(r.get("Scores")))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_playoff_games")


def _codes(raw: Any, parser: str, columns: Optional[Sequence[str]], pandas: bool) -> Frame:
    data = [{"param": r.get("param"), "name": r.get("name")} for r in F.items(raw)]
    return _fin(data, columns, pandas, parser)


def parse_vtb_infobasket_best_player_parameters(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetBestPlayerParameters/`` -> the player leader categories (a code table).

    Args:
        raw: the runtime's body (``items`` list) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per category: ``param`` (the code the leader widget takes) and ``name``. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_best_player_parameters
            parse_vtb_infobasket_best_player_parameters([{"param": "Points", "name": "Points per game"}])
    """
    return _codes(raw, "parse_vtb_infobasket_best_player_parameters", columns, return_as_pandas)


def parse_vtb_infobasket_best_team_parameters(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetBestTeamParameters/`` -> the team leader categories (a code table).

    Args:
        raw: the runtime's body (``items`` list) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per category: ``param`` (the code the leader widget takes) and ``name``. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_best_team_parameters
            parse_vtb_infobasket_best_team_parameters([{"param": "Points", "name": "Points per game"}])
    """
    return _codes(raw, "parse_vtb_infobasket_best_team_parameters", columns, return_as_pandas)


# --- people -------------------------------------------------------------------------------------------------------------


def _person_row(body: Mapping[str, Any], *, bio: bool) -> Dict[str, Any]:
    row = F.flat(body, skip=_PERSON_SKIP | {"SeasonStats", "GameStats"}, birth=bio)
    first = next(iter(F.rows(body.get("Players"))), {})
    pos = F.obj(first.get("Position"))
    row.update(
        player_position=first.get("PlayerPosition"),
        player_hand=first.get("PlayerHand"),
        is_naturalized=first.get("IsNaturalized"),
        position_id=pos.get("PosID"),
        position_name_ru=pos.get("PosNameRu"),
        position_name_en=pos.get("PosNameEn"),
    )
    if not bio:
        row = {k: v for k, v in row.items() if k not in ("player_position", "player_hand", "is_naturalized")}
        for k in ("position_id", "position_name_ru", "position_name_en"):
            row.pop(k, None)
    return row


def parse_vtb_infobasket_player(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayerPage/{personId}`` -> a player profile, one row.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``PlayerPage`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``person_id``, names (Russian and English: last, first, full), ``person_gender``, ``birth_date`` (public
        bio), ``height``, ``weight``, ``person_status``, ``player_position`` (code) and ``position_id`` with its names,
        ``player_hand``, ``is_naturalized``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_player
            parse_vtb_infobasket_player({"PersonID": 423011, "PersonLastNameEn": "Miller", "PersonBirth": "22.05.1992"})
    """
    body = F.obj(raw)
    if not body or "PersonID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_player")
    return _fin([F.tidy(_person_row(body, bio=True))], columns, return_as_pandas, "parse_vtb_infobasket_player")


def parse_vtb_infobasket_coach(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/CoachPage/{personId}`` -> a coach profile, one row (no birth data: personal data of staff).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``CoachPage`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``person_id``, names (Russian and English), ``person_gender``, ``height``, ``weight`` and ``person_status``.
        Birth date, birth year and age are never output. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_coach
            parse_vtb_infobasket_coach({"PersonID": 131047, "PersonLastNameEn": "Yudin"})
    """
    body = F.obj(raw)
    if not body or "PersonID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_coach")
    return _fin([F.tidy(_person_row(body, bio=False))], columns, return_as_pandas, "parse_vtb_infobasket_coach")


def parse_vtb_infobasket_player_season_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayerSeasonStats/{personId}`` -> a player's season-by-season totals, one row per season.

    Args:
        raw: the runtime's body (with the request stamp) or the bare body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per season: ``person_id``, ``season`` (END year; the wire sends it as ``Season.CompID``, a third meaning of
        CompID), ``season_name_ru`` / ``season_name_en``, games played and started, the totals and the per-game ``avg_*``
        columns as floats (null counts are 0). The career row is ``player_career_stats``. Zero rows for an empty or
        malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_player_season_stats
            parse_vtb_infobasket_player_season_stats({"PersonID": 423011, "SeasonStats": [{"Season": {"CompID": 2026}, "Points": 753}]})
    """
    body = F.obj(raw)
    data = []
    for s in F.rows(body.get("SeasonStats")):
        season = F.obj(s.get("Season"))
        row = {
            "person_id": body.get("PersonID"),
            "season": season.get("CompID"),
            "season_name_ru": season.get("CompShortNameRu"),
            "season_name_en": season.get("CompShortNameEn"),
            **_names(s.get("TeamName"), "team_"),
            **F.flat(s, skip={"Season", "PersonID", "TeamName"}),
        }
        data.append(F.tidy(row, counts=True))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_player_season_stats")


def parse_vtb_infobasket_player_career_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayerSeasonStats/{personId}`` -> a player's career totals in the competition tag, one row.

    Args:
        raw: the runtime's body (with the request stamp) or the bare body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``person_id``, ``has_more_seasons``, games played and started, the career totals and the per-game
        ``avg_*`` columns as floats. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_player_career_stats
            parse_vtb_infobasket_player_career_stats({"PersonID": 423011, "GameCount": 126, "Points": 1910})
    """
    body = F.obj(raw)
    if not body or "PersonID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_player_career_stats")
    row = F.tidy(F.flat(body, skip={"SeasonStats"}), counts=True)
    return _fin([row], columns, return_as_pandas, "parse_vtb_infobasket_player_career_stats")


def parse_vtb_infobasket_player_game_log(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/PlayerStats/{personId}`` -> a player's game log for one season competition and team, one row per game.

    Args:
        raw: the runtime's body (with the request stamp) or the bare body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per game: ``person_id`` (from the body: the rows carry 0), ``game_id``, ``game_number``, ``game_date``,
        ``game_status`` (21 = completed in this code set), the two teams (``team_a_id`` / ``team_b_id``, names and
        ``score_a`` / ``score_b``), the player's ``team_id`` / ``team_number``, ``is_start`` and the box-score line (null
        counts are 0) with ``kpi``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_player_game_log
            parse_vtb_infobasket_player_game_log({"PersonID": 423011, "GameStats": [{"Game": {"GameID": 921391}, "Points": 10}]})
    """
    body = F.obj(raw)
    data = []
    for g in F.rows(body.get("GameStats")):
        game = F.obj(g.get("Game"))
        by_number = {t.get("GameTeamNumber"): t for t in F.rows(game.get("GameTeams"))}
        mine = by_number.get(g.get("TeamNumber"), {})
        a, b = F.obj(g.get("TeamNameA")), F.obj(g.get("TeamNameB"))
        row = {
            "person_id": body.get("PersonID"),
            "game_id": game.get("GameID"),
            "game_number": game.get("GameNumber"),
            "game_status": game.get("GameStatus"),
            "team_id": mine.get("TeamID"),
            "team_a_id": a.get("TeamID"),
            "team_b_id": b.get("TeamID"),
            "score_a": F.obj(by_number.get(1)).get("GameTeamScore"),
            "score_b": F.obj(by_number.get(2)).get("GameTeamScore"),
            **_names(a, "team_a_"),
            **_names(b, "team_b_"),
            **F.flat(g, skip={"Game", "TeamNameA", "TeamNameB", "PersonID"}),
        }
        data.append(F.tidy(row, counts=True))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_player_game_log")


# --- teams --------------------------------------------------------------------------------------------------------------


def parse_vtb_infobasket_team(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamPage/{teamId}`` -> a club profile, one row.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamPage`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row: ``team_id``, ``team_type``, ``team_gender``, the names (Russian and English: full, short and abbreviated
        as the host sends them), region and country codes, logo and uniform codes, ``team_status`` and
        ``team_record_id`` (the id of the club-record name, a different key space from ``team_id``). Zero rows for an empty
        or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team
            parse_vtb_infobasket_team({"TeamID": 2650, "TeamNameEn": "Lokomotiv Kuban", "CurrentTeamName": {"TeamID": 2259}})
    """
    body = F.obj(raw)
    if not body or "TeamID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_team")
    skip = {
        "CurrentTeamName", "CompTeamNames", "TeamCoaches", "TeamPlayers", "TeamColors", "Organization",
        "TeamTypeIcon", "TeamArena", "TeamOrg", "TeamPlayersBirthYear", "TeamType1",
    }  # fmt: skip
    row = {**F.flat(body, skip=skip), "team_record_id": F.obj(body.get("CurrentTeamName")).get("TeamID")}
    return _fin([F.tidy(row)], columns, return_as_pandas, "parse_vtb_infobasket_team")


def parse_vtb_infobasket_team_seasons(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetTeamSeasons/{teamId}`` -> the seasons a club has data for, one row per season.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per season: ``team_id``, ``season`` (END year), ``comp_id`` and ``season_name``. The ids are the club's
        ``rfb`` tag season ids (56293 for 2027), not the VTB tag's (55613). Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_seasons
            parse_vtb_infobasket_team_seasons([{"CompID": 56293, "Year": 2027, "SeasonName": "Season 2026/2027"}])
    """
    team = F.request(raw).get("team_id")
    data = [
        {"team_id": team, "season": r.get("Year"), "comp_id": r.get("CompID"), "season_name": r.get("SeasonName")}
        for r in F.items(raw)
    ]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_seasons")


def parse_vtb_infobasket_team_comps(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/GetTeamComps/{teamId}`` -> the competitions a club played in one season, one row each.

    Args:
        raw: the runtime's body (``items`` list, with the request stamp) or a bare list.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per competition: ``team_id``, the stamped ``season``, ``comp_id``, ``parent_comp_id``, type, sort order,
        level and the names in Russian and English. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_comps
            parse_vtb_infobasket_team_comps([{"CompID": 50720, "CompNameEn": "Regular Season", "ParentCompID": 50714}])
    """
    req = F.request(raw)
    ctx = {"team_id": req.get("team_id"), "season": req.get("season")}
    data = [F.tidy({**ctx, **F.flat(r)}) for r in F.items(raw)]
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_comps")


def parse_vtb_infobasket_roster(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamRoster/{teamId}`` -> a club's player roster in one season competition, one row per player.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamRoster`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per player: ``team_id``, the stamped ``season``, ``person_id``, names, shirt numbers, ``is_captain``,
        ``birth_date`` (public bio), ``pos_id`` and ``position``, nationality and country codes, ``height``, ``weight``,
        ``is_active`` and ``active_status``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_roster
            parse_vtb_infobasket_roster({"TeamID": 2650, "Players": [{"PersonID": 423011, "PosID": 3}]})
    """
    body = F.obj(raw)
    ctx = {"team_id": body.get("TeamID"), "season": F.request(raw).get("season")}
    data = []
    for p in F.rows(body.get("Players")):
        row = {
            **ctx,
            **F.flat(p, skip={"PersonInfo", "PhotoUrl"}, birth=True),
            **_person(p.get("PersonInfo"), bio=False),
        }
        data.append(F.tidy(row))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_roster")


def parse_vtb_infobasket_roster_coaches(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamRoster/{teamId}`` -> a club's coaches in one season competition, one row per coach (no birth data).

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamRoster`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row per coach: ``team_id``, the stamped ``season``, ``person_id``, names, ``coach_number``, ``post`` (head or
        assistant coach), nationality codes, ``is_active`` and ``active_status``. Birth dates and ages are never output.
        Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_roster_coaches
            parse_vtb_infobasket_roster_coaches({"TeamID": 2650, "Coaches": [{"PersonID": 131047, "Post": "Head Coach"}]})
    """
    body = F.obj(raw)
    ctx = {"team_id": body.get("TeamID"), "season": F.request(raw).get("season")}
    data = []
    for c in F.rows(body.get("Coaches")):
        row = {**ctx, **F.flat(c, skip={"PersonInfo", "PhotoUrl"}), **_person(c.get("PersonInfo"), bio=False)}
        data.append(F.tidy(row))
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_roster_coaches")


def parse_vtb_infobasket_team_season_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamStats/{teamId}`` -> a club's season totals in one season competition, one row.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamStats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        One row with the columns of :func:`parse_vtb_infobasket_team_stats`, plus the stamped ``season``. The per-player
        and per-coach lines are ``team_player_stats`` and ``team_coach_stats``. Zero rows for an empty or malformed body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_season_stats
            parse_vtb_infobasket_team_season_stats({"TeamID": 2650, "GameCount": 50, "Points": 4000})
    """
    body = F.obj(raw)
    if not body or "TeamID" not in body:
        return _fin([], columns, return_as_pandas, "parse_vtb_infobasket_team_season_stats")
    ctx = {"season": F.request(raw).get("season")}
    return _fin([_totals_row(body, ctx)], columns, return_as_pandas, "parse_vtb_infobasket_team_season_stats")


def parse_vtb_infobasket_team_player_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamStats/{teamId}`` -> one row per player of the club: season totals in one season competition.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamStats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        The columns of :func:`parse_vtb_infobasket_team_stats_players` for the one club. Zero rows for an empty or malformed
        body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_player_stats
            parse_vtb_infobasket_team_player_stats({"TeamID": 2650, "Players": [{"PersonID": 423011, "Points": 753}]})
    """
    body = F.obj(raw)
    ctx = {"season": F.request(raw).get("season")}
    data = _people_rows([body] if body else [], "Players", ctx, player=True)
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_player_stats")


def parse_vtb_infobasket_team_coach_stats(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """``/Widget/TeamStats/{teamId}`` -> one row per coach of the club: technical fouls and games in one season competition.

    Args:
        raw: the runtime's body (with the request stamp) or the bare ``TeamStats`` body.
        return_as_pandas: return a pandas frame instead of polars.
        columns: documented columns (see the module docstring); defaults to the returns schema.

    Returns:
        The columns of :func:`parse_vtb_infobasket_team_stats_coaches` for the one club. Zero rows for an empty or malformed
        body.

    Raises:
        This parser does not raise on a malformed body; it returns a zero-row frame typed from the returns schema.

    Example:
        Quick start::

            from sportsdataverse.basketball.vtb import parse_vtb_infobasket_team_coach_stats
            parse_vtb_infobasket_team_coach_stats({"TeamID": 2650, "Coaches": [{"PersonID": 131047}]})
    """
    body = F.obj(raw)
    ctx = {"season": F.request(raw).get("season")}
    data = _people_rows([body] if body else [], "Coaches", ctx, player=False)
    return _fin(data, columns, return_as_pandas, "parse_vtb_infobasket_team_coach_stats")
