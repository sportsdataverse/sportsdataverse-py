"""Parsers for the generated ``fiba_gamepage`` wrappers (box score, play-by-play and shots of a FIBA game).

The input is the dict :func:`~sportsdataverse.basketball.fiba_hub.fiba_runtime._get` returns for a game
page: the ``game`` header, ``playByPlay``, ``gameDetails``, ``playersTeamA`` and ``playersTeamB``
objects cut out of fiba.basketball's ``text/x-component`` (Next.js RSC flight) payload, flight tokens
resolved. Every parser follows the package contract: a polars frame by default, pandas with
``return_as_pandas=True``, and a zero-row frame typed from the documented returns-schema for an
empty or malformed body (a page with no play-by-play parses to an empty ``pbp``).

**Teams.** The payload names teams by ORGANISATION id (``T_64``, play-by-play ``oId``): the stable
federation or club, not the per-competition ``team_id``. Each frame carries ``organisation_id`` and
``team_side`` (``A`` / ``B``, the schedule row's team A and team B); the schedule's ``team_id`` is
in ``fiba_gdap_schedule`` (``team_a_team_id``), which joins on ``organisation_id``.

**Players.** ``player_id`` is the global person id: box ``P_<id>``, play-by-play ``pId`` / ``p2Id``,
roster ``personId`` and ``fiba_gdap_roster.player_id`` are the same number. It is ``Int64`` everywhere
(the ``P_``, ``T_`` and ``O_`` prefixes are stripped; a float is never an id).

**Two live-stats systems.** ``statistic_system`` is ``OVR`` (World Cup, Olympics) or ``FLS`` (club
competitions, most qualifiers). They differ in the play-by-play: OVR ``action_id`` looks like
``3:0012``, ``order`` runs 1, 2, ... and ``description`` is mostly null; FLS ``action_id`` is a sequence
number, ``order`` runs 1000, 2000, ... and ``description`` is English. ``player2_id`` (the second player of
a substitution) is filled on some games of either system: the refs note it as FLS-only, but the Paris 2024
final (OVR) carries it and the FLS games sampled leave it null. ``team_bench_points_flag`` (``T_PFB``) is
bench points in FLS and 0 in OVR.

**Coverage.** Box scores start about 2006; play-by-play with shots about 2012 (OVR) and 2017-18
(FLS). ``gamePlayByPlayStatusCode`` on the schedule is unreliable (World Cup 2014 says ``EMPTY`` and has
full play-by-play): decide from the page.

**Shot coordinates** (``shot_x``, ``shot_y``) are integers on a half-court IMAGE with both teams drawn
on one basket, not feet or metres. The scale (about 18.5-19 px per metre by the 3-point arc) and which
sideline is ``x = 0`` are UNVERIFIED. Free throws are always 0, 0 (``has_location`` is False for them).
Percentages are 0-100. ``clock`` is the time REMAINING in the period (``m:ss``, counting down from 10:00).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sportsdataverse.basketball.fiba_hub.fiba_frames import Frame, _finish, _frame, _id

__all__ = [
    "parse_fiba_gamepage_box_players",
    "parse_fiba_gamepage_box_teams",
    "parse_fiba_gamepage_lineup",
    "parse_fiba_gamepage_pbp",
    "parse_fiba_gamepage_period_scores",
    "parse_fiba_gamepage_shots",
]

_SHOT_CODES = {"P2": "2PT", "P3": "3PT", "FT": "FT"}

#: box stat code -> column (player and team lines)
_STATS: Dict[str, str] = {
    "PTS": "points",
    "FGM": "fg_made",
    "FGA": "fg_attempted",
    "FGP": "fg_pct",
    "FG2M": "fg2_made",
    "FG2A": "fg2_attempted",
    "FG2P": "fg2_pct",
    "FG3M": "fg3_made",
    "FG3A": "fg3_attempted",
    "FG3P": "fg3_pct",
    "FTM": "ft_made",
    "FTA": "ft_attempted",
    "FTP": "ft_pct",
    "OR": "reb_offensive",
    "DR": "reb_defensive",
    "REB": "reb_total",
    "AS": "assists",
    "ST": "steals",
    "BS": "blocks",
    "BSR": "blocks_received",
    "TO": "turnovers",
    "PF": "fouls",
    "FD": "fouls_drawn",
    "FGIM": "paint_fg_made",
    "FGIA": "paint_fg_attempted",
    "FGIP": "paint_fg_pct",
    "EFF": "efficiency",
}
#: extra stat codes only a team line carries
_TEAM_STATS: Dict[str, str] = {
    "A_BL": "biggest_lead",
    "A_BLS": "biggest_lead_score",
    "A_BR": "biggest_run",
    "A_BRS": "biggest_run_score",
    "A_FBP": "fast_break_points",
    "A_PAT": "points_off_turnovers",
    "A_PFB": "bench_points",
    "A_PIP": "points_in_paint",
    "A_SCP": "second_chance_points",
    "A_ACT": "a_act",
    "A_FOUL": "a_foul",
    "A_TOD": "a_tod",
    "A_TOT": "a_tot",
    "T_OR": "team_reb_offensive",
    "T_DR": "team_reb_defensive",
    "T_REB": "team_reb_total",
    "T_PF": "team_fouls",
    "T_TO": "team_turnovers",
    "T_PFB": "team_bench_points_flag",
    "T_TL": "time_led",
}
_PLAYER_ONLY = {"TP": "minutes", "PM": "plus_minus", "CB": "cb", "Starter": "is_starter", "HasPlayed": "has_played"}


def _dict(obj: Any) -> Dict[str, Any]:
    return obj if isinstance(obj, dict) else {}


def _list(obj: Any) -> List[Any]:
    return obj if isinstance(obj, list) else []


def _seconds(clock: Any) -> Optional[int]:
    """``m:ss`` -> seconds (``'9:28'`` -> 568); ``None`` for anything else."""
    match = re.fullmatch(r"\s*(\d+):(\d{1,2})\s*", clock) if isinstance(clock, str) else None
    return int(match.group(1)) * 60 + int(match.group(2)) if match else None


class _Game:
    """The ids every row of a page carries: game, competition, statistic system and the two teams."""

    def __init__(self, page: Any) -> None:
        page = _dict(page)
        game, details = _dict(page.get("game")), _dict(page.get("gameDetails"))
        self.game_id = _id(game.get("gameId")) or _id(details.get("id"))
        self.competition_id = _id(_dict(game.get("competition")).get("competitionId"))
        self.system = game.get("statisticSystem")
        self.org = {
            "A": _id(_dict(details.get("teamA")).get("Id")) or _id(_dict(game.get("teamA")).get("organisationId")),
            "B": _id(_dict(details.get("teamB")).get("Id")) or _id(_dict(game.get("teamB")).get("organisationId")),
        }
        self.side_of = {org: side for side, org in self.org.items() if org is not None}

    def head(self) -> Dict[str, Any]:
        return {"game_id": self.game_id, "competition_id": self.competition_id}

    def side(self, org: Any) -> Optional[str]:
        key = _id(org)
        return self.side_of.get(key) if key is not None else None


def _people(page: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    """player_id -> lineup-card fields (name, jersey, position, captain) from ``playersTeamA`` / ``playersTeamB``."""
    out: Dict[int, Dict[str, Any]] = {}
    for key in ("playersTeamA", "playersTeamB"):
        for p in _list(page.get(key)):
            pid = _id(_dict(p).get("personId"))
            if pid is not None:
                out[pid] = _dict(p)
    return out


def _stat_cols(stats: Dict[str, Any], table: Dict[str, str]) -> Dict[str, Any]:
    return {col: stats.get(code) for code, col in table.items()}


# --- play-by-play -------------------------------------------------------------------------------------------------


def _pbp_rows(page: Any) -> List[Dict[str, Any]]:
    page, rows = _dict(page), []
    info = _Game(page)
    periods = _dict(_dict(page.get("playByPlay")).get("items"))
    for number, (name, period) in enumerate(periods.items(), start=1):
        for item in _list(_dict(period).get("items")):
            a = _dict(item)
            code = a.get("ac")
            org = _id(a.get("oId"))
            rows.append(
                {
                    **info.head(),
                    "statistic_system": info.system,
                    "period": str(_dict(period).get("name") or name),
                    "period_number": number,
                    "action_id": None if a.get("Id") is None else str(a.get("Id")),
                    "order": a.get("order"),
                    "clock": a.get("Time"),
                    "clock_seconds": _seconds(a.get("Time")),
                    "wall_time_ms": a.get("GT"),
                    "score_a": a.get("SA"),
                    "score_b": a.get("SB"),
                    "action_code": code,
                    "action_family": a.get("act"),
                    "description": a.get("txt"),
                    "organisation_id": org,
                    "team_side": info.side(org),
                    "player_id": _id(a.get("pId")),
                    "player2_id": _id(a.get("p2Id")),
                    "sub_direction": a.get("in"),
                    "made": a.get("made"),
                    "points_attempted": a.get("pts"),
                    "shot_type": _SHOT_CODES.get(code) if isinstance(code, str) else None,
                    "shot_x": a.get("x"),
                    "shot_y": a.get("y"),
                    "has_location": ("x" in a and code != "FT") if code in _SHOT_CODES else None,
                }
            )
    return rows


def parse_fiba_gamepage_pbp(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Play-by-play of one FIBA game: every action with period, clock, running score, actor and shot x/y.

    Args:
        raw: the game-page dict (``fiba_gamepage_pbp(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per action, in game order. ``player_id`` is the global person id and ``organisation_id``
        the acting team's stable id (NOT ``team_id``); both are null on team and period actions.
        ``shot_x`` / ``shot_y`` are image pixels (scale unverified). A page with no play-by-play (box-only
        eras, 2006-2011 OVR events) gives a zero-row frame typed from the returns schema.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_pbp, parse_fiba_gamepage_pbp
            page = fiba_gamepage_pbp(game_id=100720, comp=208182, return_parsed=False)
            pbp = parse_fiba_gamepage_pbp(page)

        Pandas::

            pbp_pd = parse_fiba_gamepage_pbp(page, return_as_pandas=True)
    """
    return _finish(_frame(_pbp_rows(raw)), "parse_fiba_gamepage_pbp", columns, return_as_pandas)


def parse_fiba_gamepage_shots(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Shot attempts of one FIBA game (two- and three-pointers and free throws) with their x/y.

    Args:
        raw: the game-page dict (``fiba_gamepage_shots(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per attempt (``shot_type`` ``2PT`` / ``3PT`` / ``FT``). ``shot_x`` / ``shot_y`` are integer
        pixels on a half-court image with both teams drawn on one basket (scale and which sideline is
        ``x = 0`` are unverified); a free throw is 0, 0 and ``has_location`` is False. A zero-row frame,
        typed from the returns schema, when the page has no play-by-play.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_shots, parse_fiba_gamepage_shots
            page = fiba_gamepage_shots(game_id=100720, comp=208182, return_parsed=False)
            shots = parse_fiba_gamepage_shots(page)

        Made three-pointers::

            shots.filter((shots["shot_type"] == "3PT") & (shots["made"] == True))
    """
    rows = [r for r in _pbp_rows(raw) if r["shot_type"] is not None]
    keep = (
        "game_id competition_id statistic_system period period_number action_id order clock clock_seconds "
        "action_code organisation_id team_side player_id shot_type points_attempted made shot_x shot_y has_location"
    ).split()
    return _finish(
        _frame([{k: r[k] for k in keep} for r in rows]), "parse_fiba_gamepage_shots", columns, return_as_pandas
    )


# --- box score ------------------------------------------------------------------------------------------------------


def _teams(page: Any) -> List[Tuple[_Game, Dict[str, Any], str]]:
    """``(page ids, team object, side)`` for each team of ``gameDetails.c``."""
    page = _dict(page)
    info = _Game(page)
    out = []
    for team in _list(_dict(page.get("gameDetails")).get("c")):
        team = _dict(team)
        side = info.side(team.get("Id"))
        out.append((info, team, side or ""))
    return out


def _box_player_rows(page: Any) -> List[Dict[str, Any]]:
    page, rows = _dict(page), []
    people = _people(page)
    for info, team, side in _teams(page):
        for child in _list(team.get("Children")):
            c = _dict(child)
            pid, stats = _id(c.get("Id")), _dict(c.get("Stats"))
            who = people.get(pid, {}) if pid is not None else {}
            rows.append(
                {
                    **info.head(),
                    "team_side": side or None,
                    "organisation_id": _id(team.get("Id")),
                    "player_id": pid,
                    "first_name": who.get("firstName"),
                    "last_name": who.get("lastName"),
                    "jersey": who.get("uniformNumber"),
                    "position": who.get("position"),
                    "is_captain": who.get("isCaptain"),
                    "is_starter": stats.get("Starter"),
                    "has_played": stats.get("HasPlayed"),
                    "minutes": stats.get("TP"),
                    "seconds_played": _seconds(stats.get("TP")),
                    **_stat_cols(stats, _STATS),
                    "plus_minus": stats.get("PM"),
                    "cb": stats.get("CB"),
                }
            )
    return rows


def parse_fiba_gamepage_box_players(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Player box score of one FIBA game.

    Args:
        raw: the game-page dict (``fiba_gamepage_box_players(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per rostered player of both teams (``has_played`` is False for the unused bench), with the
        lineup-card name, jersey and position joined on ``player_id``. ``organisation_id`` is the stable team
        id, ``minutes`` a ``m:ss`` string (``0:0`` = did not play) and percentages are 0-100. ``cb`` is a
        flag whose meaning is unverified. A zero-row frame, typed from the returns schema, when the page has
        no box score.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_box_players, parse_fiba_gamepage_box_players
            page = fiba_gamepage_box_players(game_id=100720, comp=208182, return_parsed=False)
            box = parse_fiba_gamepage_box_players(page)

        Players who got on the court::

            box.filter(box["has_played"] == True).sort("points", descending=True)
    """
    return _finish(_frame(_box_player_rows(raw)), "parse_fiba_gamepage_box_players", columns, return_as_pandas)


def _box_team_rows(page: Any) -> List[Dict[str, Any]]:
    page, rows = _dict(page), []
    details = _dict(page.get("gameDetails"))
    game_stats = _dict(details.get("Stats"))
    for info, team, side in _teams(page):
        stats = _dict(team.get("Stats"))
        rows.append(
            {
                **info.head(),
                "statistic_system": info.system,
                "team_side": side or None,
                "organisation_id": _id(team.get("Id")),
                "score": team.get("Score"),
                **_stat_cols(stats, _STATS),
                **_stat_cols(stats, _TEAM_STATS),
                "lead_changes": game_stats.get("A_LC"),
                "times_tied": game_stats.get("A_TT"),
                "duration": game_stats.get("DUR"),
                "attendance": game_stats.get("SPECT"),
            }
        )
    return rows


def parse_fiba_gamepage_box_teams(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Team box score of one FIBA game, with the game-level figures.

    Args:
        raw: the game-page dict (``fiba_gamepage_box_teams(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per team. Totals include the team-only figures that credit no player (``team_reb_*``,
        ``team_fouls``, ``team_turnovers``). ``a_act``, ``a_foul``, ``a_tod`` and ``a_tot`` are always 0 in the
        samples and their meaning is unverified. ``lead_changes``, ``times_tied``, ``duration`` and
        ``attendance`` are game-level and repeat on both rows. A zero-row frame, typed from the returns
        schema, when the page has no box score.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_box_teams, parse_fiba_gamepage_box_teams
            page = fiba_gamepage_box_teams(game_id=100720, comp=208182, return_parsed=False)
            teams = parse_fiba_gamepage_box_teams(page)

        Pandas::

            teams_pd = parse_fiba_gamepage_box_teams(page, return_as_pandas=True)
    """
    return _finish(_frame(_box_team_rows(raw)), "parse_fiba_gamepage_box_teams", columns, return_as_pandas)


def _period_rows(page: Any) -> List[Dict[str, Any]]:
    rows = []
    for info, team, side in _teams(page):
        for number, per in enumerate(_list(team.get("Periods")), start=1):
            p = _dict(per)
            rows.append(
                {
                    **info.head(),
                    "team_side": side or None,
                    "organisation_id": _id(team.get("Id")),
                    "period": p.get("Id"),
                    "period_number": number,
                    "points": p.get("Score"),
                    "points_first_five_min": p.get("HalfTimeScore"),
                }
            )
    return rows


def parse_fiba_gamepage_period_scores(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Points by period of both teams of one FIBA game.

    Args:
        raw: the game-page dict (``fiba_gamepage_period_scores(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per team and period (Q1..Q4, OT1...). ``points`` is the team's points in the period and
        ``points_first_five_min`` its points in the period's first five minutes (null when the page omits
        it). A zero-row frame, typed from the returns schema, when the page has no box score.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_period_scores, parse_fiba_gamepage_period_scores
            page = fiba_gamepage_period_scores(game_id=100720, comp=208182, return_parsed=False)
            periods = parse_fiba_gamepage_period_scores(page)

        Pandas::

            periods_pd = parse_fiba_gamepage_period_scores(page, return_as_pandas=True)
    """
    return _finish(_frame(_period_rows(raw)), "parse_fiba_gamepage_period_scores", columns, return_as_pandas)


def _lineup_rows(page: Any) -> List[Dict[str, Any]]:
    page, rows = _dict(page), []
    info = _Game(page)
    for side, key in (("A", "playersTeamA"), ("B", "playersTeamB")):
        for p in _list(page.get(key)):
            p = _dict(p)
            rows.append(
                {
                    **info.head(),
                    "team_side": side,
                    "organisation_id": info.org.get(side),
                    "player_id": _id(p.get("personId")),
                    "first_name": p.get("firstName"),
                    "last_name": p.get("lastName"),
                    "jersey": p.get("uniformNumber"),
                    "position": p.get("position"),
                    "is_captain": p.get("isCaptain"),
                }
            )
    return rows


def parse_fiba_gamepage_lineup(
    raw: Any, *, return_as_pandas: bool = False, columns: Optional[Sequence[str]] = None
) -> Frame:
    """Lineup card (the players who were registered for the game) of both teams of one FIBA game.

    Args:
        raw: the game-page dict (``fiba_gamepage_lineup(..., return_parsed=False)``).
        return_as_pandas: return a pandas DataFrame instead of polars.
        columns: the documented returns-schema columns (the generated wrapper passes them): an empty
            page keeps exactly these, typed.

    Returns:
        One row per registered player. ``player_id`` joins the box score, the play-by-play and the roster;
        ``jersey`` is a string. A zero-row frame, typed from the returns schema, when the page lists no players.

    Raises:
        None: a malformed page parses to a zero-row frame instead of raising.

    Example:
        Quick start::

            from sportsdataverse.basketball.fiba_hub import fiba_gamepage_lineup, parse_fiba_gamepage_lineup
            page = fiba_gamepage_lineup(game_id=100720, comp=208182, return_parsed=False)
            lineup = parse_fiba_gamepage_lineup(page)

        Pandas::

            lineup_pd = parse_fiba_gamepage_lineup(page, return_as_pandas=True)
    """
    return _finish(_frame(_lineup_rows(raw)), "parse_fiba_gamepage_lineup", columns, return_as_pandas)
