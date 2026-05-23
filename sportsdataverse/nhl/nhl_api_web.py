"""sportsdataverse.nhl.nhl_api_web — wrappers for ``api-web.nhle.com/v1/``.

This is the **modern replacement** for the NHL's deprecated public Stats API
(``statsapi.web.nhl.com/api/v1/``, retired Sep 2023). The functions in
:mod:`sportsdataverse.nhl.nhl_api` that target ``statsapi.web.nhl.com`` are
broken in production and should not be used; this module is their successor.

Endpoint catalog sourced from the OpenAPI spec at
``fastRhockey/data-raw/nhl_api_web_openapi.yaml`` (which is itself sourced
from https://github.com/dfleis/nhl-api-docs and cross-referenced with
https://github.com/RentoSaijo/nhlscraper/wiki and
https://github.com/coreyjs/nhl-api-py).

Conventions
-----------

* **Season strings** are 8-digit, e.g. ``"20242025"`` for the 2024-25 season.
  Helpers accept either the 8-digit string OR the end-year as an integer
  (e.g. ``2025`` → ``"20242025"``).
* **Game type**: ``1`` = preseason, ``2`` = regular season, ``3`` = playoffs.
* **Team**: three-letter abbreviation (e.g. ``"TOR"``, ``"BOS"``).
* **Date**: ``YYYY-MM-DD``.
* All functions return ``Dict`` (the raw JSON payload). Parsing into tidy
  polars frames is a per-endpoint follow-up — for the migration sketch the
  goal is to land a complete, documented surface that callers can mine.
"""

from __future__ import annotations

from typing import Dict, Optional, Union

from sportsdataverse.dl_utils import download

_API_WEB_BASE = "https://api-web.nhle.com"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _format_nhl_season(season: Union[int, str, None]) -> Optional[str]:
    """Normalize a season identifier to the 8-digit ``YYYYYYYY`` form.

    Accepts:
      * ``None`` — returned unchanged (callers use the ``/now`` variants).
      * An 8-digit string (``"20242025"``) — returned unchanged.
      * A 4-digit int or string representing the **end year** (``2025`` or
        ``"2025"``) — returned as ``"20242025"``.
      * A 4-digit start year (``2024``) is **ambiguous** — we treat 4-digit
        ints as the end year (matches ESPN's NHL convention used elsewhere
        in sportsdataverse).
    """
    if season is None:
        return None
    s = str(season)
    if len(s) == 8 and s.isdigit():
        return s
    if len(s) == 4 and s.isdigit():
        end_year = int(s)
        return f"{end_year - 1}{end_year}"
    raise ValueError(
        f"Unrecognized NHL season identifier {season!r}; "
        "expected 8-digit string (e.g. '20242025') or 4-digit end year (2025).",
    )


def _fetch(path: str, **kwargs) -> Dict:
    """Internal ``download() → .json()`` helper. Returns ``{}`` on failure."""
    url = f"{_API_WEB_BASE}{path}"
    resp = download(url=url, **kwargs)
    if resp is None:
        return {}
    try:
        return resp.json()
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Gamecenter (per-game)
# ---------------------------------------------------------------------------


def nhl_web_pbp(game_id: int, **kwargs) -> Dict:
    """Pull the play-by-play feed for one NHL game.

    Wraps ``GET /v1/gamecenter/{gameId}/play-by-play``. Replaces the
    deprecated :func:`sportsdataverse.nhl.nhl_api_pbp` (which targets the
    retired ``statsapi.web.nhl.com``).

    Args:
        game_id (int): NHL game id. Same identifier ESPN exposes as
            ``event_id`` is *not* compatible — use the NHL-side game id
            from :func:`nhl_web_schedule`.

    Returns:
        Dict: ``plays[]`` array (typeCode, typeDescKey, period, timeInPeriod,
        details with x/y coordinates), plus ``gameState``, ``rosterSpots[]``,
        ``homeTeam`` / ``awayTeam`` blocks with score and shots.

    Example::

        from sportsdataverse.nhl import nhl_web_pbp
        feed = nhl_web_pbp(2024020001)
        print(len(feed["plays"]))
    """
    return _fetch(f"/v1/gamecenter/{game_id}/play-by-play", **kwargs)


def nhl_web_boxscore(game_id: int, **kwargs) -> Dict:
    """Pull the boxscore for one NHL game.

    Wraps ``GET /v1/gamecenter/{gameId}/boxscore``.

    Returns:
        Dict: ``playerByGameStats.{homeTeam,awayTeam}.{forwards,defense,goalies}[]``
        plus team-level shot/goal counts, period scoring, and game status.
    """
    return _fetch(f"/v1/gamecenter/{game_id}/boxscore", **kwargs)


def nhl_web_landing(game_id: int, **kwargs) -> Dict:
    """Pull the gamecenter landing payload for one NHL game.

    Wraps ``GET /v1/gamecenter/{gameId}/landing``. The richest single-call
    shape: ``matchup, summary, three-stars, season-series, gameVideo``, etc.
    """
    return _fetch(f"/v1/gamecenter/{game_id}/landing", **kwargs)


def nhl_web_right_rail(game_id: int, **kwargs) -> Dict:
    """Pull the gamecenter right-rail payload (in-game widgets).

    Wraps ``GET /v1/gamecenter/{gameId}/right-rail``.
    """
    return _fetch(f"/v1/gamecenter/{game_id}/right-rail", **kwargs)


# ---------------------------------------------------------------------------
# Schedule / scores
# ---------------------------------------------------------------------------


def nhl_web_schedule(date: Optional[str] = None, **kwargs) -> Dict:
    """Pull the week-of NHL schedule rooted at ``date``.

    Wraps ``GET /v1/schedule/{date}`` or ``/v1/schedule/now``. The response
    carries a week's worth of games — the NHL schedules in 7-day rolls.

    Args:
        date: ``YYYY-MM-DD`` or ``None`` to use the current week.

    Returns:
        Dict: ``gameWeek[].{date, dayAbbrev, games[]}``.
    """
    suffix = "now" if date is None else date
    return _fetch(f"/v1/schedule/{suffix}", **kwargs)


def nhl_web_score(date: Optional[str] = None, **kwargs) -> Dict:
    """Pull the single-day scoreboard for ``date``.

    Wraps ``GET /v1/score/{date}`` or ``/v1/score/now``.

    Returns:
        Dict: ``games[]`` with one entry per game on that date plus the in-game
        clock / period / score.
    """
    suffix = "now" if date is None else date
    return _fetch(f"/v1/score/{suffix}", **kwargs)


def nhl_web_scoreboard(date: Optional[str] = None, team: Optional[str] = None, **kwargs) -> Dict:
    """Pull the in-game scoreboard payload.

    Wraps one of:
      * ``GET /v1/scoreboard/{date}`` — league-wide on a date
      * ``GET /v1/scoreboard/now`` — league-wide now
      * ``GET /v1/scoreboard/{team}/now`` — team-scoped now

    Args:
        date: ``YYYY-MM-DD``. If both ``date`` and ``team`` are None, defaults
            to ``/v1/scoreboard/now``.
        team: 3-letter abbreviation (mutually exclusive with ``date``).
    """
    if team is not None:
        return _fetch(f"/v1/scoreboard/{team}/now", **kwargs)
    suffix = "now" if date is None else date
    return _fetch(f"/v1/scoreboard/{suffix}", **kwargs)


def nhl_web_schedule_calendar(date: Optional[str] = None, **kwargs) -> Dict:
    """Pull the calendar of game-days for the season.

    Wraps ``GET /v1/schedule-calendar/{date}`` or ``/v1/schedule-calendar/now``.
    """
    suffix = "now" if date is None else date
    return _fetch(f"/v1/schedule-calendar/{suffix}", **kwargs)


def nhl_web_playoff_series(season: Union[int, str], series_letter: str, **kwargs) -> Dict:
    """Pull a single playoff series payload.

    Wraps ``GET /v1/schedule/playoff-series/{season}/{seriesLetter}``.

    Args:
        season: end-year int or 8-digit string.
        series_letter: ``"a"``..``"o"``, identifying the playoff matchup.
    """
    s = _format_nhl_season(season)
    return _fetch(f"/v1/schedule/playoff-series/{s}/{series_letter}", **kwargs)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def nhl_web_standings(date: Optional[str] = None, **kwargs) -> Dict:
    """Pull the NHL standings.

    Wraps ``GET /v1/standings/{date}`` or ``/v1/standings/now``.

    Returns:
        Dict: ``standings[]`` one row per team with ``teamAbbrev, conferenceName,
        divisionName, gamesPlayed, wins, losses, otLosses, points, pointPctg,
        goalFor, goalAgainst, goalDifferential, leagueSequence, divisionSequence,
        wildcardSequence``.
    """
    suffix = "now" if date is None else date
    return _fetch(f"/v1/standings/{suffix}", **kwargs)


def nhl_web_standings_season(**kwargs) -> Dict:
    """Pull the per-season standings cutover dates.

    Wraps ``GET /v1/standings-season``. Useful for resolving "the standings
    snapshot at the end of regular season N" without hard-coding dates.
    """
    return _fetch("/v1/standings-season", **kwargs)


# ---------------------------------------------------------------------------
# Club (team)
# ---------------------------------------------------------------------------


def nhl_web_club_schedule_season(team: str, season: Union[int, str, None] = None, **kwargs) -> Dict:
    """Pull a team's full-season schedule.

    Wraps ``GET /v1/club-schedule-season/{team}/{season}`` or ``/now``.
    """
    if season is None:
        return _fetch(f"/v1/club-schedule-season/{team}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/club-schedule-season/{team}/{s}", **kwargs)


def nhl_web_club_schedule_month(team: str, month: Optional[str] = None, **kwargs) -> Dict:
    """Pull a team's schedule for one month.

    Wraps ``GET /v1/club-schedule/{team}/month/{month}`` or ``/now``.

    Args:
        team: 3-letter abbreviation.
        month: ``YYYY-MM`` (e.g. ``"2024-11"``) or ``None`` for current month.
    """
    suffix = "now" if month is None else month
    return _fetch(f"/v1/club-schedule/{team}/month/{suffix}", **kwargs)


def nhl_web_club_schedule_week(team: str, date: Optional[str] = None, **kwargs) -> Dict:
    """Pull a team's schedule for one week.

    Wraps ``GET /v1/club-schedule/{team}/week/{date}`` or ``/now``.
    """
    suffix = "now" if date is None else date
    return _fetch(f"/v1/club-schedule/{team}/week/{suffix}", **kwargs)


def nhl_web_club_stats(team: str, season: Union[int, str, None] = None, game_type: int = 2, **kwargs) -> Dict:
    """Pull a team's season stat block.

    Wraps ``GET /v1/club-stats/{team}/{season}/{gameType}`` or ``/now``.

    Args:
        team: 3-letter abbreviation.
        season: end-year int / 8-digit string. ``None`` → ``/now``.
        game_type: 1=pre, 2=reg, 3=playoffs.
    """
    if season is None:
        return _fetch(f"/v1/club-stats/{team}/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/club-stats/{team}/{s}/{game_type}", **kwargs)


def nhl_web_club_stats_season(team: str, **kwargs) -> Dict:
    """Pull the seasons a team has stats for.

    Wraps ``GET /v1/club-stats-season/{team}``.
    """
    return _fetch(f"/v1/club-stats-season/{team}", **kwargs)


def nhl_web_roster(team: str, season: Union[int, str, None] = None, **kwargs) -> Dict:
    """Pull a team's roster.

    Wraps ``GET /v1/roster/{team}/{season}`` or ``/v1/roster/{team}/current``.
    """
    if season is None:
        return _fetch(f"/v1/roster/{team}/current", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/roster/{team}/{s}", **kwargs)


def nhl_web_roster_season(team: str, **kwargs) -> Dict:
    """Pull every season a team has had on file.

    Wraps ``GET /v1/roster-season/{team}``.
    """
    return _fetch(f"/v1/roster-season/{team}", **kwargs)


# ---------------------------------------------------------------------------
# Player
# ---------------------------------------------------------------------------


def nhl_web_player_landing(player_id: int, **kwargs) -> Dict:
    """Pull the player profile / overview.

    Wraps ``GET /v1/player/{playerId}/landing``. Returns the rich shape used
    by NHL.com player pages: bio, current team, career totals, season totals,
    last 5, awards, drafted info.
    """
    return _fetch(f"/v1/player/{player_id}/landing", **kwargs)


def nhl_web_player_game_log(player_id: int, season: Union[int, str, None] = None, game_type: int = 2, **kwargs) -> Dict:
    """Pull a player's game-by-game log.

    Wraps ``GET /v1/player/{playerId}/game-log/{season}/{gameType}`` or ``/now``.
    """
    if season is None:
        return _fetch(f"/v1/player/{player_id}/game-log/now", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/player/{player_id}/game-log/{s}/{game_type}", **kwargs)


def nhl_web_player_spotlight(**kwargs) -> Dict:
    """Pull the league's currently featured players.

    Wraps ``GET /v1/player-spotlight``.
    """
    return _fetch("/v1/player-spotlight", **kwargs)


# ---------------------------------------------------------------------------
# Leaders
# ---------------------------------------------------------------------------


def nhl_web_skater_leaders(season: Union[int, str, None] = None, game_type: int = 2, **kwargs) -> Dict:
    """Pull skater stat leaders.

    Wraps ``GET /v1/skater-stats-leaders/{season}/{gameType}`` or ``/current``.
    """
    if season is None:
        return _fetch("/v1/skater-stats-leaders/current", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/skater-stats-leaders/{s}/{game_type}", **kwargs)


def nhl_web_goalie_leaders(season: Union[int, str, None] = None, game_type: int = 2, **kwargs) -> Dict:
    """Pull goalie stat leaders.

    Wraps ``GET /v1/goalie-stats-leaders/{season}/{gameType}`` or ``/current``.
    """
    if season is None:
        return _fetch("/v1/goalie-stats-leaders/current", **kwargs)
    s = _format_nhl_season(season)
    return _fetch(f"/v1/goalie-stats-leaders/{s}/{game_type}", **kwargs)


# ---------------------------------------------------------------------------
# Draft
# ---------------------------------------------------------------------------


def nhl_web_draft_picks(year: Union[int, str], round_: Union[int, str] = "all", **kwargs) -> Dict:
    """Pull NHL draft picks for a year (and optionally one round).

    Wraps ``GET /v1/draft/picks/{year}/{round}`` (``round`` may be ``"all"``).
    """
    return _fetch(f"/v1/draft/picks/{year}/{round_}", **kwargs)


def nhl_web_draft_rankings(year: Union[int, str], category: int = 1, **kwargs) -> Dict:
    """Pull NHL Central Scouting rankings for a draft year.

    Wraps ``GET /v1/draft/rankings/{year}/{rankingCategory}``.

    Args:
        category: 1 = N.A. skater, 2 = N.A. goalie, 3 = Int. skater, 4 = Int. goalie.
    """
    return _fetch(f"/v1/draft/rankings/{year}/{category}", **kwargs)


def nhl_web_draft_picks_now(**kwargs) -> Dict:
    """Pull the current / most recent draft pick set.

    Wraps ``GET /v1/draft/picks/now``.
    """
    return _fetch("/v1/draft/picks/now", **kwargs)


def nhl_web_draft_rankings_now(**kwargs) -> Dict:
    """Pull the current Central Scouting rankings.

    Wraps ``GET /v1/draft/rankings/now``.
    """
    return _fetch("/v1/draft/rankings/now", **kwargs)


def nhl_web_draft_tracker_picks_now(**kwargs) -> Dict:
    """Pull the live draft-tracker pick list (during the draft itself).

    Wraps ``GET /v1/draft-tracker/picks/now``.
    """
    return _fetch("/v1/draft-tracker/picks/now", **kwargs)
