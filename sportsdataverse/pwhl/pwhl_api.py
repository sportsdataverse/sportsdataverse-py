"""Live PWHL HockeyTech wrappers — full output parity with fastRhockey (R).

Season arguments use the **end year** (e.g. ``2026`` for 2025-26), matching
fastRhockey; they are resolved to the integer HockeyTech ``season_id``.

``hockeytech_api`` is imported at module level so tests can monkeypatch it via
``monkeypatch.setattr(api, "hockeytech_api", ...)``.
"""

from __future__ import annotations

import warnings
from typing import Any, Optional


from sportsdataverse.errors import NoDataError
from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.hockeytech._analytics import enrich_pbp
from sportsdataverse.hockeytech._leagues import most_recent_season_yr

__all__ = [
    "pwhl_schedule",
    "pwhl_scorebar",
    "pwhl_game_info",
    "pwhl_game_summary",
    "pwhl_pbp",
    "pwhl_player_box",
    "pwhl_teams",
    "pwhl_team_roster",
    "pwhl_standings",
    "pwhl_player_info",
    "pwhl_player_stats",
    "pwhl_player_game_log",
    "pwhl_player_search",
    "pwhl_stats",
    "pwhl_leaders",
    "pwhl_streaks",
    "pwhl_transactions",
    "pwhl_playoff_bracket",
    "pwhl_season_id",
    "most_recent_pwhl_season",
]

_LG = "pwhl"


def pwhl_season_id(return_as_pandas: bool = False) -> Any:
    """All PWHL seasons with end-year + game-type labels (HockeyTech ``seasons``).

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per season: ``season_id`` (Int64), ``season_name``, ``season_short``,
            ``career``, ``playoff``, ``start_date``, ``end_date``, ``season_yr`` (Int64, the END year)
            and ``game_type_label``. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_season_id
            df = pwhl_season_id()

        Next step::

            import polars as pl
            df.filter(pl.col("game_type_label") == "regular")

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_seasons(hockeytech_api(_LG, "modulekit", "seasons", {}), return_as_pandas)


def most_recent_pwhl_season() -> int:
    """Newest PWHL regular season as an end-year integer.

    The highest ``season_yr`` of a regular season that is not a one-off event, so a
    preseason the feed lists before its regular season is not a default.

    Returns:
        int: The newest regular season's END year (2026 = the 2025-26 season).

    Raises:
        NoDataError: The seasons feed lists no regular season.
        AssetFetchError: The seasons feed failed.

    Example:
        Quick start::

            from sportsdataverse.pwhl import most_recent_pwhl_season
            season = most_recent_pwhl_season()

        Next step::

            print(season)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return most_recent_season_yr(pwhl_season_id(), _LG)


def _season_or_latest(season: Optional[int], season_id: Optional[int]) -> Optional[int]:
    """The caller's season; the newest only when neither season nor season_id is given."""
    return season if season is not None or season_id is not None else most_recent_pwhl_season()


def pwhl_schedule(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL schedule — one row per game of one season (matches fastRhockey ``pwhl_schedule``).

    Args:
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per game: ``game_id``, ``game_date``, ``game_status``, ``home_team`` /
            ``home_team_id`` / ``home_score``, ``away_team`` / ``away_team_id`` / ``away_score``,
            ``venue``, ``season_id`` and ``game_type``; the ``*_id`` columns are Int64, the rest
            String. Only the requested season's
            games: a regular season, its playoffs and its preseason are separate season ids, so
            pass ``season_id`` (from ``pwhl_season_id()``) for the playoffs. A pandas DataFrame
            when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404, or, with no season given, the seasons feed lists no
            regular season.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.
        ValueError: No season matches ``season``.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_schedule
            df = pwhl_schedule(season=2025)

        Next step::

            import polars as pl
            df.filter(pl.col("game_status") == "Final").head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    payload = hockeytech_api(_LG, "modulekit", "schedule", {"season_id": sid})
    return P.parse_schedule(payload, return_as_pandas, season_id=sid)


def pwhl_pbp(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL play-by-play — one row per event, fully enriched.

    Matches fastRhockey ``pwhl_pbp`` column parity, adding:

    - Coordinate transforms (``*_original``, ``*_neutral``, ``*_fixed``,
      ``*_right``, ``*_vertical``).
    - Clock columns (``minute_start``, ``second_start``, ``clock``,
      ``sec_from_start``).
    - Shot geometry (``shot_distance``, ``shot_angle``, ``scoring_chance``).
    - Game-meta join (``game_date``, ``game_season``, ``game_season_id``,
      ``home_team``, ``home_team_id``, ``away_team``, ``away_team_id``).
    - On-ice player strings (``on_ice_home``, ``on_ice_away``) derived from
      shift data.

    Goal double-rowing: the HockeyTech feed emits both a ``goal`` row and a
    twin ``shot`` row for (nearly) every goal. The twin shot row is flagged
    ``is_goal_twin = True`` — shot rows (twins included) match the official
    boxscore shots-on-goal totals, while dropping flagged rows yields a
    deduplicated event stream. See :func:`sportsdataverse.hockeytech._parsers.parse_pbp`.

    The three network fetches (PBP payload, game summary meta, and shift data)
    all go through the module-level ``hockeytech_api`` reference so tests can
    monkeypatch ``sportsdataverse.pwhl.pwhl_api.hockeytech_api`` to intercept
    all calls without touching the shared core.

    Args:
        game_id: The HockeyTech game id.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per event (shot, goal, penalty, faceoff, hit, goalie
            change ...): ``game_id``, ``event``, ``team_id``, ``period_of_game``,
            ``time_of_period``, rink ``x_coord`` / ``y_coord`` (Float64, raw 600×300 coordinates)
            and the transforms above, the primary / second / third player and goalie ids
            (Float64) and names, the plus / minus skaters on a goal, ``is_goal_twin``, and the
            clock, geometry, game-meta and on-ice columns listed above. A pandas DataFrame
            when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body,
            or an ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_pbp
            df = pwhl_pbp(game_id=164)

        Next step::

            import polars as pl
            df.filter(pl.col("event") == "goal").head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    payload = hockeytech_api(_LG, "statviewfeed", "gameCenterPlayByPlay", {"game_id": game_id, "league_id": ""})
    df = P.parse_pbp(payload, pbp_style="hockeytech_a", game_id=game_id)

    meta_payload = hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id})
    shifts_payload = hockeytech_api(_LG, "modulekit", "gameshifts", {"game_id": game_id})

    return enrich_pbp(
        df,
        _LG,
        game_id,
        meta_payload=meta_payload,
        shifts_payload=shifts_payload,
        return_as_pandas=return_as_pandas,
    )


def pwhl_standings(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL standings — one row per team.

    Args:
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per team: ``team``, ``team_code``, ``team_rank`` and ``wins`` (Int64),
            and ``games_played``, ``losses``, ``regulation_wins``, ``non_reg_wins``, ``non_reg_losses``,
            ``points``, ``goals_for``, ``goals_against``, ``games_remaining``, ``percentage`` and
            ``overall_rank`` (String, as the feed ships them). A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_standings
            df = pwhl_standings(season=2025)

        Next step::

            df.sort("team_rank").head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    payload = hockeytech_api(
        _LG,
        "statviewfeed",
        "teams",
        {
            "groupTeamsBy": "division",
            "context": "overall",
            "special": "false",
            "league_id": 1,
            "sort": "points",
            "season": sid,
        },
    )
    return P.parse_standings(payload, return_as_pandas)


def pwhl_teams(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL teams for a given season.

    Args:
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per team: ``team_id``, ``team_name``, ``team_code``,
            ``team_nickname``, ``team_label``, ``division`` and ``team_logo``; ``team_id`` is Int64, the
            rest String. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_teams
            df = pwhl_teams(season=2025)

        Next step::

            df.select("team_id", "team_name", "team_code")

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    return P.parse_teams(hockeytech_api(_LG, "modulekit", "teamsbyseason", {"season": sid}), return_as_pandas)


def pwhl_team_roster(
    team_id: int,
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL team roster for a given team + season.

    Args:
        team_id: The HockeyTech team id.
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per rostered player: ``player_id``, ``person_id``, names
            (``first_name``, ``last_name``, ``display_name``), ``position``, ``tp_jersey_number``,
            ``shoots`` / ``catches``, ``height`` / ``weight``, ``birthdate``, home and birth places,
            ``rookie``, ``veteran_status``, ``draft_status`` and ``player_image`` (String, as the feed
            ships them). A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_team_roster
            df = pwhl_team_roster(team_id=1, season=2025)

        Next step::

            import polars as pl
            df.filter(pl.col("position") == "G")

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    return P.parse_roster(
        hockeytech_api(_LG, "modulekit", "roster", {"team_id": team_id, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player season stats across all seasons.

    Args:
        player_id: The HockeyTech player id.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per season (and team) the player played: ``season_id``,
            ``season_name``, ``playoff``, ``team_id`` / ``team_name`` / ``team_code``,
            ``games_played``, ``goals``, ``assists``, ``points``, ``plus_minus``, ``penalty_minutes``,
            power-play / short-handed / shootout splits, ``shots``, ``faceoff_wins`` /
            ``faceoff_attempts``, ``ice_time`` and ``stat_type`` (String, as the feed ships them). A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_player_stats
            df = pwhl_player_stats(player_id=27)

        Next step::

            df.select("season_name", "games_played", "points")

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_player_stats(
        hockeytech_api(_LG, "modulekit", "player", {"player_id": player_id, "category": "seasonstats"}),
        return_as_pandas,
    )


def pwhl_leaders(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL statistical leaders for a given season.

    NOTE: the ``leadersExtended`` endpoint uses ``season_id`` (integer) to filter
    by season, not ``season`` (name string). The resolved integer is passed as the
    ``season_id`` param so historical-season requests return results.

    Args:
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per ranked skater (points and goals leaders): ``rank`` (Int64),
            ``player_id``, ``name``, ``jersey_number``, ``position``, ``team_id`` / ``team_name`` /
            ``team_code``, ``stat_formatted`` and ``type_formatted`` (``player_id`` / ``team_id`` Int64,
            the rest String), plus photo and logo
            URLs. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_leaders
            df = pwhl_leaders(season=2025)

        Next step::

            import polars as pl
            df.filter(pl.col("type_formatted") == "Goals").head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    payload = hockeytech_api(
        _LG,
        "statviewfeed",
        "leadersExtended",
        {
            "season_id": sid,
            "team_id": 0,
            "playerTypes": "skaters",
            "skaterStatTypes": "points,goals",
            "activeOnly": 0,
        },
    )
    return P.parse_leaders(payload, return_as_pandas)


def pwhl_game_summary(game_id: int) -> dict:
    """PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars).

    Args:
        game_id: The HockeyTech game id.

    Returns:
        dict[str, polars.DataFrame]: ``game`` (one row: ``game_id``, ``date``, ``status``, ``venue``,
            ``attendance``, both teams and scores), ``goals`` (one row per goal with the scorer, both
            assists and the plus / minus skaters), ``penalties`` (one row per penalty),
            ``shots_by_period`` (``side``, ``period``, ``shots``) and ``three_stars``.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_game_summary
            out = pwhl_game_summary(game_id=164)

        Next step::

            out["goals"].head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_game_summary(hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id}), game_id=game_id)


def pwhl_scorebar(return_as_pandas: bool = False) -> Any:
    """PWHL live scorebar (today ± 3 days).

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per game in the window, as the feed ships it (``id`` and the
            ``*_id`` columns Int64, the rest String): ``id``,
            ``season_id``, ``game_date`` / ``game_date_iso8601``, ``scheduled_time``, ``home_id`` /
            ``home_code`` / ``home_long_name`` / ``home_goals``, the same ``visitor_*`` columns,
            ``period``, ``game_clock``, ``game_status`` / ``game_status_string``, ``venue_name``, both
            teams' records, and broadcast URLs. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_scorebar
            df = pwhl_scorebar()

        Next step::

            print(df.shape)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_scorebar(
        hockeytech_api(
            _LG,
            "modulekit",
            "scorebar",
            {"numberofdaysback": 3, "numberofdaysahead": 3, "limit": 100, "league_id": 1},
        ),
        return_as_pandas,
    )


def pwhl_game_info(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL single-game metadata from the ``statviewfeed`` ``gameSummary`` view.

    Args:
        game_id: The HockeyTech game id.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row: ``game_id``, ``game_number``, ``game_date`` / ``game_date_iso``,
            ``start_time`` / ``end_time`` / ``game_duration``, ``game_venue``, ``attendance``,
            ``game_status``, ``game_season_id``, ``started`` / ``final`` flags, ``home_team_id`` /
            ``home_team`` / ``home_team_abbr`` / ``home_score``, the same ``away_*`` columns,
            ``has_shootout`` and the official report / text box score URLs (ids, scores and flags
            Int64): the published ``load_pwhl_game_info`` schema. A zero-row frame of that schema
            when the reply carries no game. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_game_info
            df = pwhl_game_info(game_id=164)
            print(df.select("game_id", "home_team", "home_score", "away_team", "away_score"))

        As pandas::

            pdf = pwhl_game_info(game_id=164, return_as_pandas=True)

        See Also:
            * `fastRhockey`_ -- R ``pwhl_game_info``

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_game_info(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_box(game_id: int, return_as_pandas: bool = False) -> dict[str, Any]:
    """PWHL player box score for a single game, skaters and goalies (fastRhockey ``pwhl_player_box``).

    Reads the ``statviewfeed`` ``gameSummary`` view. The return is a dict, as in fastRhockey and as
    the ``fastRhockey-pwhl-raw`` producer reads it (``box["skaters"]`` / ``box["goalies"]``).

    Args:
        game_id: The HockeyTech game id.
        return_as_pandas: Return pandas DataFrames instead of polars.

    Returns:
        dict[str, polars.DataFrame]: ``skaters`` -- one row per dressed skater, home team first:
            ``player_id``, ``first_name``, ``last_name``, ``position``, ``team_id``, ``game_id``,
            ``league``, ``toi`` ("mm:ss"), ``time_on_ice`` (Float64 minutes), ``goals``, ``assists``,
            ``points``, ``shots``, ``hits``, ``blocked_shots``, ``penalty_minutes``, ``plus_minus``,
            ``faceoff_attempts`` / ``faceoff_wins`` / ``faceoff_losses``, ``faceoff_pct`` (Float64,
            null on no attempts) and ``starting`` (1 = starting lineup). ``goalies`` -- one row per
            dressed goalie with the same id / name / time columns, ``saves``, ``goals_against``,
            ``shots_against``, the scoring, penalty and faceoff columns and ``starting``; a goalie
            who did not play has null ``toi`` / ``time_on_ice``. Ids and counts are Int64. Both
            frames are zero-row with these columns when the reply carries no players. pandas
            DataFrames when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_player_box
            box = pwhl_player_box(game_id=164)
            print(box["skaters"].shape, box["goalies"].shape)

        Goalies who played::

            import polars as pl
            box["goalies"].filter(pl.col("time_on_ice").is_not_null())

        See Also:
            * `fastRhockey`_ -- R ``pwhl_player_box``

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_player_box(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_info(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player biographical info from the ``statviewfeed`` ``player`` view.

    Args:
        player_id: The HockeyTech player id.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row: ``player_id`` and ``person_id`` (Int64), ``first_name``,
            ``last_name``, ``name``, ``jersey_number``, ``position``, ``shoots``, ``catches``,
            ``height``, ``weight``, ``birthdate``, ``birth_place``, ``nationality``, ``player_type``
            ("skater" or "goalie"), ``team_name`` / ``team_nickname`` (most recent team),
            ``image_url`` and ``current_season_id`` (Int64). A zero-row frame of these columns when
            the reply carries no player. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_player_info
            df = pwhl_player_info(player_id=12)
            print(df.select("player_id", "name", "position", "team_name"))

        As pandas::

            pdf = pwhl_player_info(player_id=12, return_as_pandas=True)

        See Also:
            * `fastRhockey`_ -- R ``pwhl_player_info``

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_player_info(
        hockeytech_api(_LG, "statviewfeed", "player", {"player_id": player_id}),
        return_as_pandas,
    )


def pwhl_player_game_log(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player game-by-game log.

    Args:
        player_id: The HockeyTech player id.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per game played: ``id`` (the game id), ``date_played``,
            ``home_team_code`` / ``visiting_team_code`` and names, ``player_team``, ``goals``,
            ``assists``, ``points``, ``plus_minus``, ``shots``, ``hits``, ``penalty_minutes``,
            ``ice_time_minutes_seconds``, faceoff, power-play, short-handed and shootout counts.
            Counts arrive as strings except ``points`` and the percentages (Int64). A pandas
            DataFrame when ``return_as_pandas`` is True; a zero-row frame for a player with no
            games.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or
            an ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_player_game_log
            df = pwhl_player_game_log(player_id=27)

        Next step::

            print(df.shape)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_player_game_log(
        hockeytech_api(_LG, "modulekit", "player", {"player_id": player_id, "category": "gamebygame"}),
        return_as_pandas,
    )


def pwhl_player_search(name: str, return_as_pandas: bool = False) -> Any:
    """Search for PWHL players by name.

    Args:
        name: The search text, matched against player names.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per matching player (the ``*_id`` columns Int64, the rest String):
            ``person_id``, ``player_id``,
            ``first_name`` / ``last_name``, ``position``, ``shoots`` / ``catches``, ``height`` /
            ``weight``, ``birthdate`` and birthplace, ``last_team_name`` / ``last_team_code``,
            ``role_name``, ``active``, ``last_active_date`` and the match ``score``. A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_player_search
            df = pwhl_player_search(name="Poulin")

        Next step::

            df.select("player_id", "first_name", "last_name")

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_player_search(
        hockeytech_api(_LG, "modulekit", "searchplayers", {"search_term": name}),
        return_as_pandas,
    )


def pwhl_stats(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    position: str = "skaters",
    return_as_pandas: bool = False,
) -> Any:
    """PWHL aggregate stats by season and position.

    Args:
        season: Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular
            season when neither ``season`` nor ``season_id`` is given.
        season_id: The HockeyTech season id, when it is already known.
        position: ``"skaters"`` (default) or ``"goalies"``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per player for that season and position, as the feed ships it
            (~90 columns; the ``*_id`` columns Int64, the others typed as the returns table lists):
            ``player_id``, ``name``, ``team_id`` / ``team_name`` /
            ``team_code``, ``position``, ``games_played``, and the counting, per-game and ice-time
            stats of that position (skaters: ``goals``, ``assists``, ``points``, ``plus_minus``,
            ``shots``, ``hits``, ``penalty_minutes`` ...). A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or an
            ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_stats
            df = pwhl_stats(season=2025)

        Next step::

            print(df.shape)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    sid = resolve_season_id(_LG, season=_season_or_latest(season, season_id), season_id=season_id)
    return P.parse_stats(
        hockeytech_api(_LG, "modulekit", "statviewtype", {"type": position, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_streaks(return_as_pandas: bool = False) -> Any:
    """Current PWHL player/team streaks — **non-functional: no such upstream view**.

    .. deprecated::
        The HockeyTech feed has no ``streaks`` view. ``modulekit&view=streaks``
        answers HTTP 200 with the in-body ``"Undefined Tab streaks"`` sentinel and
        ``statviewfeed&view=streaks`` answers ``{"error": "InvalidView error: streaks"}``
        (both captured 2026-07-12 during the HockeyTech API recon; see
        ``sdv-internal-refs/hockeytech/captures/``). No replacement view was found —
        the PWHL site appears to compute its Streaks page client-side from schedule
        data. fastRhockey's equivalent carries the same defect.

        This function has therefore never returned data. It emits a
        :class:`DeprecationWarning` and returns an empty frame without a request
        (the feed would only answer the sentinel, which ``hockeytech_api`` now
        raises on), so the empty result is not mistakable for "the league
        currently has no streaks". Derive streaks from
        :func:`pwhl_schedule` / :func:`pwhl_standings` instead.

    Args:
        return_as_pandas: If ``True`` return a :class:`pandas.DataFrame`
            instead of a :class:`polars.DataFrame`.

    Returns:
        An empty frame (the upstream view does not exist).

    Warns:
        DeprecationWarning: always — the upstream view is gone.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_streaks
            df = pwhl_streaks()

        Next step::

            print(df.shape)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    warnings.warn(
        "pwhl_streaks() is non-functional: the HockeyTech feed has no 'streaks' view "
        "(modulekit returns the 'Undefined Tab streaks' sentinel; statviewfeed returns "
        "'InvalidView error: streaks'). No replacement view exists upstream, so this "
        "returns an empty frame. Derive streaks from pwhl_schedule()/pwhl_standings().",
        DeprecationWarning,
        stacklevel=2,
    )
    return P.parse_streaks({}, return_as_pandas)  # ponytail: no request -- the view does not exist


def pwhl_transactions(return_as_pandas: bool = False) -> Any:
    """PWHL roster transactions.

    Args:
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per transaction on the feed's current page (the newest 20):
            ``transaction_date`` / ``transaction_time``, ``transaction_type`` / ``ttype_text``,
            ``title``, ``detail``, ``player_id`` / ``player_name`` / ``position``, and
            ``team_id`` / ``team_name`` / ``team_code`` / ``team_city`` (``player_id`` / ``team_id``
            Int64, the rest String). A pandas
            DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or
            an ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_transactions
            df = pwhl_transactions()

        Next step::

            df.head()

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    return P.parse_transactions(hockeytech_api(_LG, "modulekit", "transactions", {"league_id": 1}), return_as_pandas)


def pwhl_playoff_bracket(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL playoff bracket for a given season.

    With neither ``season`` nor ``season_id``, the newest season that has playoffs:
    the newest season overall is usually still before its playoffs, with no bracket.

    Args:
        season: Season as an END year (2026 = the 2025-26 season).
        season_id: The HockeyTech playoff season id, when it is already known.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame: One row per playoff series: ``round`` / ``round_name`` /
            ``round_type_name``, ``series_letter`` / ``series_name``, ``team1`` / ``team2``
            (team ids), ``team1_wins`` / ``team2_wins`` (Int64), ``winner`` (the winning team
            id, but the feed often leaves it empty even after a series ends, so read the
            result from the win counts), ``feeder_series1`` / ``feeder_series2``, and
            ``games`` (a list of structs, one per game: ids, both teams, goal counts, status,
            date). A pandas DataFrame when ``return_as_pandas`` is True.

    Raises:
        NoDataError: The seasons feed lists no playoff season, or the feed answered 404.
        AssetFetchError: The fetch failed: a non-2xx status, an empty or unparseable body, or
            an ``Undefined Tab`` / ``InvalidView`` sentinel.

    Example:
        Quick start::

            from sportsdataverse.pwhl import pwhl_playoff_bracket
            df = pwhl_playoff_bracket(season=2025)

        Next step::

            print(df.shape)

        See Also:
            * `fastRhockey`_ -- the R package these wrappers mirror

        .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    if season is None and season_id is None:
        seasons = pwhl_season_id()
        playoffs = seasons.filter(seasons["game_type_label"] == "playoffs") if seasons.height else seasons
        if not playoffs.height:
            raise NoDataError("PWHL: the seasons feed lists no playoff season")
        season_id = int(playoffs.sort(["season_yr", "season_id"])["season_id"][-1])
    sid = resolve_season_id(_LG, season=season, game_type="playoffs", season_id=season_id)
    return P.parse_playoff_bracket(
        hockeytech_api(_LG, "modulekit", "brackets", {"season_id": sid, "league_id": 1}),
        return_as_pandas,
    )
