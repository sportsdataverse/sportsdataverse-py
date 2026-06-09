"""Live PWHL HockeyTech wrappers — full output parity with fastRhockey (R).

Season arguments use the **end year** (e.g. ``2026`` for 2025-26), matching
fastRhockey; they are resolved to the integer HockeyTech ``season_id``.

``hockeytech_api`` is imported at module level so tests can monkeypatch it via
``monkeypatch.setattr(api, "hockeytech_api", ...)``.
"""

from __future__ import annotations

from typing import Any, Optional


from sportsdataverse.hockeytech import hockeytech_api, resolve_season_id
from sportsdataverse.hockeytech import _parsers as P
from sportsdataverse.hockeytech._analytics import enrich_pbp

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
    """All PWHL seasons with end-year + game-type labels (HockeyTech ``seasons``)."""
    return P.parse_seasons(hockeytech_api(_LG, "modulekit", "seasons", {}), return_as_pandas)


def most_recent_pwhl_season() -> int:
    """Most-recent PWHL season as an end-year integer (max ``season_yr``)."""
    df = pwhl_season_id()
    return int(df["season_yr"].max()) if df.height else 2026


def pwhl_schedule(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL schedule — one row per game (matches fastRhockey ``pwhl_schedule``)."""
    params: dict = {
        "numberofdaysback": 10000,
        "numberofdaysahead": 10000,
        "limit": 10000,
        "league_id": 1,
    }
    if season is not None or season_id is not None:
        params["season_id"] = resolve_season_id(_LG, season=season, season_id=season_id)
    return P.parse_schedule(hockeytech_api(_LG, "modulekit", "scorebar", params), return_as_pandas)


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

    The three network fetches (PBP payload, game summary meta, and shift data)
    all go through the module-level ``hockeytech_api`` reference so tests can
    monkeypatch ``sportsdataverse.pwhl.pwhl_api.hockeytech_api`` to intercept
    all calls without touching the shared core.
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
    """PWHL standings — one row per team."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
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
    """PWHL teams for a given season."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_teams(hockeytech_api(_LG, "modulekit", "teamsbyseason", {"season": sid}), return_as_pandas)


def pwhl_team_roster(
    team_id: int,
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL team roster for a given team + season."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_roster(
        hockeytech_api(_LG, "modulekit", "roster", {"team_id": team_id, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_player_stats(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player season stats across all seasons."""
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
    """
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
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
    """PWHL game summary — dict of frames (game/goals/penalties/shots_by_period/three_stars)."""
    return P.parse_game_summary(hockeytech_api(_LG, "gc", "gamesummary", {"game_id": game_id}), game_id=game_id)


def pwhl_scorebar(return_as_pandas: bool = False) -> Any:
    """PWHL live scorebar (today ± 3 days)."""
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
    """PWHL single-game metadata."""
    return P.parse_game_info(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_box(game_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player box score for a single game."""
    return P.parse_player_box(
        hockeytech_api(_LG, "statviewfeed", "gameSummary", {"game_id": game_id}),
        return_as_pandas,
    )


def pwhl_player_info(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player biographical info."""
    return P.parse_player_info(
        hockeytech_api(_LG, "statviewfeed", "player", {"player_id": player_id}),
        return_as_pandas,
    )


def pwhl_player_game_log(player_id: int, return_as_pandas: bool = False) -> Any:
    """PWHL player game-by-game log."""
    return P.parse_player_game_log(
        hockeytech_api(_LG, "modulekit", "player", {"player_id": player_id, "category": "gamebygame"}),
        return_as_pandas,
    )


def pwhl_player_search(name: str, return_as_pandas: bool = False) -> Any:
    """Search for PWHL players by name."""
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
    """PWHL aggregate stats by season and position."""
    sid = resolve_season_id(
        _LG, season=season if season is not None else most_recent_pwhl_season(), season_id=season_id
    )
    return P.parse_stats(
        hockeytech_api(_LG, "modulekit", "statviewtype", {"type": position, "season_id": sid}),
        return_as_pandas,
    )


def pwhl_streaks(return_as_pandas: bool = False) -> Any:
    """Current PWHL player/team streaks."""
    return P.parse_streaks(hockeytech_api(_LG, "modulekit", "streaks", {"league_id": 1}), return_as_pandas)


def pwhl_transactions(return_as_pandas: bool = False) -> Any:
    """PWHL roster transactions."""
    return P.parse_transactions(hockeytech_api(_LG, "modulekit", "transactions", {"league_id": 1}), return_as_pandas)


def pwhl_playoff_bracket(
    season: Optional[int] = None,
    season_id: Optional[int] = None,
    return_as_pandas: bool = False,
) -> Any:
    """PWHL playoff bracket for a given season."""
    sid = resolve_season_id(
        _LG,
        season=season if season is not None else most_recent_pwhl_season(),
        game_type="playoffs",
        season_id=season_id,
    )
    return P.parse_playoff_bracket(
        hockeytech_api(_LG, "modulekit", "brackets", {"season_id": sid, "league_id": 1}),
        return_as_pandas,
    )
