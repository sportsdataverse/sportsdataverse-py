"""Fox Sports "Bifrost" NBA wrappers (``fox_nba_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/nba/*``. Thin shims over
the shared :mod:`sportsdataverse._fox_layout` parsers. Same
``return_parsed`` / ``return_as_pandas`` contract as the ESPN ext (polars by
default; raw ``Dict`` when ``return_parsed=False``).
"""

from __future__ import annotations

from typing import Dict, Union

from sportsdataverse._fox_layout import (
    fox_get,
    frame,
    parse_boxscore,
    parse_league_leaders,
    parse_odds,
    parse_period_pbp,
    parse_roster,
    parse_standings,
    parse_team_gamelog,
    parse_team_stats,
)

__all__ = [
    "fox_nba_pbp",
    "fox_nba_boxscore",
    "fox_nba_odds",
    "fox_nba_team_roster",
    "fox_nba_team_stats",
    "fox_nba_team_gamelog",
    "fox_nba_standings",
    "fox_nba_league_leaders",
]

_SPORT = "nba"


def fox_nba_pbp(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA play-by-play (one row per play; period-based)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_period_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_nba_boxscore(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA boxscore (long: one row per player-stat)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_nba_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA game odds six-pack (spread / to-win / total per team)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_nba_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA team roster (one row per player)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_nba_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA team stat leaders by category."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_nba_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA team game log (long: one row per game-stat)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_nba_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """NBA standings for a team's conference/division."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_nba_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """NBA statistical leaders (``stats-con``); who=player|team."""
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw
