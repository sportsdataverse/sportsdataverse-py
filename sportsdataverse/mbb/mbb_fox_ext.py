"""Fox Sports "Bifrost" MBB wrappers (``fox_mbb_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/cbk/*``. Thin shims over
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
    "fox_mbb_pbp",
    "fox_mbb_boxscore",
    "fox_mbb_odds",
    "fox_mbb_team_roster",
    "fox_mbb_team_stats",
    "fox_mbb_team_gamelog",
    "fox_mbb_standings",
    "fox_mbb_league_leaders",
]

_SPORT = "cbk"


def fox_mbb_pbp(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB play-by-play (one row per play; period-based)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_period_pbp(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_mbb_boxscore(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB boxscore (long: one row per player-stat)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/data", **kwargs)
    return frame(parse_boxscore(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_mbb_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB game odds six-pack (spread / to-win / total per team)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_mbb_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB team roster (one row per player)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mbb_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB team stat leaders by category."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mbb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB team game log (long: one row per game-stat)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mbb_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MBB standings for a team's conference/division."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mbb_league_leaders(
    category: str = "scoring",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """MBB statistical leaders (``stats-con``); who=player|team."""
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw
