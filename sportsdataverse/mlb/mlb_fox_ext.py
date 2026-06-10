"""Fox Sports "Bifrost" MLB wrappers (``fox_mlb_*``).

Read-only wrappers over ``api.foxsports.com/bifrost/v1/mlb/*``. Thin shims over
the shared :mod:`sportsdataverse._fox_layout` parsers.

NOTE: unlike the other sports, Fox does not expose MLB play-by-play or boxscore
in ``event/{id}/data`` (it returns only header / top-performers), so there is no
``fox_mlb_pbp`` / ``fox_mlb_boxscore``. The team/league endpoints behave like the
other sports.
"""

from __future__ import annotations

from typing import Dict, Union

from sportsdataverse._fox_layout import (
    fox_get,
    frame,
    parse_league_leaders,
    parse_odds,
    parse_roster,
    parse_standings,
    parse_team_gamelog,
    parse_team_stats,
)

__all__ = [
    "fox_mlb_odds",
    "fox_mlb_team_roster",
    "fox_mlb_team_stats",
    "fox_mlb_team_gamelog",
    "fox_mlb_standings",
    "fox_mlb_league_leaders",
]

_SPORT = "mlb"


def fox_mlb_odds(
    game_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MLB game odds six-pack (run line / to-win / total per team)."""
    raw = fox_get(f"{_SPORT}/event/{game_id}/odds", **kwargs)
    return frame(parse_odds(raw, game_id), return_as_pandas) if return_parsed else raw


def fox_mlb_team_roster(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MLB team roster (one row per player)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/roster", **kwargs)
    return frame(parse_roster(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mlb_team_stats(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MLB team stat leaders by category."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/stats", **kwargs)
    return frame(parse_team_stats(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mlb_team_gamelog(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MLB team game log (long: one row per game-stat)."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/gamelog", **kwargs)
    return frame(parse_team_gamelog(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mlb_standings(
    team_id: Union[int, str], *, return_parsed: bool = True, return_as_pandas: bool = False, **kwargs
) -> Dict:
    """MLB standings for a team's division/league."""
    raw = fox_get(f"{_SPORT}/team/{team_id}/standings", **kwargs)
    return frame(parse_standings(raw, team_id), return_as_pandas) if return_parsed else raw


def fox_mlb_league_leaders(
    category: str = "batting",
    who: str = "player",
    page: int = 0,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """MLB statistical leaders (``stats-con``); who=player|team."""
    raw = fox_get(f"{_SPORT}/league/stats-con/{who}/{category}/{page}", **kwargs)
    return frame(parse_league_leaders(raw), return_as_pandas) if return_parsed else raw
