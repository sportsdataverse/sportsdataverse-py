"""sportsdataverse.discover — searchable function index.

The package exposes 2,000+ wrappers across 17 leagues + 5 sibling APIs
(MLB Stats API, Baseball Savant, NHL api-web, NHL EDGE, NHL Stats REST,
NHL Records). ``dir(sportsdataverse.nba)`` produces a 100+ item flat
list that's hard to scan. This module gives users a grouped + filterable
view.

::

    >>> from sportsdataverse import list_functions

    # All wrappers in the package
    >>> list_functions()
    {'cfb': [...], 'mbb': [...], ...}

    # One league
    >>> list_functions(league='nba')
    ['espn_nba_player_overview', 'espn_nba_boxscore', ...]

    # Search by substring (case-insensitive)
    >>> list_functions(search='roster')
    {'nba': ['espn_nba_team_roster', ...], 'mlb': ['espn_mlb_team_roster',
     'mlb_api_team_roster', ...], ...}

    # Filter by category — endpoints that return PBP
    >>> list_functions(search='pbp')
    {'cfb': [...], 'nfl': [...], 'nhl': ['espn_nhl_pbp', 'nhl_web_pbp'], ...}

    # Show only parser callables
    >>> list_functions(parsers_only=True)
    {'_common_espn_parsers': [...], 'nhl': ['parse_edge_*', ...], ...}
"""

from __future__ import annotations

import importlib
from typing import Dict, List, Optional, Union

# All league sub-modules under sportsdataverse.{league}.
_LEAGUES = (
    "cfb",
    "mbb",
    "mlb",
    "nba",
    "nfl",
    "nhl",
    "wbb",
    "wnba",
    "pwhl",
    # minor / additional leagues, nested under sport-group packages (0.0.65+)
    "hockey.ahl",
    "hockey.ohl",
    "hockey.qmjhl",
    "hockey.whl",
    "soccer",
    "cricket",
    "soccer.epl",
    "soccer.laliga",
    "soccer.bundesliga",
    "soccer.seriea",
    "soccer.ligue1",
    "soccer.mls",
    "soccer.ligamx",
    "soccer.ucl",
    "soccer.uel",
    "soccer.nwsl",
    "soccer.wwc",
    "soccer.wc",
    "hockey.mch",
    "hockey.wch",
    "football.ufl",
    "football.xfl",
    "football.cfl",
    "baseball.college_baseball",
    "baseball.college_softball",
)


def _list_module(mod, prefix: Optional[str] = None) -> List[str]:
    """Return callable names in a module, optionally filtered by prefix."""
    out = []
    for name in dir(mod):
        if name.startswith("_"):
            continue
        attr = getattr(mod, name)
        if not callable(attr):
            continue
        if prefix and not name.startswith(prefix):
            continue
        out.append(name)
    return sorted(out)


def list_functions(
    league: Optional[str] = None,
    *,
    search: Optional[str] = None,
    parsers_only: bool = False,
    wrappers_only: bool = False,
) -> Union[Dict[str, List[str]], List[str]]:
    """Return an index of callable functions exposed by the package.

    Args:
        league: League slug (``"nba"`` / ``"wnba"`` / ``"mbb"`` / ``"wbb"``
            / ``"cfb"`` / ``"nfl"`` / ``"mlb"`` / ``"nhl"``). When given,
            returns a flat list of function names for that league.
            With no league, returns a dict keyed by league.
        search: Case-insensitive substring to filter function names.
            ``search="roster"`` matches every ``*_roster`` / ``roster_*``
            / ``parse_*_roster`` function across leagues.
        parsers_only: If ``True``, restrict the result to ``parse_*``
            callables.
        wrappers_only: If ``True``, exclude ``parse_*`` callables.

    Returns:
        Dict (no league) or list (one league) of function names.
        Names are sorted alphabetically within each league.

    Examples::

        >>> list_functions(league="nba", search="leader")
        ['espn_nba_leaders', 'espn_nba_leaders_core',
         'espn_nba_season_type_leaders', ...]

        >>> list_functions(search="pbp")
        {'cfb': ['espn_cfb_pbp'], 'mbb': ['espn_mbb_pbp'],
         'nfl': ['espn_nfl_pbp'], 'nhl': ['espn_nhl_pbp', 'nhl_web_pbp'], ...}

        >>> list_functions(league="mlb", parsers_only=True)
        ['parse_mlb_api_list', 'parse_mlb_api_person_stats',
         'parse_mlb_api_schedule', 'parse_mlb_api_standings',
         'parse_mlb_api_team_roster', 'parse_mlb_api_teams']
    """
    if parsers_only and wrappers_only:
        raise ValueError("parsers_only and wrappers_only are mutually exclusive")

    needle = (search or "").lower()

    def _filter(names: List[str]) -> List[str]:
        if needle:
            names = [n for n in names if needle in n.lower()]
        if parsers_only:
            names = [n for n in names if n.startswith("parse_")]
        if wrappers_only:
            names = [n for n in names if not n.startswith("parse_")]
        return names

    if league is not None:
        league = league.lower()
        if league not in _LEAGUES:
            raise ValueError(
                f"Unknown league {league!r}. Choose one of {list(_LEAGUES)}.",
            )
        mod = importlib.import_module(f"sportsdataverse.{league}")
        return _filter(_list_module(mod))

    # Whole-package view, grouped by league
    out: Dict[str, List[str]] = {}
    for lg in _LEAGUES:
        mod = importlib.import_module(f"sportsdataverse.{lg}")
        names = _filter(_list_module(mod))
        if names:
            out[lg] = names
    return out


def function_count(league: Optional[str] = None) -> Union[Dict[str, int], int]:
    """Count callable functions per league, or in one league.

    Cheap way to verify "what's actually in the package" — useful as a
    sanity check after pulling a new version.

    Examples::

        >>> function_count()
        {'cfb': 145, 'mbb': 148, ..., 'nhl': 220}

        >>> function_count(league="mlb")
        178
    """
    if league is not None:
        names = list_functions(league=league)
        return len(names)  # type: ignore[arg-type]
    return {lg: len(names) for lg, names in list_functions().items()}


__all__ = ["list_functions", "function_count"]
