"""sportsdataverse.discover — searchable function index.

The package exposes thousands of wrappers across every league the docs site
lists. ``dir(sportsdataverse.nba)`` produces a several-hundred-item flat list
that's hard to scan. This module gives users a grouped + filterable view.

::

    from sportsdataverse import list_functions

    # All wrappers in the package
    list_functions()
    {'cfb': [...], 'mbb': [...], ...}

    # One league
    list_functions(league='nba')
    ['espn_nba_player_overview', 'espn_nba_boxscore', ...]

    # Search by substring (case-insensitive)
    list_functions(search='roster')
    {'nba': ['espn_nba_team_roster', ...], 'mlb': ['espn_mlb_team_roster',
     'mlb_api_team_roster', ...], ...}

    # Filter by category — endpoints that return PBP
    list_functions(search='pbp')
    {'cfb': [...], 'nfl': [...], 'nhl': ['espn_nhl_pbp', 'nhl_web_pbp'], ...}

    # Show only parser callables
    list_functions(parsers_only=True)
    {'_common_espn_parsers': [...], 'nhl': ['parse_edge_*', ...], ...}
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Dict, List, Optional, Union

# Every documented league: its prefix (the dict key and the ``league=`` value) -> its module under
# ``sportsdataverse``. Written by tools/codegen/generate.py from the league registry the docs site reads
# (docs/src/data/leagues.json); edit the registry, never this table.
# BEGIN generated: league homes
_LEAGUES: Dict[str, str] = {
    "cfb": "cfb",
    "nfl": "nfl",
    "ufl": "football.ufl",
    "xfl": "football.xfl",
    "cfl": "football.cfl",
    "nba": "nba",
    "wnba": "wnba",
    "nbagl": "nbagl",
    "nbl": "basketball.nbl",
    "fiba": "basketball.fiba",
    "olympics_mbk": "basketball.olympics_mbk",
    "olympics_wbk": "basketball.olympics_wbk",
    "nbasl_lv": "basketball.nbasl_lv",
    "nbasl_ut": "basketball.nbasl_ut",
    "nbasl_ca": "basketball.nbasl_ca",
    "nbasl_sac": "basketball.nbasl_sac",
    "nbasl_orl": "basketball.nbasl_orl",
    "nbasl_gs": "basketball.nbasl_gs",
    "mbb": "mbb",
    "wbb": "wbb",
    "euroleague": "basketball.euroleague",
    "fiba_hub": "basketball.fiba_hub",
    "genius": "basketball.genius",
    "atrium": "basketball.atrium",
    "nbl_au": "basketball.nbl_au",
    "feb": "basketball.feb",
    "scb": "basketball.scb",
    "lba": "basketball.lba",
    "acb": "basketball.acb",
    "ibba": "basketball.ibba",
    "wjbl": "basketball.wjbl",
    "vtb": "basketball.vtb",
    "kbl": "basketball.kbl",
    "bleague": "basketball.bleague",
    "kzs": "basketball.kzs",
    "lkl": "basketball.lkl",
    "tbf": "basketball.tbf",
    "cebl": "basketball.cebl",
    "aba": "basketball.aba",
    "easl": "basketball.easl",
    "bnxt": "basketball.bnxt",
    "tpbl": "basketball.tpbl",
    "plg": "basketball.plg",
    "cba": "basketball.cba",
    "dbl": "basketball.dbl",
    "nhl": "nhl",
    "mch": "hockey.mch",
    "wch": "hockey.wch",
    "pwhl": "pwhl",
    "ahl": "hockey.ahl",
    "ohl": "hockey.ohl",
    "whl": "hockey.whl",
    "qmjhl": "hockey.qmjhl",
    "echl": "hockey.echl",
    "sphl": "hockey.sphl",
    "chl": "hockey.chl",
    "ushl": "hockey.ushl",
    "bchl": "hockey.bchl",
    "ajhl": "hockey.ajhl",
    "sjhl": "hockey.sjhl",
    "ojhl": "hockey.ojhl",
    "cchl": "hockey.cchl",
    "gojhl": "hockey.gojhl",
    "mhl": "hockey.mhl",
    "nojhl": "hockey.nojhl",
    "vijhl": "hockey.vijhl",
    "kijhl": "hockey.kijhl",
    "mjhl": "hockey.mjhl",
    "sihf": "hockey.sihf",
    "del": "hockey.del_",
    "icehl": "hockey.icehl",
    "eihl": "hockey.eihl",
    "metal": "hockey.metal",
    "elh": "hockey.elh",
    "liiga": "hockey.liiga",
    "shl": "hockey.shl",
    "ha": "hockey.ha",
    "nl": "hockey.nl",
    "magnus": "hockey.magnus",
    "khl": "hockey.khl",
    "vhl": "hockey.vhl",
    "mlb": "mlb",
    "college_baseball": "baseball.college_baseball",
    "college_softball": "baseball.college_softball",
    "soccer": "soccer",
    "epl": "soccer.epl",
    "laliga": "soccer.laliga",
    "bundesliga": "soccer.bundesliga",
    "seriea": "soccer.seriea",
    "ligue1": "soccer.ligue1",
    "mls": "soccer.mls",
    "ligamx": "soccer.ligamx",
    "ucl": "soccer.ucl",
    "uel": "soccer.uel",
    "nwsl": "soccer.nwsl",
    "wwc": "soccer.wwc",
    "wc": "soccer.wc",
    "cricket": "cricket",
    "odds": "odds",
    "cbs": "providers.cbs",
    "yahoo": "providers.yahoo",
    "fox": "providers.fox",
    "f1": "f1",
    "espn_content": "providers.espn_content",
    "thesportsdb": "providers.thesportsdb",
}
# END generated: league homes

# Kept for callers that read the old name: flat leaf -> dotted import path.
_LEAF_TO_DOTTED: Dict[str, str] = _LEAGUES


def _list_module(mod, leaf: Optional[str] = None) -> List[str]:
    """Return the public callable names of a league module.

    A stats.nba.com family (``nba_stats``, ``wnba_stats``, ``nbagl_stats``) is a submodule its league
    package imports but does not star-export, so its ``__all__`` is added here: it is part of the league.
    """
    out = set()
    for name in dir(mod):
        if name.startswith("_"):
            continue
        if callable(getattr(mod, name)):
            out.add(name)
    stats = getattr(mod, f"{leaf}_stats", None) if leaf else None
    if isinstance(stats, ModuleType):
        out.update(n for n in getattr(stats, "__all__", ()) if callable(getattr(stats, n, None)))
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
        league: League prefix, as the docs site names it (``"nba"``, ``"ufl"``,
            ``"epl"``, ``"liiga"``, ``"euroleague"``, ...). When given,
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

        list_functions(league="nba", search="leader")
        ['espn_nba_leaders', 'espn_nba_leaders_core',
         'espn_nba_season_type_leaders', ...]

        list_functions(search="pbp")
        {'cfb': ['espn_cfb_pbp'], 'mbb': ['espn_mbb_pbp'],
         'nfl': ['espn_nfl_pbp'], 'nhl': ['espn_nhl_pbp', 'nhl_web_pbp'], ...}

        list_functions(league="mlb", parsers_only=True)
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
        leaf = league.lower()
        if leaf not in _LEAF_TO_DOTTED:
            raise ValueError(
                f"Unknown league {leaf!r}. Choose one of {sorted(_LEAF_TO_DOTTED)}.",
            )
        dotted = _LEAF_TO_DOTTED[leaf]
        mod = importlib.import_module(f"sportsdataverse.{dotted}")
        return _filter(_list_module(mod, leaf))

    # Whole-package view, grouped by flat leaf (back-compat: keys never include dots)
    out: Dict[str, List[str]] = {}
    for leaf, dotted in _LEAF_TO_DOTTED.items():
        mod = importlib.import_module(f"sportsdataverse.{dotted}")
        names = _filter(_list_module(mod, leaf))
        if names:
            out[leaf] = names
    return out


def function_count(league: Optional[str] = None) -> Union[Dict[str, int], int]:
    """Count callable functions per league, or in one league.

    Cheap way to verify "what's actually in the package" — useful as a
    sanity check after pulling a new version.

    Examples::

        counts = function_count()
        sorted(counts, key=counts.get, reverse=True)[:3]  # the three largest leagues

        function_count(league="nbagl")  # ESPN wrappers + engine helpers + nbagl_stats

    Args:
        league: A league prefix (``"mlb"``); ``None`` counts every league.

    Returns:
        dict[str, int] | int: ``{league: count}`` for every league, or the one league's count
            when ``league`` is given.
    """
    if league is not None:
        names = list_functions(league=league)
        return len(names)  # type: ignore[arg-type]
    return {lg: len(names) for lg, names in list_functions().items()}


__all__ = ["list_functions", "function_count"]
