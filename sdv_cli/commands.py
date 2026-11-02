"""The ``sdv`` commands, built on cyclopts and rendered with Rich.

Same contract as the classic argparse CLI (:mod:`sportsdataverse.cli`):

- Same commands, flags and positionals.
- ``--json`` output is byte-identical.
- Exit code 0 means success (for ``find-*``, a match). 1 means a ``find-*`` miss. 2 means an error,
  printed as ``sdv: <message>``, or a usage error.

``sportsdataverse`` is imported inside each command, never at module level, so ``sdv --help``
starts in a fraction of a second instead of loading every league first.
"""

from __future__ import annotations

import json
import sys
from importlib.metadata import PackageNotFoundError, version
from typing import Annotated, Any, List, Literal, Optional

from cyclopts import App, CycloptsError, Group, Parameter
from rich.console import Console

from sdv_cli import render

FindLeague = Literal["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"]
CacheMode = Literal["off", "memory", "filesystem"]

_FIND = Group("Find", sort_key=1)
_DISCOVER = Group("Discover", sort_key=2)
_CACHE = Group("Cache", sort_key=3)


def _version() -> str:
    try:
        return version("sportsdataverse")
    except PackageNotFoundError:  # a source checkout that was never installed
        return "unknown"


app = App(
    name="sdv",
    help="sportsdataverse-py from the shell: name-to-ID lookups, function discovery and the HTTP cache.",
    help_epilogue=(
        "Exit codes: 0 success (for a find command, a match) · 1 a find command found no match · "
        "2 an error, printed as 'sdv: message', or a usage error."
    ),
    version=_version,
    help_on_error=False,
)

_state = {"json": False}
_out = Console(highlight=False)
_err = Console(stderr=True, highlight=False)


def _print_json(obj: Any) -> None:
    # Byte-identical to the classic CLI's --json output.
    print(json.dumps(obj, indent=2, default=str))


def _found(result: Any) -> int:
    return 0 if result else 1


# ---------------------------------------------------------------------------
# Find
# ---------------------------------------------------------------------------


@app.command(name="find-team", group=_FIND)
def find_team(
    name: str,
    *,
    league: Annotated[FindLeague, Parameter(name=["--league", "-l"])],
    multi: Annotated[bool, Parameter(negative="")] = False,
) -> int:
    """Resolve a team name to its ESPN id.

    Args:
        name: Full or partial team name, location or abbreviation.
        league: League to search.
        multi: Return every match instead of the first.
    """
    from sportsdataverse import find_team as _find_team

    result = _find_team(name, league=league, multi=multi)
    if _state["json"]:
        _print_json(result)
    elif not result:
        _err.print(render.miss(f"{league} team", name, _team_suggestions(name, league)))
    elif multi:
        _out.print(render.team_table(result, league))
    else:
        _out.print(render.team_card(result, league))
    return _found(result)


def _team_suggestions(name: str, league: str) -> List[str]:
    from rapidfuzz import fuzz, process

    from sportsdataverse.find import _list_teams

    names = sorted({str(t.get("displayName")) for t in _list_teams(league) if t.get("displayName")})
    return [m for m, _score, _i in process.extract(name, names, scorer=fuzz.WRatio, limit=3, score_cutoff=60)]


@app.command(name="find-athlete", group=_FIND)
def find_athlete(
    name: str,
    *,
    league: Annotated[FindLeague, Parameter(name=["--league", "-l"])],
    team: Annotated[Optional[str], Parameter(name=["--team", "-t"])] = None,
    multi: Annotated[bool, Parameter(negative="")] = False,
) -> int:
    """Resolve an athlete name to ESPN athlete metadata.

    Args:
        name: Full or partial athlete name.
        league: League to search.
        team: Team filter; one roster call instead of every team's.
        multi: Return every match instead of the first.
    """
    from sportsdataverse import find_athlete as _find_athlete

    result = _find_athlete(name, league=league, team=team, multi=multi)
    if _state["json"]:
        _print_json(result)
    elif not result:
        _err.print(render.miss(f"{league} athlete", name, []))
    elif multi:
        _out.print(render.athlete_table(result, league))
    else:
        _out.print(render.athlete_card(result, league))
    return _found(result)


@app.command(name="find-event", group=_FIND)
def find_event(
    date: str,
    *,
    league: Annotated[FindLeague, Parameter(name=["--league", "-l"])],
    home: Optional[str] = None,
    away: Optional[str] = None,
    multi: Annotated[bool, Parameter(negative="")] = False,
) -> int:
    """Resolve a game to its ESPN event id.

    Args:
        date: Game date as YYYY-MM-DD or YYYYMMDD.
        league: League to search.
        home: Home-team filter (partial name).
        away: Away-team filter (partial name).
        multi: Return every match instead of the first.
    """
    from sportsdataverse import find_event as _find_event

    result = _find_event(date=date, league=league, home=home, away=away, multi=multi)
    if _state["json"]:
        _print_json(result)
    elif not result:
        _err.print(render.miss(f"{league} game on", date, []))
    else:
        _out.print(render.event_table(result if multi else [result], league))
    return _found(result)


# ---------------------------------------------------------------------------
# Discover
# ---------------------------------------------------------------------------


@app.command(name="list-functions", group=_DISCOVER)
def list_functions(
    *,
    league: Annotated[Optional[str], Parameter(name=["--league", "-l"])] = None,
    search: Annotated[Optional[str], Parameter(name=["--search", "-s"])] = None,
    parsers_only: Annotated[bool, Parameter(negative="")] = False,
    wrappers_only: Annotated[bool, Parameter(negative="")] = False,
) -> int:
    """List the package's functions, across every league or in one.

    Args:
        league: One league or provider group, e.g. cfb, nba or euroleague.
        search: Case-insensitive substring filter on function names.
        parsers_only: Only parse_* callables.
        wrappers_only: Exclude parse_* callables.
    """
    if parsers_only and wrappers_only:
        raise ValueError("--parsers-only and --wrappers-only are mutually exclusive")
    from sportsdataverse import list_functions as _list_functions

    result = _list_functions(league=league, search=search, parsers_only=parsers_only, wrappers_only=wrappers_only)
    if _state["json"]:
        _print_json(result)
    else:
        _out.print(render.function_listing(result, search))
    return 0


@app.command(name="function-count", group=_DISCOVER)
def function_count(*, league: Annotated[Optional[str], Parameter(name=["--league", "-l"])] = None) -> int:
    """Count the callable functions per league, or in one league.

    Args:
        league: One league or provider group; omit for every one.
    """
    from sportsdataverse import function_count as _function_count

    result = _function_count(league=league)
    if _state["json"]:
        _print_json(result)
    elif isinstance(result, int):
        print(result)
    else:
        _out.print(render.count_table(result))
    return 0


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

cache_app = App(name="cache", help="Inspect or control the HTTP response cache.", group=_CACHE)
app.command(cache_app)


@cache_app.command(name="mode")
def cache_mode(*, set_: Annotated[Optional[CacheMode], Parameter(name="--set")] = None) -> int:
    """Get the cache mode, or set it with --set.

    Args:
        set_: New mode; with no flag, print the current one.
    """
    from sportsdataverse import cache

    if set_:
        cache.set_cache_mode(set_)
        print(f"cache mode set to {set_!r}")
    else:
        print(cache.get_cache_mode())
    return 0


@cache_app.command(name="stats")
def cache_stats() -> int:
    """Show the cache mode, entry count and size on disk."""
    from sportsdataverse import cache

    stats = cache.cache_stats()
    if _state["json"]:
        _print_json(stats)
    else:
        _out.print(render.cache_table(stats))
    return 0


@cache_app.command(name="clear")
def cache_clear(*, pattern: Optional[str] = None) -> int:
    """Drop cached entries.

    Args:
        pattern: Filename glob, e.g. '*roster*'; omit to drop everything.
    """
    from sportsdataverse import cache

    n = cache.clear_cache(pattern=pattern)
    print(f"cleared {n} entries")
    return 0


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


@app.meta.default
def _meta(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    json: Annotated[bool, Parameter(negative="", help="Emit JSON (handy for piping to jq).")] = False,
) -> Any:
    _state["json"] = json
    command, bound, _ignored = app.parse_args(tokens, exit_on_error=False, print_error=True)
    return command(*bound.args, **bound.kwargs)


def run(argv: Optional[List[str]] = None) -> int:
    """Parse ``argv`` (``None`` reads ``sys.argv[1:]``), run the command and return the exit code."""
    tokens = sys.argv[1:] if argv is None else list(argv)
    _state["json"] = False
    try:
        result = app.meta(tokens, result_action="return_value", exit_on_error=False, print_error=True)
    except CycloptsError:
        return 2  # cyclopts already printed the usage error
    except Exception as e:
        from sportsdataverse.errors import SportsDataverseError

        # SportsDataverseError covers NoDataError (nothing there) and AssetFetchError (the fetch failed)
        if isinstance(e, (ValueError, RuntimeError, SportsDataverseError)):
            print(f"sdv: {e}", file=sys.stderr)
            return 2
        raise
    return result if isinstance(result, int) else 0
