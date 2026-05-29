"""sportsdataverse.cli — command-line interface.

Wraps the top-level QoL helpers (find_team / find_athlete / find_event /
list_functions / cache controls) so users can poke at the package without
spinning up a Python REPL.

Installed as the ``sdv`` console script (see ``pyproject.toml``):

::

    $ sdv find-team lakers --league nba
    {
      "id": "13",
      "abbreviation": "LAL",
      "displayName": "Los Angeles Lakers",
      ...
    }

    $ sdv list-functions --league mlb --search statcast
    statcast_gamefeed
    statcast_leaderboard_arm_strength
    statcast_leaderboard_bat_tracking
    ...

    $ sdv function-count
    cfb     149
    mbb     146
    mlb     196
    ...

    $ sdv cache stats
    mode: filesystem
    entries: 47
    cache_dir: /Users/.../.cache/sportsdataverse
    disk_bytes: 12348721

Use ``--json`` on any command to get raw JSON output (handy for piping
to ``jq``); the default is a friendlier human-readable format.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _print_pretty(obj: Any) -> None:
    """Human-readable output for the default (non-JSON) mode."""
    if obj is None:
        print("(no match)")
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            print(f"  {k}: {v}")
        return
    if isinstance(obj, list):
        for item in obj:
            _print_pretty(item)
            print()
        return
    print(obj)


def _print_two_col(rows: list[tuple[str, str]], header: Optional[tuple[str, str]] = None) -> None:
    """Aligned two-column table for things like function_count output."""
    if header:
        rows = [header] + rows
    widths = [max(len(str(r[0])) for r in rows), max(len(str(r[1])) for r in rows)]
    for r in rows:
        print(f"  {str(r[0]):<{widths[0]}}  {str(r[1]):>{widths[1]}}")


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------


def cmd_find_team(args: argparse.Namespace) -> int:
    from sportsdataverse import find_team

    result = find_team(args.name, league=args.league, multi=args.multi)
    if args.json:
        _print_json(result)
    else:
        _print_pretty(result)
    return 0 if result else 1


def cmd_find_athlete(args: argparse.Namespace) -> int:
    from sportsdataverse import find_athlete

    result = find_athlete(
        args.name,
        league=args.league,
        team=args.team,
        multi=args.multi,
    )
    if args.json:
        _print_json(result)
    else:
        _print_pretty(result)
    return 0 if result else 1


def cmd_find_event(args: argparse.Namespace) -> int:
    from sportsdataverse import find_event

    result = find_event(
        date=args.date,
        league=args.league,
        home=args.home,
        away=args.away,
        multi=args.multi,
    )
    if args.json:
        _print_json(result)
    else:
        _print_pretty(result)
    return 0 if result else 1


def cmd_list_functions(args: argparse.Namespace) -> int:
    from sportsdataverse import list_functions

    result = list_functions(
        league=args.league,
        search=args.search,
        parsers_only=args.parsers_only,
        wrappers_only=args.wrappers_only,
    )
    if args.json:
        _print_json(result)
    elif isinstance(result, list):
        for name in result:
            print(name)
    elif isinstance(result, dict):
        for league in sorted(result):
            names = result[league]
            print(f"[{league}]  ({len(names)} entries)")
            for name in names:
                print(f"  {name}")
            print()
    return 0


def cmd_function_count(args: argparse.Namespace) -> int:
    from sportsdataverse import function_count

    result = function_count(league=args.league)
    if args.json:
        _print_json(result)
    elif isinstance(result, int):
        print(result)
    else:
        rows = [(league, str(n)) for league, n in sorted(result.items())]
        _print_two_col(rows)
    return 0


def cmd_cache(args: argparse.Namespace) -> int:
    from sportsdataverse import cache

    if args.cache_action == "mode":
        if args.set_mode:
            cache.set_cache_mode(args.set_mode)
            print(f"cache mode set to {args.set_mode!r}")
        else:
            print(cache.get_cache_mode())
        return 0
    if args.cache_action == "stats":
        stats = cache.cache_stats()
        if args.json:
            _print_json(stats)
        else:
            _print_pretty(stats)
        return 0
    if args.cache_action == "clear":
        n = cache.clear_cache(pattern=args.pattern)
        print(f"cleared {n} entries")
        return 0
    return 1


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sdv",
        description=(
            "sportsdataverse-py CLI — name-to-ID resolution, function "
            "discovery, and HTTP cache controls without writing any Python."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON output (handy for piping to jq).",
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    # ---- find-team ---------------------------------------------------------
    p = sub.add_parser("find-team", help="Resolve a team name to its ID.")
    p.add_argument("name", help="Team identifier — full or partial name.")
    p.add_argument("--league", "-l", required=True, choices=["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"])
    p.add_argument("--multi", action="store_true", help="Return every match instead of just the first.")
    p.set_defaults(func=cmd_find_team)

    # ---- find-athlete ------------------------------------------------------
    p = sub.add_parser("find-athlete", help="Resolve an athlete name to ESPN athlete metadata.")
    p.add_argument("name", help="Athlete identifier — full or partial name.")
    p.add_argument("--league", "-l", required=True, choices=["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"])
    p.add_argument("--team", "-t", default=None, help="Optional team filter — speeds the search dramatically.")
    p.add_argument("--multi", action="store_true")
    p.set_defaults(func=cmd_find_athlete)

    # ---- find-event --------------------------------------------------------
    p = sub.add_parser("find-event", help="Resolve a game to its ESPN event ID.")
    p.add_argument("date", help="Game date as YYYY-MM-DD or YYYYMMDD.")
    p.add_argument("--league", "-l", required=True, choices=["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"])
    p.add_argument("--home", default=None, help="Optional home-team filter.")
    p.add_argument("--away", default=None, help="Optional away-team filter.")
    p.add_argument("--multi", action="store_true")
    p.set_defaults(func=cmd_find_event)

    # ---- list-functions ----------------------------------------------------
    p = sub.add_parser(
        "list-functions",
        help="Searchable function index across all 8 leagues.",
    )
    p.add_argument("--league", "-l", default=None, choices=["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"])
    p.add_argument("--search", "-s", default=None, help="Case-insensitive substring filter on function names.")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--parsers-only", action="store_true", help="Restrict to parse_* callables.")
    group.add_argument("--wrappers-only", action="store_true", help="Exclude parse_* callables.")
    p.set_defaults(func=cmd_list_functions)

    # ---- function-count ----------------------------------------------------
    p = sub.add_parser("function-count", help="Per-league callable count.")
    p.add_argument("--league", "-l", default=None, choices=["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"])
    p.set_defaults(func=cmd_function_count)

    # ---- cache -------------------------------------------------------------
    p = sub.add_parser("cache", help="Inspect / control the HTTP response cache.")
    cache_sub = p.add_subparsers(dest="cache_action", required=True)

    p_mode = cache_sub.add_parser("mode", help="Get or set the cache mode.")
    p_mode.add_argument(
        "--set",
        dest="set_mode",
        default=None,
        choices=["off", "memory", "filesystem"],
        help="New mode. With no flag, prints the current mode.",
    )

    cache_sub.add_parser("stats", help="Show entries / disk-bytes / mode.")

    p_clear = cache_sub.add_parser("clear", help="Drop cached entries.")
    p_clear.add_argument("--pattern", default=None, help="Filename glob (e.g. '*roster*'). Default: drop all.")

    p.set_defaults(func=cmd_cache)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, KeyError, RuntimeError) as e:
        print(f"sdv: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
