"""The one CLI contract every ESPN raw scraper uses.

``argparse(type=bool)`` is a trap and three of these repos fell into it: bash
hands over the *string* ``"false"``, and ``bool("false")`` is ``True``.
Combined with ``default=True`` it meant both ``-r false`` and omitting the flag
re-fetched every already-captured game, on every daily run, against ESPN.

Parse the text; never cast it. And default to NOT re-scraping: the raw tree is
the checkpoint (see each repo's README and the SDV scraping standards).
"""

from __future__ import annotations

import argparse

_TRUE = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE = frozenset({"0", "false", "f", "no", "n", "off", ""})


def str2bool(value: str | bool) -> bool:
    """Parse a shell-supplied boolean.

    Args:
        value: Either a real bool or the string form bash passes through.

    Returns:
        The parsed boolean. Unrecognised text is treated as False, because the
        expensive mistake is re-scraping the archive, not skipping a run.

    Raises:
        argparse.ArgumentTypeError: never -- kept total on purpose so a typo in
            a cron definition cannot trigger a full re-scrape.
    """
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return False


def season_args(argv: list[str] | None = None, *, rescrape_default: bool = False) -> argparse.Namespace:
    """Parse the standard ``--start_year`` / ``--end_year`` / ``--rescrape``.

    Args:
        argv: Argument list; defaults to ``sys.argv[1:]``.
        rescrape_default: Value used when ``--rescrape`` is omitted. Defaults
            to False (the archive is the checkpoint). The three hoopR/wehoop
            ESPN repos historically defaulted to True and their shell drivers
            still pass ``-r`` explicitly, so they can migrate without changing
            cron behavior by passing ``rescrape_default=True`` and flipping it
            deliberately in a separate, reviewable commit.

    Returns:
        Namespace with ``start_year: int``, ``end_year: int`` (defaulting to
        ``start_year``), and ``rescrape: bool``.

    Example:
        Typical use in a scraper entrypoint::

            from sportsdataverse.scrape.espn.cli import season_args

            args = season_args()
            for season in range(args.start_year, args.end_year + 1):
                scrape_season(season, rescrape=args.rescrape)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start_year",
        "-s",
        type=int,
        required=True,
        help="Start season end-year (YYYY, e.g. 2026 for 2025-26)",
    )
    parser.add_argument(
        "--end_year",
        "-e",
        type=int,
        default=None,
        help="End season end-year (YYYY); defaults to --start_year",
    )
    parser.add_argument(
        "--rescrape",
        "-r",
        type=str2bool,
        default=rescrape_default,
        help=f"Re-fetch payloads already on disk (default: {str(rescrape_default).lower()})",
    )
    args = parser.parse_args(argv)
    if args.end_year is None:
        args.end_year = args.start_year
    return args
