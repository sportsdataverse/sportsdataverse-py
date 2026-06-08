"""sportsdataverse.parsed -- explicit DataFrame-by-default alias modules.

As of 0.0.54 the default ``sportsdataverse.{league}`` modules already
return a polars DataFrame for parser-backed wrappers (pass
``return_parsed=False`` for the raw ``Dict``). This namespace predates
that change and is retained as an explicit, self-documenting alias:
every wrapper imported from ``sportsdataverse.parsed.{league}`` returns
a polars DataFrame by default (or pandas via ``return_as_pandas=True``),
regardless of the raw module's default.

Usage::

    # Default modules (0.0.54+): parsed DataFrame for parser-backed wrappers
    from sportsdataverse.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # -> polars
    raw = espn_nba_scoreboard(return_parsed=False)       # -> Dict

    # Explicit parsed alias (always DataFrame-by-default):
    from sportsdataverse.parsed.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # -> polars
    raw = espn_nba_scoreboard(return_parsed=False)       # -> Dict

Available leagues: ``nba``, ``wnba``, ``mbb``, ``wbb``, ``cfb``, ``nfl``,
``mlb``, ``nhl``.

Each ``sportsdataverse.parsed.{league}`` is a concrete generated module
(``tools/codegen/generate.py`` -> ``parsed_module.py.jinja``), not a
runtime ``types.ModuleType`` shim. Wrappers without a registered parser
(loaders, helpers) pass through unchanged from the raw module.
"""

from __future__ import annotations

from sportsdataverse.parsed import (  # noqa: F401
    cfb,
    mbb,
    mlb,
    nba,
    nfl,
    nhl,
    wbb,
    wnba,
)

__all__ = ["nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"]
