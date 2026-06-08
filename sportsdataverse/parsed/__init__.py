"""DEPRECATED since 0.0.54 -- sportsdataverse.parsed alias namespace.

.. deprecated:: 0.0.54
   The ``sportsdataverse.parsed`` namespace is **deprecated** and will be
   removed in a future release.  As of 0.0.54, the default
   ``sportsdataverse.{league}`` modules already return parsed polars
   DataFrames for parser-backed wrappers (pass ``return_parsed=False`` to
   recover the raw ``Dict``).  Importing any ``sportsdataverse.parsed.{league}``
   module emits a :class:`DeprecationWarning`.

Migrate by replacing ``parsed.*`` imports with direct league imports::

    # Before (deprecated):
    from sportsdataverse.parsed.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # -> polars

    # After (0.0.54+, preferred):
    from sportsdataverse.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # -> polars (default)
    raw = espn_nba_scoreboard(return_parsed=False)       # -> Dict

The ``parsed.*`` modules still function correctly (they pass
``return_parsed=True`` by default), so existing code continues to work
until the namespace is removed.

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
