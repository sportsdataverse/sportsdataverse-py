"""sportsdataverse.parsed -- DataFrame-by-default mirror of the raw API.

The default ``sportsdataverse.{league}`` modules return raw ``Dict``
payloads -- the parser layer is opt-in via ``return_parsed=True`` so
existing callers from 0.0.50 and earlier are unaffected. **This
namespace flips the default**: every wrapper imported from
``sportsdataverse.parsed.{league}`` returns a polars DataFrame by
default (or pandas via ``return_as_pandas=True``).

Usage::

    # Raw-Dict default (existing API, unchanged):
    from sportsdataverse.nba import espn_nba_scoreboard
    raw = espn_nba_scoreboard()                          # -> Dict

    # DataFrame default (this module):
    from sportsdataverse.parsed.nba import espn_nba_scoreboard
    df = espn_nba_scoreboard()                           # -> polars

    # Override in either direction:
    raw_again = espn_nba_scoreboard(return_parsed=False) # -> Dict from parsed.*
    df_again  = espn_nba_scoreboard(return_parsed=True)  # -> polars from raw

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
