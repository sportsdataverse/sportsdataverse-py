"""Yahoo Sports wrappers (``graphite-secure.sports.yahoo.com`` + editorial).

A cross-sport package rather than a league one: the Shangrila query graph spans
NFL, NBA, MLB, NHL, WNBA, college, soccer, golf, tennis, MMA, motorsport and the
Olympics, so it is homed here alongside :mod:`sportsdataverse.odds` instead of
under any one league. The older ESPN-style CFB wrappers (``yahoo_cfb_*``) remain
in :mod:`sportsdataverse.cfb.cfb_yahoo_ext`.
"""

from __future__ import annotations

from sportsdataverse.yahoo.yahoo_shangrila import *  # noqa: F401,F403
from sportsdataverse.yahoo.yahoo_shangrila_parsers import (  # noqa: F401
    parse_yahoo_editorial,
    parse_yahoo_shangrila,
    parse_yahoo_shangrila_tables,
)
