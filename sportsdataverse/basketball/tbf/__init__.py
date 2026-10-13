"""Turkish Basketball Federation wrappers over the TBF app API (codegen flat-API family ``tbf``).

One family serves every TBF league: ``competition`` is ``bsl`` (Basketbol Super Ligi), ``tbl`` (second tier) or
``kbsl`` (women's Super Ligi). Seasons are END years (2027 = 2026-2027); the runtime maps them to TBF's internal
season and activity ids. Schedules, standings, rosters and statistics reach back to 2015-16 (BSL to 1966-67); box
scores to 2006-07; play-by-play and shot charts exist from 2025-26. Every game carries its Genius LiveStats id
(``genius_id``), the join to the ``genius`` family.

The official TBF app backend, unofficial for SportsDataverse: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.tbf import tbf as _api
from sportsdataverse.basketball.tbf import tbf_parsers as _parsers
from sportsdataverse.basketball.tbf.tbf import *  # noqa: F401,F403
from sportsdataverse.basketball.tbf.tbf_parsers import *  # noqa: F401,F403

# Explicit, so the top-level ``from sportsdataverse.basketball.tbf import *`` exports only these.
__all__ = [*_api.__all__, *_parsers.__all__]
