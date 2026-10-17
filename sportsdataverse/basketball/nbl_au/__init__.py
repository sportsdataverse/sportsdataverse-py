"""NBL / WNBL / NBL1 (Australia) wrappers (codegen flat-API family ``nbl_rosetta``).

The data comes from Rosetta, the JSON proxy behind nbl.com.au, which serves three organisations from
one host: the NBL (men's top flight), the WNBL (women's) and NBL1 (the second tier). **It is this
package's only source of NBL 2025-26 play-by-play**: ESPN's ``nbl`` feed has box scores for that season
but no plays (the stats provider changed from Genius Sports to Sportradar DataCore in 2025-26), while
:func:`nbl_rosetta_match_pbp` has the full event log with shot x/y, and the same shape back to 2011-12
(WNBL about 2014-15). Box scores reach back to 1979.

Seasons are END years (2026 = 2025-26) and ``org`` picks the organisation (``"nbl"`` by default,
``"wnbl"``, ``"nbl1"``). Entity ids (``match_id``, ``team_id``, ``player_id``, ``season_id``) are
DataCore ids and join across routes. Several wrappers read the same route (the match page feeds
``nbl_rosetta_match``, ``_match_player_box``, ``_match_team_box``, ``_match_player_period_box`` and
``_match_pbp``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.nbl_au import nbl_rosetta_match, parse_nbl_rosetta_match_pbp

    raw = nbl_rosetta_match(match_id="b217a699-4bef-11f0-bc08-933162362355", return_parsed=False)
    pbp = parse_nbl_rosetta_match_pbp(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.nbl_au import nbl_rosetta as _api
from sportsdataverse.basketball.nbl_au import nbl_rosetta_parsers as _parsers
from sportsdataverse.basketball.nbl_au.nbl_rosetta import *  # noqa: F401,F403
from sportsdataverse.basketball.nbl_au.nbl_rosetta_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
