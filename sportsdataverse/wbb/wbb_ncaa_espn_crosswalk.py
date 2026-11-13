"""Women's-basketball re-export of the NCAA <-> ESPN school, team- and player-id crosswalks.

The loaders are league-parameterized and live in
:mod:`sportsdataverse.mbb.mbb_ncaa_espn_crosswalk`; importing them from
``sportsdataverse.wbb`` is purely a namespace convenience. Pass
``league="wbb"`` to read the women's tables.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_espn_crosswalk import (
    ncaa_espn_player_crosswalk,
    ncaa_espn_school_seasons,
    ncaa_espn_schools,
    ncaa_espn_team_crosswalk,
)

__all__ = ["ncaa_espn_player_crosswalk", "ncaa_espn_school_seasons", "ncaa_espn_schools", "ncaa_espn_team_crosswalk"]
