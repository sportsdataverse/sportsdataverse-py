"""Women's-basketball re-export of the NCAA <-> ESPN team-id crosswalk.

The loader is league-parameterized and lives in
:mod:`sportsdataverse.mbb.mbb_ncaa_espn_crosswalk`; importing it from
``sportsdataverse.wbb`` is purely a namespace convenience. Pass
``league="wbb"`` to read the women's table.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_espn_crosswalk import ncaa_espn_team_crosswalk

__all__ = ["ncaa_espn_team_crosswalk"]
