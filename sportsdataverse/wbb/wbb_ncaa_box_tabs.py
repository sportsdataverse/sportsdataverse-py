"""WBB re-export of the shared stats.ncaa.org basketball box-tab parsers
(officials / team stats by period / linescore).

Thin shim over :mod:`sportsdataverse.mbb.mbb_ncaa_box_tabs` -- the men's and
women's ``/officials``, ``/team_stats`` and ``/box_score`` pages are league-
agnostic (WBB runs four ``Nth Period`` quarters, MBB two halves; the parsers
handle either), so this is a pure delegation providing the canonical
``parse_ncaa_wbb_*`` names.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    LINESCORE_SCHEMA,
    OFFICIALS_SCHEMA,
    TEAM_STATS_SCHEMA,
)
from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    parse_ncaa_bb_linescore as parse_ncaa_wbb_linescore,
)
from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    parse_ncaa_bb_officials as parse_ncaa_wbb_officials,
)
from sportsdataverse.mbb.mbb_ncaa_box_tabs import (
    parse_ncaa_bb_team_stats as parse_ncaa_wbb_team_stats,
)

__all__ = [
    "LINESCORE_SCHEMA",
    "OFFICIALS_SCHEMA",
    "TEAM_STATS_SCHEMA",
    "parse_ncaa_wbb_linescore",
    "parse_ncaa_wbb_officials",
    "parse_ncaa_wbb_team_stats",
]
