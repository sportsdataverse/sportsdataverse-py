"""Women's shot-quality constants surface.

Thin shim over :mod:`sportsdataverse.mbb.mbb_shot_quality_constants` -- the
constants tables are already league-keyed, so this module re-exports the
accessors + tables **by reference**; wbb callers pass ``league="womens"``.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_shot_quality_constants import (
    BART_NATIONAL_SPLITS as BART_NATIONAL_SPLITS,
)
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    LEAGUE_CONSTANTS as LEAGUE_CONSTANTS,
)
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    PUBLISHED_ZONE_BASELINES as PUBLISHED_ZONE_BASELINES,
)
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    ShotQualityConstants as ShotQualityConstants,
)
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    get_constants as get_constants,
)
from sportsdataverse.mbb.mbb_shot_quality_constants import (
    three_point_radius as three_point_radius,
)

__all__ = [
    "BART_NATIONAL_SPLITS",
    "LEAGUE_CONSTANTS",
    "PUBLISHED_ZONE_BASELINES",
    "ShotQualityConstants",
    "get_constants",
    "three_point_radius",
]
