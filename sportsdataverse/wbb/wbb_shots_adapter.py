"""Women's canonical shot adapter.

Thin shim over :mod:`sportsdataverse.mbb.mbb_shots_adapter` with
``league="womens"`` fixed on the batch entry point; the classifiers and
schema are league-parameterized and re-exported by reference.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_shots_adapter import (
    CANONICAL_SHOT_SCHEMA as CANONICAL_SHOT_SCHEMA,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    classify_point_value as classify_point_value,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    classify_zone_geometry as classify_zone_geometry,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    classify_zone_type as classify_zone_type,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    espn_shots_to_canonical as espn_shots_to_canonical,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    fit_espn_court_scale as fit_espn_court_scale,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    mbb_shot_data,
)
from sportsdataverse.mbb.mbb_shots_adapter import (
    shot_events_to_frame as shot_events_to_frame,
)

__all__ = [
    "CANONICAL_SHOT_SCHEMA",
    "classify_point_value",
    "classify_zone_geometry",
    "classify_zone_type",
    "espn_shots_to_canonical",
    "fit_espn_court_scale",
    "shot_events_to_frame",
    "wbb_shot_data",
]

wbb_shot_data = functools.partial(mbb_shot_data, league="womens")
wbb_shot_data.__doc__ = mbb_shot_data.__doc__
