"""Women's shot-value spine (league_id="10").

Thin shim over :mod:`sportsdataverse.nba.nba_shot_value` -- the models are one
league-agnostic core switched by ``league_id`` (women's court geometry +
shrinkage constant live in ``nba_shot_value_constants`` keyed ``"10"``), so
``wnba_shot_value`` binds ``league_id="10"`` and the per-shot model functions
are re-exported **by reference**. G-League needs no shim -- call
``nba_shot_value(..., league_id="20")``.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_shot_value import make_prob_by_context as make_prob_by_context
from sportsdataverse.nba.nba_shot_value import make_prob_joint as make_prob_joint
from sportsdataverse.nba.nba_shot_value import nba_shot_value
from sportsdataverse.nba.nba_shot_value import score_shot_xpoints as score_shot_xpoints
from sportsdataverse.nba.nba_shot_value import shooter_talent as shooter_talent
from sportsdataverse.nba.nba_shot_value import shot_selection_quality as shot_selection_quality
from sportsdataverse.nba.nba_shot_value import zone_value_map as zone_value_map

__all__ = [
    "make_prob_by_context",
    "make_prob_joint",
    "score_shot_xpoints",
    "shooter_talent",
    "shot_selection_quality",
    "wnba_shot_value",
    "zone_value_map",
]

wnba_shot_value = functools.partial(nba_shot_value, league_id="10")
wnba_shot_value.__doc__ = nba_shot_value.__doc__
