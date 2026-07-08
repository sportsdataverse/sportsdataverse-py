"""Women's xPoints-per-shot (model 1 of the shot-quality spine).

Thin shim over :mod:`sportsdataverse.mbb.mbb_shot_quality` with
``league="womens"`` fixed (women's shrinkage constants + anchors).
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_shot_quality import mbb_shot_quality, mbb_shot_quality_model

__all__ = ["wbb_shot_quality", "wbb_shot_quality_model"]

wbb_shot_quality_model = functools.partial(mbb_shot_quality_model, league="womens")
wbb_shot_quality_model.__doc__ = mbb_shot_quality_model.__doc__
wbb_shot_quality = functools.partial(mbb_shot_quality, league="womens")
wbb_shot_quality.__doc__ = mbb_shot_quality.__doc__
