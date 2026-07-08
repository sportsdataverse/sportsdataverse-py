"""Women's shot-selection value (model 2 of the shot-quality spine).

Thin shim over :func:`sportsdataverse.mbb.mbb_shot_selection
.mbb_shot_selection` with ``league="womens"`` fixed.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_shot_selection import mbb_shot_selection

__all__ = ["wbb_shot_selection"]

wbb_shot_selection = functools.partial(mbb_shot_selection, league="womens")
wbb_shot_selection.__doc__ = mbb_shot_selection.__doc__
