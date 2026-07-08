"""Women's shooter true-talent (model 3 of the shot-quality spine).

Thin shim over :mod:`sportsdataverse.mbb.mbb_shooter_talent` with
``league="womens"`` fixed on the talent entry point; the split-half fitter
+ MSE helper are league-free and re-exported by reference.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_shooter_talent import (
    fit_shrinkage_k as fit_shrinkage_k,
)
from sportsdataverse.mbb.mbb_shooter_talent import (
    mbb_shooter_talent,
)
from sportsdataverse.mbb.mbb_shooter_talent import (
    talent_split_mse as talent_split_mse,
)

__all__ = ["fit_shrinkage_k", "talent_split_mse", "wbb_shooter_talent"]

wbb_shooter_talent = functools.partial(mbb_shooter_talent, league="womens")
wbb_shooter_talent.__doc__ = mbb_shooter_talent.__doc__
