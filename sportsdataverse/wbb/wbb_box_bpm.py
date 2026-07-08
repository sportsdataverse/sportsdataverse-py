"""Women's college box-BPM (model 1 of the player-value spine).

Thin shim over :func:`sportsdataverse.mbb.mbb_box_bpm.mbb_box_bpm` with
``league="womens"`` fixed -- the team-constrained scoring is league-agnostic
and the women's coefficients ship in the bundled ``wbb_box_bpm.json``
artifact. See the mbb module docstring for the methodology.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm

__all__ = ["wbb_box_bpm"]

wbb_box_bpm = functools.partial(mbb_box_bpm, league="womens")
wbb_box_bpm.__doc__ = mbb_box_bpm.__doc__
