"""Women's college archetypes (model 2 of the player-value spine).

Thin shim over :func:`sportsdataverse.mbb.mbb_archetypes.mbb_archetypes`
with ``league="womens"`` fixed -- assignment is league-agnostic; the women's
centers/labels ship in the bundled ``wbb_archetypes.json`` artifact.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_archetypes import mbb_archetypes

__all__ = ["wbb_archetypes"]

wbb_archetypes = functools.partial(mbb_archetypes, league="womens")
wbb_archetypes.__doc__ = mbb_archetypes.__doc__
