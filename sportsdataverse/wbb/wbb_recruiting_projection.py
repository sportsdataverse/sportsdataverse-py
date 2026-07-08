"""Women's recruiting -> production projection (model 3 of the spine).

Thin shim over :func:`sportsdataverse.mbb.mbb_recruiting_projection
.mbb_recruiting_projection` with ``league="womens"`` fixed; the women's
ridge ships in ``wbb_recruiting.json``.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_recruiting_projection import mbb_recruiting_projection

__all__ = ["wbb_recruiting_projection"]

wbb_recruiting_projection = functools.partial(mbb_recruiting_projection, league="womens")
wbb_recruiting_projection.__doc__ = mbb_recruiting_projection.__doc__
