"""Women's college -> WNBA draft projection (model 5 of the spine).

Thin shim over :func:`sportsdataverse.mbb.mbb_draft_projection
.mbb_draft_projection` with ``league="womens"`` fixed; the women's dual-head
coefficients + WNBA tier edges (3-round draft) ship in ``wbb_draft.json``.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_draft_projection import mbb_draft_projection

__all__ = ["wbb_draft_projection"]

wbb_draft_projection = functools.partial(mbb_draft_projection, league="womens")
wbb_draft_projection.__doc__ = mbb_draft_projection.__doc__
