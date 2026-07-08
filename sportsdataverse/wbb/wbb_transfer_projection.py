"""Women's transfer-portal projection (model 4 of the spine).

Thin shim over :mod:`sportsdataverse.mbb.mbb_transfer_projection` with
``league="womens"`` fixed; ``transfer_cohort`` is re-exported by reference
(it is league-free). The women's ridge ships in ``wbb_transfer.json``.
"""

from __future__ import annotations

import functools

from sportsdataverse.mbb.mbb_transfer_projection import (
    mbb_transfer_projection,
)
from sportsdataverse.mbb.mbb_transfer_projection import (
    transfer_cohort as transfer_cohort,
)

__all__ = ["transfer_cohort", "wbb_transfer_projection"]

wbb_transfer_projection = functools.partial(mbb_transfer_projection, league="womens")
wbb_transfer_projection.__doc__ = mbb_transfer_projection.__doc__
