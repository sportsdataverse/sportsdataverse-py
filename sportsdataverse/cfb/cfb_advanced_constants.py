"""Advanced-stats canonical constants + validation metric helpers.

Canonical published values (Bill Connelly / GameOnPaper /
collegefootballdata methodology) used by the CFB advanced-stats models
(``cfb_advanced_stats``, ``cfb_field_position``, ``cfb_adjusted_tempo``).
No threshold is hard-coded inside a model function body -- it comes from
this module.

``spearman_corr`` / ``mae`` are re-exported from
:mod:`sportsdataverse.cfb.cfb_prediction_constants` (single source).
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from sportsdataverse.cfb.cfb_prediction_constants import (  # noqa: F401
    mae,
    spearman_corr,
)

__all__ = [
    "GARBAGE_TIME_MARGIN",
    "SUCCESS_COEF",
    "EXPLOSIVE_EPA",
    "FP_ARTIFACT",
    "AdjustConfig",
    "spearman_corr",
    "mae",
    "rank_desc",
]

#: Bill Connelly garbage-time thresholds -- a play is garbage time if the
#: absolute score margin EXCEEDS this many points in the given quarter
#: (the 4th-quarter threshold applies to overtime as well).
GARBAGE_TIME_MARGIN: dict[int, int] = {1: 43, 2: 37, 3: 27, 4: 21}

#: Success = yards_gained >= SUCCESS_COEF[down] * distance
#: (Connelly/GameOnPaper 50/70/100 rule; 4th down uses the 3rd-down coef).
SUCCESS_COEF: dict[int, float] = {1: 0.5, 2: 0.7, 3: 1.0, 4: 1.0}

#: Explosive-play EPA thresholds (match cfb_pbp.CFBPlayProcess).
EXPLOSIVE_EPA: dict[str, float] = {"pass": 2.4, "rush": 1.8}

#: Bundled field-position expected-points artifact filename
#: (sportsdataverse/cfb/models/; fit by dev/cfb_advanced/fit_field_position.py).
FP_ARTIFACT = "cfb_field_position_ep.parquet"


@dataclass
class AdjustConfig:
    """Configuration for the iterative opponent-adjustment solver.

    Attributes:
        shrink: pseudo-play shrinkage toward the grand mean (0 = none).
        max_iter: maximum fixed-point iterations.
        tol: max-abs-delta convergence tolerance.
    """

    shrink: float = 0.0
    max_iter: int = 50
    tol: float = 1e-5


def rank_desc(x: pl.Expr) -> pl.Expr:
    """Dense rank, descending (rank 1 = largest value).

    Args:
        x: polars expression to rank.

    Returns:
        ``Int64`` dense-rank expression.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb.cfb_advanced_constants import rank_desc
            pl.DataFrame({"x": [3.0, 1.0, 2.0]}).with_columns(r=rank_desc(pl.col("x")))
    """
    return x.rank(method="dense", descending=True).cast(pl.Int64)
