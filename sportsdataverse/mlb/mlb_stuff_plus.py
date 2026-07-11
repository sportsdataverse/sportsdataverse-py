"""Stuff+ ① — bundled xgboost pitch-quality run-value model.

Regresses Savant per-pitch ``delta_run_exp`` on physics + fastball-relative
standardized features (no location, no count — isolates raw *stuff*), then
maps the predicted run value to the published Stuff+ ``+``-scale (100 =
league average, higher = better) via :func:`_to_plus`. Follows the
FanGraphs/Eno-Sarris Stuff+ methodology (cited as a reference; no code
copied, so no license obligation).
"""

from __future__ import annotations

from typing import List

import numpy as np

from sportsdataverse.mlb.mlb_pitching_constants import PLUS_SCALE

__all__ = ["STUFF_FEATURES", "_to_plus"]

#: physics + fastball-relative standardized features (no location, no count) --
#: isolates pitch *stuff* from command/sequencing signal.
STUFF_FEATURES: List[str] = [
    "velo_z",
    "spin_z",
    "pfx_x_z",
    "pfx_z_z",
    "release_pos_x_z",
    "release_pos_z_z",
    "extension_z",
]


def _to_plus(rv_hat: np.ndarray, mean_rv: float, sd_rv: float, *, scale: float = PLUS_SCALE) -> np.ndarray:
    """Map predicted run value to the "+"-scale (100 = league average).

    Sign is inverted: a more-negative predicted run value (good for the
    pitcher) maps to a higher "+"-scale score.

    Args:
        rv_hat: Predicted per-pitch (or aggregated) run value.
        mean_rv: League mean of ``rv_hat`` (the fitting-task output).
        sd_rv: League standard deviation of ``rv_hat``. When ``0``, every
            input maps to exactly ``100.0`` (avoids a divide-by-zero).
        scale: "+"-scale points per league SD of ``rv_hat`` (default
            :data:`sportsdataverse.mlb.mlb_pitching_constants.PLUS_SCALE`).

    Returns:
        numpy.ndarray: The "+"-scale score, same shape as ``rv_hat``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.mlb.mlb_stuff_plus import _to_plus
            _to_plus(np.array([0.0, -0.1, 0.1]), mean_rv=0.0, sd_rv=0.1, scale=10.0)
    """
    rv_hat = np.asarray(rv_hat, dtype=float)
    if sd_rv == 0:
        return np.full_like(rv_hat, 100.0)
    return 100.0 - scale * (rv_hat - mean_rv) / sd_rv
