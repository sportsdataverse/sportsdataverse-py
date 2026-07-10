"""PWHL prediction-spine constants -- thin shim over
:mod:`sportsdataverse.nhl.nhl_prediction_constants`.

The prediction spine's algorithms are entirely league-agnostic (see the
design spec's "league-agnostic algorithms, league-specific constants"
convention); this module re-exports the metric functions and the
``LEAGUE_CONSTANTS`` registry **by reference** (not a copy) so
``sportsdataverse.pwhl`` callers get the identical implementation the NHL
side uses, with no duplicated logic to drift out of sync -- the same
pattern ``wbb_rapm`` uses to re-export ``mbb_rapm``.

Women's-league constants (wider ``margin_sd``, stronger ``shrink_k`` given
the PWHL's shorter competitive history) live in the shared
``LEAGUE_CONSTANTS["pwhl"]`` row; :func:`get_constants` here is pinned to
``"pwhl"`` so callers never need to pass the league string themselves.

Example:
    Quick start::

        from sportsdataverse.pwhl.pwhl_prediction_constants import get_constants

        pwhl = get_constants()
        print(pwhl.shrink_k, pwhl.margin_sd)

See Also:
    * `nhl-api-py`_ -- companion NHL Python client (the NHL core this shim re-exports).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from sportsdataverse.nhl.nhl_prediction_constants import (
    LEAGUE_CONSTANTS,
    LeagueConstants,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
    spearman_corr,
)
from sportsdataverse.nhl.nhl_prediction_constants import get_constants as _get_constants

__all__ = [
    "LEAGUE_CONSTANTS",
    "LeagueConstants",
    "as_of_ratings_split",
    "brier_score",
    "calibration_table",
    "get_constants",
    "log_loss_score",
    "mae",
    "spearman_corr",
]


def get_constants() -> LeagueConstants:
    """Resolve the PWHL row of the shared ``LEAGUE_CONSTANTS`` table.

    Returns:
        The :class:`LeagueConstants` row for ``"pwhl"``.

    Example:
        Quick start::

            from sportsdataverse.pwhl.pwhl_prediction_constants import get_constants
            get_constants().shrink_k
    """
    return _get_constants("pwhl")
