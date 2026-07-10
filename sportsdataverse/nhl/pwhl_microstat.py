"""PWHL microstat value -- by-reference shims over the NHL cores.

The microstat algorithms (faceoff value, penalty value, expected assists,
zone transitions, EDGE composite) are league-agnostic; only the fitted
constants differ by league (see
:data:`sportsdataverse.nhl.nhl_microstat_constants.LEAGUE_CONSTANTS`). This
module re-exports the four pbp-native NHL models bound to ``league="pwhl"``
via :func:`functools.partial` -- the same by-reference pattern
:mod:`sportsdataverse.wbb.wbb_rapm` uses over
:mod:`sportsdataverse.mbb.mbb_rapm` -- so PWHL callers get the identical
implementation with the women's-league constants, no duplicated logic.

``pwhl_edge_skating_value`` is bound to ``league="pwhl"`` too, and always
returns a documented **zero-row** frame: EDGE player-tracking is
``api-web.nhle.com``-only and has no PWHL feed.

The four pbp shims run against any PWHL pbp frame supplied on the Task-0.1
contract; wrapping a live PWHL pbp source is a deferred upstream item (see the
PWHL capture-contract note in
:mod:`sportsdataverse.nhl.nhl_microstat_constants`).

Example:
    Quick start::

        from sportsdataverse.nhl.pwhl_microstat import pwhl_faceoff_value

        out = pwhl_faceoff_value(pwhl_pbp)  # league="pwhl" is pre-bound

See Also:
    * `fastRhockey`_ -- R-side hockey data incl. the PWHL.

.. _fastRhockey: https://fastRhockey.sportsdataverse.org
"""

from __future__ import annotations

import functools
from typing import Any, Callable

from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value
from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists
from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value
from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value
from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions


def _bind_pwhl(core: Callable[..., Any], name: str) -> "functools.partial[Any]":
    shim = functools.partial(core, league="pwhl")
    shim.__name__ = name  # type: ignore[attr-defined]
    shim.__qualname__ = name  # type: ignore[attr-defined]
    shim.__doc__ = (
        f"PWHL shim: :func:`sportsdataverse.nhl.{core.__module__.split('.')[-1]}.{core.__name__}` "
        f'bound to ``league="pwhl"``. See that function for the full contract.'
    )
    return shim


pwhl_faceoff_value = _bind_pwhl(nhl_faceoff_value, "pwhl_faceoff_value")
pwhl_penalty_value = _bind_pwhl(nhl_penalty_value, "pwhl_penalty_value")
pwhl_expected_assists = _bind_pwhl(nhl_expected_assists, "pwhl_expected_assists")
pwhl_zone_transitions = _bind_pwhl(nhl_zone_transitions, "pwhl_zone_transitions")
# EDGE has no PWHL feed -> always a zero-row frame (the league="pwhl" branch
# in nhl_edge_skating_value short-circuits before any network access).
pwhl_edge_skating_value = _bind_pwhl(nhl_edge_skating_value, "pwhl_edge_skating_value")

__all__ = [
    "pwhl_faceoff_value",
    "pwhl_penalty_value",
    "pwhl_expected_assists",
    "pwhl_zone_transitions",
    "pwhl_edge_skating_value",
]
