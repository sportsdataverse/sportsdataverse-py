"""PWHL by-reference shims over the NHL player-impact spine (Phase 7).

Populated once the NHL player-impact engines (``nhl_xg``, ``nhl_gsax``, ``nhl_rapm``,
``nhl_unit_ratings``, ``nhl_special_teams``, ``nhl_war``) exist -- each PWHL wrapper is a
``functools.partial`` of the NHL function with ``league="pwhl"``.
"""

from __future__ import annotations

__all__: list[str] = []
