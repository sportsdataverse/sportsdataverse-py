"""CBS -> ESPN-summary adapter for ``CFBPlayProcess`` (private, experimental).

``source="cbs"`` in :mod:`sportsdataverse.football.sources.dispatch`. Nothing here is
re-exported from :mod:`sportsdataverse.cfb`: every name is ``_``-prefixed, so no codegen or
reference-doc regeneration is involved, exactly as for the Shield and Yahoo adapters.
"""

from sportsdataverse.cfb.cbs_pbp.game_id import _resolve_cbs_game_id
from sportsdataverse.cfb.cbs_pbp.teams import _cbs_team
from sportsdataverse.cfb.cbs_pbp.to_espn_summary import (
    _cbs_adapter,
    _cbs_cfb_to_espn_summary,
    _fetch_cbs_game,
    _has_coverage,
)

__all__ = [
    "_cbs_adapter",
    "_cbs_cfb_to_espn_summary",
    "_cbs_team",
    "_fetch_cbs_game",
    "_has_coverage",
    "_resolve_cbs_game_id",
]
