"""Fox -> ESPN-summary adapter for ``NFLPlayProcess`` (private, experimental).

``source="fox"`` in :mod:`sportsdataverse.football.sources.dispatch`. Nothing here is
re-exported from :mod:`sportsdataverse.nfl`: every name is ``_``-prefixed, so no codegen or
reference-doc regeneration is involved, exactly as for the Shield and CBS adapters.
"""

from sportsdataverse.nfl.fox_pbp.to_espn_summary import (
    _fox_adapter,
    _fox_nfl_to_espn_summary,
    _resolve_fox_event_id,
    _segment_ids,
)

__all__ = ["_fox_adapter", "_fox_nfl_to_espn_summary", "_resolve_fox_event_id", "_segment_ids"]
