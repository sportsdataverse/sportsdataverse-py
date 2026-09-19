"""Fox -> ESPN-summary adapter for ``CFBPlayProcess`` (private, experimental).

``source="fox"`` in :mod:`sportsdataverse.football.sources.dispatch`. A rewrite of the shipped
:mod:`sportsdataverse.cfb.cfb_pbp_fox`, which reads Fox's absolute field coordinates as
yards-to-goal. Nothing here is re-exported from :mod:`sportsdataverse.cfb`: every name is
``_``-prefixed, so no codegen or reference-doc regeneration is involved.
"""

from sportsdataverse.cfb.fox_pbp.to_espn_summary import (
    _fox_adapter,
    _fox_cfb_to_espn_summary,
    _resolve_fox_event_id,
)

__all__ = ["_fox_adapter", "_fox_cfb_to_espn_summary", "_resolve_fox_event_id"]
