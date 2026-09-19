"""NCAA (stats.ncaa.org) -> ESPN-summary adapter for ``CFBPlayProcess`` (private, experimental).

``source="ncaa"`` in :mod:`sportsdataverse.football.sources.dispatch`. Nothing here is
re-exported from :mod:`sportsdataverse.cfb`: every name is ``_``-prefixed, so no codegen or
reference-doc regeneration is involved, exactly as for the Shield NFL and Yahoo CFB adapters.
"""

from sportsdataverse.cfb.ncaa_pbp.fetch import (
    _api_bundle,
    _archive_bundle,
    _espn_team_ids_from_bundle,
    _espn_team_ids_from_schedule,
    _fetch_bundle,
    _has_plays,
    _resolve_contest_id,
)
from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _ncaa_adapter, _ncaa_to_espn_summary

__all__ = [
    "_api_bundle",
    "_archive_bundle",
    "_espn_team_ids_from_bundle",
    "_espn_team_ids_from_schedule",
    "_fetch_bundle",
    "_has_plays",
    "_ncaa_adapter",
    "_ncaa_to_espn_summary",
    "_resolve_contest_id",
]
