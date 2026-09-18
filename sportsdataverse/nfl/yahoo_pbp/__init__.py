"""Yahoo -> ESPN-summary adapter for ``NFLPlayProcess`` (private, experimental).

``source="yahoo"`` in :mod:`sportsdataverse.football.sources.dispatch`. Nothing here is
re-exported from :mod:`sportsdataverse.nfl`: every name is ``_``-prefixed, so no codegen or
reference-doc regeneration is involved, exactly as for the Shield and Yahoo-CFB adapters.
"""

from sportsdataverse.nfl.yahoo_pbp.fetch import (
    _fetch_playbook_boxscore,
    _game_block,
    _has_plays,
    _resolve_row,
    _yahoo_nfl_game_id,
)
from sportsdataverse.nfl.yahoo_pbp.to_espn_summary import _yahoo_adapter, _yahoo_to_espn_summary

__all__ = [
    "_fetch_playbook_boxscore",
    "_game_block",
    "_has_plays",
    "_resolve_row",
    "_yahoo_adapter",
    "_yahoo_nfl_game_id",
    "_yahoo_to_espn_summary",
]
