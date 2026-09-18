"""Yahoo -> ESPN-summary adapter for ``CFBPlayProcess`` (private, experimental).

``source="yahoo"`` in :mod:`sportsdataverse.football.sources.dispatch`. Nothing here is
re-exported from :mod:`sportsdataverse.cfb`: every name is ``_``-prefixed, so no codegen or
reference-doc regeneration is involved, exactly as for the Shield NFL adapter.
"""

from sportsdataverse.cfb.yahoo_pbp.fetch import (
    _fetch_playbook_boxscore,
    _game_block,
    _has_plays,
    _resolve_game_id,
    _yahoo_cfb_game_id,
)
from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import _yahoo_adapter, _yahoo_to_espn_summary

__all__ = [
    "_fetch_playbook_boxscore",
    "_game_block",
    "_has_plays",
    "_resolve_game_id",
    "_yahoo_adapter",
    "_yahoo_cfb_game_id",
    "_yahoo_to_espn_summary",
]
