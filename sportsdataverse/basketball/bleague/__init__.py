"""B.LEAGUE (Japan: B1, B2, B3) wrappers (codegen flat-API family ``bleague``).

Game wrappers (``bleague_game_info``, ``bleague_period_scores``, ``bleague_player_box``, ``bleague_team_box``,
``bleague_team_summary``, ``bleague_pbp``, ``bleague_shots``, ``bleague_leaders``, ``bleague_score_track``) take
the global ``schedule_key``; ``bleague_schedule*`` and ``bleague_standings`` take ``season`` (the END year) and the
division ``tab``. ``bleague_codes`` decodes the play-by-play action, qualifier and shot-zone codes. Several
wrappers read the same route (one game context feeds nine frames, the schedule page four), so fetch it once and
parse it as many ways as needed::

    from sportsdataverse.basketball.bleague import bleague_pbp, parse_bleague_shots

    raw = bleague_pbp(schedule_key=504734, return_parsed=False)
    pbp, shots = parse_bleague_pbp(raw), parse_bleague_shots(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.bleague import bleague as _api
from sportsdataverse.basketball.bleague import bleague_parsers as _parsers
from sportsdataverse.basketball.bleague.bleague import *  # noqa: F401,F403
from sportsdataverse.basketball.bleague.bleague_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
