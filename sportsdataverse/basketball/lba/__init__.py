"""Lega Basket Serie A (Italy) wrappers over legabasket.it/api (codegen flat-API family ``legabasket``).

Seasons are END years (2027 = 2026-27). A game body feeds several frames (the box score route feeds ``lba_box_score``
and ``lba_team_box_score``, the play-by-play route feeds ``lba_pbp`` and ``lba_pbp_legend``, the roster route
``lba_roster`` and ``lba_roster_coach``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.lba import lba_box_score, parse_lba_team_box_score

    raw = lba_box_score(game_id=25602, return_parsed=False)
    players, teams = parse_lba_box_score(raw), parse_lba_team_box_score(raw)

Unofficial use of a public source, whose terms restrict copying: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.lba import lba_parsers as _parsers
from sportsdataverse.basketball.lba import legabasket as _api
from sportsdataverse.basketball.lba.lba_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.lba.legabasket import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
