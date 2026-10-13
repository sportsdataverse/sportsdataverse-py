"""W League (Japan, women) wrappers over api.wjbl.01core.app (codegen flat-API family ``wjbl``).

The W League is Japan's top women's basketball league (the SportsDataverse R counterpart is wehoop). Seasons are END
years (2026 = 2025-26); the API's own league edition ids are opaque and the runtime maps them. A body can feed several
frames (the game header feeds ``wjbl_game`` and ``wjbl_period_scores``, the box score ``wjbl_box_score`` and
``wjbl_team_box_score``, the standings grid ``wjbl_team_standings`` and ``wjbl_head_to_head``, the roster
``wjbl_roster`` and ``wjbl_staff``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.wjbl import wjbl_box_score, parse_wjbl_team_box_score

    raw = wjbl_box_score(play_schedule_id=7987, return_parsed=False)
    players, teams = parse_wjbl_box_score(raw), parse_wjbl_team_box_score(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.wjbl import wjbl as _api
from sportsdataverse.basketball.wjbl import wjbl_parsers as _parsers
from sportsdataverse.basketball.wjbl.wjbl import *  # noqa: F401,F403
from sportsdataverse.basketball.wjbl.wjbl_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
