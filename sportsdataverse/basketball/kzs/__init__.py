"""KZS leagues (Slovenia) wrappers over api.kzs.si (codegen flat-API family ``kzs``).

Seasons are END years (2027 = 2026/2027); KZS' own season ids are opaque and the runtime maps them. A body can feed
several frames (the match header feeds ``kzs_match`` and ``kzs_period_scores``, the box score route feeds
``kzs_box_score``, ``kzs_team_box_score`` and ``kzs_score_changes``, the play-by-play route ``kzs_pbp`` and
``kzs_pbp_players``, a competition ``kzs_competition`` and ``kzs_phases``), so fetch it once and parse it as many ways
as needed::

    from sportsdataverse.basketball.kzs import kzs_box_score, parse_kzs_team_box_score

    raw = kzs_box_score(match_id=101839, return_parsed=False)
    players, teams = parse_kzs_box_score(raw), parse_kzs_team_box_score(raw)

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.kzs import kzs as _api
from sportsdataverse.basketball.kzs import kzs_parsers as _parsers
from sportsdataverse.basketball.kzs.kzs import *  # noqa: F401,F403
from sportsdataverse.basketball.kzs.kzs_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
