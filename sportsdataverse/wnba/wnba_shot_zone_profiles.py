"""WNBA season shot profile by zone (BB-D35) -- thin shims over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_shot_zone_profiles` is league-agnostic: ``league_id="10"`` reads the
``wnba_stats_shots`` release, keeps the corner-band test alone (the WNBA corner sat nearer than
22 ft until 2013) and leaves the 1997-2010 hoop-centre placeholder attempts unlocated.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_shot_zone_profiles import nba_player_shot_zones as _player
from sportsdataverse.nba.nba_shot_zone_profiles import nba_team_shot_zones as _team
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_player_shot_zones = functools.partial(_player, league_id="10")
functools.update_wrapper(wnba_player_shot_zones, _player)
wnba_player_shot_zones.__signature__ = wnba_signature(_player)  # type: ignore[attr-defined]
wnba_player_shot_zones.__doc__ = wnba_doc(
    "WNBA season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3 (league_id='10'). "
    "See sportsdataverse.nba.nba_shot_zone_profiles.nba_player_shot_zones.",
    _player,
)

wnba_team_shot_zones = functools.partial(_team, league_id="10")
functools.update_wrapper(wnba_team_shot_zones, _team)
wnba_team_shot_zones.__signature__ = wnba_signature(_team)  # type: ignore[attr-defined]
wnba_team_shot_zones.__doc__ = wnba_doc(
    "WNBA season shot profile by zone per team, for and allowed (league_id='10'). "
    "See sportsdataverse.nba.nba_shot_zone_profiles.nba_team_shot_zones.",
    _team,
)

__all__ = ["wnba_player_shot_zones", "wnba_team_shot_zones"]
