"""Swiss Ice Hockey Federation wrappers (``sihf_*``) over the keyless SIHF statistics API.

``data.sihf.ch/statistic/api/cms`` serves every SIHF league -- National League, Swiss
League, MyHockey League, the amateur, junior and women's leagues -- as season tables
(results, standings, skater and goalie stats) and one page per game (event timeline,
box scores, team stats, lineups). Unofficial use of a public API: payloads are not
redistributed as release assets.

The six game wrappers all read the same ``/gameoverview`` page, so fetch it once and parse it
as many ways as needed::

    from sportsdataverse.hockey.sihf import parse_sihf_game_info, parse_sihf_game_lineups, sihf_pbp

    raw = sihf_pbp(game_id=20261105000325, return_parsed=False)
    info, lineups = parse_sihf_game_info(raw), parse_sihf_game_lineups(raw)
"""

from __future__ import annotations

# ``sihf`` sits beside the HockeyTech league families under ``hockey/`` but is not one of them:
# the generated flat module and its parsers are re-exported by hand here; generate.py renders
# sihf_api_web.py but does not edit this file.
from sportsdataverse.hockey.sihf import sihf_api_web as _api
from sportsdataverse.hockey.sihf import sihf_api_web_parsers as _parsers
from sportsdataverse.hockey.sihf.sihf_api_web import *  # noqa: F401,F403
from sportsdataverse.hockey.sihf.sihf_api_web_parsers import *  # noqa: F401,F403

# Explicit, so ``from sportsdataverse.hockey.sihf import *`` (the top-level package does it)
# exports the wrappers and parsers and never the submodule objects.
__all__ = [*_api.__all__, *_parsers.__all__]
