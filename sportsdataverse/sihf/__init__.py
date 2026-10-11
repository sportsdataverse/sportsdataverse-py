"""Swiss Ice Hockey Federation wrappers (``sihf_*``) over the keyless SIHF statistics API.

``data.sihf.ch/statistic/api/cms`` serves every SIHF league -- National League, Swiss
League, MyHockey League, the amateur, junior and women's leagues -- as season tables
(results, standings, skater and goalie stats) and one page per game (event timeline,
box scores, team stats, lineups). Unofficial use of a public API: payloads are not
redistributed as release assets.
"""

from __future__ import annotations

# ``sihf`` is a non-league home (like ``euroleague``): the generated flat module and its
# parsers are re-exported by hand here; generate.py renders sihf_api_web.py but does not edit this file.
from sportsdataverse.sihf.sihf_api_web import *  # noqa: F401,F403
from sportsdataverse.sihf.sihf_api_web_parsers import *  # noqa: F401,F403
