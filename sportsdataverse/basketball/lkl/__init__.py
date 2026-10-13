"""LKL (Lietuvos krepsinio lyga, Lithuania) wrappers over lkl.lt (codegen flat-API family ``lkl``).

One host, keyless: the site's livestream JSON API (box score, summary, score line, statistics, leaders,
play-by-play and shot chart per game, play-by-play and shots from 2024-25) plus three server-rendered HTML
routes parsed here (the game page, the season results list and the standings). Game wrappers take ``game_id``
and serve FINISHED games only; ``lkl_results`` and ``lkl_standings`` take ``season``, the END year
(2026 = 2025-26), which ``lkl_runtime`` maps to the site's two opaque season ids. Several wrappers read the same
route (the box score feeds ``lkl_player_box``, ``lkl_team_box`` and ``lkl_period_scores``; the game page feeds
``lkl_game_info`` and ``lkl_game_players``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.lkl import lkl_player_box, parse_lkl_period_scores, parse_lkl_team_box

    raw = lkl_player_box(game_id=11446, return_parsed=False)
    teams, periods = parse_lkl_team_box(raw), parse_lkl_period_scores(raw)

The box score, leaders and play-by-play name players by slug only; the runtime stamps the game roster so the
frames carry ``player_id``. Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._BASKETBALL_HOMES). The generated module's import is added by the
# integration codegen run; the parser module is re-exported here.
from sportsdataverse.basketball.lkl.lkl_parsers import *  # noqa: F401,F403
