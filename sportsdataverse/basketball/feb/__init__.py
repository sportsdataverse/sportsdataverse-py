"""Spanish FEB wrappers: Primera FEB, Liga Femenina Endesa, Segunda FEB (codegen flat-API family ``feb``).

Two hosts feed one family: the LiveStats JSON API (per-game box score, play-by-play, shot chart with
court positions, team totals and leaders, from 2020-21) and the baloncestoenvivo.feb.es schedule pages
(HTML, parsed). Game wrappers take ``game_id``; schedule wrappers take ``group`` and ``season``, the
season END year (2027 = 2026-27). Several wrappers read the same route (``/TeamStats`` feeds
``feb_game_info``, ``feb_period_scores`` and ``feb_team_stats``; ``/resultados.aspx`` feeds
``feb_results`` and ``feb_standings``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.feb import feb_team_stats, parse_feb_game_info, parse_feb_period_scores

    raw = feb_team_stats(game_id=2535725, return_parsed=False)
    info, periods = parse_feb_game_info(raw), parse_feb_period_scores(raw)

Unofficial use of a public source: payloads are not redistributed as release assets, and the FEB's
legal notice claims database rights (see each wrapper's Notes).
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.feb import feb as _api
from sportsdataverse.basketball.feb import feb_parsers as _parsers
from sportsdataverse.basketball.feb.feb import *  # noqa: F401,F403
from sportsdataverse.basketball.feb.feb_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
