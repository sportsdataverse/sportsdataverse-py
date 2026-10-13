"""KBL (Korean Basketball League) wrappers (codegen flat-API family ``kbl``).

One JSON host (``api.kbl.or.kr``, the API behind www.kbl.or.kr) feeds 22 wrappers over 17 routes: seasons, clubs,
schedule, game header, period scores, play-by-play (the site's text cast), player and team box scores with advanced
rates, shots with x/y, lead tracker, game leaders, season player totals, standings, rank tracks, round calendar,
playoff seeds, series and games, round MVPs, current standings, season leaders and the player directory. Game wrappers
take ``gmkey`` (``S47G01N49``); season wrappers take ``season``, the END year (2026 = 2025-26), ``division``
(``"kbl"`` or ``"d_league"``) and ``game_code``. Several wrappers read the same route (the match chart feeds
``kbl_shots``, ``kbl_score_track`` and ``kbl_game_leaders``), so fetch it once and parse it as many ways as needed::

    from sportsdataverse.basketball.kbl import kbl_shots, parse_kbl_score_track

    raw = kbl_shots("S47G01N49", return_parsed=False)
    track = parse_kbl_score_track(raw)

``kbl_codes`` holds the season crosswalk and the translated code tables (actions, substitutions, fouls, competitions).
The site's sports2i-hosted ``/record/*`` pages (``kbl-api.sports2i.com``, ``api-stats.kbl.or.kr``) are not wrapped.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

from sportsdataverse.basketball.kbl import kbl as _api
from sportsdataverse.basketball.kbl import kbl_parsers as _parsers
from sportsdataverse.basketball.kbl.kbl import *  # noqa: F401,F403
from sportsdataverse.basketball.kbl.kbl_parsers import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__]
