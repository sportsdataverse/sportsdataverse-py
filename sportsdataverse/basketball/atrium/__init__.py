"""Sportradar Atrium embed API (league sites) wrappers (codegen flat-API family ``atrium``).

One host serves every league site that embeds the Sportradar "Connect" widget; ``website_id``
picks the site (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama,
236 Austria, 326 Montenegro) and one site carries many competitions. Seasons are END years: a
year can hold several competitions on one site, so pair ``season`` with ``competition_id`` (or pass
``season_id``). Find them in the season list::

    from sportsdataverse.basketball.atrium import atrium_box_score, atrium_fixtures, atrium_seasons

    atrium_seasons(website_id=12).filter(season_end_year=2026)
    games = atrium_fixtures(website_id=12, season=2026, competition_id="3f4064bb-51ad-11f0-aaaf-2923c944b404")
    box = atrium_box_score(website_id=12, fixture_id=games["fixture_id"][0])

Several wrappers read the same route (``fixture_detail`` feeds the game header, periods, box
score, team box score, play-by-play and shots), so fetch once and parse as many ways as needed
with ``return_parsed=False`` and the ``parse_atrium_*`` functions. Statistic columns are configured
per site (a column a site omits is null), and the player stat tables carry no ids in their rows:
``person_id`` there is decoded from the widget link beside each name. See
:mod:`sportsdataverse.basketball.atrium.atrium_parsers`.

Unofficial use of a public source: payloads are not redistributed as release assets.
"""

from __future__ import annotations

# Nested from its first release (generate._NESTED_HOMES): the generated wrappers plus the parsers.
from sportsdataverse.basketball.atrium import atrium as _api
from sportsdataverse.basketball.atrium import atrium_parsers as _parsers
from sportsdataverse.basketball.atrium import atrium_runtime as _runtime
from sportsdataverse.basketball.atrium.atrium import *  # noqa: F401,F403
from sportsdataverse.basketball.atrium.atrium_parsers import *  # noqa: F401,F403
from sportsdataverse.basketball.atrium.atrium_runtime import *  # noqa: F401,F403

# The per-competition families (lnb_*, lnbp_*, nbl1_*, ...), minted from the competition registry.
from sportsdataverse.basketball.atrium import atrium_competitions_runtime as _competitions
from sportsdataverse.basketball.atrium.atrium_competitions_runtime import *  # noqa: F401,F403

__all__ = [*_api.__all__, *_parsers.__all__, *_runtime.__all__, *_competitions.__all__]
