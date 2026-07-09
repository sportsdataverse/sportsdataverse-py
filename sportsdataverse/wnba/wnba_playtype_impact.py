"""WNBA play-type/impact -- by-reference shims over the league-agnostic NBA core (league_id="10").

All four models (Synergy play-type ratings, matchup DRAPM, foul-drawing,
expected turnovers) are league-agnostic algorithms parameterized on
``league_id``; WNBA-specific inputs are handled entirely by the shared
``nba_stats``/``wnba_stats`` runtime routing. These shims only fix the league
default, mirroring the ``wbb_rapm``-re-exports-``mbb_rapm`` /
``wnba_shot_value``-wraps-``nba_shot_value`` pattern already shipped elsewhere
in the package. G-League needs no shim -- call the NBA core functions directly
with ``league_id="20"``.

Synergy + matchup coverage is sparse for the WNBA relative to the NBA; every
wrapped function degrades to a zero-row frame with the documented schema when
the upstream fetch is empty (never raises).
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_expected_turnovers import nba_expected_turnovers as _tov
from sportsdataverse.nba.nba_foul_drawing import nba_foul_drawing as _foul
from sportsdataverse.nba.nba_matchup_drapm import nba_matchup_drapm as _drapm
from sportsdataverse.nba.nba_playtype import nba_playtype_ratings as _ratings

__all__ = [
    "wnba_playtype_ratings",
    "wnba_matchup_drapm",
    "wnba_foul_drawing",
    "wnba_expected_turnovers",
]

#: WNBA Synergy play-type-adjusted offense/defense -- see
#: :func:`sportsdataverse.nba.nba_playtype.nba_playtype_ratings` (``league_id="10"``).
wnba_playtype_ratings = functools.partial(_ratings, league_id="10")

#: WNBA matchup defensive RAPM -- see
#: :func:`sportsdataverse.nba.nba_matchup_drapm.nba_matchup_drapm` (``league_id="10"``).
wnba_matchup_drapm = functools.partial(_drapm, league_id="10")

#: WNBA foul-drawing / FT-generation -- see
#: :func:`sportsdataverse.nba.nba_foul_drawing.nba_foul_drawing` (``league_id="10"``).
wnba_foul_drawing = functools.partial(_foul, league_id="10")

#: WNBA expected turnovers -- see
#: :func:`sportsdataverse.nba.nba_expected_turnovers.nba_expected_turnovers` (``league_id="10"``).
wnba_expected_turnovers = functools.partial(_tov, league_id="10")
