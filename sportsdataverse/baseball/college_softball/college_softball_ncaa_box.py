"""stats.ncaa.org college-softball (NCAA sport code ``WSB``) box-score parsers.

Thin re-export of the college-baseball box parsers **by reference** -- softball's
box_score / team_stats / individual_stats / situational_stats tabs use the
identical markup as baseball, so one set of parsers serves both. See
:mod:`sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box` for
schemas, provenance, and details.

Example:
    Quick start::

        from sportsdataverse.baseball.college_softball import (
            parse_college_softball_ncaa_linescore,
        )
        df = parse_college_softball_ncaa_linescore(open("box_score.html").read())
"""

from __future__ import annotations

from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    LINESCORE_SCHEMA,
    TEAM_STATS_SCHEMA,
)
from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    parse_college_baseball_ncaa_linescore as parse_college_softball_ncaa_linescore,
)
from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    parse_college_baseball_ncaa_player_stats as parse_college_softball_ncaa_player_stats,
)
from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    parse_college_baseball_ncaa_situational_stats as parse_college_softball_ncaa_situational_stats,
)
from sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box import (
    parse_college_baseball_ncaa_team_stats as parse_college_softball_ncaa_team_stats,
)

__all__ = [
    "LINESCORE_SCHEMA",
    "TEAM_STATS_SCHEMA",
    "parse_college_softball_ncaa_linescore",
    "parse_college_softball_ncaa_player_stats",
    "parse_college_softball_ncaa_situational_stats",
    "parse_college_softball_ncaa_team_stats",
]
