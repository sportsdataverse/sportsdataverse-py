"""G League stats parsers — the G League is served by stats.nba.com (``LeagueID=20``) in the same
``{resultSets: [{name, headers, rowSet}]}`` envelope, so re-export the generic NBA parser under
G League names for the generated wrappers.
"""

from __future__ import annotations

from sportsdataverse.nba.nba_stats_parsers import (
    parse_nba_stats_leaguedashplayerstats as parse_nbagl_stats_leaguedashplayerstats,
    parse_nba_stats_result_sets as parse_nbagl_stats_result_sets,
)

__all__ = ["parse_nbagl_stats_result_sets", "parse_nbagl_stats_leaguedashplayerstats"]
