"""WNBA stats parsers — the resultSets shape is identical to NBA, so re-export the
generic parser and expose WNBA-named aliases.

The stats.wnba.com API shares the same ``{resultSets: [{name, headers, rowSet}]}``
envelope as stats.nba.com, so the NBA generic parser works without modification.
These aliases provide WNBA-namespaced entry points for generated wrappers.
"""

from __future__ import annotations

from sportsdataverse.nba.nba_stats_parsers import (
    parse_nba_stats_leaguedashplayerstats as parse_wnba_stats_leaguedashplayerstats,
    parse_nba_stats_result_sets as parse_wnba_stats_result_sets,
)

__all__ = ["parse_wnba_stats_result_sets", "parse_wnba_stats_leaguedashplayerstats"]
