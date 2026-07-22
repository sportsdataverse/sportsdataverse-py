"""Sport rules — league parameterization of the basketball engine.

One engine, league arg (the reference sport-parameterization lesson): the
possession walk is identical across basketball leagues; only the clock
structure differs. Everything outcome-related (three rate, FT pct, pace)
is data-driven through each league's own shelf, never hardcoded here.
"""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class SportRules:
    """Clock structure for one basketball league.

    Attributes:
        league: Slug (``nba``, ``nbagl``, ``wnba``, ``mbb``, ``wbb``).
        periods: Regulation period count.
        period_seconds: Seconds per regulation period.
        ot_seconds: Seconds per overtime period.
    """

    league: str
    periods: int = 4
    period_seconds: float = 720.0
    ot_seconds: float = 300.0


NBA_RULES = SportRules("nba")
NBAGL_RULES = SportRules("nbagl")
WNBA_RULES = SportRules("wnba", periods=4, period_seconds=600.0)
MBB_RULES = SportRules("mbb", periods=2, period_seconds=1200.0)
WBB_RULES = SportRules("wbb", periods=4, period_seconds=600.0)

RULES_BY_LEAGUE = {r.league: r for r in (NBA_RULES, NBAGL_RULES, WNBA_RULES, MBB_RULES, WBB_RULES)}
