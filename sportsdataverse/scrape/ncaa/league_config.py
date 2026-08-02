"""League identity for the stats.ncaa.org college-basketball sweep engine.

The capture stack itself is league-agnostic: stats.ncaa.org serves men's and
women's pages from one contest-id namespace with identical URL shapes, HTML
structure and bot-management behavior, and sdv-py's NCAA parser stack is
already league-parametric (period model and three-point arc are selected by the
``league`` argument). ``sportsdataverse.wbb.wbb_ncaa_fetch`` is itself a
by-reference re-export of the men's fetch layer for exactly this reason.

So a league is only ever a **token threaded through calls** — which is why the
engine's public functions take ``league`` as a *required* keyword rather than
defaulting it. A shared engine that defaults to one league is precisely how a
women's run silently reads men's data: before this extraction,
``ncaa_capture``'s CLI in the MBB repo hardcoded both the schedule-master path
and the capture league to ``"mbb"``, so it could not be pointed anywhere else.

Each ``-raw`` repo binds its own league in a thin shim; these configs name the
per-league facts the engine or a caller may need.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class NcaaLeagueConfig:
    """Identity facts for one college-basketball league's raw sweep.

    Attributes:
        league: The slug threaded through every engine call (``"mbb"`` /
            ``"wbb"``). It selects the store subtree, the ``(team, season)``
            id crosswalk, the play-by-play period model (halves vs quarters)
            and the three-point arc used to classify shots.
        repo: Owning ``-raw`` repository name (log/runbook text only).
        periods: Regulation period count — men's halves vs women's quarters.
    """

    league: str
    repo: str
    periods: int


MBB = NcaaLeagueConfig(league="mbb", repo="ncaa-mbb-hoops-raw", periods=2)
WBB = NcaaLeagueConfig(league="wbb", repo="ncaa-wbb-hoops-raw", periods=4)

_BY_LEAGUE = {c.league: c for c in (MBB, WBB)}


def by_league(league: str) -> NcaaLeagueConfig:
    """The config for a league slug (raises ``KeyError`` on unknown)."""
    return _BY_LEAGUE[league]
