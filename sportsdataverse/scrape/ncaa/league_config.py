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

**Deprecated (0.1.6, removed in 0.2.0).** Nothing reads these configs: every engine
entry point takes the league slug directly, and ``WBB.periods = 4`` was a wrong
fourth owner of the period fact (women's basketball played halves through 2014-15).
Pass the league slug yourself, and get the period model from
:func:`sportsdataverse.scrape.ncaa.parse.wbb_period_model`. ``NcaaLeagueConfig``,
``MBB``, ``WBB`` and ``by_league`` still resolve but warn ``DeprecationWarning`` on access.
"""

from dataclasses import dataclass
from typing import Any

from sportsdataverse._deprecation import warn_deprecated


@dataclass(frozen=True)
class _NcaaLeagueConfig:
    """Identity facts for one college-basketball league's raw sweep (deprecated).

    Attributes:
        league: The slug threaded through every engine call (``"mbb"`` / ``"wbb"``).
        repo: Owning ``-raw`` repository name (log/runbook text only).
        periods: Regulation period count of the CURRENT era only. Wrong for WBB
            seasons through 2014-15 (halves); use
            :func:`sportsdataverse.scrape.ncaa.parse.wbb_period_model`.
    """

    league: str
    repo: str
    periods: int


_MBB = _NcaaLeagueConfig(league="mbb", repo="ncaa-mbb-hoops-raw", periods=2)
_WBB = _NcaaLeagueConfig(league="wbb", repo="ncaa-wbb-hoops-raw", periods=4)

_BY_LEAGUE = {c.league: c for c in (_MBB, _WBB)}


def _by_league(league: str) -> _NcaaLeagueConfig:
    """The config for a league slug (raises ``KeyError`` on unknown)."""
    return _BY_LEAGUE[league]


_DEPRECATED = {"NcaaLeagueConfig": _NcaaLeagueConfig, "MBB": _MBB, "WBB": _WBB, "by_league": _by_league}


def __getattr__(name: str) -> Any:  # PEP 562: warn on access, not on import
    if name in _DEPRECATED:
        warn_deprecated(
            f"sportsdataverse.scrape.ncaa.league_config.{name}",
            replacement="the league slug directly and sportsdataverse.scrape.ncaa.parse.wbb_period_model(season)",
            since="0.1.6",
            removed_in="0.2.0",
        )
        return _DEPRECATED[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
