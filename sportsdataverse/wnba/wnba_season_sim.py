"""WNBA season sim, standings and lottery odds (BB-D18) -- thin shims over the NBA core (league_id='10').

:mod:`sportsdataverse.nba.nba_season_sim` and :mod:`sportsdataverse.nba.nba_season_rules` are
league-agnostic: ``league_id="10"`` reads the ``wnba_stats_*`` releases and applies the WNBA's
one-table seeding, its tiebreakers, the top-eight playoff field and its two-pick lottery.
"""

from __future__ import annotations

import functools

from sportsdataverse.nba.nba_season_rules import nba_draft_lottery_odds as _odds
from sportsdataverse.nba.nba_season_rules import nba_season_standings as _standings
from sportsdataverse.nba.nba_season_sim import nba_season_sim as _sim
from sportsdataverse.wnba._bind import wnba_doc, wnba_signature

wnba_season_sim = functools.partial(_sim, league_id="10")
functools.update_wrapper(wnba_season_sim, _sim)
wnba_season_sim.__signature__ = wnba_signature(_sim)  # type: ignore[attr-defined]
wnba_season_sim.__doc__ = wnba_doc(
    "WNBA season sim: projected W-L, seed, playoff and lottery odds (league_id='10'). "
    "See sportsdataverse.nba.nba_season_sim.nba_season_sim.",
    _sim,
)

wnba_season_standings = functools.partial(_standings, league_id="10")
functools.update_wrapper(wnba_season_standings, _standings)
wnba_season_standings.__signature__ = wnba_signature(_standings)  # type: ignore[attr-defined]
wnba_season_standings.__doc__ = wnba_doc(
    "WNBA standings with the WNBA tiebreakers (league_id='10'). "
    "See sportsdataverse.nba.nba_season_rules.nba_season_standings.",
    _standings,
)

wnba_draft_lottery_odds = functools.partial(_odds, league_id="10")
functools.update_wrapper(wnba_draft_lottery_odds, _odds)
wnba_draft_lottery_odds.__signature__ = wnba_signature(_odds)  # type: ignore[attr-defined]
wnba_draft_lottery_odds.__doc__ = wnba_doc(
    "WNBA draft-lottery odds by slot, two picks drawn (league_id='10'). "
    "See sportsdataverse.nba.nba_season_rules.nba_draft_lottery_odds.",
    _odds,
)

__all__ = ["wnba_draft_lottery_odds", "wnba_season_sim", "wnba_season_standings"]
