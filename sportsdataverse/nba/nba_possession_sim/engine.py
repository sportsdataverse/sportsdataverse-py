"""Game walk + Monte Carlo ensemble (WS4).

Alternating-possession game simulation over the PMF shelf, with the
ensemble accumulator: n_sim boxscores collapse into score / total / margin
sample vectors whose markets are priced directly by
``sportsdataverse.odds.odds_math`` (``calc_stats`` / ``prob_over`` /
copula). ``in_game_win_prob`` re-simulates from an arbitrary mid-game
state, which makes the WP curve a by-product of the same engine.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional

import numpy as np

from sportsdataverse.nba.nba_possession_sim.attribution import PlayerAttribution, terminal_outcome
from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.nodes import simulate_possession
from sportsdataverse.nba.nba_possession_sim.rules import NBA_RULES, SportRules
from sportsdataverse.nba.nba_possession_sim.shelf import OUTCOMES, Shelf

REGULATION_PERIODS = 4
PERIOD_SECONDS = 720.0
OT_SECONDS = 300.0
#: Possession clock burn is sampled uniform around the shelf's empirical mean
#: and clipped to the legal-ish range.
MIN_POSSESSION_SECONDS = 4.0
MAX_POSSESSION_SECONDS = 24.0


@dataclasses.dataclass
class GameState:
    """Mid-game state a simulation can resume from.

    Attributes:
        score_home: Home points.
        score_away: Away points.
        period: Current period (1-4, 5+ = OT).
        clock_seconds: Seconds remaining in the current period.
        offense_is_home: Whether the home team has the ball.
    """

    score_home: int = 0
    score_away: int = 0
    period: int = 1
    clock_seconds: float = PERIOD_SECONDS
    offense_is_home: bool = True


def _finish_game(
    shelf: Shelf,
    state: GameState,
    rng: np.random.Generator,
    factors: Optional[FactorAdjustment] = None,
    attribution: Optional[PlayerAttribution] = None,
    player_sink: Optional[Dict[int, int]] = None,
    rules: SportRules = NBA_RULES,
    pbp_sink: Optional[List[Dict[str, Any]]] = None,
    event_sink: Optional[Dict[str, int]] = None,
    expanded: bool = False,
    home_factors: Optional[FactorAdjustment] = None,
    away_factors: Optional[FactorAdjustment] = None,
) -> GameState:
    """Play a game to completion from ``state`` (mutates a copy)."""
    outcome_set = set(OUTCOMES)
    st = dataclasses.replace(state)
    while True:
        while st.clock_seconds > 0:
            diff = st.score_home - st.score_away if st.offense_is_home else st.score_away - st.score_home
            # matchup asymmetry: per-side factors (team strength) override
            # the shared adjustment for the side on offense
            side_factors = (home_factors if st.offense_is_home else away_factors) or factors
            if expanded:
                from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
                    simulate_possession_expanded,
                )

                points, trail, _ = simulate_possession_expanded(
                    shelf,
                    score_diff=float(diff),
                    period=st.period,
                    clock_seconds=st.clock_seconds,
                    rng=rng,
                    factors=side_factors,
                    offense_is_home=st.offense_is_home,
                )
            else:
                points, trail = simulate_possession(
                    shelf,
                    score_diff=float(diff),
                    period=st.period,
                    clock_seconds=st.clock_seconds,
                    rng=rng,
                    factors=side_factors,
                )
            if st.offense_is_home:
                st.score_home += points
            else:
                st.score_away += points
            if attribution is not None and player_sink is not None and points > 0:
                outcome = terminal_outcome(trail, outcome_set)
                if outcome is not None:
                    scorer = attribution.sample(st.offense_is_home, outcome, rng)
                    player_sink[scorer] = player_sink.get(scorer, 0) + points
            if event_sink is not None:
                for event in trail:
                    if event in outcome_set:
                        event_sink[event] = event_sink.get(event, 0) + 1
            if pbp_sink is not None:
                pbp_sink.append(
                    {
                        "period": st.period,
                        "clock_seconds": round(st.clock_seconds, 1),
                        "offense_is_home": st.offense_is_home,
                        "events": list(trail),
                        "points": points,
                        "score_home": st.score_home,
                        "score_away": st.score_away,
                    }
                )
            if shelf.pace_rates is not None:
                pace_base = shelf.pace_for(shelf.key_for(diff, st.period, st.clock_seconds))
            else:
                pace_base = shelf.mean_possession_seconds
            burn = float(
                np.clip(
                    rng.uniform(0.5, 1.5) * pace_base,
                    MIN_POSSESSION_SECONDS,
                    MAX_POSSESSION_SECONDS,
                )
            )
            st.clock_seconds -= burn
            st.offense_is_home = not st.offense_is_home
        if st.period >= rules.periods and st.score_home != st.score_away:
            return st
        st.period += 1
        st.clock_seconds = rules.ot_seconds if st.period > rules.periods else rules.period_seconds


def simulate_game(
    shelf: Shelf,
    rng: np.random.Generator,
    *,
    start: Optional[GameState] = None,
    factors: Optional[FactorAdjustment] = None,
    rules: SportRules = NBA_RULES,
) -> GameState:
    """Simulate one full game (or finish one from a mid-game state).

    Args:
        shelf: The PMF shelf.
        rng: Numpy generator.
        start: Optional resume state; defaults to the opening tip with a
            coin-flipped first possession.
        factors: Optional auditable PMF adjustment applied to every draw.

    Returns:
        The final :class:`GameState`.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_possession_sim.engine import simulate_game
            final = simulate_game(shelf, np.random.default_rng(7))
            print(final.score_home, final.score_away)
    """
    if start is None:
        start = GameState(clock_seconds=rules.period_seconds, offense_is_home=bool(rng.random() < 0.5))
    return _finish_game(shelf, start, rng, factors, rules=rules)


def simulate_ensemble(
    shelf: Shelf,
    *,
    n_sim: int = 1000,
    seed: Optional[int] = None,
    start: Optional[GameState] = None,
    factors: Optional[FactorAdjustment] = None,
    attribution: Optional[PlayerAttribution] = None,
    rules: SportRules = NBA_RULES,
    collect_event_counts: bool = False,
    home_factors: Optional[FactorAdjustment] = None,
    away_factors: Optional[FactorAdjustment] = None,
) -> Dict[str, Any]:
    """Monte Carlo ensemble: n_sim games collapsed into sample vectors.

    Args:
        shelf: The PMF shelf.
        n_sim: Number of simulated games.
        seed: RNG seed (same seed = identical ensemble).
        start: Optional shared resume state (in-game pricing).
        factors: Optional auditable PMF adjustment; its ``summary()`` is
            returned under the ``"factors"`` key.
        attribution: Optional :class:`PlayerAttribution` — when given, the
            output gains ``player_points``: ``{player_id: np.ndarray(n_sim)}``
            per-player point sample vectors (prop-market surface) that
            conserve team totals exactly.

    Returns:
        Dict with ``score_home`` / ``score_away`` / ``total`` / ``margin``
        (numpy sample vectors, home-perspective margin), ``win_prob_home``,
        ``mean_total``, and ``n_sim``. Feed the vectors straight to
        ``odds_math.calc_stats`` / ``prob_over`` for market pricing.

    Example:
        Total-points market::

            from sportsdataverse.nba.nba_possession_sim.engine import simulate_ensemble
            from sportsdataverse.odds.odds_math import prob_over
            ens = simulate_ensemble(shelf, n_sim=2000, seed=7)
            p = prob_over(ens["total"], 224.5)
    """
    rng = np.random.default_rng(seed)
    home: np.ndarray = np.empty(n_sim, dtype=np.int64)
    away: np.ndarray = np.empty(n_sim, dtype=np.int64)
    player_arrays: Optional[Dict[int, np.ndarray]] = None
    if attribution is not None:
        player_arrays = {pid: np.zeros(n_sim, dtype=np.int64) for pid in attribution.all_player_ids()}
    event_arrays: Optional[Dict[str, np.ndarray]] = None
    if collect_event_counts:
        event_arrays = {o: np.zeros(n_sim, dtype=np.int64) for o in OUTCOMES}
    for i in range(n_sim):
        sink: Optional[Dict[int, int]] = {} if attribution is not None else None
        event_sink: Optional[Dict[str, int]] = {} if collect_event_counts else None
        game_start = (
            start
            if start is not None
            else GameState(clock_seconds=rules.period_seconds, offense_is_home=bool(rng.random() < 0.5))
        )
        final = _finish_game(
            shelf,
            game_start,
            rng,
            factors,
            attribution,
            sink,
            rules=rules,
            event_sink=event_sink,
            home_factors=home_factors,
            away_factors=away_factors,
        )
        home[i] = final.score_home
        away[i] = final.score_away
        if player_arrays is not None and sink:
            for pid, pts in sink.items():
                player_arrays[pid][i] = pts
        if event_arrays is not None and event_sink:
            for outcome_name, count in event_sink.items():
                event_arrays[outcome_name][i] = count
    margin = home - away
    return {
        "score_home": home,
        "score_away": away,
        "total": home + away,
        "margin": margin,
        "win_prob_home": float((margin > 0).mean()),
        "mean_total": float((home + away).mean()),
        "n_sim": n_sim,
        "factors": factors.summary() if factors is not None else None,
        "player_points": player_arrays,
        "event_counts": event_arrays,
    }


def in_game_win_prob(
    shelf: Shelf,
    *,
    score_home: int,
    score_away: int,
    period: int,
    clock_seconds: float,
    offense_is_home: bool,
    n_sim: int = 500,
    seed: Optional[int] = None,
    factors: Optional[FactorAdjustment] = None,
    rules: SportRules = NBA_RULES,
) -> float:
    """Home win probability from an arbitrary mid-game state.

    Args:
        shelf: The PMF shelf.
        score_home: Home points on the board.
        score_away: Away points on the board.
        period: Current period.
        clock_seconds: Seconds remaining in the period.
        offense_is_home: Whether home has the ball.
        n_sim: Simulations to run from this state.
        seed: RNG seed.
        factors: Optional auditable PMF adjustment applied to every draw.

    Returns:
        P(home wins) across the ensemble.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.engine import in_game_win_prob
            wp = in_game_win_prob(shelf, score_home=98, score_away=95, period=4,
                                  clock_seconds=120.0, offense_is_home=False, seed=7)
    """
    start = GameState(
        score_home=score_home,
        score_away=score_away,
        period=period,
        clock_seconds=clock_seconds,
        offense_is_home=offense_is_home,
    )
    ensemble = simulate_ensemble(shelf, n_sim=n_sim, seed=seed, start=start, factors=factors, rules=rules)
    return float(ensemble["win_prob_home"])


def simulate_game_pbp(
    shelf: Shelf,
    rng: np.random.Generator,
    *,
    rules: SportRules = NBA_RULES,
    start: Optional[GameState] = None,
    factors: Optional[FactorAdjustment] = None,
    expanded: bool = False,
    home_factors: Optional[FactorAdjustment] = None,
    away_factors: Optional[FactorAdjustment] = None,
) -> "tuple[GameState, List[Dict[str, Any]]]":
    """Simulate one game AND emit its full play-by-play log.

    Args:
        shelf: The PMF shelf.
        rng: Numpy generator.
        rules: League clock structure (defaults to NBA).
        start: Optional resume state.
        factors: Optional auditable PMF adjustment.

    Returns:
        ``(final_state, pbp)`` where ``pbp`` is one row per possession:
        ``period``, ``clock_seconds``, ``offense_is_home``, ``events``
        (the outcome trail), ``points``, and the running score.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_possession_sim import simulate_game_pbp
            final, pbp = simulate_game_pbp(shelf, np.random.default_rng(7))
            print(len(pbp), pbp[-1])
    """
    if start is None:
        start = GameState(clock_seconds=rules.period_seconds, offense_is_home=bool(rng.random() < 0.5))
    pbp: List[Dict[str, Any]] = []
    final = _finish_game(
        shelf,
        start,
        rng,
        factors,
        rules=rules,
        pbp_sink=pbp,
        expanded=expanded,
        home_factors=home_factors,
        away_factors=away_factors,
    )
    return final, pbp
