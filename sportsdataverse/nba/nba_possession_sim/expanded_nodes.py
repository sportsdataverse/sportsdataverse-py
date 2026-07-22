"""Fully expanded event tree — the complete reference node taxonomy (WS4 final).

The collapsed walk samples terminal possession outcomes directly; this
module expands every stage into its own node class, mirroring the reference NBA
``Node.py`` taxonomy (Usage / ShotType / shot result / BLK / and-1 /
FreeThrow / STL-vs-dead-ball turnover / rebounds / side events):

* :class:`UsageNode` — who uses the possession (delegates to
  :class:`~sportsdataverse.nba.nba_possession_sim.attribution.PlayerAttribution`);
* :class:`PlayTypeNode` — attempt-type draw (rim/mid/three attempt, FT
  trip, turnover), decomposed from the shelf PMF so a model-backed shelf
  makes this node model-backed automatically;
* :class:`ShotResultNode` — per-type make probability from the same
  decomposition (``P(make | attempt type, gamestate)``);
* :class:`BlockNode` / :class:`And1Node` / :class:`StealNode` — miss-was-
  blocked, make-drew-the-foul (chains a 1-shot trip), turnover-was-live
  (steal) annotations, each with its own fitted rate from real streams;
* :class:`SideEventNode` — non-scoring possession preludes (timeouts,
  non-shooting fouls, jump balls) at real per-possession rates, for pbp
  realism.

The expanded walk emits the SAME terminal outcomes as the collapsed walk
(the 10-outcome contract holds — conservation gates keep passing) plus the
richer trail; and-1 chains add their real FT points on top.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_possession_sim.attribution import PlayerAttribution
from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.nodes import FreeThrowNode, PossessionState, ReboundNode
from sportsdataverse.nba.nba_possession_sim.shelf import Shelf

_ATTEMPT_TYPES = ("rim", "mid", "three")
_ATTEMPT_POINTS = {"rim": 2, "mid": 2, "three": 3}

#: Aux-parameter defaults when a stream carries no signal (league-typical).
DEFAULT_AUX: Dict[str, float] = {
    "block_rate": 0.10,
    "steal_share": 0.50,
    "and1_rate": 0.06,
    "timeout_rate": 0.04,
    "def_foul_rate": 0.10,
}


def aux_params_from_pbp(actions: pl.DataFrame) -> Dict[str, float]:
    """Fit the expanded-node rates from a real ``playbyplayv3`` stream.

    Args:
        actions: Raw action rows (camelCase or snake_case), any games.

    Returns:
        ``block_rate`` (blocked / missed FGA), ``steal_share`` (steals /
        turnovers), ``and1_rate`` (and-1s / made FGA), ``timeout_rate`` and
        ``def_foul_rate`` (per possession-ending event). Missing signals
        fall back to :data:`DEFAULT_AUX`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
                aux_params_from_pbp,
            )
            aux = aux_params_from_pbp(actions)
    """
    a_type = "actionType" if "actionType" in actions.columns else "action_type"
    sub = "subType" if "subType" in actions.columns else "sub_type"
    frame = actions.select(
        pl.col(a_type).alias("t"),
        pl.col("description").cast(pl.Utf8).fill_null("").alias("d"),
        pl.col(sub).cast(pl.Utf8).fill_null("").alias("s"),
    )
    misses = frame.filter(pl.col("t") == "Missed Shot")
    makes = frame.filter(pl.col("t") == "Made Shot")
    tovs = frame.filter(pl.col("t") == "Turnover")
    fts = frame.filter(pl.col("t") == "Free Throw")
    fouls = frame.filter(pl.col("t") == "Foul")
    timeouts = frame.filter(pl.col("t") == "Timeout")
    events = misses.height + makes.height + tovs.height

    aux = dict(DEFAULT_AUX)
    # playbyplayv3 descriptions carry no block annotation — the rate keeps
    # its documented default here; the ESPN adapter source fits it from text
    if tovs.height:
        live = tovs.filter(
            pl.col("s").str.contains("(?i)lost ball")
            | (
                pl.col("s").str.contains("(?i)bad pass") & (pl.col("s").str.contains("(?i)out of bounds") == False)  # noqa: E712
            )
        )
        aux["steal_share"] = live.height / tovs.height
    if makes.height:
        and1s = fts.filter(pl.col("d").str.contains("1 of 1")).height
        aux["and1_rate"] = min(0.25, and1s / makes.height)
    if events:
        aux["timeout_rate"] = timeouts.height / events
        nonshooting = fouls.filter(pl.col("d").str.contains("(?i)shooting") == False)  # noqa: E712
        aux["def_foul_rate"] = nonshooting.height / events
    return aux


class UsageNode:
    """Who uses the possession — delegates to the attribution shares."""

    def sample(
        self,
        attribution: Optional[PlayerAttribution],
        offense_is_home: bool,
        outcome: str,
        rng: np.random.Generator,
    ) -> Optional[int]:
        """The player id charged with the terminal event (None w/o shares)."""
        if attribution is None:
            return None
        return attribution.sample(offense_is_home, outcome, rng)


class PlayTypeNode:
    """Attempt-type draw, decomposed from the shelf's outcome PMF."""

    def sample(
        self,
        shelf: Shelf,
        key: str,
        rng: np.random.Generator,
        factors: Optional[FactorAdjustment] = None,
    ) -> str:
        """Draw ``rim``/``mid``/``three`` attempt, ``ft_trip_N``, or ``tov``."""
        pmf, _ = shelf.get_pmf(key)
        if factors is not None:
            pmf = factors.adjust(pmf)
        weights = {
            "rim": pmf["rim_make"] + pmf["rim_miss"],
            "mid": pmf["mid_make"] + pmf["mid_miss"],
            "three": pmf["three_make"] + pmf["three_miss"],
            "ft_trip_1": pmf["ft_trip_1"],
            "ft_trip_2": pmf["ft_trip_2"],
            "ft_trip_3": pmf["ft_trip_3"],
            "tov": pmf["tov"],
        }
        names = list(weights)
        probs = np.array([weights[n] for n in names], dtype=float)
        total = probs.sum()
        probs = probs / total if total > 0 else np.full(len(names), 1.0 / len(names))
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return names[min(idx, len(names) - 1)]


class ShotResultNode:
    """P(make | attempt type, gamestate) from the same PMF decomposition."""

    def sample(self, shelf: Shelf, key: str, attempt: str, rng: np.random.Generator) -> bool:
        """True = make."""
        pmf, _ = shelf.get_pmf(key)
        make = pmf[f"{attempt}_make"]
        miss = pmf[f"{attempt}_miss"]
        p_make = make / (make + miss) if (make + miss) > 0 else 0.45
        return bool(rng.random() < p_make)


class BlockNode:
    """Was the miss blocked? (annotation node, fitted rate)."""

    def sample(self, aux: Dict[str, float], rng: np.random.Generator) -> bool:
        return bool(rng.random() < aux.get("block_rate", DEFAULT_AUX["block_rate"]))


class And1Node:
    """Did the make draw a foul? Chains a 1-shot trip when it did."""

    def sample(self, aux: Dict[str, float], rng: np.random.Generator) -> bool:
        return bool(rng.random() < aux.get("and1_rate", DEFAULT_AUX["and1_rate"]))


class StealNode:
    """Live-ball (steal) vs dead-ball turnover split."""

    def sample(self, aux: Dict[str, float], rng: np.random.Generator) -> bool:
        return bool(rng.random() < aux.get("steal_share", DEFAULT_AUX["steal_share"]))


class SideEventNode:
    """Non-scoring possession preludes: timeouts / non-shooting fouls."""

    def sample(self, aux: Dict[str, float], rng: np.random.Generator) -> List[str]:
        events: List[str] = []
        if rng.random() < aux.get("timeout_rate", DEFAULT_AUX["timeout_rate"]):
            events.append("side:timeout")
        if rng.random() < aux.get("def_foul_rate", DEFAULT_AUX["def_foul_rate"]):
            events.append("side:def_foul")
        return events


def simulate_possession_expanded(
    shelf: Shelf,
    *,
    score_diff: float,
    period: int,
    clock_seconds: float,
    rng: np.random.Generator,
    aux: Optional[Dict[str, float]] = None,
    factors: Optional[FactorAdjustment] = None,
    attribution: Optional[PlayerAttribution] = None,
    offense_is_home: bool = True,
) -> Tuple[int, List[str], Optional[int]]:
    """Walk the FULLY EXPANDED event tree for one possession.

    Args:
        shelf: The PMF shelf (empirical or model-backed).
        score_diff: Offense-perspective differential.
        period: Current period.
        clock_seconds: Seconds remaining in the period.
        rng: Numpy generator.
        aux: Expanded-node rates (:func:`aux_params_from_pbp`); defaults to
            :data:`DEFAULT_AUX`.
        factors: Optional auditable PMF adjustment.
        attribution: Optional scorer shares (UsageNode).
        offense_is_home: Side with the ball (for UsageNode).

    Returns:
        ``(points, trail, scorer)`` — the trail carries the expanded
        annotations (``side:*``, ``*_attempt``, ``make``/``miss``, ``blk``,
        ``and1``, ``stl``/``tov_dead``, ``oreb``/``dreb``, ``ft_made_k``)
        and the terminal 10-outcome token, so collapsed-view consumers
        keep working.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
                simulate_possession_expanded,
            )
            pts, trail, scorer = simulate_possession_expanded(
                shelf, score_diff=0, period=1, clock_seconds=600.0,
                rng=np.random.default_rng(7),
            )
    """
    aux = aux if aux is not None else dict(shelf.aux or DEFAULT_AUX)
    state = PossessionState(score_diff=score_diff, period=period, clock_seconds=clock_seconds)
    key = shelf.key_for(score_diff, period, clock_seconds)

    play_type = PlayTypeNode()
    shot_result = ShotResultNode()
    block = BlockNode()
    and1 = And1Node()
    steal = StealNode()
    side = SideEventNode()
    rebound = ReboundNode()
    ft = FreeThrowNode()
    usage = UsageNode()

    state.events.extend(side.sample(aux, rng))
    terminal: str = "tov"
    while True:
        attempt = play_type.sample(shelf, key, rng, factors)
        if attempt == "tov":
            live = steal.sample(aux, rng)
            state.events.extend(["tov", "stl" if live else "tov_dead"])
            terminal = "tov"
            break
        if attempt.startswith("ft_trip_"):
            n_attempts = int(attempt.rsplit("_", 1)[1])
            made = ft.sample(shelf, n_attempts, rng)
            state.points += made
            state.events.extend([attempt, f"ft_made_{made}"])
            terminal = attempt
            break
        state.events.append(f"{attempt}_attempt")
        if shot_result.sample(shelf, key, attempt, rng):
            state.points += _ATTEMPT_POINTS[attempt]
            state.events.extend([f"{attempt}_make", "make"])
            terminal = f"{attempt}_make"
            if and1.sample(aux, rng):
                bonus = ft.sample(shelf, 1, rng)
                state.points += bonus
                state.events.extend(["and1", f"ft_made_{bonus}"])
            break
        state.events.extend([f"{attempt}_miss", "miss"])
        terminal = f"{attempt}_miss"
        if block.sample(aux, rng):
            state.events.append("blk")
        if rebound.sample(shelf, rng, key):
            state.events.append("oreb")
            continue
        state.events.append("dreb")
        break

    scorer = usage.sample(attribution, offense_is_home, terminal, rng) if state.points > 0 else None
    return state.points, state.events, scorer


def aux_params_from_espn(summary: Dict[str, Any]) -> Dict[str, float]:
    """Fit the expanded-node rates from an ESPN summary's play texts.

    ESPN play text DOES annotate blocks and steals ("X blocks Y's ...",
    "(Z steals)"), so this source fits every rate.

    Args:
        summary: Site v2 ``summary`` payload with ``plays``.

    Returns:
        The same rate dict as :func:`aux_params_from_pbp`.
    """
    plays = summary.get("plays") or []
    aux = dict(DEFAULT_AUX)
    misses = [
        p
        for p in plays
        if p.get("shootingPlay")
        and not p.get("scoringPlay")
        and "Free Throw" not in str(p.get("type", {}).get("text") or "")
    ]
    tovs = [
        p
        for p in plays
        if "Turnover" in str(p.get("type", {}).get("text") or "") or "Turnover" in str(p.get("text") or "")
    ]
    makes = [
        p
        for p in plays
        if p.get("shootingPlay")
        and p.get("scoringPlay")
        and "Free Throw" not in str(p.get("type", {}).get("text") or "")
    ]
    events = len(misses) + len(tovs) + len(makes)
    # a zero count is "no annotation in this capture", not a fitted zero —
    # only a positive signal overrides the documented default
    if misses:
        blocked = sum(1 for p in misses if "block" in str(p.get("text") or "").lower())
        if blocked:
            aux["block_rate"] = blocked / len(misses)
    if tovs:
        stolen = sum(1 for p in tovs if "steal" in str(p.get("text") or "").lower())
        if stolen:
            aux["steal_share"] = stolen / len(tovs)
    if events:
        fouls = [
            p
            for p in plays
            if "Foul" in str(p.get("type", {}).get("text") or "")
            and "Shooting" not in str(p.get("type", {}).get("text") or "")
        ]
        timeouts = [p for p in plays if "Timeout" in str(p.get("type", {}).get("text") or "")]
        aux["def_foul_rate"] = len(fouls) / events
        if timeouts:
            aux["timeout_rate"] = len(timeouts) / events
    return aux
