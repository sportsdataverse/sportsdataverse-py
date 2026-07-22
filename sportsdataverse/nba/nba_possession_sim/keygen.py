"""Gamestate key generation — continuous state → discrete shelf key (WS4).

The reference ``key_gen_functions`` pattern with SDV-owned bins. Bins are
deliberately coarse in v1 (coverage over resolution); the learned-
discretization recipe (DecisionTree → leaf → prob table) is the documented
upgrade path when finer keys are justified by data volume.
"""

from __future__ import annotations

import dataclasses
import re
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    import polars as pl

_CLOCK_RE = re.compile(r"PT(?:(\d+)M)?(?:([\d.]+)S)?")

#: Score-differential clip bound; beyond this the game state is equivalent.
SCORE_DIFF_CLIP = 16
#: Width of each score-differential bucket.
SCORE_DIFF_WIDTH = 4


def parse_clock(clock: str) -> float:
    """Parse a stats.nba.com ISO-duration clock ("PT11M22.00S") to seconds.

    Args:
        clock: Clock string from a ``playbyplayv3`` action.

    Returns:
        Seconds remaining in the period (0.0 for blank/unparseable clocks).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.keygen import parse_clock
            parse_clock("PT11M22.00S")  # 682.0
    """
    match = _CLOCK_RE.fullmatch(clock.strip()) if clock else None
    if not match:
        return 0.0
    minutes = float(match.group(1) or 0)
    seconds = float(match.group(2) or 0.0)
    return minutes * 60.0 + seconds


def score_diff_bin(score_diff: float) -> int:
    """Bin an offense-perspective score differential (clip ±16, width 4).

    Args:
        score_diff: Offense score minus defense score before the possession.

    Returns:
        Signed bucket index in [-4, 4].

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.keygen import score_diff_bin
            score_diff_bin(-9)  # -3
    """
    clipped = max(-SCORE_DIFF_CLIP, min(SCORE_DIFF_CLIP, score_diff))
    return int(round(clipped / SCORE_DIFF_WIDTH))


def period_bin(period: int) -> int:
    """Fold overtime periods into the 4th-quarter regime.

    Args:
        period: Period number (1-4 regulation, 5+ OT).

    Returns:
        Period bucket in [1, 4].
    """
    return min(int(period), 4)


def clock_bin(seconds_remaining: float) -> str:
    """Bucket the period clock: early / mid / late / clutch.

    Args:
        seconds_remaining: Seconds left in the period.

    Returns:
        One of ``"early"`` (>=480), ``"mid"`` (>=240), ``"late"`` (>=60),
        ``"clutch"`` (<60).
    """
    if seconds_remaining >= 480:
        return "early"
    if seconds_remaining >= 240:
        return "mid"
    if seconds_remaining >= 60:
        return "late"
    return "clutch"


def gamestate_key(score_diff: float, period: int, seconds_remaining: float) -> str:
    """Compose the shelf lookup key for a gamestate.

    Args:
        score_diff: Offense-perspective score differential.
        period: Period number.
        seconds_remaining: Seconds left in the period.

    Returns:
        A compact string key, e.g. ``"d-2|p3|late"``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim.keygen import gamestate_key
            gamestate_key(-9, 3, 75.0)  # 'd-2|p3|late'
    """
    return f"d{score_diff_bin(score_diff)}|p{period_bin(period)}|{clock_bin(seconds_remaining)}"


#: Fixed feature order the learned gamestate keyer splits on.
LEARNED_KEY_FEATURES = ("score_diff", "period", "clock_seconds")


@dataclasses.dataclass(frozen=True)
class LearnedGamestateKeyer:
    """The learned-discretization upgrade path: tree leaves as shelf keys.

    Holds the fitted tree's split structure as flat tuples (walked in pure
    Python at sim time — no sklearn object at lookup) plus the auditable
    per-leaf table. Every gamestate maps to exactly one leaf, so a shelf
    built with this keyer NEVER falls back to the global PMF.

    Attributes:
        feature_index: Per tree node, the index into
            :data:`LEARNED_KEY_FEATURES` it splits on (-1 for a leaf).
        threshold: Per node, the split threshold (``value <= threshold``
            goes left — sklearn semantics); NaN for leaves.
        left: Per node, the left child id (-1 for a leaf).
        right: Per node, the right child id (-1 for a leaf).
        leaf_table: The audit artifact — one row per leaf: ``leaf_id``,
            ``n``, human-readable ``rule``, and ``p_{outcome}`` columns.
    """

    feature_index: Tuple[int, ...]
    threshold: Tuple[float, ...]
    left: Tuple[int, ...]
    right: Tuple[int, ...]
    leaf_table: "pl.DataFrame"

    def key(self, score_diff: float, period: int, seconds_remaining: float) -> str:
        """Walk the tree to this gamestate's leaf key.

        Args:
            score_diff: Offense-perspective score differential.
            period: Period number.
            seconds_remaining: Seconds left in the period.

        Returns:
            The leaf key, e.g. ``"leaf12"``.

        Example:
            Quick start::

                keyer.key(-9, 3, 75.0)
        """
        values = (float(score_diff), float(period), float(seconds_remaining))
        node = 0
        while self.feature_index[node] >= 0:
            if values[self.feature_index[node]] <= self.threshold[node]:
                node = self.left[node]
            else:
                node = self.right[node]
        return f"leaf{node}"


def fit_learned_gamestate_keyer(
    possessions: "pl.DataFrame",
    *,
    max_depth: int = 7,
    max_leaf_nodes: int = 64,
    min_samples_leaf: int = 50,
    random_state: int = 0,
) -> LearnedGamestateKeyer:
    """Fit a :class:`LearnedGamestateKeyer` from real possession events.

    Composes :func:`~sportsdataverse.modeling.features.learned_bins.fit_learned_bins`
    on the outcome events' ``(score_diff, period, clock_seconds)`` against
    the possession outcome, then extracts the tree's decision table into
    the walkable tuple form. ``min_samples_leaf`` is the coverage floor
    that keeps every learned bin estimable — the property hand-cut bins
    can't guarantee.

    Args:
        possessions: Output of
            :func:`~sportsdataverse.nba.nba_possession_sim.shelf.possessions_from_pbp`
            (TRAIN games only — fitting on evaluation games leaks).
        max_depth: Tree depth cap.
        max_leaf_nodes: Bin-count cap.
        min_samples_leaf: Minimum outcome events per leaf.
        random_state: Deterministic tie-breaking.

    Returns:
        The fitted keyer.

    Raises:
        ValueError: When the frame has no outcome events.

    Example:
        Fit, then build a learned-keyed shelf::

            from sportsdataverse.nba.nba_possession_sim import (
                build_shelf, fit_learned_gamestate_keyer, possessions_from_pbp,
            )
            poss = possessions_from_pbp(actions)
            keyer = fit_learned_gamestate_keyer(poss)
            shelf = build_shelf(poss, keyer=keyer)
    """
    import polars as pl

    from sportsdataverse.modeling.features.learned_bins import fit_learned_bins

    outcomes = possessions.filter(pl.col("kind") == "outcome")
    if outcomes.height == 0:
        raise ValueError("no outcome events — cannot fit a learned gamestate keyer")
    bins = fit_learned_bins(
        outcomes,
        features=list(LEARNED_KEY_FEATURES),
        target="outcome",
        max_depth=max_depth,
        max_leaf_nodes=max_leaf_nodes,
        min_samples_leaf=min_samples_leaf,
        random_state=random_state,
    )
    table = bins.decision_table().sort("node_id")
    feature_index = tuple(
        -1 if feature is None else LEARNED_KEY_FEATURES.index(feature) for feature in table["feature"].to_list()
    )
    threshold = tuple(float("nan") if t is None else float(t) for t in table["threshold"].to_list())
    left = tuple(-1 if v is None else int(v) for v in table["left"].to_list())
    right = tuple(-1 if v is None else int(v) for v in table["right"].to_list())
    return LearnedGamestateKeyer(
        feature_index=feature_index,
        threshold=threshold,
        left=left,
        right=right,
        leaf_table=bins.leaf_table,
    )
