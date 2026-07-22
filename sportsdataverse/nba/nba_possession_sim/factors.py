"""Auditable factor-adjustment layer (WS4 v2 seam).

The reference stack applied live PMF tweaks (injuries, rest, matchup edges) before sampling
— but unaudited, so nobody could say afterwards what was adjusted where.
This port makes the adjustment a first-class object: the multipliers are
declared once, every application is counted, and :meth:`FactorAdjustment.summary`
is the audit artifact the experiment ledger can persist alongside a run.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict

#: Multipliers are clipped to this range — a factor is a nudge, not a rewrite.
MAX_FACTOR = 5.0
MIN_FACTOR = 0.0


@dataclasses.dataclass
class FactorAdjustment:
    """Outcome-probability multipliers with application accounting.

    Attributes:
        factors: ``{outcome: multiplier}`` — outcomes absent from the map are
            untouched. Multipliers must lie in ``[MIN_FACTOR, MAX_FACTOR]``.
        n_applied: How many PMF draws this adjustment has modified.
    """

    factors: Dict[str, float]
    n_applied: int = 0

    def __post_init__(self) -> None:
        bad = {o: f for o, f in self.factors.items() if not MIN_FACTOR <= f <= MAX_FACTOR}
        if bad:
            raise ValueError(f"factors out of [{MIN_FACTOR}, {MAX_FACTOR}]: {bad}")

    def adjust(self, pmf: Dict[str, float]) -> Dict[str, float]:
        """Apply the multipliers to one PMF and renormalize.

        Args:
            pmf: An outcome→probability dict (a shelf PMF).

        Returns:
            The adjusted, renormalized PMF (the input is not mutated).
        """
        adjusted = {o: p * self.factors.get(o, 1.0) for o, p in pmf.items()}
        total = sum(adjusted.values())
        if total <= 0:  # pathological all-zero adjustment: fall back untouched
            return dict(pmf)
        self.n_applied += 1
        return {o: p / total for o, p in adjusted.items()}

    def summary(self) -> Dict[str, Any]:
        """The audit record: which factors, applied how many times.

        Returns:
            ``{"factors": {...}, "n_applied": int}`` — persist this next to
            the run (e.g. in the experiment ledger's config).

        Example:
            Quick start::

                from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
                fa = FactorAdjustment({"three_make": 1.15, "three_miss": 0.9})
                ensemble = simulate_ensemble(shelf, n_sim=500, seed=7, factors=fa)
                print(fa.summary())
        """
        return {"factors": dict(self.factors), "n_applied": self.n_applied}
