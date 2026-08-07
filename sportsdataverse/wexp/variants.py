"""Variation-matrix config system (prompt Phase 1, axes A-F).

Variants are GENERATED, not hand-written: :func:`enumerate_variants` emits
the axis cross-product pruned by the compatibility rules, and
:func:`variant_hash` gives each config the stable 12-hex id that keys its
leaderboard rows. Axis values are labels; the engines that interpret them
land per-axis (Elo shipped; ridge/FEI/state-space next).

Compatibility rules (enforced in the constructor, so an invalid variant
cannot exist):

- Axis B (response weighting) and Axis C (opponent adjustment) apply only
  to the EPA-family cores (A3 ridge_epa, A4 five_factor, A5 fei_possession);
  other cores carry the ``raw``/``none`` placeholders.
- ``market_only`` (A7) takes no preseason prior — it IS the market.
- ``glickman_stern`` (A6) carries its own dual-timescale prior, so only
  ``flat`` or ``carryover`` are meaningful (the AR *is* the carryover).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from itertools import product

__all__ = ["AXES", "VariantConfig", "enumerate_variants", "variant_hash"]

# Axis label spaces (RESEARCH mapping in the plan file).
AXES: dict[str, tuple[str, ...]] = {
    # A — strength core
    "core": (
        "elo_margin",  # A1
        "elo_blended",  # A2 (margin + WEPA share)
        "ridge_epa",  # A3
        "five_factor",  # A4
        "fei_possession",  # A5
        "glickman_stern",  # A6
        "market_only",  # A7 control
    ),
    # B — EPA/response weighting (EPA-family cores only)
    "response": ("raw", "capped", "wepa", "garbage_filtered"),
    # C — opponent adjustment (EPA-family cores only)
    "opponent_adjust": ("ridge", "iterative", "none"),
    # D — preseason prior
    "prior": ("flat", "carryover", "carryover_continuity", "market_open_informed"),
    # E — rating -> WP mapping
    "wp_map": ("elo_logistic", "margin_normal", "isotonic", "monte_carlo"),
    # F — situational HFA treatment
    "hfa": ("fixed", "per_era", "team_specific"),
}

_EPA_CORES = frozenset({"ridge_epa", "five_factor", "fei_possession"})


@dataclass(frozen=True)
class VariantConfig:
    """One point in the variation matrix (validated on construction).

    Attributes:
        core: Axis A strength core.
        response: Axis B response weighting (EPA-family cores only).
        opponent_adjust: Axis C adjustment scheme (EPA-family cores only).
        prior: Axis D preseason prior.
        wp_map: Axis E rating->WP mapping.
        hfa: Axis F HFA treatment.
        params: Free-form tunables (k, z, alpha, sigma, cap, ...) — part of
            the hash, so a re-tuned variant is a NEW variant.
    """

    core: str
    response: str
    opponent_adjust: str
    prior: str
    wp_map: str
    hfa: str
    params: tuple[tuple[str, float], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        for axis in ("core", "response", "opponent_adjust", "prior", "wp_map", "hfa"):
            value = getattr(self, axis)
            if value not in AXES[axis]:
                raise ValueError(f"unknown {axis} value {value!r}; one of {AXES[axis]}")
        if self.core not in _EPA_CORES:
            if self.response != "raw":
                raise ValueError(f"response={self.response!r} only applies to EPA-family cores, not {self.core!r}")
            if self.opponent_adjust != "none":
                raise ValueError(
                    f"opponent_adjust={self.opponent_adjust!r} only applies to EPA-family cores, not {self.core!r}"
                )
        if self.core == "market_only" and self.prior != "flat":
            raise ValueError("market_only takes no prior (prior must be 'flat')")
        if self.core == "glickman_stern" and self.prior not in ("flat", "carryover"):
            raise ValueError("glickman_stern's dual-timescale AR is its own prior; prior must be 'flat' or 'carryover'")


def variant_hash(config: VariantConfig) -> str:
    """Stable 12-hex identifier for a variant config.

    Field-order-insensitive (dataclass fields serialize in declaration
    order; ``params`` are sorted). Any change to any axis value or tunable
    produces a different hash — a re-tuned variant is a new leaderboard row.

    Args:
        config: The variant.

    Returns:
        First 12 hex chars of the SHA-256 of the canonical JSON form.

    Example:
        Quick start::

            from sportsdataverse.wexp.variants import VariantConfig, variant_hash
            variant_hash(VariantConfig(core="elo_margin", response="raw",
                                       opponent_adjust="none", prior="flat",
                                       wp_map="elo_logistic", hfa="fixed"))
    """
    payload = asdict(config)
    payload["params"] = sorted(payload["params"])
    canon = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()[:12]


def enumerate_variants() -> list[VariantConfig]:
    """Generate the pruned axis cross-product.

    EPA-family cores expand across all of B x C; other cores appear once
    per (D, E, F) cell with the placeholder B/C values. Invalid cells are
    skipped via the constructor's compatibility rules.

    Returns:
        The list of valid variants (deduplicated by construction).

    Example:
        Quick start::

            from sportsdataverse.wexp.variants import enumerate_variants
            variants = enumerate_variants()
            len(variants)
    """
    out: list[VariantConfig] = []
    for core, response, adjust, prior, wp_map, hfa in product(*AXES.values()):
        try:
            out.append(
                VariantConfig(
                    core=core,
                    response=response,
                    opponent_adjust=adjust,
                    prior=prior,
                    wp_map=wp_map,
                    hfa=hfa,
                )
            )
        except ValueError:
            continue
    return out
