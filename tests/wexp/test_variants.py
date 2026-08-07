"""Tests for the variation-matrix config system (axes A-F -> variants)."""

import pytest

from sportsdataverse.wexp.variants import (
    VariantConfig,
    enumerate_variants,
    variant_hash,
)


def test_variant_hash_is_stable_and_order_insensitive():
    v1 = VariantConfig(
        core="elo_margin", response="raw", opponent_adjust="none", prior="flat", wp_map="elo_logistic", hfa="fixed"
    )
    v2 = VariantConfig(
        hfa="fixed", wp_map="elo_logistic", prior="flat", opponent_adjust="none", response="raw", core="elo_margin"
    )
    assert variant_hash(v1) == variant_hash(v2)
    assert len(variant_hash(v1)) == 12
    v3 = VariantConfig(
        core="ridge_epa", response="raw", opponent_adjust="ridge", prior="flat", wp_map="elo_logistic", hfa="fixed"
    )
    assert variant_hash(v1) != variant_hash(v3)


def test_invalid_axis_value_refused():
    with pytest.raises(ValueError, match="core"):
        VariantConfig(
            core="nope", response="raw", opponent_adjust="none", prior="flat", wp_map="elo_logistic", hfa="fixed"
        )


def test_compatibility_pruning():
    # B (response weighting) and C (opponent adjustment) only apply to the
    # EPA-family cores (A3-A5); Elo/market cores take the fixed placeholders.
    with pytest.raises(ValueError, match="response"):
        VariantConfig(
            core="elo_margin", response="wepa", opponent_adjust="none", prior="flat", wp_map="elo_logistic", hfa="fixed"
        )
    with pytest.raises(ValueError, match="opponent_adjust"):
        VariantConfig(
            core="elo_margin", response="raw", opponent_adjust="ridge", prior="flat", wp_map="elo_logistic", hfa="fixed"
        )
    # market-only core cannot take a prior (it IS the market)
    with pytest.raises(ValueError, match="prior"):
        VariantConfig(
            core="market_only",
            response="raw",
            opponent_adjust="none",
            prior="carryover",
            wp_map="elo_logistic",
            hfa="fixed",
        )


def test_enumerate_variants_prunes_and_dedupes():
    variants = enumerate_variants()
    hashes = [variant_hash(v) for v in variants]
    assert len(hashes) == len(set(hashes))  # no duplicate configs
    # every emitted variant validates (constructor enforces compatibility)
    assert all(isinstance(v, VariantConfig) for v in variants)
    # EPA-family cores expand across B and C; elo cores appear with the
    # placeholder response/adjust only
    elo_variants = [v for v in variants if v.core == "elo_margin"]
    assert {(v.response, v.opponent_adjust) for v in elo_variants} == {("raw", "none")}
    ridge_variants = [v for v in variants if v.core == "ridge_epa"]
    assert {v.response for v in ridge_variants} == {"raw", "capped", "wepa", "garbage_filtered"}
    assert {v.opponent_adjust for v in ridge_variants} == {"ridge", "iterative", "none"}
    # the matrix is real but bounded (sanity: hundreds, not tens of thousands)
    assert 100 <= len(variants) <= 5000
