"""Tests for the nflreadpy-parity static datasets.

Covers the three module-level dicts shipped at import time:

- ``team_abbr_mapping``  -- relocation-folded historical abbreviations
- ``team_abbr_mapping_norelocate`` -- relocation-preserving variant
- ``player_name_mapping`` -- canonical-name aliasing

These are pure offline tests -- the data is bundled inline in
``sportsdataverse.nfl.datasets`` so no network or env-var gating is needed.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# team_abbr_mapping (relocations FOLDED into current franchise codes)
# ---------------------------------------------------------------------------
def test_team_abbr_mapping_resolves_known_relocations() -> None:
    """Relocated franchises must canonicalize to their current code."""
    from sportsdataverse.nfl import team_abbr_mapping

    # Raiders: Oakland -> Las Vegas (2020)
    assert team_abbr_mapping["OAK"] == "LV"
    # Chargers: San Diego -> LA (2017)
    assert team_abbr_mapping["SD"] == "LAC"
    # Rams: St Louis -> LA (2016). nflverse uses ``LA`` (not ``LAR``) as
    # the canonical post-relocation code for the Rams.
    assert team_abbr_mapping["STL"] == "LA"
    # Current-day team should map to itself.
    assert team_abbr_mapping["KC"] == "KC"
    # Common variants for current franchises:
    assert team_abbr_mapping["JAC"] == "JAX"
    assert team_abbr_mapping["WSH"] == "WAS"


# ---------------------------------------------------------------------------
# team_abbr_mapping_norelocate (relocations PRESERVED)
# ---------------------------------------------------------------------------
def test_team_abbr_mapping_norelocate_preserves_history() -> None:
    """Relocated franchise codes must remain themselves under norelocate."""
    from sportsdataverse.nfl import team_abbr_mapping_norelocate

    # Historical codes stay as themselves under no-relocate semantics.
    assert team_abbr_mapping_norelocate["OAK"] == "OAK"
    assert team_abbr_mapping_norelocate["SD"] == "SD"
    assert team_abbr_mapping_norelocate["STL"] == "STL"
    # Current-day franchise codes still resolve to themselves.
    assert team_abbr_mapping_norelocate["LV"] == "LV"
    assert team_abbr_mapping_norelocate["LAC"] == "LAC"
    assert team_abbr_mapping_norelocate["LA"] == "LA"
    # Spot-check that an unambiguous current franchise behaves identically
    # to the relocate variant.
    assert team_abbr_mapping_norelocate["KC"] == "KC"


def test_team_abbr_mapping_relocate_vs_norelocate_disagreement() -> None:
    """The two team-abbr maps MUST disagree on at least the relocated codes.

    If they ever return identical dicts, one of them is broken -- the whole
    point of shipping both is that they encode different policies for the
    same input.
    """
    from sportsdataverse.nfl import team_abbr_mapping, team_abbr_mapping_norelocate

    # Codes whose city changed AND whose post-relocation abbreviation
    # differs from the historical one. Note: ``"RAI"`` is intentionally
    # excluded here -- nflverse's own data maps it to ``"LV"`` in both
    # files (treating "RAI" as the franchise-as-of-now rather than the
    # Oakland-era variant), so it is not a useful disagreement-witness.
    relocated_codes = ["OAK", "SD", "STL", "SDC", "SDG"]
    diffs = [c for c in relocated_codes if team_abbr_mapping.get(c) != team_abbr_mapping_norelocate.get(c)]
    # Every relocated code in the list above must differ between the two
    # maps. If even one is equal, the relocation policy is inconsistent.
    assert len(diffs) == len(relocated_codes), (
        f"expected all relocated codes to disagree, but these were equal: "
        f"{[c for c in relocated_codes if c not in diffs]}"
    )


# ---------------------------------------------------------------------------
# player_name_mapping
# ---------------------------------------------------------------------------
def test_player_name_mapping_canonicalizes_known_variants() -> None:
    """Common name variants must resolve to the canonical nflverse form."""
    from sportsdataverse.nfl import player_name_mapping

    # Quarterbacks: short -> legal
    assert player_name_mapping["Mitch Trubisky"] == "Mitchell Trubisky"
    assert player_name_mapping["Pat Mahomes"] == "Patrick Mahomes"
    # WR: nickname -> legal
    assert player_name_mapping["Hollywood Brown"] == "Marquise Brown"
    # CB: nickname -> legal
    assert player_name_mapping["Sauce Gardner"] == "Ahmad Gardner"
    # Diacritic / capitalization variants
    assert player_name_mapping["Khadarel Hodge"] == "KhaDarel Hodge"
    assert player_name_mapping["AndrewVanGinkel"] == "Andrew Van Ginkel"


# ---------------------------------------------------------------------------
# shape / type invariants
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "name",
    ["team_abbr_mapping", "team_abbr_mapping_norelocate", "player_name_mapping"],
)
def test_dataset_dict_shape(name: str) -> None:
    """Each dataset must be a non-empty ``dict[str, str]``."""
    from sportsdataverse.nfl import datasets

    obj = getattr(datasets, name)
    assert isinstance(obj, dict), f"{name} is not a dict"
    assert len(obj) > 0, f"{name} is empty"
    # Spot-check a handful of items rather than iterating the whole dict --
    # the first-N policy is enough to catch a wholesale type regression
    # (e.g. someone writing list values by accident) without paying linear
    # cost on every test run.
    sample_items = list(obj.items())[:10]
    for k, v in sample_items:
        assert isinstance(k, str), f"{name}: non-string key {k!r}"
        assert isinstance(v, str), f"{name}: non-string value {v!r} for key {k!r}"


def test_dataset_module_imports_cleanly() -> None:
    """Module-level import path must work end-to-end."""
    from sportsdataverse.nfl.datasets import (
        player_name_mapping,
        team_abbr_mapping,
        team_abbr_mapping_norelocate,
    )

    assert team_abbr_mapping is not None
    assert team_abbr_mapping_norelocate is not None
    assert player_name_mapping is not None


def test_dataset_top_level_re_exports() -> None:
    """The dicts must also be reachable from ``sportsdataverse.nfl``."""
    import sportsdataverse.nfl as nfl

    assert hasattr(nfl, "team_abbr_mapping")
    assert hasattr(nfl, "team_abbr_mapping_norelocate")
    assert hasattr(nfl, "player_name_mapping")
    # And the top-level re-export must be the SAME object as the
    # module-level definition (no accidental copy).
    from sportsdataverse.nfl import datasets as datasets_mod

    assert nfl.team_abbr_mapping is datasets_mod.team_abbr_mapping
    assert nfl.team_abbr_mapping_norelocate is datasets_mod.team_abbr_mapping_norelocate
    assert nfl.player_name_mapping is datasets_mod.player_name_mapping
