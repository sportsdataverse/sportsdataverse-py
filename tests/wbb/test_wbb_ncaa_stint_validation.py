"""Identity tests: the wbb stint_validation shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_stint_validation_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_stint_validation as m
    from sportsdataverse.wbb import wbb_ncaa_stint_validation as w

    assert w.ValidationError is m.ValidationError
    assert w.ALLOWED_ERRORS is m.ALLOWED_ERRORS
    assert w.validate_lineup is m.validate_lineup
    assert w.BadLineupClump is m.BadLineupClump
    assert w.clump_bad_lineups is m.clump_bad_lineups
    assert w.categorize_bad_lineups is m.categorize_bad_lineups
    assert w.handle_common_sub_bug is m.handle_common_sub_bug
    assert w.find_missing_subs is m.find_missing_subs
    assert w.add_missing_players is m.add_missing_players
    assert w.analyze_and_fix_clumps is m.analyze_and_fix_clumps


def test_wbb_ncaa_stint_validation_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_stint_validation as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 10


def test_wbb_ncaa_stint_validation_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_stint_validation as m
    from sportsdataverse.wbb import wbb_ncaa_stint_validation as w

    assert set(w.__all__) == set(m.__all__)
