"""Identity tests: the wbb roster_parser shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_roster_parser_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_roster_parser as m
    from sportsdataverse.wbb import wbb_ncaa_roster_parser as w

    assert w.parse_roster is m.parse_roster
    assert w.get_unified_ncaa_id is m.get_unified_ncaa_id


def test_wbb_ncaa_roster_parser_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_roster_parser as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 2


def test_wbb_ncaa_roster_parser_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_roster_parser as m
    from sportsdataverse.wbb import wbb_ncaa_roster_parser as w

    assert set(w.__all__) == set(m.__all__)
