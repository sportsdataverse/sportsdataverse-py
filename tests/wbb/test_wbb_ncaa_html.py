"""Identity tests: the wbb html shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_html_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_html as m
    from sportsdataverse.wbb import wbb_ncaa_html as w

    assert w.parse_html is m.parse_html
    assert w.jsoup_text is m.jsoup_text
    assert w.td_at is m.td_at
    assert w.select_matching is m.select_matching
    assert w.select_matching_own is m.select_matching_own
    assert w.attr_regex_filter is m.attr_regex_filter
    assert w.select_contains is m.select_contains
    assert w.filter_matching_own is m.filter_matching_own


def test_wbb_ncaa_html_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_html as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 8


def test_wbb_ncaa_html_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_html as m
    from sportsdataverse.wbb import wbb_ncaa_html as w

    assert set(w.__all__) == set(m.__all__)
