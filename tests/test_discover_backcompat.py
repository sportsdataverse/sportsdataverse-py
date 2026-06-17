"""discover.function_count / list_functions keep flat-leaf keys + accept legacy flat names."""

from __future__ import annotations

from sportsdataverse.discover import function_count, list_functions


def test_function_count_keys_are_flat_leaves():
    counts = function_count()
    # moved leagues are keyed by their flat leaf, not the dotted nested path
    assert "ufl" in counts and "football.ufl" not in counts
    assert "epl" in counts and "soccer.epl" not in counts
    assert "mch" in counts and "ahl" in counts


def test_function_count_accepts_legacy_flat_league_name():
    # both the flat leaf and the major still resolve
    assert function_count(league="ufl") > 0
    assert function_count(league="epl") > 0
    assert function_count(league="nba") > 0


def test_list_functions_flat_league_filter():
    fns = list_functions(league="ufl")
    assert isinstance(fns, (list, dict)) and len(fns) > 0
