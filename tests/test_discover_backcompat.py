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


def test_every_documented_league_is_counted():
    """function_count() covers the registry the docs site reads, not a hand-kept subset (it once skipped 60 of 105)."""
    import json
    from pathlib import Path

    registry = json.loads((Path(__file__).parents[1] / "docs" / "src" / "data" / "leagues.json").read_text("utf-8"))
    prefixes = {lg["prefix"] for sport in registry["sports"] for lg in sport["leagues"]}
    counts = function_count()
    assert prefixes <= set(counts), sorted(prefixes - set(counts))
    assert all(counts[p] > 0 for p in prefixes)


def test_a_stats_family_counts_with_its_league():
    """nbagl_stats is a submodule nbagl imports but does not star-export; its wrappers still count."""
    import sportsdataverse.nbagl.nbagl_stats as stats

    fns = list_functions(league="nbagl")
    assert set(stats.__all__) <= set(fns)
    assert "nbagl_stats_leaguedashplayerstats" in fns
