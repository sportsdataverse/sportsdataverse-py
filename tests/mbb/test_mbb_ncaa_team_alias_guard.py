"""A directional `team_aliases` rewrite fixes one spelling by breaking the other.

`team_aliases` REWRITES a page name to a canonical one. That only works while
every game in the season targets the canonical spelling -- and both spellings
occur in the SAME season, so the rewrite is a perfect trade. Measured on the
inherited `Year(2021): {NIU -> Northern Ill.}` entry:

    season 2021-22 (alias active)  target `NIU` FAIL x3  `Northern Ill.` OK x3
    season 2015    (no alias)      target `NIU` OK       `Northern Ill.` FAIL

Six of those reached the skip ledger during the corpus re-parse (#374), every
one with BOTH titles present and one exactly equal to the target.

`team_name_equivalents` is symmetric and has no direction, so it holds in both
eras. The guard below makes the failure mode impossible to reintroduce
silently: any NEW alias pair must also be declared as an equivalence.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_data_quality import (
    same_school,
    team_aliases,
    team_name_equivalents,
)


def test_every_alias_pair_is_also_an_equivalence() -> None:
    """A rewrite without a matching equivalence breaks the other direction."""
    missing = []
    for year, mapping in team_aliases.items():
        for src, dst in mapping.items():
            if not same_school(src.name, dst.name):
                missing.append((year.value, src.name, dst.name))
    assert not missing, (
        "team_aliases entries with no team_name_equivalents class: "
        f"{missing}. A directional rewrite alone fails in the season whose "
        "target uses the OTHER spelling -- add a frozenset to "
        "team_name_equivalents so both directions resolve."
    )


def test_the_guard_can_actually_fail() -> None:
    """A guard over an empty table would pass vacuously."""
    assert team_aliases, "team_aliases is empty -- the guard checks nothing"
    assert team_name_equivalents, "team_name_equivalents is empty"


def test_every_equivalence_class_resolves_in_both_directions() -> None:
    """Each declared class must actually work, in either argument order.

    The alias guard above only inspects pairs drawn from `team_aliases`, so an
    equivalence added for a school that is NOT in that table -- `UAH` /
    `Alabama Huntsville`, the last WBB team-name failure -- would be silently
    unprotected: deleting it would leave every other test green.

    Driven off `team_name_equivalents` itself, so a class added later is
    covered without touching this test.
    """
    from itertools import permutations

    assert team_name_equivalents, "no classes declared"
    for cls in team_name_equivalents:
        assert len(cls) >= 2, f"a one-name class equates nothing: {cls}"
        for a, b in permutations(sorted(cls), 2):
            assert same_school(a, b), f"{a} / {b} declared equivalent but same_school says no"


def test_the_known_equivalences_are_present() -> None:
    """Pin the three measured pairs by name, so a deletion is loud.

    Each was found by the skip ledger during the corpus re-parse, not guessed.
    """
    for a, b in (
        ("UAH", "Alabama Huntsville"),
        ("NIU", "Northern Ill."),
        ("New Orleans", "LSU New Orleans"),
    ):
        assert same_school(a, b), f"{a} / {b} equivalence missing"
        assert same_school(b, a), f"{b} / {a} not symmetric"


def test_distinct_schools_stay_distinct() -> None:
    """The constraint the whole equivalence approach rests on.

    A silently wrong team is far worse than a dropped game, and these differ
    by less than a typo.
    """
    for a, b in (
        ("Miami (FL)", "Miami (OH)"),
        ("New Orleans", "Southern-N.O."),
        ("Loyola (IL)", "Loyola (MD)"),
        ("NIU", "Northern Colo."),
        ("UAH", "Alabama A&M"),
    ):
        assert not same_school(a, b), f"{a} and {b} must not be merged"
