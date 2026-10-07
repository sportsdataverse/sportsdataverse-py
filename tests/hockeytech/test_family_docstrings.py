"""Every function build_family mints documents its return, for every league."""

from __future__ import annotations

import inspect

import pytest

from sportsdataverse.hockeytech._family import build_family
from sportsdataverse.hockeytech._leagues import LEAGUES

_SUFFIXES = [
    "_season_id",
    "_schedule",
    "_pbp",
    "_standings",
    "_teams",
    "_team_roster",
    "_player_stats",
    "_leaders",
    "_game_summary",
    "_game_shifts",
    "_player_toi",
    "_game_corsi",
]


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_every_minted_callable_documents_its_return(league):
    fam = build_family(league)
    missing = [name for name, fn in fam.items() if "Returns:" not in (inspect.getdoc(fn) or "")]
    assert not missing, f"{league}: {missing}"


@pytest.mark.parametrize("league", sorted(LEAGUES))
def test_the_docstring_names_the_league(league):
    fam = build_family(league)
    label = LEAGUES[league].name
    for name, fn in fam.items():
        assert label in (inspect.getdoc(fn) or ""), f"{league}.{name} does not name {label!r}"


@pytest.mark.parametrize("suffix", _SUFFIXES)
def test_the_family_still_mints_every_function(suffix):
    assert f"ahl{suffix}" in build_family("ahl")


def test_most_recent_season_documents_an_int_return():
    doc = inspect.getdoc(build_family("ahl")["most_recent_ahl_season"]) or ""
    assert "Returns:" in doc
    assert "int" in doc


def test_the_family_covers_every_minted_name():
    """13 callables per league; the Returns gate counted 247 of them in league scopes."""
    assert {len(build_family(lg)) for lg in LEAGUES} == {13}
