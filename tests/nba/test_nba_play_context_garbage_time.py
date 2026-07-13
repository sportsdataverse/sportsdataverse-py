"""CTG's garbage-time definition, including the "<=2 starters on the floor" clause.

CTG (verbatim): "the game has to be in the **4th quarter**, the score differential
has to be **>= 25 for minutes 12-9, >= 20 for minutes 9-6, and >= 10 for the
remainder of the quarter**. Additionally, there have to be **two or fewer starters
on the floor combined between the two teams**."

The margin x minutes half is computable from the possession frame alone and shipped
already. The starters half needs the 10 on-court players and each team's starting
five, which is what :func:`starters_on_court_counts` supplies.

The load-bearing property is **containment**: the full rule (margin AND starters) is
a strict subset of the margin-only rule. Adding a conjunct can only ever *remove*
possessions from the flag — so if the "full" rule ever flags a possession the
margin-only rule did not, the implementation is wrong. That is asserted directly,
which is a much stronger check than eyeballing a count.

The clause is **not cosmetic**. Observed on the three committed fixtures
(margin-only -> margin+starters garbage-time possessions):

===========  ===========  ================
game         margin-only  margin+starters
===========  ===========  ================
0022100001            24                11
0022200001            21                 0
0022300001             0                 0
===========  ===========  ================

0022200001 is the case CTG's second conjunct exists for: the score ran far enough
ahead to satisfy the margin band while both coaches still had their starters on the
floor — i.e. not garbage time at all. Without the clause the margin-only superset
over-flags by up to 21 possessions in a single game, and those possessions are
exactly the competitive ones you least want silently dropped from the default view.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    _starters_from_boxscore_v3,
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_play_context import (
    add_play_context,
    flag_garbage_time,
    starters_on_court_counts,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022100001", "0022200001", "0022300001"]


def _parts(game_id: str):
    fx = FXROOT / game_id
    enh = enhanced_pbp_from_payload(json.loads((fx / "playbyplayv3.json").read_text()))
    box = json.loads((fx / "boxscoretraditionalv3.json").read_text())
    home, away = boxscore_home_away(box)
    oncourt = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(json.loads((fx / "gamerotation.json").read_text())),
        home_team_id=home,
        away_team_id=away,
    )
    poss = attach_possession_lineups(build_possessions(enh), oncourt, enh, home_team_id=home)
    return enh, poss, _starters_from_boxscore_v3(box)


@pytest.fixture(scope="module")
def parts() -> dict:
    return {g: _parts(g) for g in GAMES}


@pytest.mark.parametrize("game_id", GAMES)
def test_starter_counts_are_in_range(parts, game_id: str) -> None:
    _enh, poss, starters = parts[game_id]
    counts = starters_on_court_counts(poss, starters)
    assert len(counts) == poss.height
    assert set(counts) == set(poss["possession_number"].to_list())
    assert all(0 <= v <= 10 for v in counts.values()), "starters-on-floor outside 0..10"


@pytest.mark.parametrize("game_id", GAMES)
def test_opening_possession_has_all_ten_starters(parts, game_id: str) -> None:
    """The tip-off possession is, by definition, 10 starters on the floor.

    This is the check that catches a wrong-team attribution: if the offense/defense
    player columns were mapped to the wrong teams' starter sets, the opening count
    would not be 10.
    """
    _enh, poss, starters = parts[game_id]
    counts = starters_on_court_counts(poss, starters)
    first = poss.filter(pl.col("period") == 1).sort("possession_number")["possession_number"][0]
    assert counts[first] == 10, f"{game_id}: opening possession had {counts[first]} starters, expected 10"


@pytest.mark.parametrize("game_id", GAMES)
def test_full_rule_is_a_subset_of_margin_only(parts, game_id: str) -> None:
    """CONTAINMENT GATE. margin AND starters can only ever be a SUBSET of margin alone."""
    enh, poss, starters = parts[game_id]
    counts = starters_on_court_counts(poss, starters)

    margin_only = flag_garbage_time(poss, enh)
    full = flag_garbage_time(poss, enh, starters_on_court=counts)

    assert margin_only["garbage_time_basis"].unique().to_list() == ["margin_only"]
    assert full["garbage_time_basis"].unique().to_list() == ["margin_and_starters"]

    m = set(margin_only.filter(pl.col("is_garbage_time"))["possession_number"].to_list())
    f = set(full.filter(pl.col("is_garbage_time"))["possession_number"].to_list())
    assert f <= m, f"{game_id}: full rule flagged possessions margin-only did not: {sorted(f - m)}"


@pytest.mark.parametrize("game_id", GAMES)
def test_full_rule_never_flags_a_possession_with_three_plus_starters(parts, game_id: str) -> None:
    """The clause itself: <=2 starters combined, or it is not garbage time."""
    enh, poss, starters = parts[game_id]
    counts = starters_on_court_counts(poss, starters)
    full = flag_garbage_time(poss, enh, starters_on_court=counts)
    for pn in full.filter(pl.col("is_garbage_time"))["possession_number"].to_list():
        assert counts[pn] <= 2, f"possession {pn} flagged garbage with {counts[pn]} starters on floor"


def test_add_play_context_threads_the_starters_clause(parts) -> None:
    """The clause reaches the one-call surface (add_play_context), not just the flagger."""
    enh, poss, starters = parts[GAMES[0]]
    counts = starters_on_court_counts(poss, starters)

    default = add_play_context(enh)
    exact = add_play_context(enh, starters_on_court=counts)

    assert default["garbage_time_basis"].unique().to_list() == ["margin_only"]
    assert exact["garbage_time_basis"].unique().to_list() == ["margin_and_starters"]
    assert int(exact["is_garbage_time"].sum()) <= int(default["is_garbage_time"].sum())


def test_missing_lineup_columns_raises(parts) -> None:
    _enh, poss, starters = parts[GAMES[0]]
    with pytest.raises(ValueError, match="def_player"):
        starters_on_court_counts(poss.drop([f"def_player_{i}" for i in range(1, 6)]), starters)


def test_empty_starters_map_yields_zero_counts(parts) -> None:
    """A malformed/empty box must not silently produce a CTG-exact-looking flag.

    With no starters known, every possession has 0 starters on the floor, which
    would make the <=2 clause vacuously true and flag EVERY margin-qualifying
    possession as garbage time. The counts are honest (all zero); the caller is
    responsible for not passing an empty map — so this test documents the
    behaviour rather than pretending it is safe.
    """
    _enh, poss, _starters = parts[GAMES[0]]
    counts = starters_on_court_counts(poss, {})
    assert set(counts.values()) == {0}
