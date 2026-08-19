"""Sibling teammates must not delete a team's season.

`build_player_code` (a faithful ExtractorUtils.scala port) keys a player as
`{first-two-letters}{Surname}`, so teammates sharing a surname AND their first
two initials collide. `validate_box_score` used to reject the whole game --
and since every game has the same roster, one sibling pair removed the team's
ENTIRE season from `lineups`, `matchup_stints` and `possessions`.

Measured across 2010-2026: 79 D-I team-seasons affected. Kansas published
0 of 36 games in 2010 because of the Morris twins.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_boxscore_parser import validate_box_score
from sportsdataverse.mbb.mbb_ncaa_data_quality import ParseError
from sportsdataverse.mbb.mbb_ncaa_models import TeamId

_TEAM = TeamId("Kansas")


def _codes(names):
    out = validate_box_score(_TEAM, names)
    assert not isinstance(out, ParseError), out
    return [c.code for c in out]


class TestSiblingDisambiguation:
    def test_morris_twins_no_longer_fail_the_game(self):
        """Kansas 2010: Markieff and Marcus Morris both coded MaMorris."""
        codes = _codes(["Morris, Markieff", "Morris, Marcus", "Collins, Sherron"])
        assert len(set(codes)) == 3, codes
        assert "MarkieffMorris" in codes and "MarcusMorris" in codes

    def test_non_colliding_players_keep_the_ported_code(self):
        """Only the collision is re-coded -- lineup_keys must not churn."""
        codes = _codes(["Morris, Markieff", "Morris, Marcus", "Collins, Sherron"])
        assert "ShCollins" in codes, codes

    def test_a_roster_with_no_collision_is_untouched(self):
        before = _codes(["Collins, Sherron", "Aldrich, Cole"])
        assert before == ["ShCollins", "CoAldrich"]

    def test_womens_twins_case(self):
        """San Diego 2015 rostered Sophie AND Sophia Ederaine."""
        codes = _codes(["Ederaine, Sophie", "Ederaine, Sophia"])
        assert len(set(codes)) == 2, codes

    def test_three_way_collision(self):
        codes = _codes(["Wolf, Madison", "Wolf, Mackenzie", "Wolf, Marcus"])
        assert len(set(codes)) == 3, codes

    def test_identical_full_names_still_raise(self):
        """A true ambiguity is NOT a coding artefact -- it must still fail."""
        out = validate_box_score(_TEAM, ["Smith, John", "Smith, John"])
        # build_sub_error returns a bare ParseError, not a list of them.
        assert isinstance(out, ParseError), type(out)

    def test_space_separated_names_disambiguate_too(self):
        codes = _codes(["Markieff Morris", "Marcus Morris"])
        assert len(set(codes)) == 2, codes
