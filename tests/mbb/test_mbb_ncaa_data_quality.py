"""Oracle-replay tests for ``sportsdataverse.mbb.mbb_ncaa_data_quality``
(Task 5b.1).

``combos``/``alias_combos`` cases transliterate the inline literals from
``DataQualityIssuesTests.scala`` (``utest``, ``"DataQualityIssuesTests"``
block); the ``ParseError``/``build_sub_error`` cases replay the
``ParseError("", "[value]"/"[rank]"/"[error]"/"[team]", _)`` shape asserted
throughout ``ParseUtilsTests.scala`` and ``ExtractorUtils.scala``'s
``parse_team_name``. This can't exhaustively test all ~300 curated data
rows -- it asserts the table STRUCTURE, a representative sample of entries,
and the ``misspellings`` team-scoped/generic-fallback semantics. The
``build_player_code``/``parse_team_name``/``tidy_player`` oracle tests in
Tasks 5b.2/5b.3/5b.5 exercise these tables end-to-end.
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_ncaa_data_quality import (
    ParseError,
    alias_combos,
    build_sub_error,
    combos,
    fix_combos,
    generic_misspellings,
    misspellings,
    players_with_duplicate_names,
    team_aliases,
)
from sportsdataverse.mbb.mbb_ncaa_models import TeamId, Year


class TestParseError:
    """``ParseError.scala:9-21`` (plain constructor + single-message companion)."""

    def test_plain_constructor_takes_a_message_list(self) -> None:
        err = ParseError("", "err1", [])
        assert err.location == ""
        assert err.id == "err1"
        assert err.messages == []

    def test_single_wraps_one_message_in_a_list(self) -> None:
        err = ParseError.single("", "[value]", "Failed to locate a numeric field")
        assert err == ParseError("", "[value]", ["Failed to locate a numeric field"])

    def test_equality_is_structural(self) -> None:
        assert ParseError("loc", "id", ["m"]) == ParseError("loc", "id", ["m"])


class TestBuildSubError:
    """``ParseUtils.build_sub_error``, ``:83-85`` -- ``ParseError("", "[id]", [msg])``
    oracle shape from ``ParseUtilsTests.scala:32/43/57`` and
    ``ExtractorUtils.scala:262`` (``parse_team_name``'s ``"team"`` subid)."""

    def test_single_subid_wraps_in_brackets(self) -> None:
        err = build_sub_error("value", error="Failed to locate a numeric field")
        assert err == ParseError("", "[value]", ["Failed to locate a numeric field"])

    def test_team_subid_matches_parse_team_name_usage(self) -> None:
        err = build_sub_error("team", error="Could not find/match team names")
        assert err.id == "[team]"
        assert err.location == ""
        assert err.messages == ["Could not find/match team names"]

    def test_no_subids_yields_empty_id(self) -> None:
        err = build_sub_error(error="generic failure")
        assert err.id == ""

    def test_multiple_subids_concatenate(self) -> None:
        err = build_sub_error("a", "b", error="msg")
        assert err.id == "[a][b]"


class TestCombosFixComboAliasCombos:
    """``DataQualityIssues.combos``/``fix_combos``/``alias_combos``,
    ``:330-356`` -- oracle values from ``DataQualityIssuesTests.scala:33-58``
    (Mitchell/Hamilton/Davis/Cumberland set, lower-cased) and ``:60-86``
    (Fordham + Cincinnati ``alias_combos`` maps)."""

    def test_combos_shape(self) -> None:
        assert combos("Makhi", "Mitchell") == [
            "Mitchell, Makhi",
            "Makhi Mitchell",
            "MITCHELL,MAKHI",
        ]

    def test_fix_combos_pairs_each_variant_with_code_start(self) -> None:
        assert fix_combos("Hana", "Abdel Aal", "Hn") == [
            ("Abdel Aal, Hana", "Hn"),
            ("Hana Abdel Aal", "Hn"),
            ("ABDEL AAL,HANA", "Hn"),
        ]

    def test_alias_combos_matches_fordham_oracle(self) -> None:
        assert alias_combos("Josh", "Colon", "Navarro, Josh") == {
            "Colon, Josh": "Navarro, Josh",
            "Josh Colon": "Navarro, Josh",
            "COLON,JOSH": "Navarro, Josh",
        }

    def test_alias_combos_matches_cincinnati_oracle(self) -> None:
        assert alias_combos("Jaevin", "Cumberland", "Cumberland, Jaev") == {
            "Cumberland, Jaevin": "Cumberland, Jaev",
            "Jaevin Cumberland": "Cumberland, Jaev",
            "CUMBERLAND,JAEVIN": "Cumberland, Jaev",
        }


class TestPlayersWithDuplicateNames:
    """``DataQualityIssues.players_with_duplicate_names``, ``:36-160`` --
    lower-cased lookup with ``Some(code)``/``None`` values."""

    def test_lowercased_lookup_hits_for_known_name(self) -> None:
        # From the Mitchell-brothers combos oracle (DataQualityIssuesTests.scala:47).
        assert "mitchell, makhi" in players_with_duplicate_names
        assert players_with_duplicate_names["mitchell, makhi"] is None

    def test_explicit_special_case_code(self) -> None:
        # All three combos() variants (new-box/new-PbP/legacy-PbP) resolve
        # to the same forced code_start.
        assert players_with_duplicate_names["abdel aal, hana"] == "Hn"
        assert players_with_duplicate_names["hana abdel aal"] == "Hn"
        assert players_with_duplicate_names["abdel aal,hana"] == "Hn"

    def test_absent_name_is_a_true_miss(self) -> None:
        assert "nobody, special" not in players_with_duplicate_names

    def test_table_is_nonempty(self) -> None:
        assert len(players_with_duplicate_names) > 100


class TestMisspellings:
    """``DataQualityIssues.misspellings``, ``:165-322`` -- team-scoped map
    merged with :data:`generic_misspellings`, falling back to the generic
    map (copy) for any team (or ``None``) absent from the table."""

    def test_team_specific_lookup(self) -> None:
        table = misspellings(TeamId("NJIT"))
        assert table["Lewal, Levi"] == "Lawal, Levi"

    def test_team_specific_lookup_merges_alias_combos(self) -> None:
        table = misspellings(TeamId("Fordham"))
        assert table["Josh Colon"] == "Navarro, Josh"
        assert table["COLON,JOSH"] == "Navarro, Josh"

    def test_unlisted_team_falls_back_to_generic(self) -> None:
        assert misspellings(TeamId("Some Unlisted Team")) == generic_misspellings

    def test_none_team_falls_back_to_generic(self) -> None:
        assert misspellings(None) == generic_misspellings

    def test_fallback_returns_a_copy_not_the_shared_dict(self) -> None:
        table = misspellings(None)
        table["mutated"] = "oops"
        assert "mutated" not in generic_misspellings

    def test_generic_misspellings_currently_empty(self) -> None:
        assert generic_misspellings == {}


class TestTeamAliases:
    """``DataQualityIssues.team_aliases``, ``:9-13`` -- season-scoped team renames."""

    def test_2021_niu_alias(self) -> None:
        assert team_aliases[Year(2021)][TeamId("NIU")] == TeamId("Northern Ill.")

    def test_other_seasons_have_no_aliases(self) -> None:
        assert Year(2020) not in team_aliases
