"""RAPM input adapter: possessions + rosters -> player-id-keyed stint rows.

The published ``ncaa_{mbb,wbb}_possessions`` datasets key players by NAME-CODE
(``ANTONIA.BATES.``), not by id. ``team_rosters`` is the bridge: it carries
``player_id`` alongside ``player`` in the same name-code format.

Two entities in that data look like absence but must be modelled as presence,
and both would corrupt RAPM silently if ingested naively:

* ``TEAM`` occupies a player slot but is a pseudo-player (team rebounds and
  turnovers). Left in, it becomes a phantom player with enormous minutes on
  every roster.
* Non-D-I opponents have no roster at all. 619 teams appear on the floor in
  WBB 2024 while only 358 have rosters -- the surplus are D2/D3/NAIA
  exhibition opponents. Under ``non_di="pool"`` they collapse to ONE explicitly
  named pseudo-team, never to null, because a null opponent silently takes
  whatever branch the join gives missing keys.

Match-rate gates against real published data live in the oracle-gate phase;
these cover the transformation logic and its edge cases.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_rapm_input import (
    NON_DI_TEAM,
    TEAM_PSEUDO_PLAYER,
    build_player_xwalk,
    normalize_player_key,
    resolve_possessions,
)

_ROSTERS = pl.DataFrame(
    {
        "season": ["2024"] * 4,
        "team": ["Duke", "Duke", "Duke", "Iowa"],
        "player": ["ALECIA.WESTBROOK", "ANTONIA.BATES", "KIA.SMITH", "CAITLIN.CLARK"],
        "player_id": ["1", "2", "3", "4"],
    }
)


def _poss(home="Duke", away="Iowa", **over):
    row = {
        "home": home,
        "away": away,
        "poss_team": home,
        "home_1": "ALECIA.WESTBROOK",
        "home_2": "ANTONIA.BATES.",  # trailing dot -- as it appears in possessions
        "home_3": "KIA.SMITH",
        "home_4": "ALECIA.WESTBROOK",
        "home_5": "KIA.SMITH",
        "away_1": "CAITLIN.CLARK",
        "away_2": "CAITLIN.CLARK",
        "away_3": "CAITLIN.CLARK",
        "away_4": "CAITLIN.CLARK",
        "away_5": "CAITLIN.CLARK",
        "pts": 2,
    }
    row.update(over)
    return pl.DataFrame([row])


class TestNormalizeKey:
    def test_strips_trailing_dots(self):
        """possessions writes ANTONIA.BATES. ; rosters writes ANTONIA.BATES."""
        df = pl.DataFrame({"p": ["ANTONIA.BATES.", "KIA.SMITH", "A.B..."]})
        got = df.select(normalize_player_key(pl.col("p")).alias("k"))["k"].to_list()
        assert got == ["ANTONIA.BATES", "KIA.SMITH", "A.B"]

    def test_does_not_strip_interior_dots(self):
        """Interior dots are the FIRST.LAST separator -- load-bearing."""
        df = pl.DataFrame({"p": ["A.B.C."]})
        assert df.select(normalize_player_key(pl.col("p")))["p"][0] == "A.B.C"


class TestBuildPlayerXwalk:
    def test_maps_team_player_to_id(self):
        x = build_player_xwalk(_ROSTERS)
        assert set(x.columns) >= {"team", "player_key", "player_id"}
        row = x.filter((pl.col("team") == "Duke") & (pl.col("player_key") == "KIA.SMITH"))
        assert row["player_id"][0] == "3"

    def test_player_id_is_utf8(self):
        """One dtype per id, Utf8 -- never a float that stringifies as '3.0'."""
        assert build_player_xwalk(_ROSTERS).schema["player_id"] == pl.Utf8

    def test_same_name_on_two_teams_stays_distinct(self):
        """The key is (team, player) -- a shared name must not collapse."""
        r = pl.concat(
            [
                _ROSTERS,
                pl.DataFrame(
                    {
                        "season": ["2024"],
                        "team": ["Iowa"],
                        "player": ["KIA.SMITH"],
                        "player_id": ["99"],
                    }
                ),
            ]
        )
        x = build_player_xwalk(r)
        ids = {(t, p): i for t, p, i in zip(x["team"], x["player_key"], x["player_id"]) if p == "KIA.SMITH"}
        assert ids[("Duke", "KIA.SMITH")] == "3"
        assert ids[("Iowa", "KIA.SMITH")] == "99"


class TestResolvePossessions:
    def test_resolves_ids_across_the_trailing_dot(self):
        out = resolve_possessions(_poss(), build_player_xwalk(_ROSTERS))
        assert out["home_2_id"][0] == "2"  # ANTONIA.BATES. -> ANTONIA.BATES -> 2

    def test_team_pseudo_player_is_excluded_not_resolved(self):
        """TEAM must never receive a player_id -- it is not a person."""
        out = resolve_possessions(_poss(home_3=TEAM_PSEUDO_PLAYER), build_player_xwalk(_ROSTERS))
        assert out["home_3_id"][0] is None

    def test_non_di_drop_removes_the_possession(self):
        out = resolve_possessions(_poss(away="Adelphi"), build_player_xwalk(_ROSTERS), non_di="drop")
        assert out.height == 0

    def test_non_di_pool_keeps_it_under_a_named_team(self):
        """Pooled non-D-I is an explicit entity, never null."""
        out = resolve_possessions(_poss(away="Adelphi"), build_player_xwalk(_ROSTERS), non_di="pool")
        assert out.height == 1
        assert out["away"][0] == NON_DI_TEAM
        assert out["away"][0] is not None

    def test_pool_does_not_touch_di_opponents(self):
        out = resolve_possessions(_poss(), build_player_xwalk(_ROSTERS), non_di="pool")
        assert out["away"][0] == "Iowa"

    def test_rejects_unknown_non_di_mode(self):
        with pytest.raises(ValueError, match="non_di"):
            resolve_possessions(_poss(), build_player_xwalk(_ROSTERS), non_di="nope")

    def test_empty_input_returns_documented_schema(self):
        empty = _poss().clear()
        out = resolve_possessions(empty, build_player_xwalk(_ROSTERS))
        assert out.height == 0
        assert "home_1_id" in out.columns


# --- alias expansion ---------------------------------------------------------
#
# Possessions and rosters render the same person differently. Measured on real
# WBB data, three patterns account for nearly all of it:
#
#   possessions            roster                  pattern
#   ANAELLE.DUTAT          ANAËLLE.DUTAT           diacritics
#   PAULA.REUS             PAULA.REUS.PIZA         truncated compound surname
#   MELANNIE.DALEY         MEL.DALEY               shortened first name
#
# Expansion is only safe when the candidate is UNIQUE on that team. Purdue 2024
# carries both MADISON.LAYDENZAY and MCKENNA.LAYDEN -- a loose surname match
# would attach possessions to the wrong sibling, and a silent wrong match is
# worse than a drop because it corrupts two players' coefficients at once.

from sportsdataverse.mbb.mbb_ncaa_rapm_input import expand_xwalk_aliases  # noqa: E402


def _obs(pairs):
    return pl.DataFrame({"team": [t for t, _ in pairs], "player": [p for _, p in pairs]})


class TestFolding:
    def test_diacritics_fold_in_the_exact_path(self):
        r = pl.DataFrame(
            {
                "season": ["2024"],
                "team": ["Rhode Island"],
                "player": ["ANAËLLE.DUTAT"],
                "player_id": ["7"],
            }
        )
        x = build_player_xwalk(r)
        assert x["player_key"][0] == "ANAELLE.DUTAT"


class TestExpandXwalkAliases:
    _R = pl.DataFrame(
        {
            "season": ["2024"] * 4,
            "team": ["New Mexico", "Northwestern", "Purdue", "Purdue"],
            "player": [
                "PAULA.REUS.PIZA",
                "MEL.DALEY",
                "MADISON.LAYDENZAY",
                "MCKENNA.LAYDEN",
            ],
            "player_id": ["10", "11", "12", "13"],
        }
    )

    def _ids(self, observed):
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(x, _obs(observed))
        return {(t, k): i for t, k, i in zip(out["team"], out["player_key"], out["player_id"])}

    def test_truncated_compound_surname(self):
        ids = self._ids([("New Mexico", "PAULA.REUS")])
        assert ids[("New Mexico", "PAULA.REUS")] == "10"

    def test_shortened_first_name(self):
        ids = self._ids([("Northwestern", "MELANNIE.DALEY")])
        assert ids[("Northwestern", "MELANNIE.DALEY")] == "11"

    def test_ambiguous_surname_is_dropped_not_guessed(self):
        """Two LAYDENs on Purdue: LAYDEN must NOT bind to either."""
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(x, _obs([("Purdue", "SOMEONE.LAYDEN")]))
        assert out.filter(pl.col("player_key") == "SOMEONE.LAYDEN").height == 0

    def test_unique_first_name_disambiguates_the_siblings(self):
        """MADISON.LAYDEN -> LAYDENZAY only; MCKENNA is a different first name."""
        ids = self._ids([("Purdue", "MADISON.LAYDEN")])
        assert ids[("Purdue", "MADISON.LAYDEN")] == "12"

    def test_genuinely_absent_player_stays_absent(self):
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(x, _obs([("New Mexico", "MIAH.MONAHAN")]))
        assert out.filter(pl.col("player_key") == "MIAH.MONAHAN").height == 0

    def test_expansion_never_drops_original_rows(self):
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(x, _obs([("New Mexico", "PAULA.REUS")]))
        assert out.height >= x.height
        assert out.filter(pl.col("player_id") == "10").height >= 1

    def test_is_idempotent(self):
        x = build_player_xwalk(self._R)
        o = _obs([("New Mexico", "PAULA.REUS")])
        once = expand_xwalk_aliases(x, o)
        assert expand_xwalk_aliases(once, o).height == once.height


# --- name-change crosswalk ---------------------------------------------------
#
# stats.ncaa.org re-renders roster/box pages with a player's CURRENT name while
# the play-by-play preserves the game-time name, so a player who changes their
# name never matches -- and no safe string rule bridges KATELYNN.LIMARDO ->
# KATELYNN.MARTIN or MICHELLE.DUCHEMIN -> SHELLEY.DUCHEMIN.
#
# The box_score page binds both renderings to one numeric player id, which is
# where the crosswalk comes from (dev/ncaa_rapm/build_name_changes.py). Here it
# is injected, so the library stays pure and testable.


def _changes(rows, season="2023"):
    """Crosswalk fixture. Carries ``season`` because the real
    ``ncaa_{lg}_name_changes`` parquet does -- the fold is season-scoped, so a
    fixture without it would not model the contract."""
    return pl.DataFrame(
        {
            "season": [(r[3] if len(r) > 3 else season) for r in rows],
            "team": [r[0] for r in rows],
            "name_game_time": [r[1] for r in rows],
            "name_current": [r[2] for r in rows],
        }
    )


class TestNameChangeCrosswalk:
    _R = pl.DataFrame(
        {
            "season": ["2024", "2024"],
            "team": ["Montana St.", "Purdue"],
            "player": ["KATELYNN.MARTIN", "MADISON.LAYDENZAY"],
            "player_id": ["20", "21"],
        }
    )

    def test_resolves_a_surname_change(self):
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(
            x,
            _obs([("Montana St.", "KATELYNN.LIMARDO")]),
            name_changes=_changes([("Montana St.", "KATELYNN.LIMARDO", "KATELYNN.MARTIN")]),
        )
        got = out.filter(pl.col("player_key") == "KATELYNN.LIMARDO")
        assert got.height == 1 and got["player_id"][0] == "20"

    def test_change_pointing_at_an_unknown_player_is_ignored(self):
        """A mapping whose target is not on the roster resolves to nothing."""
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(
            x,
            _obs([("Montana St.", "OLD.NAME")]),
            name_changes=_changes([("Montana St.", "OLD.NAME", "NOT.ON.ROSTER")]),
        )
        assert out.filter(pl.col("player_key") == "OLD.NAME").height == 0

    def test_conflicting_mappings_are_dropped(self):
        """One game-time name mapping to two different people is ambiguous."""
        x = build_player_xwalk(self._R)
        out = expand_xwalk_aliases(
            x,
            _obs([("Montana St.", "AMBIG.NAME")]),
            name_changes=_changes(
                [
                    ("Montana St.", "AMBIG.NAME", "KATELYNN.MARTIN"),
                    ("Montana St.", "AMBIG.NAME", "SOMEONE.ELSE"),
                ]
            ),
        )
        assert out.filter(pl.col("player_key") == "AMBIG.NAME").height == 0

    def test_crosswalk_beats_the_prefix_tiers(self):
        """An authoritative id-bound mapping wins over a heuristic prefix match."""
        r = pl.concat(
            [
                self._R,
                pl.DataFrame(
                    {
                        "season": ["2024"],
                        "team": ["Montana St."],
                        "player": ["KATELYNN.LIM"],
                        "player_id": ["22"],
                    }
                ),
            ]
        )
        x = build_player_xwalk(r)
        out = expand_xwalk_aliases(
            x,
            _obs([("Montana St.", "KATELYNN.LIMARDO")]),
            name_changes=_changes([("Montana St.", "KATELYNN.LIMARDO", "KATELYNN.MARTIN")]),
        )
        got = out.filter(pl.col("player_key") == "KATELYNN.LIMARDO")
        assert got["player_id"][0] == "20", "crosswalk must win, not the LIM prefix"

    def test_none_crosswalk_is_the_previous_behaviour(self):
        x = build_player_xwalk(self._R)
        a = expand_xwalk_aliases(x, _obs([("Purdue", "MADISON.LAYDEN")]))
        b = expand_xwalk_aliases(x, _obs([("Purdue", "MADISON.LAYDEN")]), name_changes=None)
        assert a.height == b.height


# --- cross-season person key -------------------------------------------------
#
# `player_id` is a per-season ROSTER-ENTRY id, not a person id: across 17
# seasons, 83,518 roster rows carry 83,518 distinct ids, none appearing twice.
# RAPM conventionally pools 2-3 seasons, so pooling on player_id would treat
# every player as a new person each year. The key therefore has to be
# synthesized.
#
# Measured invariants driving the rule:
#   name        99.4% nationally unique within a season (0.59% collide)
#   ht_inches   99.4% identical / 99.5% within 1in across consecutive seasons
#   class       only 88.8% advance by +1; 10.5% stay flat (redshirt/medical),
#               so a "+1 required" rule would break one link in ten. Only
#               BACKWARDS movement (0.1%) is treated as disqualifying.

from sportsdataverse.mbb.mbb_ncaa_rapm_input import build_person_keys  # noqa: E402


def _ros(rows):
    return pl.DataFrame(
        {
            "season": [r[0] for r in rows],
            "team": [r[1] for r in rows],
            "player": [r[2] for r in rows],
            "player_id": [r[3] for r in rows],
            "ht_inches": [r[4] for r in rows],
            "class": [r[5] for r in rows],
        }
    )


def _pid(df, season, player):
    return df.filter((pl.col("season") == season) & (pl.col("player_key") == player))["person_id"][0]


class TestBuildPersonKeys:
    def test_same_player_across_seasons_gets_one_person_id(self):
        r = _ros(
            [
                ("2023", "Duke", "KIA.SMITH", "1", 70, "Fr."),
                ("2024", "Duke", "KIA.SMITH", "2", 70, "So."),
            ]
        )
        out = build_person_keys(r)
        assert _pid(out, "2023", "KIA.SMITH") == _pid(out, "2024", "KIA.SMITH")

    def test_per_season_player_id_is_preserved(self):
        """The synthetic key ADDS to the provider id, never replaces it."""
        out = build_person_keys(_ros([("2023", "Duke", "KIA.SMITH", "1", 70, "Fr.")]))
        assert out["player_id"][0] == "1"

    def test_transfer_keeps_one_person_id(self):
        """Team is not part of the key -- a transfer is the same person."""
        r = _ros(
            [
                ("2023", "Duke", "KIA.SMITH", "1", 70, "Fr."),
                ("2024", "Iowa", "KIA.SMITH", "2", 70, "So."),
            ]
        )
        out = build_person_keys(r)
        assert _pid(out, "2023", "KIA.SMITH") == _pid(out, "2024", "KIA.SMITH")

    def test_redshirt_flat_class_still_links(self):
        """10.5% of real links do not advance class -- must not break them."""
        r = _ros(
            [
                ("2023", "Duke", "KIA.SMITH", "1", 70, "Fr."),
                ("2024", "Duke", "KIA.SMITH", "2", 70, "Fr."),
            ]
        )
        out = build_person_keys(r)
        assert _pid(out, "2023", "KIA.SMITH") == _pid(out, "2024", "KIA.SMITH")

    def test_height_mismatch_splits_two_people(self):
        """Same name, very different height -> two different people."""
        r = _ros(
            [
                ("2023", "Duke", "KIA.SMITH", "1", 62, "Sr."),
                ("2024", "Iowa", "KIA.SMITH", "2", 78, "Fr."),
            ]
        )
        out = build_person_keys(r)
        assert _pid(out, "2023", "KIA.SMITH") != _pid(out, "2024", "KIA.SMITH")

    def test_one_inch_drift_still_links(self):
        r = _ros(
            [
                ("2023", "Duke", "KIA.SMITH", "1", 70, "Fr."),
                ("2024", "Duke", "KIA.SMITH", "2", 71, "So."),
            ]
        )
        out = build_person_keys(r)
        assert _pid(out, "2023", "KIA.SMITH") == _pid(out, "2024", "KIA.SMITH")

    def test_two_same_named_players_in_one_season_stay_distinct(self):
        r = _ros(
            [
                ("2024", "Duke", "KIA.SMITH", "1", 70, "Fr."),
                ("2024", "Iowa", "KIA.SMITH", "2", 70, "Fr."),
            ]
        )
        out = build_person_keys(r)
        ids = out.filter(pl.col("player_key") == "KIA.SMITH")["person_id"].to_list()
        assert len(set(ids)) == 2, "same-season namesakes must not merge"

    def test_name_change_unifies_one_person(self):
        r = _ros(
            [
                ("2023", "Montana St.", "KATELYNN.LIMARDO", "1", 70, "Jr."),
                ("2024", "Montana St.", "KATELYNN.MARTIN", "2", 70, "Sr."),
            ]
        )
        out = build_person_keys(
            r,
            name_changes=_changes([("Montana St.", "KATELYNN.LIMARDO", "KATELYNN.MARTIN")]),
        )
        a = _pid(out, "2023", "KATELYNN.LIMARDO")
        b = _pid(out, "2024", "KATELYNN.MARTIN")
        assert a == b

    def test_person_id_is_utf8_and_never_null(self):
        out = build_person_keys(_ros([("2024", "Duke", "KIA.SMITH", "1", 70, "Fr.")]))
        assert out.schema["person_id"] == pl.Utf8
        assert out["person_id"].null_count() == 0


class TestReviewRegressions:
    """One test per real finding from the #382 review."""

    def test_ambiguous_roster_key_is_omitted_not_arbitrarily_picked(self):
        """Two people normalizing to one key must yield NO xwalk row.

        Keeping an arbitrary row assigns every possession for that name to a
        coin-flip player -- the wrong-attribution class this whole stack
        exists to avoid.
        """
        ros = pl.DataFrame(
            {
                "team": ["Duke", "Duke", "Duke"],
                "player": ["KIA.SMITH", "KIA.SMITH", "JOE.BLOGGS"],
                "player_id": ["11", "22", "33"],
            }
        )
        out = build_player_xwalk(ros)
        keys = set(zip(out["team"], out["player_key"]))
        assert ("Duke", "KIA.SMITH") not in keys, "ambiguous key must be dropped"
        assert ("Duke", "JOE.BLOGGS") in keys, "unambiguous key must survive"

    def test_person_id_is_independent_of_row_order(self):
        """A published person_id must survive a reordered roster extract."""
        rows = [
            ("2023", "Duke", "KIA.SMITH", "1", 70, "Jr."),
            ("2024", "Duke", "KIA.SMITH", "2", 70, "Sr."),
            ("2023", "Iowa", "ANN.LEE", "3", 68, "Fr."),
        ]
        fwd = build_person_keys(_ros(rows))
        rev = build_person_keys(_ros(list(reversed(rows))))

        def mapping(df):
            return dict(zip(zip(df["season"], df["player_key"]), df["person_id"]))

        assert mapping(fwd) == mapping(rev)

    def test_rename_does_not_leak_into_another_season(self):
        """A 2023 rename must not rewrite a 2019 player with the same name."""
        ros = _ros(
            [
                ("2019", "Montana St.", "KATELYNN.LIMARDO", "9", 62, "Fr."),
                ("2023", "Montana St.", "KATELYNN.LIMARDO", "1", 70, "Jr."),
                ("2024", "Montana St.", "KATELYNN.MARTIN", "2", 70, "Sr."),
            ]
        )
        out = build_person_keys(
            ros,
            name_changes=_changes([("Montana St.", "KATELYNN.LIMARDO", "KATELYNN.MARTIN", "2023")]),
        )
        assert _pid(out, "2023", "KATELYNN.LIMARDO") == _pid(out, "2024", "KATELYNN.MARTIN")
        assert _pid(out, "2019", "KATELYNN.LIMARDO") != _pid(out, "2024", "KATELYNN.MARTIN")
