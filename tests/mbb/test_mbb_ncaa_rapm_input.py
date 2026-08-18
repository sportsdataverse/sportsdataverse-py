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
